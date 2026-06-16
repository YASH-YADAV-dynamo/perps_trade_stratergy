
"""
Personal Volume Bot -- Multi-Instrument Volume Maximizer on Hotstuff DEX
========================================================================

Account separation:
  A1 = Pure Basis Mean-Reversion (IOC only, clean alpha)
  A2 = Pure Passive Market Making (GTC only, earns spread both legs)

Strategies (per instrument):
  1. BASIS MEAN-REVERSION -- IOC when Hotstuff-Binance basis diverges
  2. TIGHT PASSIVE MM     -- GTC bid/ask centered on Binance fair price
  3. TIME-OF-DAY REGIME   -- Adaptive spread by hour
  4. MEAN-REVERSION OVERLAY -- Lean quotes against multi-hour moves
  5. VOL-REGIME ADAPTIVE    -- Tighten/widen by recent realized vol

Instruments: configurable via VB_INSTRUMENTS (e.g. HYPE-PERP,BTC-PERP,...)
Sizing: USD notional based, auto-converted to instrument units.
"""

import asyncio
import csv
import json
import logging
import math
import os
import signal as os_signal
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from dotenv import load_dotenv
from eth_account import Account

from hotstuff import (
    WebSocketTransport,
    InfoClient,
    ExchangeClient,
    SubscriptionClient,
    WebSocketTransportOptions,
    PlaceOrderParams,
    UnitOrder,
    CancelAllParams,
)
from hotstuff.methods.subscription.channels import (
    BBOSubscriptionParams,
    FillsSubscriptionParams,
    PositionsSubscriptionParams,
)

load_dotenv()


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("volume_bot")


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

INST_BINANCE_MAP: Dict[str, str] = {
    "HYPE-PERP": "hypeusdt",
    "BTC-PERP": "btcusdt",
    "ETH-PERP": "ethusdt",
    "SOL-PERP": "solusdt",
    "XRP-PERP": "xrpusdt",
    "ZEC-PERP": "zecusdt",
}

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR / "data"

TOD_SPREAD_MULT = {
    0: 0.7, 1: 0.7, 2: 0.7, 3: 0.7, 4: 0.7, 5: 0.7, 6: 0.7, 7: 0.7,
    8: 1.0, 9: 1.0, 10: 1.0, 11: 1.0, 12: 1.0,
    13: 1.3, 14: 1.3, 15: 1.5, 16: 1.3, 17: 1.3,
    18: 0.9, 19: 0.9, 20: 0.9, 21: 0.9, 22: 0.9, 23: 0.9,
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sf(obj, key: str, default: float = 0.0) -> float:
    try:
        if isinstance(obj, dict):
            return float(obj.get(key, default))
        return float(getattr(obj, key, default))
    except (TypeError, ValueError):
        return default


def _round_dn(val: float, step: float) -> float:
    if step <= 0.0:
        return val
    return math.floor(val / step) * step


def _round_up(val: float, step: float) -> float:
    if step <= 0.0:
        return val
    return math.ceil(val / step) * step


def _fmt(val: float, step: float) -> str:
    if step <= 0.0 or step >= 1.0:
        return f"{val:.0f}"
    d = max(0, -math.floor(math.log10(step)))
    return f"{val:.{d}f}"


# ---------------------------------------------------------------------------
# Volatility Tracker
# ---------------------------------------------------------------------------

class VolTracker:
    __slots__ = ("_prices", "_ts", "_max_age")

    def __init__(self, max_age_s: float = 300.0):
        self._prices: deque = deque()
        self._ts: deque = deque()
        self._max_age = max_age_s

    def update(self, price: float, ts: float):
        self._prices.append(price)
        self._ts.append(ts)
        cutoff = ts - self._max_age
        while self._ts and self._ts[0] < cutoff:
            self._ts.popleft()
            self._prices.popleft()

    def vol_bps(self) -> float:
        n = len(self._prices)
        if n < 10:
            return 0.0
        sum_sq = 0.0
        for i in range(1, n):
            ret = (self._prices[i] - self._prices[i - 1]) / self._prices[i - 1]
            sum_sq += ret * ret
        return math.sqrt(sum_sq / (n - 1)) * 10000.0


# ---------------------------------------------------------------------------
# Mean-Reversion Tracker (2-4hr horizon)
# ---------------------------------------------------------------------------

class MeanRevTracker:
    __slots__ = ("_prices", "_ts", "_lookback")

    def __init__(self, lookback_s: float = 7200.0):
        self._prices: deque = deque()
        self._ts: deque = deque()
        self._lookback = lookback_s

    def update(self, price: float, ts: float):
        self._prices.append(price)
        self._ts.append(ts)
        cutoff = ts - self._lookback * 1.1
        while self._ts and self._ts[0] < cutoff:
            self._ts.popleft()
            self._prices.popleft()

    def return_bps(self) -> float:
        if len(self._prices) < 2:
            return 0.0
        return (self._prices[-1] - self._prices[0]) / self._prices[0] * 10000.0


# ---------------------------------------------------------------------------
# Per-Instrument Context
# ---------------------------------------------------------------------------

@dataclass
class InstCtx:
    symbol: str = ""
    bn_symbol: str = ""
    inst_id: int = 0
    tick: float = 0.001
    lot: float = 0.01
    min_notional: float = 10.0
    hs_bid: float = 0.0
    hs_ask: float = 0.0
    hs_mid: float = 0.0
    fair_bid: float = 0.0
    fair_ask: float = 0.0
    fair_mid: float = 0.0
    basis_bps: float = 0.0
    vol: VolTracker = field(default_factory=lambda: VolTracker(300.0))
    mr: MeanRevTracker = field(default_factory=lambda: MeanRevTracker(7200.0))
    vol_regime_mult: float = 1.0
    mr_shift_bps: float = 0.0
    hs_last_update_ts: float = 0.0


# ---------------------------------------------------------------------------
# Per-Instrument Position State
# ---------------------------------------------------------------------------

@dataclass
class PosState:
    inventory: float = 0.0
    avg_entry: float = 0.0
    entry_cost: float = 0.0
    entry_size: float = 0.0
    last_change_ts: float = 0.0


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Config:
    pk1: str
    addr1: str
    pk2: str
    addr2: str
    instruments: tuple
    basis_instruments: tuple
    # Basis sniper
    basis_entry_bps: float
    basis_exit_bps: float
    basis_max_hold_s: float
    basis_notional: float
    basis_slippage_bps: float
    basis_cooldown_s: float
    # Passive MM
    mm_half_spread_bps: float
    mm_levels: int
    mm_notional: float
    mm_size_scale: float
    mm_requote_interval_s: float
    mm_post_only: bool
    mm_enabled: bool
    # Mean-reversion overlay
    mr_lookback_s: float
    mr_threshold_bps: float
    mr_factor: float
    # Vol regime
    vol_window_s: float
    vol_widen_mult: float
    vol_tighten_mult: float
    vol_high_threshold: float
    vol_low_threshold: float
    # Risk
    max_notional_per_inst: float
    max_loss_usd: float
    leverage: int
    # Execution
    enable_trading: bool
    order_expiry_ms: int

    @classmethod
    def from_env(cls) -> "Config":
        pk1 = os.getenv("HOTSTUFF_PRIVATE_KEY", "")
        addr1 = os.getenv("HOTSTUFF_AGENT_ADDRESS", "")
        pk2 = os.getenv("HEDGE_PRIVATE_KEY", "")
        addr2 = os.getenv("HEDGE_AGENT_ADDRESS", "")
        if not pk1 or not addr1:
            log.error("HOTSTUFF_PRIVATE_KEY / HOTSTUFF_AGENT_ADDRESS not set")
            sys.exit(1)
        if not pk2 or not addr2:
            log.error("HEDGE_PRIVATE_KEY / HEDGE_AGENT_ADDRESS not set")
            sys.exit(1)
        inst_str = os.getenv("VB_INSTRUMENTS", os.getenv("VB_SYMBOL", "HYPE-PERP"))
        instruments = tuple(s.strip() for s in inst_str.split(",") if s.strip())
        basis_str = os.getenv("VB_BASIS_INSTRUMENTS", "")
        basis_instruments = tuple(s.strip() for s in basis_str.split(",") if s.strip()) if basis_str else instruments
        return cls(
            pk1=pk1, addr1=addr1, pk2=pk2, addr2=addr2,
            instruments=instruments, basis_instruments=basis_instruments,
            basis_entry_bps=float(os.getenv("VB_BASIS_ENTRY_BPS", "15")),
            basis_exit_bps=float(os.getenv("VB_BASIS_EXIT_BPS", "5")),
            basis_max_hold_s=float(os.getenv("VB_BASIS_MAX_HOLD_S", "60")),
            basis_notional=float(os.getenv("VB_BASIS_NOTIONAL", "26.0")),
            basis_slippage_bps=float(os.getenv("VB_BASIS_SLIPPAGE_BPS", "5")),
            basis_cooldown_s=float(os.getenv("VB_BASIS_COOLDOWN_S", "3")),
            mm_half_spread_bps=float(os.getenv("VB_MM_HALF_SPREAD_BPS", "4")),
            mm_levels=int(os.getenv("VB_MM_LEVELS", "2")),
            mm_notional=float(os.getenv("VB_MM_NOTIONAL", "20.0")),
            mm_size_scale=float(os.getenv("VB_MM_SIZE_SCALE", "1.5")),
            mm_requote_interval_s=float(os.getenv("VB_MM_REQUOTE_INTERVAL_S", "3")),
            mm_post_only=os.getenv("VB_MM_POST_ONLY", "true").lower() == "true",
            mm_enabled=os.getenv("VB_MM_ENABLED", "true").lower() == "true",
            mr_lookback_s=float(os.getenv("VB_MR_LOOKBACK_S", "7200")),
            mr_threshold_bps=float(os.getenv("VB_MR_THRESHOLD_BPS", "50")),
            mr_factor=float(os.getenv("VB_MR_FACTOR", "0.15")),
            vol_window_s=float(os.getenv("VB_VOL_WINDOW_S", "300")),
            vol_widen_mult=float(os.getenv("VB_VOL_WIDEN_MULT", "1.5")),
            vol_tighten_mult=float(os.getenv("VB_VOL_TIGHTEN_MULT", "0.8")),
            vol_high_threshold=float(os.getenv("VB_VOL_HIGH_THRESHOLD", "6.0")),
            vol_low_threshold=float(os.getenv("VB_VOL_LOW_THRESHOLD", "1.5")),
            max_notional_per_inst=float(os.getenv("VB_MAX_NOTIONAL_PER_INST", "130.0")),
            max_loss_usd=float(os.getenv("VB_MAX_LOSS_USD", "5.0")),
            leverage=int(os.getenv("VB_LEVERAGE", "20")),
            enable_trading=os.getenv("VB_ENABLE_TRADING", "false").lower() == "true",
            order_expiry_ms=int(os.getenv("VB_ORDER_EXPIRY_MS", "300000")),
        )


# ---------------------------------------------------------------------------
# Per-Account State
# ---------------------------------------------------------------------------

@dataclass
class AccState:
    label: str = ""
    addr: str = ""
    positions: Dict[str, PosState] = field(default_factory=dict)
    session_pnl: float = 0.0
    total_fills: int = 0
    total_volume_usd: float = 0.0
    round_trips: int = 0
    basis_fills: int = 0
    basis_volume_usd: float = 0.0
    mm_fills: int = 0
    mm_volume_usd: float = 0.0
    cloid_seq: int = 0
    last_ioc_ts: Dict[str, float] = field(default_factory=dict)
    equity: float = 0.0

    def pos(self, symbol: str) -> PosState:
        if symbol not in self.positions:
            self.positions[symbol] = PosState()
        return self.positions[symbol]

    def last_ioc(self, symbol: str) -> float:
        return self.last_ioc_ts.get(symbol, 0.0)

    def set_last_ioc(self, symbol: str, ts: float):
        self.last_ioc_ts[symbol] = ts


# ---------------------------------------------------------------------------
# VolumeBot
# ---------------------------------------------------------------------------

class VolumeBot:

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._st1 = AccState(label="A1", addr=cfg.addr1)
        self._st2 = AccState(label="A2", addr=cfg.addr2)
        self._running = False
        self._subs: List[Any] = []

        self._instruments: Dict[str, InstCtx] = {}
        self._bn_to_hs: Dict[str, str] = {}
        self._id_to_hs: Dict[int, str] = {}

        for sym in cfg.instruments:
            bn = INST_BINANCE_MAP.get(sym)
            if bn is None:
                log.warning("No Binance mapping for %s -- skipping", sym)
                continue
            ictx = InstCtx(
                symbol=sym, bn_symbol=bn,
                vol=VolTracker(cfg.vol_window_s),
                mr=MeanRevTracker(cfg.mr_lookback_s),
            )
            self._instruments[sym] = ictx
            self._bn_to_hs[bn] = sym

        self._ws: Optional[WebSocketTransport] = None
        self._info: Optional[InfoClient] = None
        self._ex1: Optional[ExchangeClient] = None
        self._ex2: Optional[ExchangeClient] = None
        self._sub_client: Optional[SubscriptionClient] = None
        self._bn_session: Optional[aiohttp.ClientSession] = None
        self._tasks: List[asyncio.Task] = []

        self._csv_file = None
        self._csv_writer = None
        self._csv_date: Optional[str] = None

    # ---------------------------------------------------------------
    # Sizing helpers
    # ---------------------------------------------------------------

    def _compute_size(self, ictx: InstCtx, notional_usd: float) -> float:
        if ictx.fair_mid <= 0.0:
            return 0.0
        return _round_dn(notional_usd / ictx.fair_mid, ictx.lot)

    def _max_inv_units(self, ictx: InstCtx) -> float:
        if ictx.fair_mid <= 0.0:
            return 0.0
        return self.cfg.max_notional_per_inst / ictx.fair_mid

    # ---------------------------------------------------------------
    # Symbol matching
    # ---------------------------------------------------------------

    def _match_symbol(self, inst_raw: str) -> Optional[str]:
        if inst_raw in self._instruments:
            return inst_raw
        try:
            inst_id = int(float(inst_raw))
            return self._id_to_hs.get(inst_id)
        except (TypeError, ValueError):
            pass
        for sym in self._instruments:
            if sym in inst_raw or inst_raw in sym:
                return sym
        return None

    # ---------------------------------------------------------------
    # Lifecycle
    # ---------------------------------------------------------------

    async def start(self):
        self._running = True
        n = len(self._instruments)
        log.info("=== Personal Volume Bot starting ===")
        log.info("Instruments (%d) : %s", n, ", ".join(self._instruments.keys()))
        log.info("Basis instruments: %s", ", ".join(self.cfg.basis_instruments))
        log.info("Mode             : %s", "LIVE" if self.cfg.enable_trading else "DRY-RUN")
        log.info("Basis sniper     : entry=%sbps exit=%sbps notional=$%.0f cooldown=%.1fs",
                 self.cfg.basis_entry_bps, self.cfg.basis_exit_bps,
                 self.cfg.basis_notional, self.cfg.basis_cooldown_s)
        log.info("Passive MM       : half_spread=%sbps levels=%d notional=$%.0f scale=%.1f every=%.1fs",
                 self.cfg.mm_half_spread_bps, self.cfg.mm_levels,
                 self.cfg.mm_notional, self.cfg.mm_size_scale,
                 self.cfg.mm_requote_interval_s)
        log.info("Risk             : max_notional/inst=$%.0f max_loss=$%.1f leverage=%dx",
                 self.cfg.max_notional_per_inst, self.cfg.max_loss_usd, self.cfg.leverage)
        log.info("A1 [BASIS]       : %s", self.cfg.addr1)
        log.info("A2 [MM]          : %s", self.cfg.addr2)

        w1 = Account.from_key(self.cfg.pk1)
        w2 = Account.from_key(self.cfg.pk2)
        self._w1 = w1
        self._w2 = w2

        self._info = InfoClient(is_testnet=False)
        self._ex1 = ExchangeClient(wallet=w1, is_testnet=False)
        self._ex2 = ExchangeClient(wallet=w2, is_testnet=False)

        self._consecutive_http_errors = 0
        self._last_successful_trade_ts = time.monotonic()

        ws_server = {"mainnet": "wss://api.hotstuff.trade/ws/"}
        self._ws = WebSocketTransport(WebSocketTransportOptions(
            is_testnet=False, timeout=15.0,
            keep_alive={"interval": 20.0, "timeout": 10.0},
            auto_connect=True, server=ws_server,
        ))
        self._sub_client = SubscriptionClient(transport=self._ws)

        await self._resolve_instruments()
        await self._sync_accounts()
        await self._sync_positions()
        await self._fetch_initial_prices()

        self._bn_session = aiohttp.ClientSession()
        self._tasks.append(asyncio.create_task(self._run_binance_ws()))

        await self._subscribe_hotstuff()

        if self.cfg.enable_trading:
            await self._cancel_all(self._ex1, "A1")
            await self._cancel_all(self._ex2, "A2")
            log.info("Cleared stale orders on both accounts")
            await self._flatten_all_positions()

        self._init_csv()

        self._tasks.append(asyncio.create_task(self._run_basis_loop(self._ex1, self._st1)))
        self._tasks.append(asyncio.create_task(self._run_basis_loop(self._ex2, self._st2)))
        if self.cfg.mm_enabled:
            self._tasks.append(asyncio.create_task(self._run_mm_loop(self._ex2, self._st2)))
        self._tasks.append(asyncio.create_task(self._run_position_poll()))
        self._tasks.append(asyncio.create_task(self._run_stats_logger()))
        self._tasks.append(asyncio.create_task(self._run_transport_recycler()))
        self._tasks.append(asyncio.create_task(self._run_inventory_flatten()))
        self._tasks.append(asyncio.create_task(self._run_hs_watchdog()))

        mode = "A1+A2=BASIS" if not self.cfg.mm_enabled else "A1=BASIS / A2=MM"
        log.info("=== Volume Bot ready -- %s -- %d instruments ===", mode, n)

        try:
            await asyncio.gather(*self._tasks)
        except asyncio.CancelledError:
            pass

    async def stop(self):
        log.info("Shutting down...")
        self._running = False

        if self.cfg.enable_trading:
            for ex, label in [(self._ex1, "A1"), (self._ex2, "A2")]:
                try:
                    await self._cancel_all(ex, label)
                except Exception as exc:
                    log.error("Cancel on shutdown (%s): %s", label, exc)

        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            try:
                await t
            except asyncio.CancelledError:
                pass

        if self._bn_session:
            await self._bn_session.close()
        for sub in self._subs:
            try:
                if isinstance(sub, dict) and "unsubscribe" in sub:
                    sub["unsubscribe"]()
            except Exception:
                pass
        if self._ws:
            try:
                self._ws.disconnect()
            except Exception:
                pass
        for client in (self._info, self._ex1, self._ex2):
            if client and hasattr(client, "transport"):
                try:
                    client.transport.close()
                except Exception:
                    pass

        self._close_csv()

        for st in (self._st1, self._st2):
            log.info("%s summary: pnl=$%.4f fills=%d vol=$%.2f (basis=%d/$%.2f mm=%d/$%.2f) rt=%d",
                     st.label, st.session_pnl, st.total_fills, st.total_volume_usd,
                     st.basis_fills, st.basis_volume_usd, st.mm_fills, st.mm_volume_usd,
                     st.round_trips)
        log.info("=== Shutdown complete ===")

    # ---------------------------------------------------------------
    # Init helpers
    # ---------------------------------------------------------------

    async def _resolve_instruments(self):
        raw = await asyncio.to_thread(
            self._info.transport.request,
            "info", {"method": "instruments", "params": {"type": "perps"}},
        )
        perps = raw.get("perps", []) if isinstance(raw, dict) else []
        resolved = set()
        for p in perps:
            name = p.get("name")
            if name in self._instruments:
                ictx = self._instruments[name]
                ictx.inst_id = int(p["id"])
                ictx.tick = float(p.get("tick_size", 0.001))
                ictx.lot = float(p.get("lot_size", 0.01))
                ictx.min_notional = float(p.get("min_notional_usd", 10))
                self._id_to_hs[ictx.inst_id] = name
                resolved.add(name)
                log.info("  %s id=%d tick=%s lot=%s",
                         name, ictx.inst_id,
                         _fmt(ictx.tick, 0.000001), _fmt(ictx.lot, 0.000001))

        missing = set(self._instruments.keys()) - resolved
        for sym in list(missing):
            log.error("Instrument %s not found on exchange -- removing", sym)
            bn = self._instruments[sym].bn_symbol
            del self._instruments[sym]
            self._bn_to_hs.pop(bn, None)

        if not self._instruments:
            log.error("No valid instruments found")
            sys.exit(1)

    async def _sync_accounts(self):
        for addr, st in [(self.cfg.addr1, self._st1), (self.cfg.addr2, self._st2)]:
            try:
                raw = await asyncio.to_thread(
                    self._info.transport.request,
                    "info",
                    {"method": "accountSummary", "params": {"user": addr}},
                )
                eq = float(raw.get("total_account_equity", 0))
                if eq <= 0.0:
                    eq = float(raw.get("available_balance", 0))
                st.equity = eq
                log.info("%s equity: $%.2f", st.label, eq)
            except Exception as exc:
                log.warning("%s equity sync: %s", st.label, exc)

    async def _sync_positions(self):
        now = time.monotonic()
        for addr, st in [(self.cfg.addr1, self._st1), (self.cfg.addr2, self._st2)]:
            try:
                raw = await asyncio.to_thread(
                    self._info.transport.request,
                    "info",
                    {"method": "positions", "params": {"user": addr}},
                )
                positions = raw if isinstance(raw, list) else []
                for pos in positions:
                    inst_raw = pos.get("instrument", pos.get("symbol", ""))
                    symbol = self._match_symbol(str(inst_raw))
                    if not symbol:
                        continue
                    size = float(pos.get("size", 0))
                    ps = st.pos(symbol)
                    ps.inventory = size
                    ps.last_change_ts = now
                    if abs(size) > 0.001:
                        log.info("%s %s inventory: %.6f", st.label, symbol, size)
            except Exception as exc:
                log.warning("%s position sync: %s", st.label, exc)

    async def _fetch_initial_prices(self):
        for sym, ictx in self._instruments.items():
            try:
                raw = await asyncio.to_thread(
                    self._info.transport.request,
                    "info", {"method": "ticker", "params": {"symbol": sym}},
                )
                tickers = raw if isinstance(raw, list) else [raw] if raw else []
                if tickers:
                    t = tickers[0]
                    bid = float(t.get("best_bid_price", 0))
                    ask = float(t.get("best_ask_price", 0))
                    mid = float(t.get("mid_price", 0))
                    if bid > 0.0:
                        ictx.hs_bid = bid
                    if ask > 0.0:
                        ictx.hs_ask = ask
                    ictx.hs_mid = mid if mid > 0.0 else (bid + ask) * 0.5 if bid > 0 and ask > 0 else 0.0
                    ictx.fair_mid = ictx.hs_mid
                    ictx.fair_bid = bid
                    ictx.fair_ask = ask
                    log.info("  %s mid=%s", sym, _fmt(ictx.hs_mid, ictx.tick))
            except Exception as exc:
                log.warning("Ticker %s: %s", sym, exc)

    # ---------------------------------------------------------------
    # Order helpers
    # ---------------------------------------------------------------

    def _next_cloid(self, st: AccState, prefix: str, symbol: str = "") -> str:
        st.cloid_seq += 1
        short = symbol.split("-")[0].lower() if symbol else "x"
        return f"{prefix}-{st.label}-{short}-{int(time.time())}-{st.cloid_seq}"

    async def _cancel_all(self, ex: ExchangeClient, label: str):
        try:
            now_ms = int(time.time() * 1000)
            await asyncio.to_thread(
                ex.cancel_all, CancelAllParams(expiresAfter=now_ms + 60_000),
            )
        except Exception as exc:
            log.debug("cancel_all %s: %s", label, exc)

    async def _fire_ioc(
        self, ex: ExchangeClient, st: AccState, ictx: InstCtx,
        side: str, size: float, price: float, tag: str,
    ) -> bool:
        cloid = self._next_cloid(st, tag, ictx.symbol)
        log.info("%s IOC %s %s: %s %s @ %s [%s]",
                 st.label, tag.upper(), ictx.symbol, side.upper(),
                 _fmt(size, ictx.lot), _fmt(price, ictx.tick), cloid)

        if not self.cfg.enable_trading:
            return False

        try:
            now_ms = int(time.time() * 1000)
            resp = await asyncio.to_thread(ex.place_order, PlaceOrderParams(
                orders=[UnitOrder(
                    instrumentId=ictx.inst_id,
                    side="b" if side == "buy" else "s",
                    positionSide="BOTH",
                    price=_fmt(price, ictx.tick),
                    size=_fmt(size, ictx.lot),
                    tif="IOC",
                    ro=False,
                    po=False,
                    cloid=cloid,
                )],
                expiresAfter=now_ms + 60_000,
            ))
            log.debug("%s IOC resp: %s", st.label, resp)
            self._track_http_ok()
            return True
        except Exception as exc:
            log.error("%s IOC %s failed: %s", st.label, ictx.symbol, exc)
            self._track_http_err()
            return False

    # ---------------------------------------------------------------
    # Subscriptions
    # ---------------------------------------------------------------

    async def _subscribe_hotstuff(self):
        for sym in self._instruments:
            sub = await asyncio.to_thread(
                self._sub_client.bbo,
                BBOSubscriptionParams(symbol=sym),
                lambda msg, _sym=sym: self._on_bbo(msg, _sym))
            self._subs.append(sub)
        log.info("Sub: BBO for %d instruments", len(self._instruments))

        for addr, st in [(self.cfg.addr1, self._st1), (self.cfg.addr2, self._st2)]:
            sub = await asyncio.to_thread(
                self._sub_client.fills,
                FillsSubscriptionParams(user=addr),
                lambda msg, _st=st: self._on_fill(msg, _st))
            self._subs.append(sub)

            sub = await asyncio.to_thread(
                self._sub_client.positions,
                PositionsSubscriptionParams(user=addr),
                lambda msg, _st=st: self._on_position(msg, _st))
            self._subs.append(sub)
            log.info("Sub: fills+positions for %s", st.label)

    # ---------------------------------------------------------------
    # Hotstuff WS health monitor + reconnect
    # ---------------------------------------------------------------

    _HS_STALE_THRESHOLD_S = 15.0

    def _hs_prices_stale(self) -> bool:
        now = time.monotonic()
        for ictx in self._instruments.values():
            if ictx.hs_last_update_ts > 0 and now - ictx.hs_last_update_ts > self._HS_STALE_THRESHOLD_S:
                return True
        if all(ictx.hs_last_update_ts == 0.0 for ictx in self._instruments.values()):
            return False
        return False

    async def _reconnect_hotstuff_ws(self):
        log.warning("Hotstuff WS stale -- forcing reconnect")
        for sub in self._subs:
            try:
                if isinstance(sub, dict) and "unsubscribe" in sub:
                    sub["unsubscribe"]()
            except Exception:
                pass
        self._subs.clear()

        if self._ws:
            try:
                self._ws.disconnect()
            except Exception:
                pass

        await asyncio.sleep(2.0)

        ws_server = {"mainnet": "wss://api.hotstuff.trade/ws/"}
        self._ws = WebSocketTransport(WebSocketTransportOptions(
            is_testnet=False, timeout=15.0,
            keep_alive={"interval": 20.0, "timeout": 10.0},
            auto_connect=True, server=ws_server,
        ))
        self._sub_client = SubscriptionClient(transport=self._ws)
        await self._subscribe_hotstuff()
        log.info("Hotstuff WS reconnected and resubscribed")

    async def _run_hs_watchdog(self):
        await asyncio.sleep(30.0)
        while self._running:
            try:
                await asyncio.sleep(5.0)
                if not self._running:
                    break
                if self._hs_prices_stale():
                    stale_syms = []
                    now = time.monotonic()
                    for sym, ictx in self._instruments.items():
                        if ictx.hs_last_update_ts > 0:
                            age = now - ictx.hs_last_update_ts
                            if age > self._HS_STALE_THRESHOLD_S:
                                stale_syms.append(f"{sym}:{age:.0f}s")
                    log.warning("Hotstuff BBO stale: %s", " ".join(stale_syms))
                    if self.cfg.enable_trading:
                        await self._cancel_all(self._ex1, "A1")
                        await self._cancel_all(self._ex2, "A2")
                    await self._reconnect_hotstuff_ws()
            except asyncio.CancelledError:
                return
            except Exception as exc:
                log.error("HS watchdog error: %s", exc)
                await asyncio.sleep(5.0)

    # ---------------------------------------------------------------
    # Binance WS (multi-stream)
    # ---------------------------------------------------------------

    async def _run_binance_ws(self):
        streams = "/".join(f"{ictx.bn_symbol}@bookTicker"
                           for ictx in self._instruments.values())
        url = f"wss://fstream.binance.com/stream?streams={streams}"

        while self._running:
            try:
                async with self._bn_session.ws_connect(url, heartbeat=20) as ws:
                    log.info("Binance WS connected (%d streams)", len(self._instruments))
                    async for msg in ws:
                        if not self._running:
                            break
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                payload = json.loads(msg.data)
                                stream = payload.get("stream", "")
                                data = payload.get("data", {})
                                bn_sym = stream.split("@")[0] if "@" in stream else ""
                                symbol = self._bn_to_hs.get(bn_sym)
                                if not symbol:
                                    continue
                                ictx = self._instruments.get(symbol)
                                if not ictx:
                                    continue
                                bid = float(data.get("b", 0))
                                ask = float(data.get("a", 0))
                                if bid > 0.0 and ask > 0.0:
                                    now = time.monotonic()
                                    mid = (bid + ask) * 0.5
                                    ictx.fair_bid = bid
                                    ictx.fair_ask = ask
                                    ictx.fair_mid = mid
                                    ictx.vol.update(mid, now)
                                    ictx.mr.update(mid, now)
                                    self._update_basis(ictx)
                                    self._update_overlays(ictx)
                            except (json.JSONDecodeError, KeyError, ValueError):
                                pass
                        elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                            break
            except asyncio.CancelledError:
                return
            except Exception as exc:
                log.warning("Binance WS error: %s -- reconnect 3s", exc)
                await asyncio.sleep(3.0)

    def _update_basis(self, ictx: InstCtx):
        if ictx.fair_mid > 0.0 and ictx.hs_mid > 0.0:
            ictx.basis_bps = (ictx.hs_mid - ictx.fair_mid) / ictx.fair_mid * 10000.0

    def _update_overlays(self, ictx: InstCtx):
        vb = ictx.vol.vol_bps()
        if vb >= self.cfg.vol_high_threshold:
            ictx.vol_regime_mult = self.cfg.vol_widen_mult
        elif vb <= self.cfg.vol_low_threshold:
            ictx.vol_regime_mult = self.cfg.vol_tighten_mult
        else:
            ictx.vol_regime_mult = 1.0

        ret = ictx.mr.return_bps()
        if abs(ret) > self.cfg.mr_threshold_bps:
            ictx.mr_shift_bps = -ret * self.cfg.mr_factor
        else:
            ictx.mr_shift_bps = 0.0

    # ---------------------------------------------------------------
    # Hotstuff callbacks
    # ---------------------------------------------------------------

    def _on_bbo(self, msg, symbol: str):
        try:
            data = msg.data
            bid = _sf(data, "best_bid_price") or _sf(data, "bestBidPrice")
            ask = _sf(data, "best_ask_price") or _sf(data, "bestAskPrice")
            ictx = self._instruments.get(symbol)
            if not ictx:
                return
            if bid > 0.0:
                ictx.hs_bid = bid
            if ask > 0.0:
                ictx.hs_ask = ask
            if bid > 0.0 and ask > 0.0:
                ictx.hs_mid = (bid + ask) * 0.5
                ictx.hs_last_update_ts = time.monotonic()
                self._update_basis(ictx)
        except Exception:
            pass

    def _on_fill(self, msg, st: AccState):
        try:
            data = msg.data if hasattr(msg, "data") else msg
            fills = data if isinstance(data, list) else [data] if isinstance(data, dict) else []
            for f in fills:
                self._process_fill(f, st)
        except Exception as exc:
            log.error("Fill callback error: %s", exc)

    def _process_fill(self, fill: dict, st: AccState):
        inst_raw = fill.get("instrument", fill.get("symbol", ""))
        symbol = self._match_symbol(str(inst_raw))
        if not symbol:
            return
        ictx = self._instruments.get(symbol)
        if not ictx:
            return

        side = str(fill.get("side", "")).upper()
        price = float(fill.get("price", 0))
        size = float(fill.get("size", 0))
        cloid = str(fill.get("cloid", ""))
        if price <= 0.0 or size <= 0.0:
            return

        is_buy = side in ("BUY", "B", "LONG")
        is_basis = cloid.startswith("basis-")
        notional = size * price

        st.total_fills += 1
        st.total_volume_usd += notional
        if is_basis:
            st.basis_fills += 1
            st.basis_volume_usd += notional
        else:
            st.mm_fills += 1
            st.mm_volume_usd += notional

        ps = st.pos(symbol)
        old_inv = ps.inventory

        if old_inv == 0.0 or (old_inv > 0.0 and is_buy) or (old_inv < 0.0 and not is_buy):
            ps.entry_cost += price * size
            ps.entry_size += size
            ps.avg_entry = ps.entry_cost / ps.entry_size if ps.entry_size > 0 else price
        else:
            close_size = min(size, abs(old_inv))
            if ps.avg_entry > 0.0:
                if old_inv > 0.0:
                    pnl = (price - ps.avg_entry) * close_size
                else:
                    pnl = (ps.avg_entry - price) * close_size
                st.session_pnl += pnl
                st.round_trips += 1
                tag = "BASIS" if is_basis else "MM"
                log.info("%s %s %s RT PnL: $%.4f (entry=%.4f exit=%.4f sz=%.6f) | session=$%.4f",
                         st.label, symbol, tag, pnl, ps.avg_entry, price, close_size, st.session_pnl)

            remaining = size - close_size
            if abs(old_inv) - close_size < ictx.lot:
                ps.entry_cost = 0.0
                ps.entry_size = 0.0
                ps.avg_entry = 0.0
            elif remaining > 0.0:
                ps.entry_cost = price * remaining
                ps.entry_size = remaining
                ps.avg_entry = price

        ps.last_change_ts = time.monotonic()

        tag = "BASIS" if is_basis else "MM"
        log.info("%s FILL [%s] %s: %s %s @ %s | pnl=$%.4f",
                 st.label, tag, symbol,
                 side, _fmt(size, ictx.lot), _fmt(price, ictx.tick),
                 st.session_pnl)

        try:
            fill_event = "fill_basis" if is_basis else "fill_mm"
            side_str = "buy" if is_buy else "sell"
            self._write_event(fill_event, st, symbol, side_str, size, price, ictx.basis_bps)
        except Exception:
            pass

    def _on_position(self, msg, st: AccState):
        try:
            data = msg.data if hasattr(msg, "data") else msg
            positions = data if isinstance(data, list) else [data] if isinstance(data, dict) else []
            for pos in positions:
                if not isinstance(pos, dict):
                    continue
                inst_raw = pos.get("instrument", "") or pos.get("instrument_name", "")
                inst_id = 0
                try:
                    inst_id = int(float(pos.get("instrument_id", 0)))
                except (TypeError, ValueError):
                    pass

                symbol = self._match_symbol(str(inst_raw)) if inst_raw else None
                if not symbol and inst_id:
                    symbol = self._id_to_hs.get(inst_id)
                if not symbol:
                    continue
                ictx = self._instruments.get(symbol)
                if not ictx:
                    continue

                ps = st.pos(symbol)
                old = ps.inventory

                legs = pos.get("legs")
                if legs and isinstance(legs, list):
                    net = sum(float(leg.get("size", 0)) for leg in legs)
                else:
                    net = float(pos.get("size", 0))

                if abs(net - old) > ictx.lot * 2:
                    log.info("%s %s INV RECONCILE: %s -> %s", st.label, symbol,
                             _fmt(old, ictx.lot), _fmt(net, ictx.lot))
                if abs(net - old) > ictx.lot:
                    ps.last_change_ts = time.monotonic()
                ps.inventory = net
        except Exception as exc:
            log.error("Position callback error: %s", exc)

    # ---------------------------------------------------------------
    # Strategy 1: BASIS MEAN-REVERSION (A1 only, all instruments)
    # ---------------------------------------------------------------

    async def _run_basis_loop(self, ex: ExchangeClient, st: AccState):
        log.info("%s Basis sniper loop started (%d instruments)", st.label, len(self._instruments))
        while self._running:
            try:
                await asyncio.sleep(0.05)
                if not self._running:
                    break
                await self._check_basis(ex, st)
            except asyncio.CancelledError:
                return
            except Exception as exc:
                log.error("%s Basis loop error: %s", st.label, exc)
                await asyncio.sleep(0.5)

    async def _check_basis(self, ex: ExchangeClient, st: AccState):
        if self._hs_prices_stale():
            return
        now = time.monotonic()
        entry_thresh = self.cfg.basis_entry_bps
        exit_thresh = self.cfg.basis_exit_bps
        slip = self.cfg.basis_slippage_bps / 10000.0

        combined_loss = self._st1.session_pnl + self._st2.session_pnl
        if combined_loss < -self.cfg.max_loss_usd:
            return

        for sym, ictx in self._instruments.items():
            if sym not in self.cfg.basis_instruments:
                continue
            if ictx.fair_mid <= 0.0 or ictx.hs_mid <= 0.0:
                continue
            if now - st.last_ioc(sym) < self.cfg.basis_cooldown_s:
                continue

            basis = ictx.basis_bps
            ps = st.pos(sym)
            inv = ps.inventory
            max_inv = self._max_inv_units(ictx)
            size = self._compute_size(ictx, self.cfg.basis_notional)

            if size <= 0.0 or size * ictx.fair_mid < ictx.min_notional:
                continue

            acted = False
            if basis > entry_thresh:
                room = max_inv + inv
                if room > ictx.lot:
                    price = ictx.hs_bid * (1.0 - slip)
                    price = _round_dn(price, ictx.tick)
                    sz = _round_dn(min(size, room), ictx.lot)
                    if sz > 0 and sz * price >= ictx.min_notional:
                        st.set_last_ioc(sym, now)
                        ps.last_change_ts = now
                        await self._fire_ioc(ex, st, ictx, "sell", sz, price, "basis")
                        self._write_event("basis_entry", st, sym, "sell", sz, price, basis)
                        acted = True

            elif basis < -entry_thresh:
                room = max_inv - inv
                if room > ictx.lot:
                    price = ictx.hs_ask * (1.0 + slip)
                    price = _round_up(price, ictx.tick)
                    sz = _round_dn(min(size, room), ictx.lot)
                    if sz > 0 and sz * price >= ictx.min_notional:
                        st.set_last_ioc(sym, now)
                        ps.last_change_ts = now
                        await self._fire_ioc(ex, st, ictx, "buy", sz, price, "basis")
                        self._write_event("basis_entry", st, sym, "buy", sz, price, basis)
                        acted = True

            elif abs(inv) > ictx.lot:
                should_exit = False
                if abs(basis) < exit_thresh:
                    should_exit = True
                elif ps.avg_entry > 0.0:
                    if inv > 0:
                        upnl = (ictx.hs_bid - ps.avg_entry) * abs(inv)
                    else:
                        upnl = (ps.avg_entry - ictx.hs_ask) * abs(inv)
                    if upnl > 0.02:
                        should_exit = True

                if should_exit:
                    if inv > 0:
                        price = ictx.hs_bid * (1.0 - slip)
                        price = _round_dn(price, ictx.tick)
                        sz = _round_dn(min(size, abs(inv)), ictx.lot)
                        side_str = "sell"
                    else:
                        price = ictx.hs_ask * (1.0 + slip)
                        price = _round_up(price, ictx.tick)
                        sz = _round_dn(min(size, abs(inv)), ictx.lot)
                        side_str = "buy"
                    if sz > 0 and sz * price >= ictx.min_notional:
                        st.set_last_ioc(sym, now)
                        ps.last_change_ts = now
                        await self._fire_ioc(ex, st, ictx, side_str, sz, price, "basis")
                        self._write_event("basis_exit", st, sym, side_str, sz, price, basis)
                        acted = True

            if not acted and abs(inv) > ictx.lot:
                hold_s = now - ps.last_change_ts if ps.last_change_ts > 0 else 0.0
                if hold_s > self.cfg.basis_max_hold_s:
                    log.info("%s %s basis force-close (stale %.0fs, basis=%+.1fbps, inv=%s)",
                             st.label, sym, hold_s, basis, _fmt(inv, ictx.lot))
                    if inv > 0:
                        price = ictx.hs_bid * (1.0 - slip)
                        price = _round_dn(price, ictx.tick)
                        side_str = "sell"
                    else:
                        price = ictx.hs_ask * (1.0 + slip)
                        price = _round_up(price, ictx.tick)
                        side_str = "buy"
                    sz = _round_dn(min(size, abs(inv)), ictx.lot)
                    if sz > 0 and sz * price >= ictx.min_notional:
                        st.set_last_ioc(sym, now)
                        await self._fire_ioc(ex, st, ictx, side_str, sz, price, "basis")
                        self._write_event("basis_force_exit", st, sym, side_str, sz, price, basis)

    # ---------------------------------------------------------------
    # Strategy 2-5: PASSIVE MM (A2 only, all instruments)
    # ---------------------------------------------------------------

    async def _run_mm_loop(self, ex: ExchangeClient, st: AccState):
        log.info("%s MM loop started (%d instruments)", st.label, len(self._instruments))
        while self._running:
            try:
                await asyncio.sleep(self.cfg.mm_requote_interval_s)
                if not self._running:
                    break
                await self._do_mm_requote(ex, st)
            except asyncio.CancelledError:
                return
            except Exception as exc:
                log.error("%s MM error: %s", st.label, exc)
                await asyncio.sleep(2.0)

    async def _do_mm_requote(self, ex: ExchangeClient, st: AccState):
        if self._hs_prices_stale():
            if self.cfg.enable_trading:
                await self._cancel_all(ex, st.label)
            return
        combined_loss = self._st1.session_pnl + self._st2.session_pnl
        if combined_loss < -self.cfg.max_loss_usd:
            if self.cfg.enable_trading:
                await self._cancel_all(ex, st.label)
            return

        hour_utc = datetime.now(timezone.utc).hour
        tod_mult = TOD_SPREAD_MULT.get(hour_utc, 1.0)

        all_orders: List[UnitOrder] = []

        for sym, ictx in self._instruments.items():
            fair = ictx.fair_mid
            if fair <= 0.0:
                continue

            vol_mult = ictx.vol_regime_mult
            mr_shift = ictx.mr_shift_bps / 10000.0 * fair

            base_hs = self.cfg.mm_half_spread_bps / 10000.0 * fair
            half_spread = base_hs * tod_mult * vol_mult
            center = fair + mr_shift

            ps = st.pos(sym)
            inv = ps.inventory
            max_inv = self._max_inv_units(ictx)
            inv_ratio = abs(inv) / max_inv if max_inv > 0 else 0.0

            skip_bids = inv >= max_inv
            skip_asks = inv <= -max_inv

            now_mm = time.monotonic()
            stale_s = now_mm - ps.last_change_ts if ps.last_change_ts > 0 else 0.0

            close_skew = 0.0
            if inv_ratio > 0.3:
                urgency = min(3.0, 1.0 + stale_s / 60.0)
                skew_pct = min(1.0, (inv_ratio - 0.3) / 0.7)
                close_skew = half_spread * skew_pct * 0.8 * urgency

            for i in range(self.cfg.mm_levels):
                offset = half_spread * i * 0.5
                sz = self._compute_size(ictx, self.cfg.mm_notional * (self.cfg.mm_size_scale ** i))
                if sz <= 0.0 or sz * fair < ictx.min_notional:
                    continue

                if not skip_bids:
                    bid_skew = close_skew if inv < 0 else 0.0
                    bp = _round_dn(center - half_spread - offset + bid_skew, ictx.tick)
                    if bp > 0.0 and (ictx.hs_ask <= 0.0 or bp < ictx.hs_ask):
                        all_orders.append(UnitOrder(
                            instrumentId=ictx.inst_id,
                            side="b", positionSide="BOTH",
                            price=_fmt(bp, ictx.tick), size=_fmt(sz, ictx.lot),
                            tif="GTC", ro=False, po=self.cfg.mm_post_only,
                            cloid=self._next_cloid(st, "mm", sym),
                        ))

                if not skip_asks:
                    ask_skew = close_skew if inv > 0 else 0.0
                    ap = _round_up(center + half_spread + offset - ask_skew, ictx.tick)
                    if ap > 0.0 and (ictx.hs_bid <= 0.0 or ap > ictx.hs_bid):
                        all_orders.append(UnitOrder(
                            instrumentId=ictx.inst_id,
                            side="s", positionSide="BOTH",
                            price=_fmt(ap, ictx.tick), size=_fmt(sz, ictx.lot),
                            tif="GTC", ro=False, po=self.cfg.mm_post_only,
                            cloid=self._next_cloid(st, "mm", sym),
                        ))

        if not self.cfg.enable_trading:
            basis_str = " ".join(f"{s.split('-')[0]}:{ictx.basis_bps:+.1f}"
                                 for s, ictx in self._instruments.items()
                                 if ictx.fair_mid > 0)
            log.info("DRY %s | %d orders | basis=[%s]", st.label, len(all_orders), basis_str)
            return

        await self._cancel_all(ex, st.label)

        if all_orders:
            try:
                now_ms = int(time.time() * 1000)
                resp = await asyncio.to_thread(ex.place_order, PlaceOrderParams(
                    orders=all_orders,
                    expiresAfter=now_ms + self.cfg.order_expiry_ms,
                ))
                log.debug("%s placed %d MM orders: %s", st.label, len(all_orders), resp)
                self._track_http_ok()
            except Exception as exc:
                log.error("%s MM place failed: %s", st.label, exc)
                self._track_http_err()

        basis_str = " ".join(f"{s.split('-')[0]}:{ictx.basis_bps:+.1f}"
                             for s, ictx in self._instruments.items()
                             if ictx.fair_mid > 0)
        log.info("LIVE %s | %d orders across %d instruments | basis=[%s]",
                 st.label, len(all_orders), len(self._instruments), basis_str)

    # ---------------------------------------------------------------
    # Inventory flatten (startup + ongoing)
    # ---------------------------------------------------------------

    async def _flatten_all_positions(self):
        slip = self.cfg.basis_slippage_bps / 10000.0
        flattened = 0
        for ex, st in [(self._ex1, self._st1), (self._ex2, self._st2)]:
            for sym, ictx in self._instruments.items():
                ps = st.pos(sym)
                inv = ps.inventory
                if abs(inv) <= ictx.lot:
                    continue
                if ictx.hs_mid <= 0.0:
                    continue
                if inv > 0:
                    price = ictx.hs_bid * (1.0 - slip)
                    price = _round_dn(price, ictx.tick)
                    side_str = "sell"
                else:
                    price = ictx.hs_ask * (1.0 + slip)
                    price = _round_up(price, ictx.tick)
                    side_str = "buy"
                sz = _round_dn(abs(inv), ictx.lot)
                if sz > 0 and sz * price >= ictx.min_notional:
                    log.info("FLATTEN %s %s: %s %.6f @ %s (inv was %s)",
                             st.label, sym, side_str, sz, _fmt(price, ictx.tick),
                             _fmt(inv, ictx.lot))
                    await self._fire_ioc(ex, st, ictx, side_str, sz, price, "flatten")
                    now_f = time.monotonic()
                    st.set_last_ioc(sym, now_f)
                    ps.last_change_ts = now_f
                    flattened += 1
                    await asyncio.sleep(0.3)
        if flattened:
            log.info("Startup flatten: sent %d IOC orders -- waiting 8s for confirms...", flattened)
            await asyncio.sleep(8.0)
            await self._sync_positions()
            log.info("Startup flatten complete, positions re-synced")
        else:
            log.info("Startup flatten: no stuck inventory to clear")

    async def _run_inventory_flatten(self):
        stale_threshold_s = 120.0
        while self._running:
            try:
                await asyncio.sleep(15.0)
                if not self._running:
                    break
                if self._hs_prices_stale():
                    continue
                now = time.monotonic()
                slip = self.cfg.basis_slippage_bps / 10000.0
                for ex, st in [(self._ex1, self._st1), (self._ex2, self._st2)]:
                    for sym, ictx in self._instruments.items():
                        ps = st.pos(sym)
                        inv = ps.inventory
                        if abs(inv) <= ictx.lot:
                            continue
                        if ictx.hs_mid <= 0.0:
                            continue
                        stale_s = now - ps.last_change_ts if ps.last_change_ts > 0 else 0.0
                        if stale_s < stale_threshold_s:
                            continue
                        size = self._compute_size(ictx, self.cfg.basis_notional)
                        if size <= 0.0:
                            continue
                        sz = _round_dn(min(size, abs(inv)), ictx.lot)
                        if inv > 0:
                            price = ictx.hs_bid * (1.0 - slip)
                            price = _round_dn(price, ictx.tick)
                            side_str = "sell"
                        else:
                            price = ictx.hs_ask * (1.0 + slip)
                            price = _round_up(price, ictx.tick)
                            side_str = "buy"
                        if sz > 0 and sz * price >= ictx.min_notional:
                            log.info("STALE_FLATTEN %s %s: %s %s @ %s (stale %.0fs, inv=%s)",
                                     st.label, sym, side_str, _fmt(sz, ictx.lot),
                                     _fmt(price, ictx.tick), stale_s,
                                     _fmt(inv, ictx.lot))
                            await self._fire_ioc(ex, st, ictx, side_str, sz, price, "flatten")
                            await asyncio.sleep(0.3)
            except asyncio.CancelledError:
                return
            except Exception as exc:
                log.error("Inventory flatten error: %s", exc)
                await asyncio.sleep(5.0)

    # ---------------------------------------------------------------
    # Background tasks
    # ---------------------------------------------------------------

    async def _run_position_poll(self):
        while self._running:
            try:
                await asyncio.sleep(30.0)
                if not self._running:
                    break
                for addr, st in [(self.cfg.addr1, self._st1), (self.cfg.addr2, self._st2)]:
                    raw = await asyncio.to_thread(
                        self._info.transport.request,
                        "info",
                        {"method": "positions", "params": {"user": addr}},
                    )
                    positions = raw if isinstance(raw, list) else []
                    for pos in positions:
                        inst_raw = pos.get("instrument", pos.get("symbol", ""))
                        symbol = self._match_symbol(str(inst_raw))
                        if not symbol:
                            continue
                        ictx = self._instruments.get(symbol)
                        if not ictx:
                            continue
                        ps = st.pos(symbol)
                        old = ps.inventory
                        net = float(pos.get("size", 0))
                        if abs(net - old) > ictx.lot:
                            log.info("%s %s REST reconcile: %s -> %s",
                                     st.label, symbol, _fmt(old, ictx.lot), _fmt(net, ictx.lot))
                            ps.inventory = net
            except asyncio.CancelledError:
                return
            except Exception as exc:
                log.warning("Position poll: %s", exc)

    async def _run_stats_logger(self):
        while self._running:
            try:
                await asyncio.sleep(60.0)
                if not self._running:
                    break

                s1, s2 = self._st1, self._st2
                log.info(
                    "STATS A1[BASIS] | pnl=$%.4f | fills=%d | vol=$%.2f",
                    s1.session_pnl, s1.total_fills, s1.total_volume_usd,
                )
                log.info(
                    "STATS A2[MM]    | pnl=$%.4f | fills=%d | vol=$%.2f",
                    s2.session_pnl, s2.total_fills, s2.total_volume_usd,
                )
                combined_pnl = s1.session_pnl + s2.session_pnl
                combined_vol = s1.total_volume_usd + s2.total_volume_usd
                log.info(
                    "STATS COMBINED  | pnl=$%.4f | fills=%d | vol=$%.2f",
                    combined_pnl, s1.total_fills + s2.total_fills, combined_vol,
                )

                inv_parts = []
                basis_parts = []
                for sym, ictx in self._instruments.items():
                    short = sym.split("-")[0]
                    a1_inv = s1.pos(sym).inventory
                    a2_inv = s2.pos(sym).inventory
                    if abs(a1_inv) > ictx.lot or abs(a2_inv) > ictx.lot:
                        inv_parts.append(f"{short}:A1={_fmt(a1_inv, ictx.lot)}/A2={_fmt(a2_inv, ictx.lot)}")
                    if ictx.fair_mid > 0:
                        basis_parts.append(f"{short}:{ictx.basis_bps:+.1f}")

                if inv_parts:
                    log.info("STATS INV       | %s", " | ".join(inv_parts))
                log.info("STATS BASIS     | %s", " ".join(basis_parts))

            except asyncio.CancelledError:
                return
            except Exception:
                pass

    # ---------------------------------------------------------------
    # Transport recycler -- prevents stale HTTP connections
    # ---------------------------------------------------------------

    def _recycle_transports(self):
        for client in (self._info, self._ex1, self._ex2):
            if client and hasattr(client, "transport"):
                try:
                    client.transport.close()
                except Exception:
                    pass
        self._consecutive_http_errors = 0
        self._last_successful_trade_ts = time.monotonic()
        log.info("RECYCLED all HTTP transports (sessions reset)")

    async def _run_transport_recycler(self):
        while self._running:
            try:
                await asyncio.sleep(60.0)
                if not self._running:
                    break
                stale_s = time.monotonic() - self._last_successful_trade_ts
                if self._consecutive_http_errors >= 3:
                    log.warning("Circuit breaker: %d consecutive HTTP errors -- recycling",
                                self._consecutive_http_errors)
                    self._recycle_transports()
                elif stale_s > 120.0 and self._consecutive_http_errors > 0:
                    log.warning("No successful trade for %.0fs with %d errors -- recycling",
                                stale_s, self._consecutive_http_errors)
                    self._recycle_transports()
            except asyncio.CancelledError:
                return
            except Exception as exc:
                log.error("Transport recycler: %s", exc)

    def _track_http_ok(self):
        self._consecutive_http_errors = 0
        self._last_successful_trade_ts = time.monotonic()

    def _track_http_err(self):
        self._consecutive_http_errors += 1

    # ---------------------------------------------------------------
    # CSV
    # ---------------------------------------------------------------

    def _close_csv(self):
        if self._csv_file:
            try:
                self._csv_file.close()
            except Exception:
                pass
            self._csv_file = None
            self._csv_writer = None
            self._csv_date = None

    def _init_csv(self):
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        path = DATA_DIR / f"vb_{date_str}.csv"
        is_new = not path.exists()
        self._csv_file = open(path, "a", newline="")
        self._csv_writer = csv.writer(self._csv_file)
        self._csv_date = date_str
        if is_new:
            self._csv_writer.writerow([
                "timestamp", "event", "account", "instrument", "side", "size", "price",
                "basis_bps", "fair_mid", "hs_mid", "inventory",
                "session_pnl", "total_fills", "total_volume_usd",
            ])
            self._csv_file.flush()
        log.info("CSV: %s", path)

    def _write_event(
        self, event: str, st: AccState, symbol: str,
        side: str, size: float, price: float, basis: float,
    ):
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if self._csv_date != today:
            self._close_csv()
            self._init_csv()
        if not self._csv_writer:
            return
        ictx = self._instruments.get(symbol)
        ps = st.pos(symbol)
        try:
            ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            self._csv_writer.writerow([
                ts, event, st.label, symbol, side,
                f"{size:.6f}" if size > 0 else "",
                f"{price:.6f}" if price > 0 else "",
                f"{basis:.2f}",
                f"{ictx.fair_mid:.6f}" if ictx else "",
                f"{ictx.hs_mid:.6f}" if ictx else "",
                f"{ps.inventory:.6f}",
                f"{st.session_pnl:.4f}", st.total_fills, f"{st.total_volume_usd:.2f}",
            ])
            self._csv_file.flush()
        except Exception as exc:
            log.debug("CSV write: %s", exc)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main():
    cfg = Config.from_env()
    bot = VolumeBot(cfg)
    loop = asyncio.get_running_loop()
    for sig in (os_signal.SIGINT, os_signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(bot.stop()))
    try:
        await bot.start()
    except KeyboardInterrupt:
        pass
    finally:
        await bot.stop()


if __name__ == "__main__":
    asyncio.run(main())
