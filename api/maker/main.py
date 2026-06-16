#!/usr/bin/env python3
"""
Aggressive Market Maker V4 for Hotstuff DEX -- Multi-Symbol
============================================================

Single process handles multiple symbols with shared connections.
Per-symbol state lives in SymbolCtx; shared HotStuff/Binance resources
on AggressiveMM. Orders batched across instruments in one API call.
"""

import asyncio
import csv
import json
import logging
import os
import signal as os_signal
import socket
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiohttp
from dotenv import load_dotenv
from eth_account import Account

from hotstuff import (
    WebSocketTransport,
    InfoClient,
    ExchangeClient,
    SubscriptionClient,
    WebSocketTransportOptions,
    CancelAllParams,
    ApproveBrokerFeeParams,
)
from hotstuff.methods.subscription.channels import (
    FillsSubscriptionParams,
    PositionsSubscriptionParams,
    OrderbookSubscriptionParams,
)

_pkg_dir = Path(__file__).resolve().parent
if str(_pkg_dir) not in sys.path:
    sys.path.insert(0, str(_pkg_dir))

from config import Config
from pricing import MedianOffsetFairPrice, VolTracker
from signals import (
    OBITracker, BarPortionTracker, TradeImbalanceTracker, RegimeDetector,
    CVDTracker, RSITracker, ADXRegimeFilter, SuperTrendFilter, PivotTracker,
)
from position import PositionTracker
from quoter import Quoter, Quote, _fmt, _round_dn, _round_up
from execution import OrderManager
from guard import Guard
from reflect import Reflect

if os.getenv("BOT_STRATEGY_DISABLE_DOTENV", "").lower() not in ("1", "true", "yes"):
    load_dotenv()

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("aggressive_mm")

INST_BINANCE_MAP: Dict[str, str] = {
    "HYPE-PERP": "hypeusdt",
    "BTC-PERP": "btcusdt",
    "ETH-PERP": "ethusdt",
    "SOL-PERP": "solusdt",
    "XRP-PERP": "xrpusdt",
    "ZEC-PERP": "zecusdt",
    "BNB-PERP": "bnbusdt",
    "GOLD-PERP": "xauusdt",
    "SILVER-PERP": "xagusdt",
    "WTIOIL-PERP": "clusdt",
    "BRENTOIL-PERP": "bzusdt",
    "NATGAS-PERP": "natgasusdt",
}

# Reverse map: binance symbol -> hotstuff symbol
_BN_TO_HS: Dict[str, str] = {v: k for k, v in INST_BINANCE_MAP.items()}

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR / "data"
_LOW_EQUITY_USD_THRESHOLD = 10.0
_LOW_EQUITY_MIN_ORDER_SIZE_USD = 12.0
_OPEN_ORDERS_RECONCILE_S = 1.0
_FORCE_CLOSE_IOC_MIN_INTERVAL_S = 0.75
_POST_CLOSE_COOLDOWN_S = 15.0
_STALE_INV_IOC_RAMP_S = 60.0
_ALPHA_ENTRY_GATE_THRESH = 0.4
_STALE_GUARD_BPS = 3.0
_GUARD_HALT_LOG_MIN_INTERVAL_S = float(os.getenv("MM_GUARD_HALT_LOG_MIN_INTERVAL_S", "30.0"))
_HS_OBI_ALPHA_WEIGHT = 0.15
_MAX_HS_SPREAD_BPS = float(os.getenv("MM_MAX_HS_SPREAD_BPS", "10.0"))
_MAX_HS_BN_BASIS_BPS = float(os.getenv("MM_MAX_HS_BN_BASIS_BPS", "10.0"))
_MAX_FAIR_BN_BASIS_BPS = float(os.getenv("MM_MAX_FAIR_BN_BASIS_BPS", "10.0"))
_MAX_L2_ENTRY_BN_DEV_BPS = float(os.getenv("MM_MAX_L2_ENTRY_BN_DEV_BPS", "10.0"))
_EMERGENCY_CLOSE_MAX_DEV_BPS = float(os.getenv("MM_EMERGENCY_CLOSE_MAX_DEV_BPS", "20.0"))
_WS_LIST_KEYS = ("entries", "data", "fills", "positions", "rows", "items")
_WS_OBJECT_KEYS = ("entry", "fill", "position", "payload", "result", "data")


def _sf(obj, key: str, default: float = 0.0) -> float:
    try:
        if isinstance(obj, dict):
            return float(obj.get(key, default))
        return float(getattr(obj, key, default))
    except (TypeError, ValueError):
        return default


def _pos_entry_price(pos: Dict[str, Any]) -> float:
    for key in (
        "entry_price",
        "entryPrice",
        "avg_entry_price",
        "average_entry_price",
        "avgEntryPrice",
        "average_price",
    ):
        val = _sf(pos, key, 0.0)
        if val > 0.0:
            return val
    size = _sf(pos, "size", 0.0)
    pos_val = _sf(pos, "position_value", 0.0)
    if abs(size) > 0.0 and abs(pos_val) > 0.0:
        return abs(pos_val) / abs(size)
    return 0.0


def _clamp_to_ref_bps(value: float, ref: float, max_bps: float) -> float:
    if value <= 0.0 or ref <= 0.0 or max_bps <= 0.0:
        return value
    max_dev = ref * max_bps / 10000.0
    lo = ref - max_dev
    hi = ref + max_dev
    if value < lo:
        return lo
    if value > hi:
        return hi
    return value


def _safe_hs_bbo(best_bid: float, best_ask: float, bn_mid: float) -> tuple[float, float, float]:
    """Return validated HS BBO; zeros mean invalid/outlier."""
    if best_bid <= 0.0 or best_ask <= 0.0:
        return 0.0, 0.0, 0.0
    if best_bid >= best_ask:
        return 0.0, 0.0, 0.0
    mid = (best_bid + best_ask) * 0.5
    if mid <= 0.0:
        return 0.0, 0.0, 0.0
    spread_bps = (best_ask - best_bid) / mid * 10000.0
    if spread_bps > _MAX_HS_SPREAD_BPS:
        return 0.0, 0.0, 0.0
    if bn_mid > 0.0:
        basis_bps = abs(mid - bn_mid) / bn_mid * 10000.0
        if basis_bps > _MAX_HS_BN_BASIS_BPS:
            return 0.0, 0.0, 0.0
    return best_bid, best_ask, mid


def _iter_ws_rows(payload: Any):
    """Yield dict rows from WS/info payloads with varying envelope shapes."""
    data = payload.data if hasattr(payload, "data") else payload

    if isinstance(data, list):
        for row in data:
            if isinstance(row, dict):
                yield row
        return

    cur = data
    for _ in range(4):
        if isinstance(cur, list):
            for row in cur:
                if isinstance(row, dict):
                    yield row
            return
        if not isinstance(cur, dict):
            return

        for key in _WS_LIST_KEYS:
            rows = cur.get(key)
            if isinstance(rows, list):
                for row in rows:
                    if isinstance(row, dict):
                        yield row
                return

        next_obj = None
        for key in _WS_OBJECT_KEYS:
            obj = cur.get(key)
            if isinstance(obj, dict):
                next_obj = obj
                break
        if next_obj is None:
            yield cur
            return
        cur = next_obj

    if isinstance(cur, dict):
        yield cur


# ---------------------------------------------------------------------------
# L2 Orderbook (local copy from WS snapshot + deltas)
# ---------------------------------------------------------------------------

class L2Orderbook:
    __slots__ = ("_bids", "_asks", "_best_bid", "_best_ask", "_mid",
                 "_seq", "_ready", "_on_bbo_change", "_last_update_ts",
                 "_bn_ref")

    def __init__(self, on_bbo_change=None):
        self._bids: Dict[float, float] = {}
        self._asks: Dict[float, float] = {}
        self._best_bid: float = 0.0
        self._best_ask: float = 0.0
        self._mid: float = 0.0
        self._seq: int = 0
        self._ready = False
        self._on_bbo_change = on_bbo_change
        self._last_update_ts: float = 0.0
        self._bn_ref: float = 0.0

    def update_bn_ref(self, bn_mid: float):
        self._bn_ref = bn_mid

    def on_message(self, msg):
        data = msg.data if hasattr(msg, "data") else msg
        if not isinstance(data, dict):
            return
        update_type = data.get("update_type", "")
        books = data.get("books", data)
        if not isinstance(books, dict):
            return
        bids_raw = books.get("bids", [])
        asks_raw = books.get("asks", [])
        seq = books.get("sequence_number", 0)

        ref = self._bn_ref
        max_dev = ref * _MAX_L2_ENTRY_BN_DEV_BPS / 10000.0 if ref > 0.0 else 0.0
        if update_type == "snapshot":
            self._bids.clear()
            self._asks.clear()
            for entry in bids_raw:
                p, s = float(entry["price"]), float(entry["size"])
                if s > 0:
                    if max_dev > 0.0 and abs(p - ref) > max_dev:
                        continue
                    self._bids[p] = s
            for entry in asks_raw:
                p, s = float(entry["price"]), float(entry["size"])
                if s > 0:
                    if max_dev > 0.0 and abs(p - ref) > max_dev:
                        continue
                    self._asks[p] = s
            self._ready = True
        else:
            if seq <= self._seq and self._seq > 0:
                return
            for entry in bids_raw:
                p, s = float(entry["price"]), float(entry["size"])
                if s <= 0:
                    self._bids.pop(p, None)
                elif max_dev > 0.0 and abs(p - ref) > max_dev:
                    self._bids.pop(p, None)
                else:
                    self._bids[p] = s
            for entry in asks_raw:
                p, s = float(entry["price"]), float(entry["size"])
                if s <= 0:
                    self._asks.pop(p, None)
                elif max_dev > 0.0 and abs(p - ref) > max_dev:
                    self._asks.pop(p, None)
                else:
                    self._asks[p] = s

        self._seq = max(self._seq, seq)
        self._last_update_ts = time.monotonic()
        self._recompute_bbo()

    def _recompute_bbo(self):
        old_bid, old_ask = self._best_bid, self._best_ask
        self._best_bid = max(self._bids.keys()) if self._bids else 0.0
        self._best_ask = min(self._asks.keys()) if self._asks else 0.0
        if self._best_bid > 0 and self._best_ask > 0 and self._best_bid < self._best_ask:
            self._mid = (self._best_bid + self._best_ask) * 0.5
        else:
            self._mid = 0.0
        if self._on_bbo_change and (self._best_bid != old_bid or self._best_ask != old_ask):
            self._on_bbo_change()

    @property
    def best_bid(self) -> float: return self._best_bid
    @property
    def best_ask(self) -> float: return self._best_ask
    @property
    def mid(self) -> float: return self._mid
    @property
    def ready(self) -> bool: return self._ready

    def age_s(self) -> float:
        if self._last_update_ts <= 0.0:
            return 999.0
        return time.monotonic() - self._last_update_ts

    def top_obi(self, depth: int = 5) -> float:
        if not self._bids or not self._asks:
            return 0.0
        bid_qty = 0.0
        n = 0
        for p in sorted(self._bids, reverse=True):
            bid_qty += self._bids[p]
            n += 1
            if n >= depth:
                break
        ask_qty = 0.0
        n = 0
        for p in sorted(self._asks):
            ask_qty += self._asks[p]
            n += 1
            if n >= depth:
                break
        total = bid_qty + ask_qty
        if total <= 0.0:
            return 0.0
        return (bid_qty - ask_qty) / total


# ---------------------------------------------------------------------------
# SymbolCtx -- per-symbol state
# ---------------------------------------------------------------------------

class SymbolCtx:
    """Holds all state for a single traded symbol."""

    __slots__ = (
        "cfg", "symbol", "bn_symbol", "instrument_id", "tick", "lot", "min_notional",
        "fair", "vol", "bn_mid", "auto_order_size",
        "obi", "bp", "timb", "cvd", "rsi_div", "adx_regime", "supertrend",
        "pivots", "pivot_day", "regime", "book",
        "pos", "quoter", "guard", "reflect",
        "last_alpha", "last_spread_bps", "last_quote_mode", "last_toxic", "last_guard_spread_mult",
        "last_force_close_ioc_ts",
        "last_quoted_fair", "last_quote_ts", "close_mode_entered_ts",
        "last_flat_ts",
        "last_guard_halt_reason", "last_guard_halt_log_ts",
        "csv_file", "csv_writer", "csv_date",
    )

    def __init__(self, cfg: Config, on_bbo_change):
        self.cfg = cfg
        self.symbol = cfg.symbol
        self.bn_symbol = INST_BINANCE_MAP.get(cfg.symbol, "")
        self.instrument_id = 0
        self.tick = 0.001
        self.lot = 0.01
        self.min_notional = 10.0

        self.fair = MedianOffsetFairPrice(
            window_s=cfg.fair_price_window_s,
            min_samples=int(cfg.fair_price_warmup_s),
        )
        self.vol = VolTracker(cfg.vol_window_s)
        self.bn_mid: float = 0.0
        self.auto_order_size: float = 0.0

        self.obi = OBITracker(cfg.signal_ema_span, cfg.obi_extreme)
        self.bp = BarPortionTracker(cfg.bp_lookback)
        self.timb = TradeImbalanceTracker(cfg.trade_imb_window_s, cfg.signal_ema_span)
        self.cvd = CVDTracker(cfg.cvd_lookback)
        self.rsi_div = RSITracker(cfg.rsi_period, cfg.rsi_div_lookback)
        self.adx_regime = ADXRegimeFilter(cfg.adx_period)
        self.supertrend = SuperTrendFilter(cfg.supertrend_atr_period, cfg.supertrend_multiplier)
        self.pivots = PivotTracker()
        self.pivot_day: int = 0
        self.regime = RegimeDetector(cfg.obi_flip_threshold, cfg.vol_spike_mult)
        self.book = L2Orderbook(on_bbo_change=on_bbo_change)

        self.pos: Optional[PositionTracker] = None
        self.quoter: Optional[Quoter] = None
        self.guard: Optional[Guard] = None
        self.reflect: Optional[Reflect] = None

        self.last_alpha: float = 0.0
        self.last_spread_bps: float = 0.0
        self.last_quote_mode: str = "none"
        self.last_toxic: float = 0.0
        self.last_guard_spread_mult: float = 1.0
        self.last_force_close_ioc_ts: float = 0.0
        self.last_quoted_fair: float = 0.0
        self.last_quote_ts: float = 0.0
        self.close_mode_entered_ts: float = 0.0
        self.last_flat_ts: float = 0.0
        self.last_guard_halt_reason: str = ""
        self.last_guard_halt_log_ts: float = 0.0

        self.csv_file = None
        self.csv_writer = None
        self.csv_date: Optional[str] = None

    def init_components(self):
        cfg = self.cfg
        self.pos = PositionTracker(
            close_threshold_usd=cfg.close_threshold_usd,
            max_inventory=cfg.max_inventory,
            lot_size=self.lot,
            skew_start_pct=cfg.inv_skew_start_pct,
            skip_open_pct=cfg.inv_skip_open_pct,
        )
        self.quoter = Quoter(self.tick, self.lot, self.min_notional)
        self.guard = Guard(
            max_session_loss_usd=cfg.guard_max_session_loss_usd,
            max_drawdown_pct=cfg.guard_max_drawdown_pct,
            cooldown_after_loss_s=cfg.guard_cooldown_s,
            loss_streak_trigger=cfg.guard_loss_streak_trigger,
            adverse_rate_threshold=cfg.guard_adverse_rate_threshold,
            adverse_rate_widen=cfg.guard_adverse_rate_widen,
            inventory_decay_s=cfg.guard_inventory_decay_s,
            inventory_stale_mult=cfg.guard_inventory_stale_mult,
            adverse_alpha_threshold=cfg.guard_adverse_alpha_threshold,
            lot_size=self.lot,
        )
        self.reflect = Reflect(max_fills=500)


# ---------------------------------------------------------------------------
# AggressiveMM Bot (multi-symbol)
# ---------------------------------------------------------------------------

class AggressiveMM:

    def __init__(self, configs: Dict[str, Config], session_id: Optional[str] = None,
                 relay_port: Optional[int] = None):
        self._session_id = session_id
        self._relay_port = relay_port
        self._running = False
        self._subs: List[Any] = []
        self._requote_event = asyncio.Event()
        self._last_throttle_ts: float = 0.0
        self._force_requote: bool = False

        self._symbols: Dict[str, SymbolCtx] = {}
        self._bn_to_ctx: Dict[str, SymbolCtx] = {}
        self._inst_to_ctx: Dict[int, SymbolCtx] = {}

        for symbol, cfg in configs.items():
            bn_sym = INST_BINANCE_MAP.get(symbol)
            ctx = SymbolCtx(cfg, on_bbo_change=None)
            self._symbols[symbol] = ctx
            if bn_sym:
                self._bn_to_ctx[bn_sym] = ctx
            else:
                log.warning("No Binance mapping for %s, using HotStuff-only fair/mid fallback", symbol)

        self._account_equity: float = 0.0

        self._ws: Optional[WebSocketTransport] = None
        self._info: Optional[InfoClient] = None
        self._exchange: Optional[ExchangeClient] = None
        self._sub_client: Optional[SubscriptionClient] = None
        self._orders: Optional[OrderManager] = None
        self._bn_session: Optional[aiohttp.ClientSession] = None
        self._tasks: List[asyncio.Task] = []
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._cancel_all_task: Optional[asyncio.Task] = None

    # -------------------------------------------------------------------
    # Lifecycle
    # -------------------------------------------------------------------

    async def start(self):
        self._running = True
        self._loop = asyncio.get_running_loop()
        first_cfg = next(iter(self._symbols.values())).cfg
        sym_names = list(self._symbols.keys())

        log.info("=== Aggressive MM V4 starting (%d symbols) ===", len(sym_names))
        log.info("Symbols        : %s", ", ".join(sym_names))
        log.info("Mode           : %s", "LIVE" if first_cfg.enable_trading else "DRY-RUN")
        log.info(
            "Cadence        : interval=%.0fms throttle=%.0fms trigger=%.3fbps reconcile=%.0fms",
            first_cfg.requote_interval_s * 1000.0,
            first_cfg.update_throttle_ms,
            first_cfg.requote_threshold_bps,
            _OPEN_ORDERS_RECONCILE_S * 1000.0,
        )

        for sym, ctx in self._symbols.items():
            cfg = ctx.cfg
            bias_str = "neutral" if cfg.market_bias == 0.0 else ("LONG %.1f" % cfg.market_bias if cfg.market_bias > 0 else "SHORT %.1f" % cfg.market_bias)
            log.info("[%s] spread=%.1fbps x%.1f levels=%d spacing=%.1fbps max_inv=$%.0f bias=%s",
                     sym, cfg.min_spread_bps, cfg.spread_vol_mult,
                     cfg.levels, cfg.level_spacing_bps, cfg.max_inventory, bias_str)

        wallet = Account.from_key(first_cfg.private_key)
        self._info = InfoClient(is_testnet=False)
        self._exchange = ExchangeClient(wallet=wallet, is_testnet=False)

        ws_server = {"mainnet": "wss://api.hotstuff.trade/ws/"}
        self._ws = WebSocketTransport(WebSocketTransportOptions(
            is_testnet=False, timeout=15.0,
            keep_alive={"interval": 20.0, "timeout": 10.0},
            auto_connect=True, server=ws_server,
        ))
        self._sub_client = SubscriptionClient(transport=self._ws)

        await self._resolve_instruments()
        await self._sync_account()

        if first_cfg.broker_address and first_cfg.enable_trading:
            await self._ensure_broker_approved(first_cfg)

        for ctx in self._symbols.values():
            ctx.init_components()

        tick_map = {ctx.instrument_id: ctx.tick for ctx in self._symbols.values()}
        lot_map = {ctx.instrument_id: ctx.lot for ctx in self._symbols.values()}
        self._orders = OrderManager(
            self._exchange, tick_map, lot_map,
            first_cfg.post_only, first_cfg.order_expiry_ms,
            broker_address=first_cfg.broker_address, broker_fee=first_cfg.broker_fee,
        )

        await self._sync_positions()

        self._bn_session = aiohttp.ClientSession()
        for ctx in self._symbols.values():
            await self._load_daily_pivots(ctx)

        if self._relay_port:
            self._tasks.append(asyncio.create_task(self._run_relay_receiver()))
            log.info("Using relay on UDP port %d", self._relay_port)
        else:
            self._tasks.append(asyncio.create_task(self._run_binance_ws()))

        await self._subscribe_hotstuff()

        if first_cfg.enable_trading:
            await self._orders.cancel_all()

        for ctx in self._symbols.values():
            self._init_csv(ctx)

        self._tasks.append(asyncio.create_task(self._run_position_poll()))
        if first_cfg.enable_trading:
            self._tasks.append(asyncio.create_task(self._run_open_orders_reconcile()))
        self._tasks.append(asyncio.create_task(self._run_stats_logger()))
        if self._session_id:
            self._tasks.append(asyncio.create_task(self._run_metrics_emitter()))

        log.info("=== MM V4 ready (%d symbols) ===", len(self._symbols))

        self._tasks.append(asyncio.create_task(self._run_loop()))

        try:
            await asyncio.gather(*self._tasks)
        except asyncio.CancelledError:
            pass

    async def stop(self):
        log.info("Shutting down...")
        self._running = False
        self._requote_event.set()

        first_cfg = next(iter(self._symbols.values())).cfg if self._symbols else None
        if first_cfg and first_cfg.enable_trading and self._orders:
            try:
                await self._orders.cancel_all()
            except Exception as exc:
                log.error("Cancel on shutdown: %s", exc)

        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            try:
                await t
            except asyncio.CancelledError:
                pass

        if self._cancel_all_task and not self._cancel_all_task.done():
            self._cancel_all_task.cancel()
            try:
                await self._cancel_all_task
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

        for ctx in self._symbols.values():
            self._close_csv(ctx)
            if ctx.pos:
                st = ctx.pos.state
                log.info("[%s] pnl=$%.4f fills=%d vol=$%.2f rt=%d",
                         ctx.symbol, st.session_pnl, st.total_fills,
                         st.total_volume_usd, st.round_trips)

        log.info("=== Shutdown complete ===")

    # -------------------------------------------------------------------
    # Init helpers
    # -------------------------------------------------------------------

    async def _resolve_instruments(self):
        raw = await asyncio.to_thread(
            self._info.transport.request,
            "info", {"method": "instruments", "params": {"type": "perps"}},
        )
        perps = raw.get("perps", []) if isinstance(raw, dict) else []
        resolved = 0
        for p in perps:
            name = p.get("name", "")
            ctx = self._symbols.get(name)
            if ctx is None:
                continue
            ctx.instrument_id = int(p["id"])
            ctx.tick = float(p.get("tick_size", 0.001))
            ctx.lot = float(p.get("lot_size", 0.01))
            ctx.min_notional = float(p.get("min_notional_usd", 10))
            self._inst_to_ctx[ctx.instrument_id] = ctx
            log.info("Instrument: %s id=%d tick=%s lot=%s",
                     name, ctx.instrument_id,
                     _fmt(ctx.tick, 0.000001), _fmt(ctx.lot, 0.000001))
            resolved += 1

        missing = [s for s, c in self._symbols.items() if c.instrument_id == 0]
        if missing:
            log.error("Instruments not found: %s", missing)
            sys.exit(1)

    def _recompute_auto_order_sizes(self, emit_logs: bool = True):
        n_symbols = len(self._symbols)
        if n_symbols <= 0:
            return
        for ctx in self._symbols.values():
            cfg = ctx.cfg
            if cfg.order_size_usd > 0:
                continue
            if self._account_equity <= 0.0:
                ctx.auto_order_size = 0.0
                continue

            scale_sum = sum(cfg.level_size_scale ** i for i in range(cfg.levels))
            per_symbol_equity = self._account_equity / n_symbols
            auto_size = (per_symbol_equity * cfg.target_exposure_x) / (2.0 * scale_sum)
            if self._account_equity < _LOW_EQUITY_USD_THRESHOLD:
                auto_size = max(auto_size, _LOW_EQUITY_MIN_ORDER_SIZE_USD)

            prev = ctx.auto_order_size
            ctx.auto_order_size = auto_size
            if emit_logs and abs(prev - auto_size) >= 0.01:
                if self._account_equity < _LOW_EQUITY_USD_THRESHOLD:
                    log.info(
                        "[%s] Low-equity min order size active: $%.2f/level (eq=$%.2f < $%.2f)",
                        ctx.symbol,
                        ctx.auto_order_size,
                        self._account_equity,
                        _LOW_EQUITY_USD_THRESHOLD,
                    )
                else:
                    log.info(
                        "[%s] Auto order size: $%.2f/level (eq_share=$%.2f, %.1fx, %d lvl)",
                        ctx.symbol,
                        ctx.auto_order_size,
                        per_symbol_equity,
                        cfg.target_exposure_x,
                        cfg.levels,
                    )

    async def _sync_account(self):
        first_cfg = next(iter(self._symbols.values())).cfg
        try:
            raw = await asyncio.to_thread(
                self._info.transport.request,
                "info",
                {"method": "accountSummary", "params": {"user": first_cfg.account_address}},
            )
            eq = float(raw.get("total_account_equity", 0))
            if eq <= 0.0:
                eq = float(raw.get("available_balance", 0))
            self._account_equity = eq
            log.info("Account equity: $%.2f", eq)
        except Exception as exc:
            log.warning("Equity sync: %s", exc)

        self._recompute_auto_order_sizes(emit_logs=True)

    async def _ensure_broker_approved(self, cfg: Config):
        try:
            resp = await asyncio.to_thread(
                self._info.transport.request,
                "info",
                {"method": "brokersCheck", "params": {
                    "user": cfg.account_address,
                    "broker": cfg.broker_address,
                }},
            )
            records = resp.get("data", []) if isinstance(resp, dict) else resp
            if records:
                log.info("Broker %s already approved", cfg.broker_address[:10])
                return
        except Exception as exc:
            log.warning("Broker check failed: %s -- will try approval", exc)

        try:
            result = await asyncio.to_thread(
                self._exchange.approve_broker_fee,
                ApproveBrokerFeeParams(
                    broker=cfg.broker_address,
                    maxFeeRate=cfg.broker_fee,
                ),
            )
            log.info("Broker approved: %s (fee=%s) tx=%s",
                     cfg.broker_address[:10], cfg.broker_fee,
                     result.get("tx_hash", "?"))
        except Exception as exc:
            log.error("Broker approval failed: %s", exc)

    async def _sync_positions(self):
        first_cfg = next(iter(self._symbols.values())).cfg
        try:
            raw = await asyncio.to_thread(
                self._info.transport.request,
                "info",
                {"method": "positions", "params": {"user": first_cfg.account_address}},
            )
            per_ctx: Dict[str, Dict[str, float]] = {}
            for pos in _iter_ws_rows(raw):
                ctx = self._ctx_from_row(pos)
                if ctx is None:
                    continue
                legs = pos.get("legs")
                if legs and isinstance(legs, list):
                    net = 0.0
                    for leg in legs:
                        if isinstance(leg, dict):
                            net += _sf(leg, "size", 0.0)
                else:
                    net = _sf(pos, "size", 0.0)
                entry = _pos_entry_price(pos)
                bucket = per_ctx.setdefault(ctx.symbol, {"net": 0.0, "entry_notional": 0.0, "entry_abs": 0.0})
                bucket["net"] += net
                if entry > 0.0 and abs(net) > 0.0:
                    sz_abs = abs(net)
                    bucket["entry_notional"] += entry * sz_abs
                    bucket["entry_abs"] += sz_abs

            for ctx in self._symbols.values():
                info = per_ctx.get(ctx.symbol)
                if not info:
                    prev_inv = ctx.pos.inventory
                    ctx.pos.reconcile(0.0)
                    self._on_inventory_reconcile(ctx, prev_inv, source="startup-sync")
                    self._apply_position_entry(ctx, 0.0, 0.0)
                    continue
                net = info["net"]
                entry = (info["entry_notional"] / info["entry_abs"]) if info["entry_abs"] > 0.0 else 0.0
                prev_inv = ctx.pos.inventory
                ctx.pos.reconcile(net)
                self._on_inventory_reconcile(ctx, prev_inv, source="startup-sync")
                self._apply_position_entry(ctx, net, entry)
                if abs(net) > 0:
                    log.info("[%s] Initial inventory: %.4f @ %.4f", ctx.symbol, net, entry)
        except Exception as exc:
            log.warning("Position sync: %s", exc)

    def _apply_position_entry(self, ctx: SymbolCtx, net: float, entry: float):
        st = ctx.pos.state
        if abs(net) <= ctx.lot:
            st.entry_size = 0.0
            st.entry_cost = 0.0
            st.avg_entry = 0.0
            return
        if entry > 0.0:
            abs_sz = abs(net)
            st.entry_size = abs_sz
            st.entry_cost = entry * abs_sz
            st.avg_entry = entry

    def _ctx_from_row(self, row: Dict[str, Any]) -> Optional[SymbolCtx]:
        inst_id_raw = row.get("instrument_id", row.get("instrumentId", 0))
        try:
            inst_id = int(float(inst_id_raw or 0))
        except (TypeError, ValueError):
            inst_id = 0
        if inst_id > 0:
            ctx = self._inst_to_ctx.get(inst_id)
            if ctx is not None:
                return ctx

        inst = row.get("instrument", row.get("symbol", "")) or row.get("instrument_name", "")
        if not inst:
            return None
        inst_str = str(inst)
        ctx = self._symbols.get(inst_str)
        if ctx is not None:
            return ctx
        for c in self._symbols.values():
            if c.symbol in inst_str:
                return c
        return None

    def _on_inventory_reconcile(self, ctx: SymbolCtx, prev_inv: float, source: str):
        if not ctx.pos or not ctx.guard:
            return
        now_inv = ctx.pos.inventory
        if abs(now_inv - prev_inv) <= ctx.lot:
            return

        now = time.monotonic()
        if abs(now_inv) <= ctx.lot:
            ctx.last_flat_ts = now
            return

        if ctx.guard.state.last_fill_ts <= 0.0:
            log.info(
                "[%s] Guard inventory timer seeded from %s (inv=%.4f)",
                ctx.symbol, source, now_inv,
            )
        ctx.guard.state.last_fill_ts = now

    # -------------------------------------------------------------------
    # Subscriptions
    # -------------------------------------------------------------------

    async def _subscribe_hotstuff(self):
        first_cfg = next(iter(self._symbols.values())).cfg
        addr = first_cfg.account_address

        if not self._relay_port:
            for ctx in self._symbols.values():
                sub = await asyncio.to_thread(
                    self._sub_client.orderbook,
                    OrderbookSubscriptionParams(instrument_id=ctx.symbol),
                    ctx.book.on_message,
                )
                self._subs.append(sub)
                log.info("Sub: L2 orderbook for %s", ctx.symbol)

        sub = await asyncio.to_thread(
            self._sub_client.fills,
            FillsSubscriptionParams(user=addr),
            self._on_fill,
        )
        self._subs.append(sub)
        log.info("Sub: fills (account-wide)")

        sub = await asyncio.to_thread(
            self._sub_client.positions,
            PositionsSubscriptionParams(user=addr),
            self._on_position,
        )
        self._subs.append(sub)
        log.info("Sub: positions (account-wide)")

    def _schedule_cancel_all(self):
        if not self._orders:
            return

        async def _run():
            try:
                await self._orders.cancel_all()
            except Exception as exc:
                log.warning("cancel_all task: %s", exc)

        def _start():
            if self._cancel_all_task and not self._cancel_all_task.done():
                return
            self._cancel_all_task = asyncio.create_task(_run())

        if self._loop and self._loop.is_running():
            self._loop.call_soon_threadsafe(_start)
        else:
            _start()

    def _schedule_force_close_ioc(
        self,
        ctx: SymbolCtx,
        fair: float,
        close_size: float,
        reason: str,
        close_mult: float,
    ):
        if not self._orders or not ctx.pos:
            return
        if fair <= 0.0:
            return
        now = time.monotonic()
        if now - ctx.last_force_close_ioc_ts < _FORCE_CLOSE_IOC_MIN_INTERVAL_S:
            return
        if abs(ctx.pos.inventory) <= ctx.lot:
            return
        size = _round_dn(close_size, ctx.lot)
        if size <= 0.0:
            return
        max_dev_bps = None
        has_hl_ref_only = not bool(ctx.bn_symbol)
        if has_hl_ref_only:
            st = ctx.pos.state
            if st.avg_entry > 0.0 and abs(st.size_base) > 0.0:
                unrealized_pnl = (fair - st.avg_entry) * st.size_base
                if unrealized_pnl < 0.0:
                    max_dev_bps = _EMERGENCY_CLOSE_MAX_DEV_BPS

        mult = max(0.05, min(1.0, close_mult))
        close_bps = max(0.1, ctx.cfg.close_spread_bps)
        cross = max(ctx.tick, fair * close_bps / 10000.0 * mult)
        hs_bid, hs_ask, _ = _safe_hs_bbo(ctx.book.best_bid, ctx.book.best_ask, ctx.bn_mid)
        if ctx.pos.inventory > 0.0:
            side = "s"
            anchor = hs_bid if hs_bid > 0.0 else fair
            price = _round_dn(max(ctx.tick, anchor - cross), ctx.tick)
        else:
            side = "b"
            anchor = hs_ask if hs_ask > 0.0 else fair
            price = _round_up(anchor + cross, ctx.tick)
        if ctx.bn_mid > 0.0:
            bounded = _clamp_to_ref_bps(price, ctx.bn_mid, _MAX_FAIR_BN_BASIS_BPS)
            if side == "s":
                price = _round_dn(max(ctx.tick, bounded), ctx.tick)
            else:
                price = _round_up(max(ctx.tick, bounded), ctx.tick)
        if price <= 0.0:
            return

        ctx.last_force_close_ioc_ts = now

        async def _run():
            try:
                await self._orders.force_close_ioc(
                    instrument_id=ctx.instrument_id,
                    side=side,
                    price=price,
                    size=size,
                    note=reason,
                    max_dev_bps=max_dev_bps,
                )
            except Exception as exc:
                log.warning("[%s] force_close_ioc task: %s", ctx.symbol, exc)

        def _start():
            asyncio.create_task(_run())

        if self._loop and self._loop.is_running():
            self._loop.call_soon_threadsafe(_start)
        else:
            _start()

    def _on_fill(self, msg):
        if self._loop and self._loop.is_running():
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                self._loop.call_soon_threadsafe(self._on_fill, msg)
                return

        for f in _iter_ws_rows(msg):
            ctx = self._ctx_from_row(f)
            if ctx is None:
                continue

            side = str(f.get("side", "")).upper()
            price = _sf(f, "price", 0.0)
            size = _sf(f, "size", 0.0)
            if price <= 0 or size <= 0:
                continue
            is_buy = side in ("BUY", "B", "LONG")
            had_inv = abs(ctx.pos.inventory) > ctx.lot
            rt_pnl = ctx.pos.apply_fill(is_buy, size, price)
            if had_inv and abs(ctx.pos.inventory) <= ctx.lot:
                ctx.last_flat_ts = time.monotonic()

            ctx.guard.on_fill()
            if rt_pnl != 0.0:
                ctx.guard.on_round_trip(rt_pnl)

            fill_side = "b" if is_buy else "s"
            mid = ctx.book.mid if ctx.book.mid > 0.0 else ctx.bn_mid
            ctx.reflect.record_fill(fill_side, price, size, mid)

            log.info("[%s] FILL: %s %.4f @ %s | inv=%.4f pnl=$%.4f",
                     ctx.symbol, side, size, _fmt(price, ctx.tick),
                     ctx.pos.inventory, ctx.pos.state.session_pnl)

            if ctx.pos.is_close_mode and self._orders:
                self._schedule_cancel_all()
            self._requote_event.set()

    def _on_position(self, msg):
        if self._loop and self._loop.is_running():
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                self._loop.call_soon_threadsafe(self._on_position, msg)
                return

        per_ctx: Dict[str, Dict[str, float]] = {}
        for pos in _iter_ws_rows(msg):
            ctx = self._ctx_from_row(pos)
            if ctx is None:
                continue
            legs = pos.get("legs")
            if legs and isinstance(legs, list):
                sz = 0.0
                for leg in legs:
                    if isinstance(leg, dict):
                        sz += _sf(leg, "size", 0.0)
            else:
                sz = _sf(pos, "size", 0.0)
            entry = _pos_entry_price(pos)
            bucket = per_ctx.setdefault(ctx.symbol, {"net": 0.0, "entry_notional": 0.0, "entry_abs": 0.0})
            bucket["net"] += sz
            if entry > 0.0 and abs(sz) > 0.0:
                sz_abs = abs(sz)
                bucket["entry_notional"] += entry * sz_abs
                bucket["entry_abs"] += sz_abs

        for sym, info in per_ctx.items():
            ctx = self._symbols.get(sym)
            if ctx and ctx.pos:
                net = info["net"]
                prev_inv = ctx.pos.inventory
                ctx.pos.reconcile(net)
                self._on_inventory_reconcile(ctx, prev_inv, source="ws-position")
                entry = (info["entry_notional"] / info["entry_abs"]) if info["entry_abs"] > 0.0 else 0.0
                self._apply_position_entry(ctx, net, entry)

    # -------------------------------------------------------------------
    # Binance REST (daily pivots on startup)
    # -------------------------------------------------------------------

    async def _load_daily_pivots(self, ctx: SymbolCtx):
        if not ctx.cfg.pivot_enabled:
            return
        if not ctx.bn_symbol:
            return
        try:
            url = "https://fapi.binance.com/fapi/v1/klines"
            params = {"symbol": ctx.bn_symbol.upper(), "interval": "1d", "limit": 2}
            async with self._bn_session.get(url, params=params) as resp:
                data = await resp.json()
                if len(data) >= 2:
                    prev = data[0]
                    ctx.pivots.set_previous_day(
                        float(prev[2]), float(prev[3]), float(prev[4])
                    )
                    log.info("[%s] Pivots loaded", ctx.symbol)
        except Exception as exc:
            log.warning("[%s] Pivot load failed: %s", ctx.symbol, exc)

    # -------------------------------------------------------------------
    # Binance WS (all symbols, single connection)
    # -------------------------------------------------------------------

    async def _run_binance_ws(self):
        stream_parts = []
        for ctx in self._symbols.values():
            bs = ctx.bn_symbol
            if not bs:
                continue
            stream_parts.extend([
                f"{bs}@bookTicker",
                f"{bs}@aggTrade",
                f"{bs}@kline_1m",
            ])
        if not stream_parts:
            log.info("Binance WS skipped: no mapped symbols in this session")
            return
        streams = "/".join(stream_parts)
        url = f"wss://fstream.binance.com/stream?streams={streams}"

        while self._running:
            try:
                async with self._bn_session.ws_connect(url, heartbeat=20) as ws:
                    log.info("Binance WS connected (%d streams)", len(stream_parts))
                    async for msg in ws:
                        if not self._running:
                            break
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                payload = json.loads(msg.data)
                                stream = payload.get("stream", "")
                                data = payload.get("data", {})
                                at_idx = stream.find("@")
                                if at_idx <= 0:
                                    continue
                                bn_sym = stream[:at_idx]
                                ctx = self._bn_to_ctx.get(bn_sym)
                                if ctx is None:
                                    continue
                                if "bookTicker" in stream:
                                    self._on_bn_book(ctx, data)
                                elif "aggTrade" in stream:
                                    self._on_bn_trade(ctx, data)
                                elif "kline" in stream:
                                    self._on_bn_kline(ctx, data)
                            except (json.JSONDecodeError, KeyError, ValueError):
                                pass
                        elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                            break
            except asyncio.CancelledError:
                return
            except Exception as exc:
                log.warning("Binance WS error: %s -- reconnect 3s", exc)
                await asyncio.sleep(3.0)

    def _on_bn_book(self, ctx: SymbolCtx, data: dict):
        bid = float(data.get("b", 0))
        ask = float(data.get("a", 0))
        bid_qty = float(data.get("B", 0))
        ask_qty = float(data.get("A", 0))
        if bid <= 0.0 or ask <= 0.0:
            return

        now = time.monotonic()
        mid = (bid + ask) * 0.5
        prev = ctx.bn_mid
        ctx.bn_mid = mid
        ctx.book.update_bn_ref(mid)
        if self._orders:
            self._orders.update_ref_price(ctx.instrument_id, mid)

        ctx.vol.update(mid, now)
        ctx.obi.update(bid_qty, ask_qty)

        _, _, hs_mid = _safe_hs_bbo(ctx.book.best_bid, ctx.book.best_ask, mid)
        if hs_mid > 0.0:
            ctx.fair.add_sample(hs_mid, mid)

        if ctx.reflect and mid > 0.0:
            ctx.reflect.tick(mid)

        anchor = ctx.last_quoted_fair
        if anchor > 0.0:
            move_bps = abs(mid - anchor) / anchor * 10000.0
            if move_bps >= _STALE_GUARD_BPS:
                if self._orders and ctx.cfg.enable_trading:
                    self._orders.stage_quotes(ctx.instrument_id, [])
                self._force_requote = True
                self._requote_event.set()
            elif move_bps >= ctx.cfg.requote_threshold_bps:
                self._requote_event.set()
        elif prev > 0.0:
            move_bps = abs(mid - prev) / prev * 10000.0
            if move_bps >= ctx.cfg.requote_threshold_bps:
                self._requote_event.set()

    def _on_bn_trade(self, ctx: SymbolCtx, data: dict):
        price = float(data.get("p", 0))
        qty = float(data.get("q", 0))
        is_sell = data.get("m", False)
        if price > 0.0 and qty > 0.0:
            ctx.timb.on_trade(qty, price, is_sell, time.monotonic())
            ctx.cvd.on_trade(qty, price, is_sell)

    def _on_bn_kline(self, ctx: SymbolCtx, data: dict):
        k = data.get("k", {})
        o = float(k.get("o", 0))
        h = float(k.get("h", 0))
        low = float(k.get("l", 0))
        c = float(k.get("c", 0))
        closed = k.get("x", False)
        if o > 0.0:
            ctx.bp.on_kline(o, h, low, c, closed)
            if closed and c > 0.0:
                kline_open_ms = k.get("t", 0)
                if kline_open_ms > 0:
                    kday = datetime.fromtimestamp(kline_open_ms / 1000, tz=timezone.utc).day
                    if ctx.pivot_day != 0 and kday != ctx.pivot_day:
                        ctx.pivots.on_day_boundary()
                    ctx.pivot_day = kday
                ctx.cvd.on_candle_close(c)
                ctx.rsi_div.on_candle_close(c)
                ctx.adx_regime.on_candle_close(o, h, low, c)
                ctx.supertrend.on_candle_close(o, h, low, c)
                ctx.pivots.on_candle_close(o, h, low, c)

    # -------------------------------------------------------------------
    # Relay receiver (routes by "s" field to correct SymbolCtx)
    # -------------------------------------------------------------------

    async def _run_relay_receiver(self):
        loop = asyncio.get_running_loop()
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setblocking(False)
        sock.bind(("127.0.0.1", self._relay_port))
        log.info("Relay receiver bound to 127.0.0.1:%d", self._relay_port)

        try:
            while self._running:
                try:
                    raw = await loop.sock_recv(sock, 65536)
                except asyncio.CancelledError:
                    return
                if not raw:
                    continue
                try:
                    msg = json.loads(raw)
                except (json.JSONDecodeError, ValueError):
                    continue

                sym = msg.get("s", "")
                ctx = self._symbols.get(sym)
                if ctx is None:
                    continue

                t = msg.get("t")
                if t == "bn_book":
                    self._on_bn_book(ctx, msg)
                elif t == "bn_trade":
                    self._on_bn_trade(ctx, msg)
                elif t == "bn_kline":
                    self._on_bn_kline(ctx, msg)
                elif t == "hs_book":
                    try:
                        ctx.book.on_message(msg.get("data", msg))
                    except Exception as exc:
                        log.warning("[%s] hs_book parse failed: %s", ctx.symbol, exc)
        finally:
            sock.close()

    # -------------------------------------------------------------------
    # Main loop
    # -------------------------------------------------------------------

    _MIN_QUOTE_HOLD_S = 0.8
    _MIN_CLOSE_HOLD_S = 1.5

    async def _run_loop(self):
        first_cfg = next(iter(self._symbols.values())).cfg
        while self._running:
            loop_timeout = first_cfg.requote_interval_s
            self._requote_event.clear()
            try:
                await asyncio.wait_for(
                    self._requote_event.wait(),
                    timeout=loop_timeout,
                )
            except asyncio.TimeoutError:
                pass

            if not self._running:
                break

            now = time.monotonic()
            elapsed_ms = (now - self._last_throttle_ts) * 1000.0
            throttle_ms = first_cfg.update_throttle_ms
            if elapsed_ms < throttle_ms and not self._force_requote:
                continue
            self._force_requote = False
            self._last_throttle_ts = now

            any_staged = False
            for ctx in self._symbols.values():
                if self._do_requote(ctx):
                    any_staged = True

            if any_staged and first_cfg.enable_trading and self._orders:
                await self._orders.flush()

    # -------------------------------------------------------------------
    # Core requote (per-symbol, returns True if quotes were staged)
    # -------------------------------------------------------------------

    def _do_requote(self, ctx: SymbolCtx) -> bool:
        cfg = ctx.cfg
        if ctx.bn_mid <= 0.0:
            if cfg.enable_trading and self._orders:
                self._orders.stage_quotes(ctx.instrument_id, [])
            return True
        hs_bid_safe, hs_ask_safe, hs_mid_safe = _safe_hs_bbo(
            ctx.book.best_bid,
            ctx.book.best_ask,
            ctx.bn_mid,
        )
        fair = ctx.fair.get_fair_price(ctx.bn_mid)
        if fair is None:
            if cfg.enable_trading and self._orders:
                self._orders.stage_quotes(ctx.instrument_id, [])
            return True
        fair = _clamp_to_ref_bps(fair, ctx.bn_mid, _MAX_FAIR_BN_BASIS_BPS)

        ctx.pos.update_fair_price(fair)
        now = time.monotonic()

        anchor = ctx.last_quoted_fair
        if anchor > 0.0:
            move_from_anchor = abs(fair - anchor) / anchor * 10000.0
            is_close_now = ctx.pos.is_close_mode
            min_hold = self._MIN_CLOSE_HOLD_S if is_close_now else self._MIN_QUOTE_HOLD_S
            age = now - ctx.last_quote_ts
            if age < min_hold and move_from_anchor < cfg.requote_threshold_bps:
                return False

        alpha_shift = 0.0
        alpha = 0.0
        if cfg.use_alpha:
            sig_obi = ctx.obi.contrarian_signal
            sig_bp = ctx.bp.signal
            sig_timb = ctx.timb.signal
            sig_cvd = ctx.cvd.signal
            sig_rsi = ctx.rsi_div.signal
            alpha = (
                cfg.w_contrarian_obi * sig_obi
                + cfg.w_bar_portion * sig_bp
                + cfg.w_trade_imbalance * sig_timb
                + cfg.w_cvd_divergence * sig_cvd
                + cfg.w_rsi_divergence * sig_rsi
            )
            alpha = max(-1.0, min(1.0, alpha))
            if cfg.supertrend_enabled:
                alpha = ctx.supertrend.confirm_alpha(alpha)
                alpha = max(-1.0, min(1.0, alpha))
            if ctx.book.ready and ctx.book.age_s() < 30.0 and hs_mid_safe > 0.0:
                hs_obi = ctx.book.top_obi(5)
                alpha = max(-1.0, min(1.0, alpha + _HS_OBI_ALPHA_WEIGHT * hs_obi))
            alpha_shift = alpha * cfg.alpha_bps / 10000.0 * fair

        if cfg.market_bias != 0.0:
            alpha_shift += cfg.market_bias * cfg.alpha_bps / 10000.0 * fair

        spread_mult = 1.0
        recent_move = ctx.vol.recent_move_bps(cfg.fast_move_window_s)
        if recent_move >= cfg.fast_move_bps:
            spread_mult = cfg.fast_move_widen

        if cfg.adx_regime_enabled:
            spread_mult *= ctx.adx_regime.spread_mult

        if cfg.pivot_enabled and ctx.pivots.ready:
            spread_mult *= ctx.pivots.spread_mult(fair)

        baseline_move = max(1.0, ctx.vol.vol_bps()) if ctx.vol.count >= 10 else 5.0
        ctx.regime.update(ctx.obi.obi_raw, recent_move, baseline_move)
        toxic = ctx.regime.toxic_score

        allow_bids, allow_asks = ctx.pos.get_allowed_sides(fair)
        is_close = ctx.pos.is_close_mode
        close_size = ctx.pos.get_close_size() if is_close else 0.0
        inv_skew_factor = ctx.pos.get_inventory_skew_factor()

        if cfg.market_bias != 0.0 and not is_close:
            inv_skew_factor -= cfg.market_bias * 0.3
            inv_skew_factor = max(-1.0, min(1.0, inv_skew_factor))
            inv = ctx.pos.inventory
            max_inv_base = ctx.pos._max_inventory
            if cfg.market_bias > 0.0 and not allow_bids and inv < max_inv_base:
                allow_bids = True
            elif cfg.market_bias < 0.0 and not allow_asks and inv > -max_inv_base:
                allow_asks = True

        if is_close and ctx.close_mode_entered_ts <= 0.0:
            ctx.close_mode_entered_ts = time.monotonic()
        elif not is_close:
            ctx.close_mode_entered_ts = 0.0

        if toxic >= 0.85 and not is_close:
            inv = ctx.pos.inventory
            if inv > ctx.lot:
                allow_bids = False
            elif inv < -ctx.lot:
                allow_asks = False

        ctx.guard.update_adverse_rate(ctx.reflect.adverse_rate)

        guard_mark_px = fair if fair > 0.0 else (hs_mid_safe if hs_mid_safe > 0.0 else ctx.bn_mid)
        guard_unrealized_pnl = 0.0
        st = ctx.pos.state
        if guard_mark_px > 0.0 and abs(st.size_base) > 0.0 and st.avg_entry > 0.0:
            guard_unrealized_pnl = (guard_mark_px - st.avg_entry) * st.size_base
        guard_session_pnl = st.session_pnl + guard_unrealized_pnl

        guard_decision = ctx.guard.evaluate(
            session_pnl=guard_session_pnl,
            account_equity=self._account_equity,
            inventory=ctx.pos.inventory,
            max_inventory=ctx.pos._max_inventory,
            alpha_combined=alpha,
            toxic_score=toxic,
        )

        inv_abs = abs(ctx.pos.inventory)

        if (
            inv_abs <= ctx.lot
            and ctx.last_flat_ts > 0.0
            and (now - ctx.last_flat_ts) < _POST_CLOSE_COOLDOWN_S
        ):
            if cfg.enable_trading and self._orders:
                self._orders.stage_quotes(ctx.instrument_id, [])
            return True

        if guard_decision.halt:
            reason = guard_decision.reason or "GUARD_HALT"
            if cfg.enable_trading and self._orders:
                self._orders.stage_quotes(ctx.instrument_id, [])
                if inv_abs > ctx.lot:
                    self._schedule_force_close_ioc(
                        ctx=ctx,
                        fair=fair,
                        close_size=ctx.pos.get_close_size(),
                        reason=reason,
                        close_mult=1.0,
                    )
            if (
                reason != ctx.last_guard_halt_reason
                or (now - ctx.last_guard_halt_log_ts) >= _GUARD_HALT_LOG_MIN_INTERVAL_S
            ):
                ctx.last_guard_halt_reason = reason
                ctx.last_guard_halt_log_ts = now
                log.warning("[%s] GUARD HALT: %s", ctx.symbol, reason)
            return True

        ctx.last_guard_halt_reason = ""
        if not guard_decision.allow_bids:
            allow_bids = False
        if not guard_decision.allow_asks:
            allow_asks = False

        if guard_decision.force_close and inv_abs > ctx.lot:
            is_close = True
            close_size = ctx.pos.get_close_size()
            if ctx.pos.inventory > 0:
                allow_bids = False
                allow_asks = True
            else:
                allow_bids = True
                allow_asks = False

            now_mono = time.monotonic()
            if ctx.close_mode_entered_ts <= 0.0:
                ctx.close_mode_entered_ts = now_mono
            stuck_s = now_mono - ctx.close_mode_entered_ts
            if stuck_s >= 120.0:
                close_cross_mult = 5.0
            elif stuck_s >= 60.0:
                close_cross_mult = 2.0
            elif stuck_s >= 30.0:
                close_cross_mult = 1.0
            elif stuck_s >= 10.0:
                close_cross_mult = 0.5
            else:
                close_cross_mult = guard_decision.spread_mult

            if cfg.enable_trading and self._orders:
                self._schedule_force_close_ioc(
                    ctx=ctx,
                    fair=fair,
                    close_size=close_size,
                    reason=guard_decision.reason or "FORCE_CLOSE",
                    close_mult=close_cross_mult,
                )

        tp_decay_pct = 0.0
        if (
            cfg.fixed_tp_enabled
            and not is_close
            and inv_abs > ctx.lot
            and ctx.pos.state.avg_entry > 0.0
        ):
            entry = ctx.pos.state.avg_entry
            if ctx.pos.inventory > 0.0:
                profit_bps = (fair - entry) / entry * 10000.0
            else:
                profit_bps = (entry - fair) / entry * 10000.0

            if profit_bps >= cfg.fixed_tp_bps:
                is_close = True
                close_size = ctx.pos.get_close_size()
                if ctx.pos.inventory > 0.0:
                    allow_bids = False
                    allow_asks = True
                else:
                    allow_bids = True
                    allow_asks = False

                now_mono = time.monotonic()
                if ctx.close_mode_entered_ts <= 0.0:
                    ctx.close_mode_entered_ts = now_mono
                    log.info(
                        "[%s] TP TRIGGER: profit=%.1fbps >= %.1fbps, close decay starts",
                        ctx.symbol, profit_bps, cfg.fixed_tp_bps,
                    )

                tp_age_s = now_mono - ctx.close_mode_entered_ts
                tp_decay_pct = min(1.0, tp_age_s / 30.0)

                if tp_decay_pct >= 1.0 and profit_bps > cfg.taker_fee_bps:
                    if cfg.enable_trading and self._orders:
                        self._schedule_force_close_ioc(
                            ctx=ctx,
                            fair=fair,
                            close_size=close_size,
                            reason=(
                                f"TP_DECAY_IOC: profit={profit_bps:.1f}bps > fee={cfg.taker_fee_bps:.1f}bps "
                                f"after {tp_age_s:.0f}s"
                            ),
                            close_mult=0.1,
                        )

        if (
            not is_close
            and inv_abs > ctx.lot
            and ctx.guard.state.last_fill_ts > 0.0
            and cfg.guard_inventory_decay_s > 0.0
        ):
            hold_age_s = now - ctx.guard.state.last_fill_ts
            if hold_age_s > cfg.guard_inventory_decay_s:
                is_close = True
                close_size = ctx.pos.get_close_size()
                if ctx.pos.inventory > 0.0:
                    allow_bids = False
                    allow_asks = True
                else:
                    allow_bids = True
                    allow_asks = False
                if ctx.close_mode_entered_ts <= 0.0:
                    ctx.close_mode_entered_ts = now
                    log.info(
                        "[%s] STALE INV: held %.0fs > %.0fs, forcing close",
                        ctx.symbol, hold_age_s, cfg.guard_inventory_decay_s,
                    )
                stale_age_s = now - ctx.close_mode_entered_ts
                if stale_age_s >= _STALE_INV_IOC_RAMP_S and cfg.enable_trading and self._orders:
                    self._schedule_force_close_ioc(
                        ctx=ctx, fair=fair,
                        close_size=close_size,
                        reason=f"STALE_IOC: held {hold_age_s:.0f}s",
                        close_mult=0.5,
                    )

        if (
            not is_close
            and inv_abs <= ctx.lot
            and cfg.use_alpha
            and abs(alpha) > _ALPHA_ENTRY_GATE_THRESH
        ):
            if alpha > 0.0:
                allow_asks = False
            else:
                allow_bids = False

        vol_bps = ctx.vol.vol_bps() if ctx.vol.count >= 10 else max(0.1, cfg.min_spread_bps)

        quote_alpha_shift = alpha_shift
        quote_noise_bps = cfg.noise_bps
        quote_spread_mult = spread_mult
        quote_guard_mult = guard_decision.spread_mult
        if is_close:
            quote_alpha_shift = 0.0
            quote_noise_bps = 0.0
            quote_spread_mult = 1.0
            quote_guard_mult = min(1.0, quote_guard_mult)

        order_size = ctx.auto_order_size if cfg.order_size_usd <= 0 else cfg.order_size_usd
        if cfg.order_size_usd <= 0:
            min_open_order_usd = ctx.min_notional * 1.2
            if order_size < min_open_order_usd:
                order_size = min_open_order_usd
        effective_close_spread = cfg.close_spread_bps
        if tp_decay_pct > 0.0:
            effective_close_spread = cfg.close_spread_bps * max(0.05, 1.0 - tp_decay_pct * 0.95)
        quotes = ctx.quoter.generate(
            fair=fair,
            vol_bps=vol_bps,
            min_spread_bps=cfg.min_spread_bps,
            spread_vol_mult=cfg.spread_vol_mult,
            close_spread_bps=effective_close_spread,
            inventory_skew_factor=inv_skew_factor,
            inventory_skew_bps=cfg.inventory_skew_bps,
            alpha_shift=quote_alpha_shift,
            noise_bps=quote_noise_bps,
            allow_bids=allow_bids,
            allow_asks=allow_asks,
            order_size_usd=order_size,
            close_size=close_size,
            is_close_mode=is_close,
            hs_bid=hs_bid_safe,
            hs_ask=hs_ask_safe,
            levels=cfg.levels,
            level_spacing_bps=cfg.level_spacing_bps,
            level_size_scale=cfg.level_size_scale,
            spread_mult=quote_spread_mult,
            toxic_score=toxic,
            toxic_threshold=cfg.toxic_threshold,
            guard_spread_mult=quote_guard_mult,
        )

        if ctx.bn_mid > 0.0 and quotes:
            band = ctx.bn_mid * _MAX_FAIR_BN_BASIS_BPS / 10000.0
            lo = ctx.bn_mid - band
            hi = ctx.bn_mid + band
            before = len(quotes)
            quotes = [q for q in quotes if lo <= q.price <= hi]
            dropped = before - len(quotes)
            if dropped > 0:
                log.warning(
                    "[%s] QUOTE SANITY: dropped %d/%d outside bn band "
                    "(bn=%.4f lo=%.4f hi=%.4f)",
                    ctx.symbol, dropped, before, ctx.bn_mid, lo, hi,
                )

        if self._orders and ctx.pos._max_inventory > 0.0 and not is_close:
            inv_now = ctx.pos.inventory
            max_inv = ctx.pos._max_inventory

            bid_rest, ask_rest = self._orders.resting_size_by_side(ctx.instrument_id)
            remaining_bid = max(0.0, max_inv - inv_now - bid_rest)
            remaining_ask = max(0.0, max_inv + inv_now - ask_rest)

            if abs(inv_now) > max_inv * 2.0:
                log.warning(
                    "[%s] HARD BREAKER: inv=%.4f exceeds 2x max=%.2f -- "
                    "blocking all opens, forcing IOC close",
                    ctx.symbol, inv_now, max_inv,
                )
                quotes = []
                self._schedule_force_close_ioc(
                    ctx=ctx, fair=fair,
                    close_size=ctx.pos.get_close_size(),
                    reason="HARD_BREAKER",
                    close_mult=1.0,
                )
            else:
                capped = []
                bid_budget = remaining_bid
                ask_budget = remaining_ask
                for q in quotes:
                    if q.side == "b":
                        if bid_budget >= ctx.lot:
                            cap_sz = min(q.size, bid_budget)
                            cap_sz = _round_dn(cap_sz, ctx.lot)
                            if cap_sz >= ctx.lot:
                                capped.append(Quote(q.side, q.price, cap_sz))
                                bid_budget -= cap_sz
                    else:
                        if ask_budget >= ctx.lot:
                            cap_sz = min(q.size, ask_budget)
                            cap_sz = _round_dn(cap_sz, ctx.lot)
                            if cap_sz >= ctx.lot:
                                capped.append(Quote(q.side, q.price, cap_sz))
                                ask_budget -= cap_sz
                quotes = capped

        bid_q = next((q for q in quotes if q.side == "b"), None)
        ask_q = next((q for q in quotes if q.side == "s"), None)
        bid_str = _fmt(bid_q.price, ctx.tick) if bid_q else "-"
        ask_str = _fmt(ask_q.price, ctx.tick) if ask_q else "-"
        sprd_bps = 0.0
        quote_mode = "none"
        if bid_q and ask_q and fair > 0:
            sprd_bps = (ask_q.price - bid_q.price) / fair * 10000.0
            quote_mode = "two_sided"
        elif bid_q and fair > 0:
            # Close mode often quotes one side only; expose meaningful distance-to-fair.
            sprd_bps = max(0.0, (fair - bid_q.price) / fair * 10000.0)
            quote_mode = "bid_only"
        elif ask_q and fair > 0:
            sprd_bps = max(0.0, (ask_q.price - fair) / fair * 10000.0)
            quote_mode = "ask_only"

        tier = ctx.pos.get_inventory_tier(fair)
        tier_names = {0: "NORMAL", 1: "SKEW", 2: "SKIP", 3: "CLOSE"}
        mode_str = tier_names.get(tier, "NORMAL")
        n_bids = sum(1 for q in quotes if q.side == "b")
        n_asks = sum(1 for q in quotes if q.side == "s")

        ctx.last_quoted_fair = fair
        ctx.last_quote_ts = time.monotonic()
        ctx.last_alpha = alpha
        ctx.last_spread_bps = sprd_bps
        ctx.last_quote_mode = quote_mode
        ctx.last_toxic = toxic
        ctx.last_guard_spread_mult = guard_decision.spread_mult

        self._write_csv(ctx, fair, alpha, toxic, sprd_bps, is_close, vol_bps)

        guard_str = guard_decision.reason if guard_decision.reason else ""

        if not cfg.enable_trading:
            log.info(
                "DRY [%s/%s] | bid=%s(%d) ask=%s(%d) sprd=%.1fbps | "
                "inv=%.2f toxic=%.2f alpha=%.3f vol=%.1f | "
                "pnl=$%.4f adv=%.0f%% %s",
                ctx.symbol, mode_str, bid_str, n_bids, ask_str, n_asks, sprd_bps,
                ctx.pos.inventory, toxic, alpha, vol_bps,
                ctx.pos.state.session_pnl,
                ctx.reflect.adverse_rate * 100,
                guard_str,
            )
            return False

        changed = self._orders.stage_quotes(ctx.instrument_id, quotes)
        if changed:
            log.info(
                "LIVE [%s/%s] | bid=%s(%d) ask=%s(%d) sprd=%.1fbps | "
                "inv=%.2f toxic=%.2f vol=%.1f | "
                "pnl=$%.4f adv=%.0f%% %s",
                ctx.symbol, mode_str, bid_str, n_bids, ask_str, n_asks, sprd_bps,
                ctx.pos.inventory, toxic, vol_bps,
                ctx.pos.state.session_pnl,
                ctx.reflect.adverse_rate * 100,
                guard_str,
            )
        return changed

    # -------------------------------------------------------------------
    # Background tasks
    # -------------------------------------------------------------------

    async def _run_open_orders_reconcile(self):
        first_cfg = next(iter(self._symbols.values())).cfg
        while self._running:
            try:
                await asyncio.sleep(_OPEN_ORDERS_RECONCILE_S)
                if not self._running or not self._orders:
                    break
                raw = await asyncio.to_thread(
                    self._info.transport.request,
                    "info",
                    {"method": "openOrders", "params": {"user": first_cfg.account_address}},
                )
                rows = raw.get("data", raw) if isinstance(raw, dict) else raw
                if not isinstance(rows, list):
                    continue

                remote: Dict[int, set[str]] = {}
                remote_side_sz: Dict[int, Dict[str, float]] = {}
                for od in rows:
                    if not isinstance(od, dict):
                        continue
                    cloid = str(od.get("cloid") or od.get("client_order_id") or "")
                    if not cloid:
                        continue
                    inst_raw = od.get("instrument_id", od.get("instrumentId", od.get("instrument")))
                    inst_id = 0
                    if isinstance(inst_raw, str) and inst_raw in self._symbols:
                        inst_id = self._symbols[inst_raw].instrument_id
                    else:
                        try:
                            inst_id = int(float(inst_raw or 0))
                        except (TypeError, ValueError):
                            inst_id = 0
                    if inst_id <= 0:
                        continue
                    bucket = remote.get(inst_id)
                    if bucket is None:
                        bucket = set()
                        remote[inst_id] = bucket
                    bucket.add(cloid)

                    side = str(od.get("side", "")).lower()
                    sz = float(od.get("size", 0) or od.get("unfilled", 0) or 0)
                    if side in ("b", "s") and sz > 0.0:
                        ss = remote_side_sz.get(inst_id)
                        if ss is None:
                            ss = {"b": 0.0, "s": 0.0}
                            remote_side_sz[inst_id] = ss
                        ss[side] += sz

                removed, orphans = self._orders.reconcile_from_exchange(remote)
                if orphans:
                    log.warning(
                        "Reconcile found %d orphan remote orders -- cancelling",
                        len(orphans),
                    )
                    await self._orders.cancel_orphans(orphans)
                if removed > 0:
                    log.warning(
                        "Order reconcile removed %d stale local orders (remote_instruments=%d)",
                        removed,
                        len(remote),
                    )

                overexposed = False
                for ctx in self._symbols.values():
                    ss = remote_side_sz.get(ctx.instrument_id)
                    if ss is None:
                        continue
                    if ctx.pos._max_inventory <= 0.0 and ctx.bn_mid > 0.0:
                        ctx.pos.update_fair_price(ctx.bn_mid)
                    max_inv = ctx.pos._max_inventory
                    if max_inv <= 0.0:
                        continue
                    inv = ctx.pos.inventory
                    bid_resting = ss.get("b", 0.0)
                    ask_resting = ss.get("s", 0.0)
                    bid_exposure = inv + bid_resting
                    ask_exposure = -inv + ask_resting
                    if bid_exposure > max_inv * 1.5 or ask_exposure > max_inv * 1.5:
                        log.warning(
                            "[%s] OVEREXPOSURE: inv=%.2f bid_rest=%.2f ask_rest=%.2f "
                            "max=%.2f -- cancelling all",
                            ctx.symbol, inv, bid_resting, ask_resting, max_inv,
                        )
                        overexposed = True

                if overexposed:
                    await self._orders.cancel_all()

                if removed > 0 or orphans or overexposed:
                    self._requote_event.set()
            except asyncio.CancelledError:
                return
            except Exception as exc:
                log.warning("Open orders reconcile: %s", exc)

    async def _run_position_poll(self):
        first_cfg = next(iter(self._symbols.values())).cfg
        while self._running:
            try:
                await asyncio.sleep(5.0)
                if not self._running:
                    break
                raw = await asyncio.to_thread(
                    self._info.transport.request,
                    "info",
                    {"method": "positions", "params": {"user": first_cfg.account_address}},
                )
                per_ctx: Dict[str, Dict[str, float]] = {}
                for pos in _iter_ws_rows(raw):
                    ctx = self._ctx_from_row(pos)
                    if ctx is None:
                        continue
                    legs = pos.get("legs")
                    if legs and isinstance(legs, list):
                        net = 0.0
                        for leg in legs:
                            if isinstance(leg, dict):
                                net += _sf(leg, "size", 0.0)
                    else:
                        net = _sf(pos, "size", 0.0)
                    entry = _pos_entry_price(pos)
                    bucket = per_ctx.setdefault(
                        ctx.symbol,
                        {"net": 0.0, "entry_notional": 0.0, "entry_abs": 0.0},
                    )
                    bucket["net"] += net
                    if entry > 0.0 and abs(net) > 0.0:
                        sz_abs = abs(net)
                        bucket["entry_notional"] += entry * sz_abs
                        bucket["entry_abs"] += sz_abs

                for sym, info in per_ctx.items():
                    ctx = self._symbols.get(sym)
                    if ctx and ctx.pos:
                        net = info["net"]
                        prev_inv = ctx.pos.inventory
                        ctx.pos.reconcile(net)
                        self._on_inventory_reconcile(ctx, prev_inv, source="position-poll")
                        entry = (info["entry_notional"] / info["entry_abs"]) if info["entry_abs"] > 0.0 else 0.0
                        self._apply_position_entry(ctx, net, entry)

                for ctx in self._symbols.values():
                    if ctx.symbol not in per_ctx and ctx.pos:
                        prev_inv = ctx.pos.inventory
                        ctx.pos.reconcile(0.0)
                        self._on_inventory_reconcile(ctx, prev_inv, source="position-poll")
                        self._apply_position_entry(ctx, 0.0, 0.0)

                try:
                    raw = await asyncio.to_thread(
                        self._info.transport.request,
                        "info",
                        {"method": "accountSummary", "params": {"user": first_cfg.account_address}},
                    )
                    eq = float(raw.get("total_account_equity", 0))
                    if eq <= 0.0:
                        eq = float(raw.get("available_balance", 0))
                    if eq > 0.0:
                        self._account_equity = eq
                        self._recompute_auto_order_sizes(emit_logs=False)
                except Exception:
                    pass

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
                for ctx in self._symbols.values():
                    if not ctx.pos:
                        continue
                    st = ctx.pos.state
                    vol_str = f"{ctx.vol.vol_bps():.1f}" if ctx.vol.count >= 10 else "warm"
                    log.info(
                        "[%s] STATS | pnl=$%.4f fills=%d vol=$%.2f rt=%d inv=%.4f "
                        "orders=%d vol=%sbps",
                        ctx.symbol, st.session_pnl, st.total_fills, st.total_volume_usd,
                        st.round_trips, st.size_base,
                        self._orders.active_count(ctx.instrument_id) if self._orders else 0,
                        vol_str,
                    )
            except asyncio.CancelledError:
                return
            except Exception:
                pass

    async def _run_metrics_emitter(self):
        """Emit per-symbol JSON metrics to stdout for the DXD API collector."""
        while self._running:
            try:
                await asyncio.sleep(5.0)
                if not self._running:
                    break
                for ctx in self._symbols.values():
                    st = ctx.pos.state if ctx.pos else None
                    if st is None:
                        continue
                    markouts = ctx.reflect.avg_markout_bps if ctx.reflect else {}
                    _, _, hs_mid_safe = _safe_hs_bbo(ctx.book.best_bid, ctx.book.best_ask, ctx.bn_mid)
                    fair_opt = ctx.fair.get_fair_price(ctx.bn_mid) if ctx.bn_mid > 0.0 else None
                    if fair_opt is None and hs_mid_safe > 0.0:
                        fair_opt = hs_mid_safe
                    if fair_opt is None and ctx.bn_mid > 0.0:
                        fair_opt = ctx.bn_mid
                    fair = fair_opt or 0.0
                    if fair > 0.0 and ctx.bn_mid > 0.0:
                        fair = _clamp_to_ref_bps(fair, ctx.bn_mid, _MAX_FAIR_BN_BASIS_BPS)
                    mark_px = fair if fair > 0.0 else (hs_mid_safe if hs_mid_safe > 0.0 else ctx.bn_mid)
                    unrealized_pnl = 0.0
                    if mark_px > 0.0 and abs(st.size_base) > 0.0 and st.avg_entry > 0.0:
                        unrealized_pnl = (mark_px - st.avg_entry) * st.size_base
                    realized_pnl = st.session_pnl
                    total_pnl = realized_pnl + unrealized_pnl
                    payload = {
                        "session_id": self._session_id,
                        "symbol": ctx.symbol,
                        "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "pnl": round(total_pnl, 6),
                        "pnl_realized": round(realized_pnl, 6),
                        "pnl_unrealized": round(unrealized_pnl, 6),
                        "inventory": round(st.size_base, 6),
                        "inv_tier": ctx.pos.get_inventory_tier(fair) if fair > 0 else 0,
                        "total_fills": st.total_fills,
                        "total_volume_usd": round(st.total_volume_usd, 2),
                        "round_trips": st.round_trips,
                        "spread_bps": round(ctx.last_spread_bps, 2),
                        "quote_mode": ctx.last_quote_mode,
                        "vol_bps": round(ctx.vol.vol_bps() if ctx.vol.count >= 10 else 0.0, 2),
                        "alpha": round(ctx.last_alpha, 4),
                        "toxic": round(ctx.last_toxic, 4),
                        "adverse_rate": round(ctx.reflect.adverse_rate if ctx.reflect else 0.0, 4),
                        "avg_markout_1s": round(markouts.get("1s", 0.0), 4),
                        "avg_markout_5s": round(markouts.get("5s", 0.0), 4),
                        "guard_interventions": ctx.guard.state.interventions if ctx.guard else 0,
                        "guard_halted": ctx.guard.is_halted if ctx.guard else False,
                        "guard_spread_mult": round(ctx.last_guard_spread_mult, 4),
                        "account_equity": round(self._account_equity, 2),
                        "fair_mid": round(fair or 0.0, 6),
                        "hs_mid": round(ctx.book.mid, 6),
                        "bn_mid": round(ctx.bn_mid, 6),
                    }
                    sys.stdout.write(json.dumps(payload) + "\n")
                sys.stdout.flush()
            except asyncio.CancelledError:
                return
            except Exception:
                pass

    # -------------------------------------------------------------------
    # CSV (per-symbol files)
    # -------------------------------------------------------------------

    def _close_csv(self, ctx: SymbolCtx):
        if ctx.csv_file:
            try:
                ctx.csv_file.close()
            except Exception:
                pass
            ctx.csv_file = None
            ctx.csv_writer = None
            ctx.csv_date = None

    def _init_csv(self, ctx: SymbolCtx):
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        safe_sym = ctx.symbol.replace("-", "_").lower()
        path = DATA_DIR / f"mm_{safe_sym}_{date_str}.csv"
        is_new = not path.exists()
        ctx.csv_file = open(path, "a", newline="")
        ctx.csv_writer = csv.writer(ctx.csv_file)
        ctx.csv_date = date_str
        if is_new:
            ctx.csv_writer.writerow([
                "timestamp", "fair_mid", "hs_mid", "bn_mid",
                "alpha", "toxic", "spread_bps", "vol_bps", "is_close",
                "inventory", "inv_tier", "session_pnl",
                "total_fills", "total_volume",
                "adverse_rate", "guard_interventions",
            ])
            ctx.csv_file.flush()

    def _write_csv(self, ctx: SymbolCtx, fair: float, alpha: float, toxic: float,
                   spread_bps: float, is_close: bool, vol_bps: float):
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if ctx.csv_date != today:
            self._close_csv(ctx)
            self._init_csv(ctx)
        if not ctx.csv_writer:
            return
        try:
            ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            st = ctx.pos.state
            tier = ctx.pos.get_inventory_tier(fair) if fair > 0 else 0
            ctx.csv_writer.writerow([
                ts, f"{fair:.4f}", f"{ctx.book.mid:.4f}", f"{ctx.bn_mid:.4f}",
                f"{alpha:.4f}", f"{toxic:.4f}", f"{spread_bps:.2f}",
                f"{vol_bps:.2f}", int(is_close),
                f"{st.size_base:.4f}", tier, f"{st.session_pnl:.4f}",
                st.total_fills, f"{st.total_volume_usd:.2f}",
                f"{ctx.reflect.adverse_rate:.4f}",
                ctx.guard.state.interventions,
            ])
            ctx.csv_file.flush()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main():
    session_id = None
    relay_port = None
    symbols_arg = ""
    symbol_configs_json = ""
    args = sys.argv[1:]
    i = 0
    while i < len(args):
        if args[i] == "--session-id" and i + 1 < len(args):
            session_id = args[i + 1]
            i += 2
        elif args[i] == "--relay-port" and i + 1 < len(args):
            relay_port = int(args[i + 1])
            i += 2
        elif args[i] == "--symbols" and i + 1 < len(args):
            symbols_arg = args[i + 1]
            i += 2
        elif args[i] == "--symbol-configs" and i + 1 < len(args):
            symbol_configs_json = args[i + 1]
            i += 2
        else:
            i += 1

    if session_id:
        root = logging.getLogger()
        for h in root.handlers[:]:
            root.removeHandler(h)
        logging.basicConfig(
            level=getattr(logging, LOG_LEVEL, logging.INFO),
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            stream=sys.stderr,
        )

    if symbols_arg:
        symbol_list = [s.strip() for s in symbols_arg.split(",") if s.strip()]
    else:
        symbol_list = [os.getenv("MM_SYMBOL", "HYPE-PERP")]

    per_symbol_overrides = {}
    if symbol_configs_json:
        try:
            per_symbol_overrides = json.loads(symbol_configs_json)
        except json.JSONDecodeError:
            log.error("Invalid --symbol-configs JSON")
            sys.exit(1)

    configs: Dict[str, Config] = {}
    for sym in symbol_list:
        cfg = Config.from_env(symbol_override=sym)
        overrides = per_symbol_overrides.get(sym, {})
        if overrides:
            cfg_dict = {f.name: getattr(cfg, f.name) for f in cfg.__dataclass_fields__.values()}
            for k, v in overrides.items():
                if k in cfg_dict:
                    target_type = type(cfg_dict[k])
                    if target_type is bool:
                        cfg_dict[k] = str(v).lower() in ("true", "1", "yes")
                    else:
                        cfg_dict[k] = target_type(v)
            cfg = Config(**cfg_dict)
        configs[sym] = cfg

    bot = AggressiveMM(configs, session_id=session_id, relay_port=relay_port)
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
