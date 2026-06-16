
"""
Cross-Exchange Latency Arbitrage for Hotstuff DEX
==================================================

Monitors Binance Futures bookTicker (fastest public feed) for reference prices
on multiple instruments. When the Hotstuff DEX quotes become stale relative to
Binance, fires aggressive IOC orders to capture the mispricing before the market
maker updates.

Instruments: configurable, default HYPE-PERP, ZEC-PERP, XRP-PERP, SOL-PERP
Reference:   Binance Futures bookTicker (HYPEUSDT, ZECUSDT, XRPUSDT, SOLUSDT)

Core logic:
  1. Binance bookTicker updates fair mid for each instrument (~50ms latency).
  2. Hotstuff BBO subscription tracks the DEX quotes.
  3. When |binance_mid - hs_mid| / hs_mid > entry_threshold_bps, the DEX
     quote is stale. Fire IOC at the stale price.
  4. Close position when divergence narrows or on timeout.

Data sources:
  Binance Futures combined stream: bookTicker + aggTrade (all instruments)
  Hotstuff WS: BBO, fills, positions, account_summary
"""

import asyncio
import importlib
import json
import logging
import math
import os
import signal as os_signal
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from dotenv import load_dotenv
from eth_account import Account

from hotstuff import (
    HttpTransport,
    WebSocketTransport,
    InfoClient,
    ExchangeClient,
    SubscriptionClient,
    HttpTransportOptions,
    WebSocketTransportOptions,
    PlaceOrderParams,
    UnitOrder,
    CancelAllParams,
)
from hotstuff.utils.signing import sign_action as _sign_action
from hotstuff.methods.exchange.op_codes import EXCHANGE_OP_CODES as _OP_CODES

_sub_global = importlib.import_module("hotstuff.methods.subscription.global")
BBOSubscriptionParams = _sub_global.BBOSubscriptionParams
FillsSubscriptionParams = _sub_global.FillsSubscriptionParams
PositionsSubscriptionParams = _sub_global.PositionsSubscriptionParams

load_dotenv()


# ---------------------------------------------------------------------------
# Monkey-patch: SDK hardcodes is_testnet=True -- override for mainnet
# ---------------------------------------------------------------------------

_original_execute = ExchangeClient._execute_action


async def _patched_execute(self, request, signal=None, execute=True):
    action = request["action"]
    params = request["params"]
    if "nonce" not in params or params["nonce"] is None:
        params["nonce"] = await self.nonce()
    for order in params.get("orders", []):
        if isinstance(order, dict) and order.get("isMarket") is None:
            order["isMarket"] = False
    signature = await _sign_action(
        wallet=self.wallet,
        action=params,
        tx_type=_OP_CODES[action],
        is_testnet=False,
    )
    if execute:
        response = await self.transport.request(
            "exchange",
            {
                "action": {
                    "data": params,
                    "type": str(_OP_CODES[action]),
                },
                "signature": signature,
                "nonce": params["nonce"],
            },
            signal,
        )
        return response
    return {"params": params, "signature": signature}


ExchangeClient._execute_action = _patched_execute


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("latency_arb")


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
# Velocity tracker -- measures price movement speed from Binance
# ---------------------------------------------------------------------------

class VelocityTracker:
    """Tracks recent price velocity (bps/second) for fast-move detection."""
    __slots__ = ("_prices", "_ts", "_max_age")

    def __init__(self, max_age_s: float = 5.0):
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

    def velocity_bps(self) -> float:
        """Signed velocity in bps/second over the window."""
        n = len(self._prices)
        if n < 2:
            return 0.0
        dt = self._ts[-1] - self._ts[0]
        if dt < 0.01:
            return 0.0
        move_bps = (self._prices[-1] - self._prices[0]) / self._prices[0] * 10000.0
        return move_bps / dt

    def move_bps(self, window_s: float) -> float:
        """Absolute move in bps over last window_s seconds."""
        if len(self._prices) < 2:
            return 0.0
        now = self._ts[-1]
        cutoff = now - window_s
        oldest = self._prices[-1]
        for i in range(len(self._ts) - 1, -1, -1):
            if self._ts[i] < cutoff:
                break
            oldest = self._prices[i]
        if oldest <= 0.0:
            return 0.0
        return abs(self._prices[-1] - oldest) / oldest * 10000.0


# ---------------------------------------------------------------------------
# Basis tracker -- EMA of structural divergence between exchanges
# Fires only when divergence suddenly DEVIATES from the rolling baseline
# ---------------------------------------------------------------------------

class BasisTracker:
    """Tracks the rolling baseline basis (structural divergence) between
    Binance and Hotstuff via EMA. The 'signal' is how far current divergence
    deviates from this baseline -- positive means Binance just jumped up
    relative to Hotstuff (stale ask), negative means it just dropped (stale bid).

    Only fires when the deviation exceeds the threshold, filtering out the
    persistent structural basis that always exists between exchanges."""
    __slots__ = ("_ema", "_alpha", "_initialized", "_warmup_ticks", "_tick_count")

    def __init__(self, ema_half_life_ticks: int = 200):
        self._alpha = 2.0 / (ema_half_life_ticks + 1)
        self._ema = 0.0
        self._initialized = False
        self._warmup_ticks = ema_half_life_ticks
        self._tick_count = 0

    def update(self, divergence_bps: float) -> float:
        """Feed raw divergence, returns deviation from baseline (signal)."""
        self._tick_count += 1
        if not self._initialized:
            self._ema = divergence_bps
            self._initialized = True
            return 0.0
        self._ema += self._alpha * (divergence_bps - self._ema)
        return divergence_bps - self._ema

    @property
    def baseline_bps(self) -> float:
        return self._ema

    @property
    def warmed_up(self) -> bool:
        return self._tick_count >= self._warmup_ticks


# ---------------------------------------------------------------------------
# Per-instrument state
# ---------------------------------------------------------------------------

@dataclass
class InstrumentSpec:
    hs_symbol: str
    bn_symbol: str
    instrument_id: int = 0
    tick_size: float = 0.001
    lot_size: float = 0.01
    min_notional: float = 10.0


@dataclass
class InstrumentState:
    spec: InstrumentSpec
    # Binance reference
    bn_bid: float = 0.0
    bn_ask: float = 0.0
    bn_mid: float = 0.0
    bn_last_ts: float = 0.0
    # Hotstuff BBO
    hs_bid: float = 0.0
    hs_ask: float = 0.0
    hs_mid: float = 0.0
    hs_last_ts: float = 0.0
    # Position
    position: float = 0.0
    avg_entry: float = 0.0
    entry_ts: float = 0.0
    entry_bn_mid: float = 0.0
    # PnL
    realized_pnl: float = 0.0
    round_trips: int = 0
    total_fills: int = 0
    total_volume: float = 0.0
    # Order tracking
    last_order_ts: float = 0.0
    pending_order: bool = False
    cloid_seq: int = 0
    # Velocity
    velocity: Optional[VelocityTracker] = None
    # Basis tracker
    basis: Optional[BasisTracker] = None

    def __post_init__(self):
        if self.velocity is None:
            self.velocity = VelocityTracker(max_age_s=5.0)
        if self.basis is None:
            self.basis = BasisTracker(ema_half_life_ticks=200)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Config:
    private_key: str
    agent_address: str
    # Instruments: parallel lists
    hs_symbols: Tuple[str, ...]
    bn_symbols: Tuple[str, ...]
    # Sizing: target notional per order in USD (auto-computes lot size from live price)
    target_notional_usd: float
    # Thresholds
    entry_threshold_bps: float
    exit_threshold_bps: float
    velocity_min_bps_s: float
    # Position management
    max_position_usd: float
    position_timeout_s: float
    exit_slippage_bps: float
    # Order control
    cooldown_ms: float
    order_expiry_ms: int
    # Risk
    max_total_loss_usd: float
    leverage: int
    # Execution
    enable_trading: bool

    @classmethod
    def from_env(cls) -> "Config":
        pk = os.getenv("HOTSTUFF_PRIVATE_KEY", "")
        if not pk:
            log.error("HOTSTUFF_PRIVATE_KEY not set")
            sys.exit(1)
        addr = os.getenv("HOTSTUFF_AGENT_ADDRESS", "")
        if not addr:
            log.error("HOTSTUFF_AGENT_ADDRESS not set")
            sys.exit(1)

        hs_raw = os.getenv("LA_HS_SYMBOLS", "HYPE-PERP,ZEC-PERP,XRP-PERP,SOL-PERP")
        bn_raw = os.getenv("LA_BN_SYMBOLS", "HYPEUSDT,ZECUSDT,XRPUSDT,SOLUSDT")

        hs_symbols = tuple(s.strip() for s in hs_raw.split(",") if s.strip())
        bn_symbols = tuple(s.strip() for s in bn_raw.split(",") if s.strip())

        if len(hs_symbols) != len(bn_symbols):
            log.error("LA_HS_SYMBOLS and LA_BN_SYMBOLS must have same count")
            sys.exit(1)

        return cls(
            private_key=pk,
            agent_address=addr,
            hs_symbols=hs_symbols,
            bn_symbols=bn_symbols,
            target_notional_usd=float(os.getenv("LA_TARGET_NOTIONAL_USD", "100")),
            entry_threshold_bps=float(os.getenv("LA_ENTRY_THRESHOLD_BPS", "4.0")),
            exit_threshold_bps=float(os.getenv("LA_EXIT_THRESHOLD_BPS", "1.0")),
            velocity_min_bps_s=float(os.getenv("LA_VELOCITY_MIN_BPS_S", "0.0")),
            max_position_usd=float(os.getenv("LA_MAX_POSITION_USD", "2000")),
            position_timeout_s=float(os.getenv("LA_POSITION_TIMEOUT_S", "60")),
            exit_slippage_bps=float(os.getenv("LA_EXIT_SLIPPAGE_BPS", "10")),
            cooldown_ms=float(os.getenv("LA_COOLDOWN_MS", "300")),
            order_expiry_ms=int(os.getenv("LA_ORDER_EXPIRY_MS", "60000")),
            max_total_loss_usd=float(os.getenv("LA_MAX_TOTAL_LOSS_USD", "50")),
            leverage=int(os.getenv("LA_LEVERAGE", "20")),
            enable_trading=os.getenv("LA_ENABLE_TRADING", "false").lower() == "true",
        )


# ---------------------------------------------------------------------------
# LatencyArbBot
# ---------------------------------------------------------------------------

class LatencyArbBot:

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._instruments: Dict[str, InstrumentState] = {}
        self._bn_to_hs: Dict[str, str] = {}
        self._running = False
        self._subs: List[Any] = []
        self._fire_event = asyncio.Event()
        self._order_lock = asyncio.Lock()

        self._http: Optional[HttpTransport] = None
        self._ws: Optional[WebSocketTransport] = None
        self._info: Optional[InfoClient] = None
        self._exchange: Optional[ExchangeClient] = None
        self._sub_client: Optional[SubscriptionClient] = None

        self._bn_task: Optional[asyncio.Task] = None
        self._bn_session: Optional[aiohttp.ClientSession] = None
        self._exit_task: Optional[asyncio.Task] = None
        self._pos_poll_task: Optional[asyncio.Task] = None

        self._account_equity: float = 0.0
        self._total_session_pnl: float = 0.0
        self._paused: bool = False

        for i, hs_sym in enumerate(cfg.hs_symbols):
            spec = InstrumentSpec(
                hs_symbol=hs_sym,
                bn_symbol=cfg.bn_symbols[i],
            )
            self._instruments[hs_sym] = InstrumentState(spec=spec)
            self._bn_to_hs[cfg.bn_symbols[i].lower()] = hs_sym

    # ---------------------------------------------------------------
    # Lifecycle
    # ---------------------------------------------------------------

    async def start(self):
        self._running = True

        log.info("=== Latency Arb starting ===")
        log.info("Mode         : %s", "LIVE" if self.cfg.enable_trading else "DRY-RUN")
        log.info("Instruments  : %s", ", ".join(self.cfg.hs_symbols))
        log.info("Binance refs : %s", ", ".join(self.cfg.bn_symbols))
        log.info("Notional/ord : $%.0f (auto-sizes from live price)", self.cfg.target_notional_usd)
        log.info("Entry thresh : %.1f bps", self.cfg.entry_threshold_bps)
        log.info("Exit thresh  : %.1f bps", self.cfg.exit_threshold_bps)
        log.info("Velocity min : %.1f bps/s", self.cfg.velocity_min_bps_s)
        log.info("Max pos USD  : $%.0f per instrument", self.cfg.max_position_usd)
        log.info("Pos timeout  : %.0fs", self.cfg.position_timeout_s)
        log.info("Cooldown     : %.0fms", self.cfg.cooldown_ms)
        log.info("Max loss     : $%.0f", self.cfg.max_total_loss_usd)

        http_server = {
            "mainnet": {
                "api": "https://api.hotstuff.trade/",
                "rpc": "https://rpc.hotstuff.trade/",
            }
        }
        ws_server = {"mainnet": "wss://api.hotstuff.trade/ws"}
        self._http = HttpTransport(HttpTransportOptions(
            is_testnet=False, timeout=10.0, server=http_server,
        ))
        self._ws = WebSocketTransport(WebSocketTransportOptions(
            is_testnet=False, timeout=15.0,
            keep_alive={"interval": 20.0, "timeout": 10.0},
            auto_connect=True, server=ws_server,
        ))

        wallet = Account.from_key(self.cfg.private_key)
        self._info = InfoClient(transport=self._http)
        self._exchange = ExchangeClient(transport=self._http, wallet=wallet)
        self._sub_client = SubscriptionClient(transport=self._ws)

        await self._resolve_instruments()
        await self._sync_account()
        await self._sync_positions()

        self._bn_session = aiohttp.ClientSession()
        await self._subscribe_hotstuff()
        self._bn_task = asyncio.create_task(self._run_binance_ws())
        self._exit_task = asyncio.create_task(self._position_exit_loop())
        self._pos_poll_task = asyncio.create_task(self._position_poll_loop())

        if self.cfg.enable_trading:
            await self._cancel_all()

        log.info("=== Latency Arb ready -- monitoring for stale quotes ===")
        await self._main_loop()

    async def stop(self):
        log.info("Shutting down...")
        self._running = False
        self._fire_event.set()

        if self.cfg.enable_trading and self._exchange:
            try:
                await self._cancel_all()
            except Exception as exc:
                log.error("Cancel on shutdown: %s", exc)

        for task in (self._bn_task, self._exit_task, self._pos_poll_task):
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        if self._bn_session:
            await self._bn_session.close()

        for sub in self._subs:
            try:
                if isinstance(sub, dict) and "unsubscribe" in sub:
                    await sub["unsubscribe"]()
            except Exception:
                pass

        if self._ws:
            try:
                await self._ws.disconnect()
            except Exception:
                pass
        if self._http:
            try:
                await self._http.close()
            except Exception:
                pass

        total_vol = sum(s.total_volume for s in self._instruments.values())
        total_rt = sum(s.round_trips for s in self._instruments.values())
        log.info("Session PnL: $%.4f | Round trips: %d | Volume: $%.2f",
                 self._total_session_pnl, total_rt, total_vol)
        for sym, st in self._instruments.items():
            if st.total_fills > 0:
                log.info("  %s: PnL=$%.4f fills=%d vol=$%.2f rt=%d",
                         sym, st.realized_pnl, st.total_fills,
                         st.total_volume, st.round_trips)
        log.info("=== Shutdown complete ===")

    # ---------------------------------------------------------------
    # Initialization
    # ---------------------------------------------------------------

    async def _resolve_instruments(self):
        raw = await self._http.request(
            "info", {"method": "instruments", "params": {"type": "perps"}},
        )
        perps = raw.get("perps", []) if isinstance(raw, dict) else []
        found = 0
        for p in perps:
            name = p.get("name", "")
            if name in self._instruments:
                spec = self._instruments[name].spec
                spec.instrument_id = int(p["id"])
                spec.tick_size = float(p.get("tick_size", 0.001))
                spec.lot_size = float(p.get("lot_size", 0.01))
                spec.min_notional = float(p.get("min_notional_usd", 10))
                log.info("Resolved %s: id=%d tick=%s lot=%s min=$%.0f",
                         name, spec.instrument_id,
                         _fmt(spec.tick_size, 0.000001),
                         _fmt(spec.lot_size, 0.000001),
                         spec.min_notional)
                found += 1
        if found < len(self._instruments):
            missing = [s for s, st in self._instruments.items() if st.spec.instrument_id == 0]
            log.error("Missing instruments on Hotstuff: %s", missing)
            sys.exit(1)

    async def _sync_account(self):
        try:
            raw = await self._http.request(
                "info",
                {"method": "accountSummary", "params": {"user": self.cfg.agent_address}},
            )
            eq = float(raw.get("total_account_equity", 0))
            if eq <= 0.0:
                eq = float(raw.get("available_balance", 0))
            if eq > 0.0:
                self._account_equity = eq
            log.info("Account equity: $%.2f", self._account_equity)
        except Exception as exc:
            log.warning("Equity sync: %s", exc)

    async def _sync_positions(self):
        try:
            raw = await self._http.request(
                "info",
                {"method": "positions", "params": {"user": self.cfg.agent_address}},
            )
            positions = raw if isinstance(raw, list) else []
            for pos in positions:
                inst = pos.get("instrument", "")
                if inst in self._instruments:
                    size = float(pos.get("size", 0))
                    self._instruments[inst].position = size
                    if abs(size) > 0.0:
                        self._instruments[inst].avg_entry = float(pos.get("entry_price", 0))
                        self._instruments[inst].entry_ts = time.monotonic()
                        log.info("Existing position %s: %.6f", inst, size)
        except Exception as exc:
            log.warning("Position sync: %s", exc)

    # ---------------------------------------------------------------
    # Order helpers
    # ---------------------------------------------------------------

    def _next_cloid(self, inst_state: InstrumentState) -> str:
        inst_state.cloid_seq += 1
        return f"la-{int(time.time())}-{inst_state.cloid_seq}"

    async def _cancel_all(self):
        try:
            now_ms = int(time.time() * 1000)
            await self._exchange.cancel_all(
                CancelAllParams(expires_after=now_ms + 60_000)
            )
        except Exception as exc:
            log.warning("cancel_all: %s", exc)

    async def _fire_ioc(self, sym: str, is_buy: bool, price: float, size: float) -> bool:
        """Fire a single IOC order. Returns True if order was sent."""
        st = self._instruments[sym]
        spec = st.spec

        price_str = _fmt(price, spec.tick_size)
        size_str = _fmt(size, spec.lot_size)

        if size * price < spec.min_notional:
            return False

        cloid = self._next_cloid(st)
        try:
            now_ms = int(time.time() * 1000)
            params = PlaceOrderParams(
                orders=[UnitOrder(
                    instrument_id=spec.instrument_id,
                    side="b" if is_buy else "s",
                    position_side="BOTH",
                    price=price_str,
                    size=size_str,
                    tif="IOC",
                    ro=False,
                    po=False,
                    cloid=cloid,
                )],
                expires_after=now_ms + self.cfg.order_expiry_ms,
            )
            resp = await self._exchange.place_order(params)
            st.last_order_ts = time.monotonic()
            log.info("IOC %s %s %.6f @ %s | cloid=%s | resp=%s",
                     "BUY" if is_buy else "SELL", sym, size, price_str, cloid, resp)
            return True
        except Exception as exc:
            log.error("IOC %s failed: %s", sym, exc)
            return False

    # ---------------------------------------------------------------
    # Hotstuff subscriptions
    # ---------------------------------------------------------------

    async def _subscribe_hotstuff(self):
        addr = self.cfg.agent_address

        for sym in self._instruments:
            sub = await self._sub_client.bbo(
                BBOSubscriptionParams(symbol=sym), self._make_bbo_cb(sym))
            self._subs.append(sub)
            log.info("Sub: %s BBO", sym)

        fills_key = "user" if "user" in FillsSubscriptionParams.model_fields else "address"
        sub = await self._sub_client.fills(
            FillsSubscriptionParams(**{fills_key: addr}), self._on_fill)
        self._subs.append(sub)
        log.info("Sub: fills")

        pos_key = "user" if "user" in PositionsSubscriptionParams.model_fields else "address"
        sub = await self._sub_client.positions(
            PositionsSubscriptionParams(**{pos_key: addr}), self._on_position)
        self._subs.append(sub)
        log.info("Sub: positions")

        sub = await self._ws.subscribe(
            "account_summary", {"user": addr}, self._on_account_summary)
        self._subs.append(sub)
        log.info("Sub: account_summary")

    def _make_bbo_cb(self, sym: str):
        def cb(msg):
            data = msg.data
            bid = _sf(data, "best_bid_price") or _sf(data, "bestBidPrice")
            ask = _sf(data, "best_ask_price") or _sf(data, "bestAskPrice")
            st = self._instruments[sym]
            if bid > 0.0:
                st.hs_bid = bid
            if ask > 0.0:
                st.hs_ask = ask
            if bid > 0.0 and ask > 0.0:
                st.hs_mid = (bid + ask) * 0.5
                st.hs_last_ts = time.monotonic()
        return cb

    def _on_fill(self, msg):
        data = msg.data if hasattr(msg, "data") else msg
        if isinstance(data, list):
            for f in data:
                self._process_fill(f)
        elif isinstance(data, dict):
            self._process_fill(data)

    def _process_fill(self, fill: dict):
        inst = fill.get("instrument", fill.get("symbol", ""))
        if inst not in self._instruments:
            return

        st = self._instruments[inst]
        side = str(fill.get("side", "")).upper()
        price = float(fill.get("price", 0))
        size = float(fill.get("size", 0))
        if price <= 0.0 or size <= 0.0:
            return

        is_buy = side in ("BUY", "B", "LONG")
        st.total_fills += 1
        st.total_volume += size * price
        old_pos = st.position

        if old_pos == 0.0 or (old_pos > 0 and is_buy) or (old_pos < 0 and not is_buy):
            st.avg_entry = price
            if old_pos == 0.0:
                st.entry_ts = time.monotonic()
                st.entry_bn_mid = st.bn_mid
        else:
            close_size = min(size, abs(old_pos))
            if st.avg_entry > 0.0:
                if old_pos > 0.0:
                    pnl = (price - st.avg_entry) * close_size
                else:
                    pnl = (st.avg_entry - price) * close_size
                st.realized_pnl += pnl
                self._total_session_pnl += pnl
                st.round_trips += 1
                log.info("ROUND-TRIP %s: $%.4f (entry=%.4f exit=%.4f size=%.4f) | Total: $%.4f",
                         inst, pnl, st.avg_entry, price, close_size, self._total_session_pnl)

            remaining = size - close_size
            if remaining > 0.0:
                st.avg_entry = price
                st.entry_ts = time.monotonic()
            elif abs(abs(old_pos) - close_size) < st.spec.lot_size:
                st.avg_entry = 0.0
                st.entry_ts = 0.0

        log.info("FILL %s: %s %.6f @ %s | pos=%.6f | pnl=$%.4f",
                 inst, side, size, _fmt(price, st.spec.tick_size),
                 st.position, st.realized_pnl)

    def _on_position(self, msg):
        data = msg.data if hasattr(msg, "data") else msg
        positions = data if isinstance(data, list) else [data] if isinstance(data, dict) else []
        for pos in positions:
            if not isinstance(pos, dict):
                continue
            inst = pos.get("instrument", "") or pos.get("instrument_name", "")
            if not inst:
                inst_id = 0
                try:
                    inst_id = int(float(pos.get("instrument_id", 0)))
                except (TypeError, ValueError):
                    pass
                for sym, st in self._instruments.items():
                    if st.spec.instrument_id == inst_id:
                        inst = sym
                        break
            if inst not in self._instruments:
                continue

            st = self._instruments[inst]
            legs = pos.get("legs")
            net = 0.0
            if legs and isinstance(legs, list):
                for leg in legs:
                    net += float(leg.get("size", 0))
            else:
                net = float(pos.get("size", 0))

            old = st.position
            st.position = net
            if abs(net - old) > st.spec.lot_size:
                log.info("POS UPDATE %s: %.6f -> %.6f", inst, old, net)

    def _on_account_summary(self, msg):
        data = msg if isinstance(msg, dict) else getattr(msg, "data", msg)
        if isinstance(data, dict) and "data" in data:
            data = data["data"]
        if isinstance(data, dict):
            eq = _sf(data, "total_account_equity")
            if eq > 0.0:
                self._account_equity = eq

    # ---------------------------------------------------------------
    # Binance WebSocket
    # ---------------------------------------------------------------

    def _build_binance_url(self) -> str:
        streams = []
        for bn_sym in self.cfg.bn_symbols:
            s = bn_sym.lower()
            streams.append(f"{s}@bookTicker")
            streams.append(f"{s}@aggTrade")
        return "wss://fstream.binance.com/stream?streams=" + "/".join(streams)

    async def _run_binance_ws(self):
        url = self._build_binance_url()
        while self._running:
            try:
                async with self._bn_session.ws_connect(url, heartbeat=20) as ws:
                    log.info("Binance WS connected (%d streams)", len(self.cfg.bn_symbols) * 2)
                    async for msg in ws:
                        if not self._running:
                            break
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                payload = json.loads(msg.data)
                                stream = payload.get("stream", "")
                                data = payload.get("data", {})
                                if "bookTicker" in stream:
                                    self._on_binance_book_ticker(stream, data)
                                elif "aggTrade" in stream:
                                    self._on_binance_agg_trade(stream, data)
                            except (json.JSONDecodeError, KeyError, ValueError):
                                pass
                        elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                            break
            except asyncio.CancelledError:
                return
            except Exception as exc:
                log.warning("Binance WS error: %s -- reconnect 3s", exc)
                await asyncio.sleep(3.0)

    def _on_binance_book_ticker(self, stream: str, data: dict):
        bid = float(data.get("b", 0))
        ask = float(data.get("a", 0))
        if bid <= 0.0 or ask <= 0.0:
            return

        bn_sym = stream.split("@")[0]
        hs_sym = self._bn_to_hs.get(bn_sym)
        if not hs_sym:
            return

        now = time.monotonic()
        st = self._instruments[hs_sym]
        mid = (bid + ask) * 0.5

        st.bn_bid = bid
        st.bn_ask = ask
        st.bn_mid = mid
        st.bn_last_ts = now
        st.velocity.update(mid, now)

        self._check_opportunity(hs_sym, now)

    def _on_binance_agg_trade(self, stream: str, data: dict):
        bn_sym = stream.split("@")[0]
        hs_sym = self._bn_to_hs.get(bn_sym)
        if not hs_sym:
            return
        price = float(data.get("p", 0))
        if price > 0.0:
            st = self._instruments[hs_sym]
            st.velocity.update(price, time.monotonic())

    # ---------------------------------------------------------------
    # Core signal: stale quote detection and IOC firing
    # ---------------------------------------------------------------

    def _check_opportunity(self, sym: str, now: float):
        if self._paused or not self._running:
            return

        st = self._instruments[sym]

        if st.hs_mid <= 0.0 or st.bn_mid <= 0.0:
            return

        cooldown_s = self.cfg.cooldown_ms / 1000.0
        if now - st.last_order_ts < cooldown_s:
            return

        raw_div_bps = (st.bn_mid - st.hs_mid) / st.hs_mid * 10000.0

        # Feed raw divergence into the basis tracker; get back deviation
        # from the rolling baseline. This filters out the persistent
        # structural basis between exchanges and only fires on sudden
        # widening (Binance just moved, Hotstuff is lagging).
        signal_bps = st.basis.update(raw_div_bps)

        if not st.basis.warmed_up:
            return

        if self.cfg.velocity_min_bps_s > 0.0:
            vel = abs(st.velocity.velocity_bps())
            if vel < self.cfg.velocity_min_bps_s:
                return

        pos_notional = abs(st.position) * st.hs_mid
        if pos_notional >= self.cfg.max_position_usd:
            return

        threshold = self.cfg.entry_threshold_bps

        # signal_bps > 0: divergence just widened upward (Binance jumped up,
        # Hotstuff hasn't caught up) -> stale ask -> BUY
        # signal_bps < 0: divergence just widened downward -> stale bid -> SELL
        should_buy = signal_bps > threshold
        should_sell = signal_bps < -threshold

        if abs(st.position) > st.spec.lot_size:
            is_long = st.position > 0.0
            if is_long and should_buy:
                return
            if not is_long and should_sell:
                return

        if should_buy or should_sell:
            self._fire_event.set()
            asyncio.get_event_loop().create_task(
                self._execute_signal(sym, should_buy, raw_div_bps, signal_bps))

    async def _execute_signal(self, sym: str, is_buy: bool,
                              raw_div_bps: float, signal_bps: float):
        async with self._order_lock:
            st = self._instruments[sym]
            spec = st.spec
            now = time.monotonic()

            cooldown_s = self.cfg.cooldown_ms / 1000.0
            if now - st.last_order_ts < cooldown_s:
                return

            if self._total_session_pnl < -self.cfg.max_total_loss_usd:
                if not self._paused:
                    self._paused = True
                    log.warning("PAUSED: session pnl $%.4f exceeds -$%.0f loss limit",
                                self._total_session_pnl, self.cfg.max_total_loss_usd)
                return

            ref_price = st.bn_mid if st.bn_mid > 0.0 else st.hs_mid
            if ref_price <= 0.0:
                return
            size = _round_dn(self.cfg.target_notional_usd / ref_price, spec.lot_size)
            if size <= 0.0 or size * ref_price < spec.min_notional:
                return

            if is_buy:
                price = st.hs_ask
                if price <= 0.0:
                    return
                price = _round_up(price, spec.tick_size)
            else:
                price = st.hs_bid
                if price <= 0.0:
                    return
                price = _round_dn(price, spec.tick_size)

            if not self.cfg.enable_trading:
                vel = st.velocity.velocity_bps()
                hs_spread_bps = 0.0
                if st.hs_bid > 0.0 and st.hs_ask > 0.0 and st.hs_mid > 0.0:
                    hs_spread_bps = (st.hs_ask - st.hs_bid) / st.hs_mid * 10000.0
                notional = size * price
                log.info("SNIPE | %s %s %.4f @ %s ($%.0f) | signal=%+.1fbps raw_div=%+.1fbps "
                         "basis=%+.1fbps vel=%+.1fbps/s | bn=%.4f hs=%.4f/%.4f sprd=%.1fbps | pos=%.4f",
                         "BUY" if is_buy else "SELL", sym, size,
                         _fmt(price, spec.tick_size), notional,
                         signal_bps, raw_div_bps, st.basis.baseline_bps, vel,
                         st.bn_mid, st.hs_bid, st.hs_ask, hs_spread_bps, st.position)
                st.last_order_ts = now
                return

            log.info("SNIPE LIVE | %s %s %.4f @ %s | signal=%+.1fbps raw=%+.1fbps basis=%+.1fbps",
                     "BUY" if is_buy else "SELL", sym, size,
                     _fmt(price, spec.tick_size), signal_bps, raw_div_bps,
                     st.basis.baseline_bps)
            await self._fire_ioc(sym, is_buy, price, size)

    # ---------------------------------------------------------------
    # Position exit loop
    # ---------------------------------------------------------------

    async def _position_exit_loop(self):
        """Periodically checks open positions for exit conditions."""
        logged_exits: dict[str, float] = {}
        while self._running:
            try:
                await asyncio.sleep(0.5)
                if not self._running:
                    break
                now = time.monotonic()

                for sym, st in self._instruments.items():
                    if abs(st.position) < st.spec.lot_size:
                        logged_exits.pop(sym, None)
                        continue

                    timed_out = (st.entry_ts > 0.0
                                 and now - st.entry_ts > self.cfg.position_timeout_s)

                    if st.bn_mid > 0.0 and st.hs_mid > 0.0:
                        raw_div = (st.bn_mid - st.hs_mid) / st.hs_mid * 10000.0
                        signal = raw_div - st.basis.baseline_bps
                    else:
                        signal = 0.0
                        raw_div = 0.0

                    is_long = st.position > 0.0
                    converged = False
                    if is_long and signal <= self.cfg.exit_threshold_bps:
                        converged = True
                    elif not is_long and signal >= -self.cfg.exit_threshold_bps:
                        converged = True

                    if converged or timed_out:
                        reason = "TIMEOUT" if timed_out else "CONVERGED"
                        hold_s = now - st.entry_ts if st.entry_ts > 0.0 else 0.0

                        last_log = logged_exits.get(sym, 0.0)
                        if now - last_log >= 10.0:
                            log.info("EXIT %s %s: pos=%.6f sig=%+.1fbps raw=%+.1fbps hold=%.1fs",
                                     reason, sym, st.position, signal, raw_div, hold_s)
                            logged_exits[sym] = now

                        if self.cfg.enable_trading:
                            await self._close_position(sym)

            except asyncio.CancelledError:
                return
            except Exception as exc:
                log.error("Exit loop error: %s", exc)

    async def _close_position(self, sym: str):
        async with self._order_lock:
            st = self._instruments[sym]
            if abs(st.position) < st.spec.lot_size:
                return

            is_buy = st.position < 0.0
            size = abs(st.position)
            size = _round_dn(size, st.spec.lot_size)
            if size <= 0.0:
                return

            slip = self.cfg.exit_slippage_bps / 10000.0
            if is_buy:
                ref = st.hs_ask if st.hs_ask > 0.0 else st.bn_mid
                price = ref * (1.0 + slip)
                price = _round_up(price, st.spec.tick_size)
            else:
                ref = st.hs_bid if st.hs_bid > 0.0 else st.bn_mid
                price = ref * (1.0 - slip)
                price = _round_dn(price, st.spec.tick_size)

            log.info("CLOSE %s: %s %.6f @ %s",
                     sym, "BUY" if is_buy else "SELL", size,
                     _fmt(price, st.spec.tick_size))
            await self._fire_ioc(sym, is_buy, price, size)

    # ---------------------------------------------------------------
    # Position poll (REST backup)
    # ---------------------------------------------------------------

    async def _position_poll_loop(self):
        while self._running:
            try:
                await asyncio.sleep(15.0)
                if not self._running:
                    break
                raw = await self._http.request(
                    "info",
                    {"method": "positions", "params": {"user": self.cfg.agent_address}},
                )
                positions = raw if isinstance(raw, list) else []
                for pos in positions:
                    inst = pos.get("instrument", "")
                    if inst in self._instruments:
                        net = float(pos.get("size", 0))
                        st = self._instruments[inst]
                        old = st.position
                        if abs(net - old) > st.spec.lot_size:
                            log.info("REST POLL reconcile %s: %.6f -> %.6f", inst, old, net)
                            st.position = net
            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.warning("Position poll error: %s", exc)

    # ---------------------------------------------------------------
    # Main loop
    # ---------------------------------------------------------------

    async def _main_loop(self):
        """Status logging loop. Actual signals fire from Binance callbacks."""
        while self._running:
            try:
                await asyncio.sleep(10.0)
            except asyncio.CancelledError:
                break

            if not self._running:
                break

            if self._paused and self._total_session_pnl >= -self.cfg.max_total_loss_usd:
                self._paused = False
                log.info("RESUMED: pnl recovered above loss limit")

            parts = []
            for sym, st in self._instruments.items():
                if st.bn_mid <= 0.0 or st.hs_mid <= 0.0:
                    parts.append(f"{sym}:NODATA")
                    continue
                raw_div = (st.bn_mid - st.hs_mid) / st.hs_mid * 10000.0
                basis = st.basis.baseline_bps
                signal = raw_div - basis
                vel = st.velocity.velocity_bps()
                pos_str = f"{st.position:+.4f}" if abs(st.position) > st.spec.lot_size else "flat"
                warm = "ok" if st.basis.warmed_up else "warmup"
                short_sym = sym.replace("-PERP", "")
                parts.append(f"{short_sym}:sig={signal:+.1f} raw={raw_div:+.1f} basis={basis:+.1f} vel={vel:+.1f} pos={pos_str} [{warm}]")

            log.info("STATUS | pnl=$%.4f eq=$%.0f | %s",
                     self._total_session_pnl, self._account_equity,
                     " | ".join(parts))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main():
    cfg = Config.from_env()
    bot = LatencyArbBot(cfg)
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
