
"""
Wick Catcher V2 for Hotstuff DEX
==================================

Aggressive wick catcher that places stacked limit orders at orderbook gaps
and liquidity cliffs to catch fat-finger / wick fills, then immediately
closes position with IOC retry loop to lock in the spread.

Reference: https://armv7l.substack.com/p/the-day-i-meet-my-counterparty-my

V2 changes:
  - Equity-based sizing: total notional per side = equity * EQUITY_NOTIONAL_MULT
  - Orderbook gap mapping: subscribe to full depth, place orders at gaps
  - IOC close retry: up to N attempts with widening slippage
  - Tighter inner levels: 30 bps inner, 250 bps outer, 3 levels

Architecture:
  Account 1 (Quoter)    - stacked GTC quotes at orderbook gaps or fixed offsets,
                           catches wicks, IOC close with retry to lock profit.
  Account 2 (Emergency)  - cross-account safety net. Only fires if Account 1
                           IOC retries all fail.

Event-driven:
  - BBO subscription drives requoting (no polling)
  - Orderbook subscription for gap detection
  - Fill subscription triggers immediate IOC close with retry
  - Account summary subscription tracks equity for dynamic sizing + loss limit
"""

import asyncio
import importlib
import logging
import math
import os
import signal
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

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
from hotstuff.methods.exchange.trading import CancelByCloidParams, UnitCancelByClOrderId
from hotstuff.utils.signing import sign_action as _sign_action
from hotstuff.methods.exchange.op_codes import EXCHANGE_OP_CODES as _OP_CODES

_info_global = importlib.import_module("hotstuff.methods.info.global")
InstrumentsParams = _info_global.InstrumentsParams
TickerParams = _info_global.TickerParams

_sub_global = importlib.import_module("hotstuff.methods.subscription.global")
BBOSubscriptionParams = _sub_global.BBOSubscriptionParams
FillsSubscriptionParams = _sub_global.FillsSubscriptionParams
PositionsSubscriptionParams = _sub_global.PositionsSubscriptionParams
TickerSubscriptionParams = _sub_global.TickerSubscriptionParams
TradeSubscriptionParams = _sub_global.TradeSubscriptionParams

try:
    from hotstuff.methods.info.account import PositionsParams, AccountSummaryParams
except ImportError:
    pass

load_dotenv()


# ---------------------------------------------------------------------------
# Monkey-patch: SDK hardcodes is_testnet=True in _execute_action.
# Override to sign as mainnet.
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

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("wick_catcher")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _round_price_down(price: float, tick: float) -> float:
    if tick <= 0.0:
        return price
    return math.floor(price / tick) * tick


def _round_price_up(price: float, tick: float) -> float:
    if tick <= 0.0:
        return price
    return math.ceil(price / tick) * tick


def _round_size_down(size: float, lot: float) -> float:
    if lot <= 0.0:
        return size
    return math.floor(size / lot) * lot


def _fmt_price(price: float, tick: float) -> str:
    if tick <= 0.0 or tick >= 1.0:
        return f"{price:.0f}"
    d = max(0, -math.floor(math.log10(tick)))
    return f"{price:.{d}f}"


def _fmt_size(size: float, lot: float) -> str:
    if lot <= 0.0 or lot >= 1.0:
        return f"{size:.0f}"
    d = max(0, -math.floor(math.log10(lot)))
    return f"{size:.{d}f}"


def _safe_float(obj, key: str, default: float = 0.0) -> float:
    try:
        if isinstance(obj, dict):
            return float(obj.get(key, default))
        return float(getattr(obj, key, default))
    except (TypeError, ValueError):
        return default


def _safe_str(obj, key: str, default: str = "") -> str:
    if isinstance(obj, dict):
        return str(obj.get(key, default))
    return str(getattr(obj, key, default))


# ---------------------------------------------------------------------------
# OrderbookTracker -- maintains local book from WS snapshot + deltas
# ---------------------------------------------------------------------------

class OrderbookTracker:
    """Maintains a local orderbook from WS snapshot + delta updates.
    Provides gap detection for intelligent order placement."""

    def __init__(self):
        self.bids: List[List[float]] = []   # [[price, size], ...] sorted desc
        self.asks: List[List[float]] = []   # [[price, size], ...] sorted asc
        self.last_update_ts: float = 0.0
        self.seq: int = 0

    def on_message(self, msg):
        data = msg if isinstance(msg, dict) else getattr(msg, "data", msg)
        if isinstance(data, dict) and "data" in data:
            data = data["data"]
        if not isinstance(data, dict):
            return

        update_type = data.get("update_type", "")
        books = data.get("books", data)

        raw_bids = books.get("bids", [])
        raw_asks = books.get("asks", [])
        seq = books.get("sequence_number", 0)

        if update_type == "snapshot" or self.seq == 0:
            self.bids = [[float(b["price"]), float(b["size"])] for b in raw_bids if float(b.get("size", 0)) > 0]
            self.asks = [[float(a["price"]), float(a["size"])] for a in raw_asks if float(a.get("size", 0)) > 0]
            self.bids.sort(key=lambda x: -x[0])
            self.asks.sort(key=lambda x: x[0])
        else:
            self._apply_delta(raw_bids, raw_asks)

        self.seq = seq
        self.last_update_ts = time.monotonic()

    def _apply_delta(self, bid_deltas, ask_deltas):
        for d in bid_deltas:
            px = float(d["price"])
            sz = float(d.get("size", 0))
            found = False
            for i, level in enumerate(self.bids):
                if abs(level[0] - px) < 1e-12:
                    if sz <= 0:
                        self.bids.pop(i)
                    else:
                        level[1] = sz
                    found = True
                    break
            if not found and sz > 0:
                self.bids.append([px, sz])
        for d in ask_deltas:
            px = float(d["price"])
            sz = float(d.get("size", 0))
            found = False
            for i, level in enumerate(self.asks):
                if abs(level[0] - px) < 1e-12:
                    if sz <= 0:
                        self.asks.pop(i)
                    else:
                        level[1] = sz
                    found = True
                    break
            if not found and sz > 0:
                self.asks.append([px, sz])
        self.bids.sort(key=lambda x: -x[0])
        self.asks.sort(key=lambda x: x[0])

    def is_fresh(self, max_age_s: float = 5.0) -> bool:
        if self.seq == 0:
            return False
        return (time.monotonic() - self.last_update_ts) < max_age_s

    def find_bid_gaps(self, mid: float, min_gap_bps: float, max_levels: int = 50) -> List[float]:
        """Find prices just below bid-side liquidity cliffs.
        Returns list of prices where we should place buy orders."""
        gaps = []
        levels = self.bids[:max_levels]
        if len(levels) < 2:
            return gaps
        for i in range(len(levels) - 1):
            upper_px = levels[i][0]
            lower_px = levels[i + 1][0]
            if upper_px <= 0:
                continue
            gap_bps = (upper_px - lower_px) / upper_px * 10000.0
            if gap_bps >= min_gap_bps:
                # Place order just below the cliff (1 tick below upper level)
                gaps.append(lower_px)
        return gaps

    def find_ask_gaps(self, mid: float, min_gap_bps: float, max_levels: int = 50) -> List[float]:
        """Find prices just above ask-side liquidity cliffs.
        Returns list of prices where we should place sell orders."""
        gaps = []
        levels = self.asks[:max_levels]
        if len(levels) < 2:
            return gaps
        for i in range(len(levels) - 1):
            lower_px = levels[i][0]
            upper_px = levels[i + 1][0]
            if lower_px <= 0:
                continue
            gap_bps = (upper_px - lower_px) / lower_px * 10000.0
            if gap_bps >= min_gap_bps:
                gaps.append(upper_px)
        return gaps


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Config:
    # Account 1 - Quoter
    private_key: str
    agent_address: str
    # Account 2 - Emergency hedger
    hedge_private_key: str
    hedge_agent_address: str
    # Instrument
    symbol: str
    # Stacked quoting
    quote_start_bps: float
    quote_end_bps: float
    quote_levels: int
    equity_notional_mult: float
    size_scale_factor: float
    requote_threshold_bps: float
    order_expiry_ms: int
    post_only_quotes: bool
    # Close / hedge
    min_profit_bps: float
    hedge_slippage_bps: float
    close_max_retries: int
    close_retry_widen_bps: float
    emergency_hedge_delay_s: float
    emergency_hedge_slippage_bps: float
    # Risk
    max_position_size: float
    leverage: int
    max_loss_pct: float
    # Orderbook gaps
    gap_min_bps: float
    gap_fallback: bool
    # Execution
    enable_trading: bool
    min_requote_interval_s: float

    @classmethod
    def from_env(cls) -> "Config":
        pk = os.getenv("HOTSTUFF_PRIVATE_KEY", "")
        if not pk:
            log.error("HOTSTUFF_PRIVATE_KEY is not set")
            sys.exit(1)
        addr = os.getenv("HOTSTUFF_AGENT_ADDRESS", "")
        if not addr:
            log.error("HOTSTUFF_AGENT_ADDRESS is not set")
            sys.exit(1)
        h_pk = os.getenv("HEDGE_PRIVATE_KEY", "")
        h_addr = os.getenv("HEDGE_AGENT_ADDRESS", "")
        if not h_pk or not h_addr:
            log.warning("Hedge account not configured -- emergency hedge disabled")
        return cls(
            private_key=pk,
            agent_address=addr,
            hedge_private_key=h_pk,
            hedge_agent_address=h_addr,
            symbol=os.getenv("SYMBOL", "HYPE-PERP"),
            quote_start_bps=float(os.getenv("QUOTE_START_BPS", "30")),
            quote_end_bps=float(os.getenv("QUOTE_END_BPS", "250")),
            quote_levels=int(os.getenv("QUOTE_LEVELS", "3")),
            equity_notional_mult=float(os.getenv("EQUITY_NOTIONAL_MULT", "4.0")),
            size_scale_factor=float(os.getenv("SIZE_SCALE_FACTOR", "1.5")),
            requote_threshold_bps=float(os.getenv("REQUOTE_THRESHOLD_BPS", "50")),
            order_expiry_ms=int(os.getenv("ORDER_EXPIRY_MS", "3600000")),
            post_only_quotes=os.getenv("POST_ONLY_QUOTES", "true").lower() == "true",
            min_profit_bps=float(os.getenv("MIN_PROFIT_BPS", "5")),
            hedge_slippage_bps=float(os.getenv("HEDGE_SLIPPAGE_BPS", "10")),
            close_max_retries=int(os.getenv("CLOSE_MAX_RETRIES", "3")),
            close_retry_widen_bps=float(os.getenv("CLOSE_RETRY_WIDEN_BPS", "20")),
            emergency_hedge_delay_s=float(os.getenv("EMERGENCY_HEDGE_DELAY_S", "2.0")),
            emergency_hedge_slippage_bps=float(os.getenv("EMERGENCY_HEDGE_SLIPPAGE_BPS", "25")),
            max_position_size=float(os.getenv("MAX_POSITION_SIZE", "50.0")),
            leverage=int(os.getenv("LEVERAGE", "20")),
            max_loss_pct=float(os.getenv("MAX_LOSS_PCT", "5.0")),
            gap_min_bps=float(os.getenv("GAP_MIN_BPS", "15")),
            gap_fallback=os.getenv("GAP_FALLBACK", "true").lower() == "true",
            enable_trading=os.getenv("ENABLE_TRADING", "false").lower() == "true",
            min_requote_interval_s=float(os.getenv("MIN_REQUOTE_INTERVAL_S", "30")),
        )

    @property
    def hedge_enabled(self) -> bool:
        return bool(self.hedge_private_key and self.hedge_agent_address)


# ---------------------------------------------------------------------------
# Mutable state
# ---------------------------------------------------------------------------

@dataclass
class State:
    mid_price: float = 0.0
    best_bid: float = 0.0
    best_ask: float = 0.0
    last_quote_mid: float = 0.0
    net_position: float = 0.0
    account_equity: float = 0.0
    hedge_net_position: float = 0.0
    instrument_id: int = 0
    tick_size: float = 0.01
    lot_size: float = 0.01
    cloid_seq: int = 0
    # Cash-flow PnL (running sum of all fills -- only accurate when flat)
    cashflow_pnl: float = 0.0
    # Round-trip PnL (only updates on close fills -- safe for loss limit)
    round_trip_pnl: float = 0.0
    # Entry tracking for round-trip PnL
    avg_entry_price: float = 0.0
    entry_total_cost: float = 0.0
    entry_total_size: float = 0.0
    total_wick_fills: int = 0
    total_close_fills: int = 0
    total_emergency: int = 0
    paused: bool = False
    last_requote_ts: float = 0.0


# ---------------------------------------------------------------------------
# Bot
# ---------------------------------------------------------------------------

class WickCatcher:

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.st = State()
        self._running = False
        self._subs: List[Dict[str, Any]] = []
        self._quote_lock = asyncio.Lock()
        self._hedge_lock = asyncio.Lock()
        self._requote_event = asyncio.Event()
        self._active_cloids: List[str] = []
        self._ob = OrderbookTracker()
        # Timestamp until which position subscription updates that DECREASE
        # exposure are rejected (prevents stale sub data from overriding fills)
        self._pos_fill_lock_ts: float = 0.0

        self._level_offsets: List[float] = []
        self._scale_weights: List[float] = []
        self._build_level_offsets()

        self._http: Optional[HttpTransport] = None
        self._ws: Optional[WebSocketTransport] = None
        self._info: Optional[InfoClient] = None
        self._exchange: Optional[ExchangeClient] = None
        self._hedge_exchange: Optional[ExchangeClient] = None
        self._sub_client: Optional[SubscriptionClient] = None

    def _build_level_offsets(self):
        """Precompute bps offsets and geometric scale weights (sizes computed at requote time)."""
        n = self.cfg.quote_levels
        start = self.cfg.quote_start_bps
        end = self.cfg.quote_end_bps
        scale = self.cfg.size_scale_factor

        if n <= 0:
            return
        if n == 1:
            self._level_offsets = [start / 10000.0]
            self._scale_weights = [1.0]
            return

        step = (end - start) / (n - 1)
        offsets = []
        weights = []
        for i in range(n):
            offsets.append((start + i * step) / 10000.0)
            weights.append(scale ** i)
        self._level_offsets = offsets
        self._scale_weights = weights

    def _compute_level_sizes(self, mid: float) -> List[float]:
        """Compute per-level sizes from equity, notional mult, and mid price."""
        equity = self.st.account_equity
        if equity <= 0.0 or mid <= 0.0:
            return [0.0] * len(self._scale_weights)
        total_notional = equity * self.cfg.equity_notional_mult
        total_size = total_notional / mid
        weight_sum = sum(self._scale_weights)
        if weight_sum <= 0.0:
            return [0.0] * len(self._scale_weights)
        per_weight = total_size / weight_sum
        return [per_weight * w for w in self._scale_weights]

    # ---------------------------------------------------------------
    # Lifecycle
    # ---------------------------------------------------------------

    async def start(self):
        log.info("=== Wick Catcher V2 starting ===")
        log.info("Symbol          : %s", self.cfg.symbol)
        log.info("Network         : MAINNET")
        log.info("Trading         : %s", "LIVE" if self.cfg.enable_trading else "DRY-RUN")
        log.info("Equity mult     : %.1fx (notional per side = equity * this)", self.cfg.equity_notional_mult)
        log.info("Quote levels    : %d (%.0f bps -> %.0f bps)",
                 self.cfg.quote_levels, self.cfg.quote_start_bps, self.cfg.quote_end_bps)
        log.info("Scale factor    : %.2f", self.cfg.size_scale_factor)
        log.info("Max position    : %.4f", self.cfg.max_position_size)
        log.info("Leverage        : %dx", self.cfg.leverage)
        log.info("Loss limit      : %.1f%% of equity", self.cfg.max_loss_pct)
        log.info("Requote thresh  : %.0f bps / min %.0fs", self.cfg.requote_threshold_bps, self.cfg.min_requote_interval_s)
        log.info("Close retries   : %d (widen +%.0f bps each)", self.cfg.close_max_retries, self.cfg.close_retry_widen_bps)
        log.info("Gap detection   : min %.0f bps | fallback=%s", self.cfg.gap_min_bps, self.cfg.gap_fallback)
        log.info("Emergency hedge : %s", "ENABLED" if self.cfg.hedge_enabled else "DISABLED")

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
            is_testnet=False,
            timeout=15.0,
            keep_alive={"interval": 20.0, "timeout": 10.0},
            auto_connect=True,
            server=ws_server,
        ))

        wallet_1 = Account.from_key(self.cfg.private_key)
        self._info = InfoClient(transport=self._http)
        self._exchange = ExchangeClient(transport=self._http, wallet=wallet_1)
        self._sub_client = SubscriptionClient(transport=self._ws)

        if self.cfg.hedge_enabled:
            wallet_2 = Account.from_key(self.cfg.hedge_private_key)
            self._hedge_exchange = ExchangeClient(transport=self._http, wallet=wallet_2)
            log.info("Hedge account loaded: %s", self.cfg.hedge_agent_address)

        await self._resolve_instrument()
        await self._set_leverage()
        await self._sync_account_equity()
        await self._sync_positions()
        await self._fetch_initial_prices()

        # Log computed sizes after equity is known
        mid = self.st.mid_price if self.st.mid_price > 0 else 30.0
        sizes = self._compute_level_sizes(mid)
        for i, (off, sz) in enumerate(zip(self._level_offsets, sizes)):
            log.info("  Level %d: %.2f bps | size ~%.2f (~$%.0f)",
                     i, off * 10000, sz, sz * mid)
        log.info("Total size/side : ~%.2f (~$%.0f)", sum(sizes), sum(sizes) * mid)

        if self.cfg.enable_trading:
            await self._cancel_all()

        await self._subscribe_all()

        self._running = True
        log.info("=== Bot ready -- event-driven quoting active ===")
        await self._run_loop()

    async def stop(self):
        log.info("Shutting down...")
        self._running = False
        self._requote_event.set()

        if self.cfg.enable_trading and self._exchange:
            try:
                await self._cancel_all()
                log.info("Account 1 orders cancelled")
            except Exception as exc:
                log.error("Cancel on shutdown failed: %s", exc)

        for sub in self._subs:
            try:
                await sub["unsubscribe"]()
            except Exception:
                pass
        self._subs.clear()

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

        log.info("Round-trip PnL: $%.2f | Cashflow PnL: $%.2f | Wick fills: %d | Close fills: %d | Emergency: %d",
                 self.st.round_trip_pnl, self.st.cashflow_pnl,
                 self.st.total_wick_fills, self.st.total_close_fills, self.st.total_emergency)
        log.info("=== Shutdown complete ===")

    # ---------------------------------------------------------------
    # Initialisation
    # ---------------------------------------------------------------

    async def _resolve_instrument(self):
        raw = await self._http.request(
            "info",
            {"method": "instruments", "params": {"type": "perps"}},
        )
        perps = raw.get("perps", []) if isinstance(raw, dict) else []
        for p in perps:
            if p.get("name") == self.cfg.symbol:
                self.st.instrument_id = int(p["id"])
                self.st.tick_size = float(p.get("tick_size", 0.01))
                self.st.lot_size = float(p.get("lot_size", 0.01))
                max_lev = int(p.get("max_leverage", 1))
                log.info("Instrument: %s id=%d tick=%.6f lot=%.6f maxLev=%d",
                         self.cfg.symbol, self.st.instrument_id,
                         self.st.tick_size, self.st.lot_size, max_lev)
                return
        log.error("Instrument %s not found on Hotstuff", self.cfg.symbol)
        sys.exit(1)

    async def _set_leverage(self):
        if not self.cfg.enable_trading:
            return
        try:
            from hotstuff.methods.exchange.account import UpdatePerpInstrumentLeverageParams
            params = UpdatePerpInstrumentLeverageParams(
                instrument_id=self.st.instrument_id,
                leverage=self.cfg.leverage,
            )
            await self._exchange.update_perp_instrument_leverage(params)
            log.info("Account 1 leverage: %dx", self.cfg.leverage)
            if self._hedge_exchange:
                await self._hedge_exchange.update_perp_instrument_leverage(params)
                log.info("Account 2 leverage: %dx", self.cfg.leverage)
        except Exception as exc:
            log.warning("Set leverage failed: %s", exc)

    async def _sync_account_equity(self):
        try:
            raw = await self._http.request(
                "info",
                {"method": "accountSummary", "params": {"user": self.cfg.agent_address}},
            )
            equity = float(raw.get("total_account_equity", 0))
            if equity > 0.0:
                self.st.account_equity = equity
                log.info("Account 1 equity: $%.2f", equity)
            else:
                avail = float(raw.get("available_balance", 0))
                if avail > 0.0:
                    self.st.account_equity = avail
                    log.info("Account 1 equity (from avail): $%.2f", avail)
        except Exception as exc:
            log.warning("Equity sync failed: %s", exc)

    async def _sync_positions(self):
        try:
            raw = await self._http.request(
                "info",
                {"method": "positions", "params": {"user": self.cfg.agent_address}},
            )
            positions = raw if isinstance(raw, list) else []
            self.st.net_position = self._calc_net_position(positions)
            log.info("Account 1 net position: %.4f", self.st.net_position)
        except Exception as exc:
            log.warning("Position sync failed: %s", exc)
        if self.cfg.hedge_enabled:
            try:
                raw = await self._http.request(
                    "info",
                    {"method": "positions", "params": {"user": self.cfg.hedge_agent_address}},
                )
                positions = raw if isinstance(raw, list) else []
                self.st.hedge_net_position = self._calc_net_position(positions)
                log.info("Account 2 net position: %.4f", self.st.hedge_net_position)
            except Exception as exc:
                log.warning("Hedge position sync failed: %s", exc)

    async def _fetch_initial_prices(self):
        try:
            raw = await self._http.request(
                "info",
                {"method": "ticker", "params": {"symbol": self.cfg.symbol}},
            )
            tickers = raw if isinstance(raw, list) else [raw] if raw else []
            if tickers:
                t = tickers[0]
                bid = float(t.get("best_bid_price", 0))
                ask = float(t.get("best_ask_price", 0))
                mid = float(t.get("mid_price", 0))
                mark = float(t.get("mark_price", 0))
                if bid > 0.0:
                    self.st.best_bid = bid
                if ask > 0.0:
                    self.st.best_ask = ask
                if mid > 0.0:
                    self.st.mid_price = mid
                elif bid > 0.0 and ask > 0.0:
                    self.st.mid_price = (bid + ask) * 0.5
                elif mark > 0.0:
                    self.st.mid_price = mark
                log.info("Prices: mid=%.4f bid=%.4f ask=%.4f mark=%.4f",
                         self.st.mid_price, self.st.best_bid, self.st.best_ask, mark)
        except Exception as exc:
            log.error("Ticker fetch failed: %s", exc)

    # ---------------------------------------------------------------
    # Subscriptions
    # ---------------------------------------------------------------

    async def _subscribe_all(self):
        addr1 = self.cfg.agent_address
        sym = self.cfg.symbol

        sub = await self._sub_client.bbo(
            BBOSubscriptionParams(symbol=sym), self._on_bbo)
        self._subs.append(sub)
        log.info("Sub: BBO")

        sub = await self._sub_client.ticker(
            TickerSubscriptionParams(symbol=sym), self._on_ticker)
        self._subs.append(sub)
        log.info("Sub: ticker")

        sub = await self._sub_client.trade(
            TradeSubscriptionParams(instrument_id=sym), self._on_market_trade)
        self._subs.append(sub)
        log.info("Sub: trades")

        # Orderbook depth subscription for gap detection
        sub = await self._ws.subscribe(
            "orderbook", {"symbol": sym}, self._on_orderbook)
        self._subs.append(sub)
        log.info("Sub: orderbook depth")

        sub = await self._sub_client.fills(
            FillsSubscriptionParams(address=addr1), self._on_fill)
        self._subs.append(sub)
        log.info("Sub: Account 1 fills")

        sub = await self._ws.subscribe(
            "orders", {"user": addr1}, self._on_order_update)
        self._subs.append(sub)
        log.info("Sub: Account 1 orders")

        sub = await self._sub_client.positions(
            PositionsSubscriptionParams(address=addr1), self._on_position_update)
        self._subs.append(sub)
        log.info("Sub: Account 1 positions")

        sub = await self._ws.subscribe(
            "account_summary", {"user": addr1}, self._on_account_summary)
        self._subs.append(sub)
        log.info("Sub: Account 1 summary")

        if self.cfg.hedge_enabled:
            addr2 = self.cfg.hedge_agent_address
            sub = await self._sub_client.fills(
                FillsSubscriptionParams(address=addr2), self._on_hedge_fill)
            self._subs.append(sub)
            log.info("Sub: Account 2 fills")

            sub = await self._sub_client.positions(
                PositionsSubscriptionParams(address=addr2), self._on_hedge_position_update)
            self._subs.append(sub)
            log.info("Sub: Account 2 positions")

    # ---------------------------------------------------------------
    # Callbacks -- market data
    # ---------------------------------------------------------------

    def _on_bbo(self, msg):
        data = msg.data
        bid = _safe_float(data, "best_bid_price") or _safe_float(data, "bestBidPrice")
        ask = _safe_float(data, "best_ask_price") or _safe_float(data, "bestAskPrice")
        if bid > 0.0:
            self.st.best_bid = bid
        if ask > 0.0:
            self.st.best_ask = ask

        if bid > 0.0 and ask > 0.0:
            self.st.mid_price = (bid + ask) * 0.5
        elif bid > 0.0:
            self.st.mid_price = bid
        elif ask > 0.0:
            self.st.mid_price = ask
        else:
            return

        last = self.st.last_quote_mid
        if last > 0.0:
            move_bps = abs(self.st.mid_price - last) / last * 10000.0
            if move_bps >= self.cfg.requote_threshold_bps:
                self._requote_event.set()
        elif self.st.mid_price > 0.0:
            self._requote_event.set()

    def _on_ticker(self, msg):
        data = msg.data
        mid = _safe_float(data, "mid_price") or _safe_float(data, "midPrice")
        if mid > 0.0:
            self.st.mid_price = mid
        bid = _safe_float(data, "best_bid_price") or _safe_float(data, "bestBidPrice")
        ask = _safe_float(data, "best_ask_price") or _safe_float(data, "bestAskPrice")
        if bid > 0.0:
            self.st.best_bid = bid
        if ask > 0.0:
            self.st.best_ask = ask

    def _on_market_trade(self, msg):
        data = msg.data
        price = _safe_float(data, "price")
        size = _safe_float(data, "size")
        if price > 0.0 and size > 0.0:
            notional = price * size
            if notional > 5000.0:
                side = _safe_str(data, "side")
                log.info("LARGE TRADE: %s %.4f @ %.4f ($%.0f)", side, size, price, notional)

    def _on_orderbook(self, msg):
        data = msg if isinstance(msg, dict) else getattr(msg, "data", msg)
        self._ob.on_message(data)

    # ---------------------------------------------------------------
    # Callbacks -- Account 1 fills
    # ---------------------------------------------------------------

    def _on_fill(self, msg):
        raw = msg.data
        fills = raw if isinstance(raw, list) else [raw]
        for f in fills:
            inst = _safe_str(f, "instrument")
            if inst and inst != self.cfg.symbol:
                continue
            side = _safe_str(f, "side")
            price = _safe_float(f, "price")
            size = _safe_float(f, "size")
            if size <= 0.0 or price <= 0.0:
                continue

            is_buy = side in ("b", "buy", "B", "BUY")

            # Cash-flow PnL (only correct when position is fully flat)
            if is_buy:
                self.st.cashflow_pnl -= price * size
            else:
                self.st.cashflow_pnl += price * size

            prev_abs = abs(self.st.net_position)
            if is_buy:
                self.st.net_position += size
            else:
                self.st.net_position -= size
            new_abs = abs(self.st.net_position)

            half_lot = self.st.lot_size * 0.5
            if new_abs > prev_abs + half_lot:
                # WICK FILL: exposure increased -- update entry tracking
                self.st.entry_total_cost += price * size
                self.st.entry_total_size += size
                self.st.avg_entry_price = self.st.entry_total_cost / self.st.entry_total_size
                # Lock position from stale subscription overrides for 5 seconds
                self._pos_fill_lock_ts = time.monotonic() + 5.0
                self.st.total_wick_fills += 1
                log.info("WICK FILL #%d: %s %.4f @ %.4f | net=%.4f | avg_entry=%.4f | rt_pnl=$%.2f",
                         self.st.total_wick_fills, side, size, price,
                         self.st.net_position, self.st.avg_entry_price,
                         self.st.round_trip_pnl)
                asyncio.ensure_future(self._close_position(side, size, price))
            else:
                # CLOSE FILL: exposure decreased -- compute round-trip profit
                chunk_pnl = 0.0
                if self.st.avg_entry_price > 0.0:
                    if is_buy:
                        chunk_pnl = (self.st.avg_entry_price - price) * size
                    else:
                        chunk_pnl = (price - self.st.avg_entry_price) * size
                    self.st.round_trip_pnl += chunk_pnl
                    self.st.entry_total_size -= size
                    if self.st.entry_total_size <= self.st.lot_size * 0.5:
                        self.st.entry_total_cost = 0.0
                        self.st.entry_total_size = 0.0
                        self.st.avg_entry_price = 0.0
                    else:
                        self.st.entry_total_cost = self.st.avg_entry_price * self.st.entry_total_size
                self.st.total_close_fills += 1
                log.info("CLOSE FILL #%d: %s %.4f @ %.4f | net=%.4f | rt_pnl=$%.2f (chunk=$%.4f)",
                         self.st.total_close_fills, side, size, price,
                         self.st.net_position, self.st.round_trip_pnl, chunk_pnl)

    def _on_order_update(self, msg):
        raw = msg.data
        orders = raw if isinstance(raw, list) else [raw]
        for o in orders:
            if not isinstance(o, dict):
                continue
            oid = o.get("order_id") or o.get("orderId") or o.get("oid", "?")
            status = o.get("status", "?")
            side = o.get("side", "?")
            price = o.get("price", "?")
            size = o.get("size") or o.get("original_size") or o.get("originalSize", "?")
            filled = o.get("filled_size") or o.get("filledSize", "0")
            cloid = o.get("cloid", "")
            inst = o.get("instrument") or o.get("instrumentId", "")
            log.info("ORDER: id=%s %s %s px=%s sz=%s filled=%s status=%s cloid=%s",
                     oid, side, inst, price, size, filled, status, cloid)

    def _on_position_update(self, msg):
        data = msg.data
        positions = data if isinstance(data, list) else [data]
        net = self._calc_net_position(positions)
        drift = abs(net - self.st.net_position)

        # Guard: after a wick fill, subscription may deliver stale data
        # (fill not yet propagated server-side). Reject updates that would
        # DECREASE our tracked exposure during the lock window.
        now = time.monotonic()
        if now < self._pos_fill_lock_ts:
            sub_abs = abs(net)
            tracked_abs = abs(self.st.net_position)
            if sub_abs < tracked_abs - self.st.lot_size:
                log.warning("Position sub BLOCKED (stale): sub=%.4f < tracked=%.4f (lock %.1fs left)",
                            net, self.st.net_position, self._pos_fill_lock_ts - now)
                return

        if drift > self.st.lot_size:
            log.info("Position reconcile: fill-tracked=%.4f sub=%.4f (drift=%.4f)",
                     self.st.net_position, net, drift)
        self.st.net_position = net

    def _on_account_summary(self, msg):
        data = msg.data
        equity = (_safe_float(data, "total_account_equity")
                  or _safe_float(data, "totalAccountEquity")
                  or _safe_float(data, "total_equity")
                  or _safe_float(data, "totalEquity"))
        if equity > 0.0:
            self.st.account_equity = equity
            log.debug("Account 1 equity: $%.2f", equity)

    # ---------------------------------------------------------------
    # Callbacks -- Account 2
    # ---------------------------------------------------------------

    def _on_hedge_fill(self, msg):
        raw = msg.data
        fills = raw if isinstance(raw, list) else [raw]
        for f in fills:
            side = _safe_str(f, "side")
            price = _safe_float(f, "price")
            size = _safe_float(f, "size")
            if size > 0.0:
                log.info("EMERGENCY FILL: Account 2 %s %.4f @ %.4f", side, size, price)

    def _on_hedge_position_update(self, msg):
        data = msg.data
        positions = data if isinstance(data, list) else [data]
        self.st.hedge_net_position = self._calc_net_position(positions)
        log.info("Account 2 position -> net: %.4f", self.st.hedge_net_position)

    # ---------------------------------------------------------------
    # Helpers
    # ---------------------------------------------------------------

    def _calc_net_position(self, positions) -> float:
        """Parse net position from API/subscription data.
        Hotstuff uses signed size: positive = long, negative = short.
        position_side is always 'BOTH' (one-way mode)."""
        net = 0.0
        if not positions:
            return net
        for pos in positions:
            inst = (_safe_str(pos, "instrument")
                    or _safe_str(pos, "instrument_name"))
            if inst and inst != self.cfg.symbol:
                inst_id = 0
                try:
                    inst_id = int(_safe_float(pos, "instrument_id"))
                except (TypeError, ValueError):
                    pass
                if inst_id != self.st.instrument_id:
                    continue
            # size is SIGNED: positive = long, negative = short
            size = _safe_float(pos, "size")
            net += size
        return net

    def _get_max_loss_usd(self) -> float:
        equity = self.st.account_equity
        if equity <= 0.0:
            return 500.0
        return equity * self.cfg.max_loss_pct / 100.0

    # ---------------------------------------------------------------
    # Event-driven main loop
    # ---------------------------------------------------------------

    async def _run_loop(self):
        requote_task = asyncio.create_task(self._requote_loop())
        sync_task = asyncio.create_task(self._sync_loop())
        await asyncio.gather(requote_task, sync_task)

    async def _requote_loop(self):
        while self._running:
            await self._requote_event.wait()
            self._requote_event.clear()
            if not self._running:
                break
            now = time.monotonic()
            elapsed = now - self.st.last_requote_ts
            remaining = self.cfg.min_requote_interval_s - elapsed
            if remaining > 0.0:
                log.debug("Requote throttled: %.1fs until next allowed", remaining)
                await asyncio.sleep(remaining)
                self._requote_event.clear()
                if not self._running:
                    break
            try:
                await self._do_requote()
                self.st.last_requote_ts = time.monotonic()
            except Exception as exc:
                log.error("Requote error: %s", exc)
            await asyncio.sleep(0.2)

    async def _sync_loop(self):
        while self._running:
            await asyncio.sleep(60.0)
            if not self._running:
                break
            try:
                await self._sync_positions()
            except Exception:
                pass
            try:
                await self._sync_account_equity()
            except Exception:
                pass

    # ---------------------------------------------------------------
    # Requoting
    # ---------------------------------------------------------------

    async def _do_requote(self):
        mid = self.st.mid_price
        if mid <= 0.0:
            return

        if self.st.paused:
            return

        max_loss = self._get_max_loss_usd()
        if self.st.round_trip_pnl < -max_loss:
            if not self.st.paused:
                log.warning("Loss limit hit: rt_pnl=$%.2f (%.1f%% of $%.2f equity). Pausing.",
                            self.st.round_trip_pnl, self.cfg.max_loss_pct,
                            self.st.account_equity)
                self.st.paused = True
                if self.cfg.enable_trading:
                    await self._cancel_all()
            return

        if not self.cfg.enable_trading:
            sizes = self._compute_level_sizes(mid)
            log.info("[DRY] mid=%.4f pos=%.4f equity=$%.2f rt_pnl=$%.2f",
                     mid, self.st.net_position,
                     self.st.account_equity, self.st.round_trip_pnl)
            for i, (off, sz) in enumerate(zip(self._level_offsets, sizes)):
                log.info("[DRY]   L%d buy=%.4f sell=%.4f sz=%.2f",
                         i, mid * (1.0 - off), mid * (1.0 + off), sz)
            self.st.last_quote_mid = mid
            return

        async with self._quote_lock:
            await self._place_quotes(mid)

    # ---------------------------------------------------------------
    # Stacked quoting -- gap-aware + equity-based sizing
    # ---------------------------------------------------------------

    async def _place_quotes(self, mid: float):
        tick = self.st.tick_size
        lot = self.st.lot_size
        net = self.st.net_position
        max_pos = self.cfg.max_position_size
        level_sizes = self._compute_level_sizes(mid)

        # Determine price levels: gaps if available, else fixed offsets
        buy_prices, sell_prices = self._get_quote_prices(mid, tick)

        orders: List[UnitOrder] = []
        new_cloids: List[str] = []
        now_ms = int(time.time() * 1000)
        expiry = now_ms + self.cfg.order_expiry_ms
        total_buy_sz = 0.0
        total_sell_sz = 0.0

        n_levels = min(len(buy_prices), len(sell_prices), len(level_sizes))

        for i in range(n_levels):
            raw_size = level_sizes[i]
            buy_px = buy_prices[i]
            sell_px = sell_prices[i]

            # --- Buy side ---
            buy_sz = _round_size_down(raw_size, lot)
            remaining_buy = max_pos - net - total_buy_sz
            if buy_sz > remaining_buy:
                buy_sz = _round_size_down(max(0.0, remaining_buy), lot)

            if buy_sz > 0.0 and buy_px > 0.0:
                self.st.cloid_seq += 1
                cloid = f"wc-b{i}-{self.st.cloid_seq}"
                orders.append(UnitOrder(
                    instrument_id=self.st.instrument_id,
                    side="b",
                    position_side="BOTH",
                    price=_fmt_price(buy_px, tick),
                    size=_fmt_size(buy_sz, lot),
                    tif="GTC",
                    ro=False,
                    po=self.cfg.post_only_quotes,
                    cloid=cloid,
                ))
                new_cloids.append(cloid)
                total_buy_sz += buy_sz

            # --- Sell side ---
            sell_sz = _round_size_down(raw_size, lot)
            remaining_sell = max_pos + net - total_sell_sz
            if sell_sz > remaining_sell:
                sell_sz = _round_size_down(max(0.0, remaining_sell), lot)

            if sell_sz > 0.0 and sell_px > 0.0:
                self.st.cloid_seq += 1
                cloid = f"wc-s{i}-{self.st.cloid_seq}"
                orders.append(UnitOrder(
                    instrument_id=self.st.instrument_id,
                    side="s",
                    position_side="BOTH",
                    price=_fmt_price(sell_px, tick),
                    size=_fmt_size(sell_sz, lot),
                    tif="GTC",
                    ro=False,
                    po=self.cfg.post_only_quotes,
                    cloid=cloid,
                ))
                new_cloids.append(cloid)
                total_sell_sz += sell_sz

        if not orders:
            log.info("No quotes (limits reached). net=%.4f", net)
            return

        old_cloids = list(self._active_cloids)
        try:
            result = await self._exchange.place_order(
                PlaceOrderParams(orders=orders, expires_after=expiry)
            )
            self._active_cloids = new_cloids
            self.st.last_quote_mid = mid
            n_buy = sum(1 for o in orders if o.side == "b")
            n_sell = sum(1 for o in orders if o.side == "s")
            gap_mode = "GAP" if self._ob.is_fresh() else "FIXED"
            log.info("QUOTES [%s]: %d buy (%.2f) + %d sell (%.2f) mid=%.4f eq=$%.0f",
                     gap_mode, n_buy, total_buy_sz, n_sell, total_sell_sz,
                     mid, self.st.account_equity)
        except Exception as exc:
            log.error("Place quotes failed: %s", exc)
            return

        if old_cloids:
            await self._cancel_cloids(old_cloids)

    def _get_quote_prices(self, mid: float, tick: float) -> Tuple[List[float], List[float]]:
        """Get quote prices from orderbook gaps or fall back to fixed bps offsets."""
        n = len(self._level_offsets)

        # Try gap-based pricing
        if self._ob.is_fresh():
            bid_gaps = self._ob.find_bid_gaps(mid, self.cfg.gap_min_bps, max_levels=60)
            ask_gaps = self._ob.find_ask_gaps(mid, self.cfg.gap_min_bps, max_levels=60)

            # Filter: only gaps within reasonable range of mid (within quote_end_bps * 2)
            max_dist = self.cfg.quote_end_bps * 2.0 / 10000.0
            bid_gaps = [p for p in bid_gaps if p > 0 and (mid - p) / mid <= max_dist]
            ask_gaps = [p for p in ask_gaps if p > 0 and (p - mid) / mid <= max_dist]

            # Sort by distance from mid (closest first)
            bid_gaps.sort(key=lambda p: mid - p)
            ask_gaps.sort(key=lambda p: p - mid)

            if bid_gaps and ask_gaps:
                buy_prices = [_round_price_down(p, tick) for p in bid_gaps[:n]]
                sell_prices = [_round_price_up(p, tick) for p in ask_gaps[:n]]
                # Pad with fixed offsets if not enough gaps
                while len(buy_prices) < n:
                    idx = len(buy_prices)
                    off = self._level_offsets[min(idx, len(self._level_offsets) - 1)]
                    buy_prices.append(_round_price_down(mid * (1.0 - off), tick))
                while len(sell_prices) < n:
                    idx = len(sell_prices)
                    off = self._level_offsets[min(idx, len(self._level_offsets) - 1)]
                    sell_prices.append(_round_price_up(mid * (1.0 + off), tick))

                for i, (bp, sp) in enumerate(zip(buy_prices, sell_prices)):
                    b_bps = (mid - bp) / mid * 10000 if mid > 0 else 0
                    s_bps = (sp - mid) / mid * 10000 if mid > 0 else 0
                    log.debug("  Gap L%d: buy=%.4f (%.0f bps) sell=%.4f (%.0f bps)",
                              i, bp, b_bps, sp, s_bps)
                return buy_prices, sell_prices

        # Fallback: fixed bps offsets
        if not self.cfg.gap_fallback and self._ob.is_fresh():
            return [], []

        buy_prices = [_round_price_down(mid * (1.0 - off), tick) for off in self._level_offsets]
        sell_prices = [_round_price_up(mid * (1.0 + off), tick) for off in self._level_offsets]
        return buy_prices, sell_prices

    # ---------------------------------------------------------------
    # Two-tier close with IOC retry
    # ---------------------------------------------------------------

    async def _close_position(self, fill_side: str, fill_size: float, fill_price: float):
        async with self._hedge_lock:
            closed = await self._primary_close_with_retry(fill_side, fill_size, fill_price)

            if closed:
                return

            if not self.cfg.hedge_enabled or not self._hedge_exchange:
                return

            await asyncio.sleep(self.cfg.emergency_hedge_delay_s)

            # Use the larger of net_position and fill_size to avoid stale-sub zero
            residual = max(abs(self.st.net_position), fill_size)
            if residual <= self.st.lot_size:
                return

            log.info("Residual %.4f after %d close retries. Emergency hedge.",
                     residual, self.cfg.close_max_retries)
            await self._emergency_hedge(fill_side, residual)

    async def _primary_close_with_retry(self, fill_side: str, fill_size: float, fill_price: float) -> bool:
        """IOC close with retry loop. Returns True if position is flat."""
        is_buy_fill = fill_side in ("b", "buy", "B", "BUY")
        close_side = "s" if is_buy_fill else "b"
        tick = self.st.tick_size
        lot = self.st.lot_size
        base_slip = self.cfg.hedge_slippage_bps
        widen = self.cfg.close_retry_widen_bps

        for attempt in range(self.cfg.close_max_retries):
            # On first attempt, use fill_size directly (we KNOW this is the right amount).
            # On retries, use net_position (which should be stable by then).
            if attempt == 0:
                residual = fill_size
            else:
                residual = abs(self.st.net_position)
            if residual <= self.st.lot_size:
                return True

            # Profit check on first attempt only
            if attempt == 0:
                if is_buy_fill:
                    ref_px = self.st.best_bid
                    if ref_px <= 0.0:
                        log.warning("Close skipped: no best_bid")
                        return False
                    spread_bps = (ref_px - fill_price) / fill_price * 10000.0
                    log.info("Profit check [BUY fill]: fill=%.4f best_bid=%.4f spread=%.1f bps",
                             fill_price, ref_px, spread_bps)
                else:
                    ref_px = self.st.best_ask
                    if ref_px <= 0.0:
                        log.warning("Close skipped: no best_ask")
                        return False
                    spread_bps = (fill_price - ref_px) / fill_price * 10000.0
                    log.info("Profit check [SELL fill]: fill=%.4f best_ask=%.4f spread=%.1f bps",
                             fill_price, ref_px, spread_bps)

                if spread_bps < self.cfg.min_profit_bps:
                    log.warning("Close SKIPPED: spread=%.1f bps < min=%.1f bps -> emergency hedge",
                                spread_bps, self.cfg.min_profit_bps)
                    return False

            if not self.cfg.enable_trading:
                log.info("[DRY] Would close attempt %d: %s %.4f", attempt + 1, close_side, residual)
                return True

            slip_bps = base_slip + attempt * widen
            slip = slip_bps / 10000.0

            if is_buy_fill:
                raw_px = self.st.best_bid * (1.0 - slip) if self.st.best_bid > 0 else 0.0
                close_px = _round_price_down(raw_px, tick)
            else:
                raw_px = self.st.best_ask * (1.0 + slip) if self.st.best_ask > 0 else 0.0
                close_px = _round_price_up(raw_px, tick)

            close_sz = _round_size_down(residual, lot)
            if close_sz <= 0.0 or close_px <= 0.0:
                log.warning("Close retry %d skipped: px=%.4f sz=%.4f", attempt + 1, close_px, close_sz)
                return False

            self.st.cloid_seq += 1
            cloid = f"wc-c{close_side}-r{attempt}-{self.st.cloid_seq}"
            now_ms = int(time.time() * 1000)

            try:
                result = await self._exchange.place_order(
                    PlaceOrderParams(
                        orders=[UnitOrder(
                            instrument_id=self.st.instrument_id,
                            side=close_side,
                            position_side="BOTH",
                            price=_fmt_price(close_px, tick),
                            size=_fmt_size(close_sz, lot),
                            tif="IOC",
                            ro=False,
                            po=False,
                            cloid=cloid,
                        )],
                        expires_after=now_ms + 60_000,
                    )
                )
                log.info("CLOSE IOC attempt %d/%d: %s %.4f @ %.4f (slip=%.0f bps) -> %s",
                         attempt + 1, self.cfg.close_max_retries,
                         close_side, close_sz, close_px, slip_bps, result)
            except Exception as exc:
                log.error("CLOSE attempt %d FAILED: %s", attempt + 1, exc)

            # Wait for fill to propagate
            await asyncio.sleep(0.5)

        residual = abs(self.st.net_position)
        return residual <= self.st.lot_size

    async def _emergency_hedge(self, original_fill_side: str, residual_size: float):
        if not self.cfg.enable_trading:
            log.info("[DRY] Emergency: Account 2 would take %.4f", residual_size)
            return

        tick = self.st.tick_size
        lot = self.st.lot_size
        slip = self.cfg.emergency_hedge_slippage_bps / 10000.0
        is_buy_fill = original_fill_side in ("b", "buy", "B", "BUY")

        if is_buy_fill:
            hedge_side = "s"
            raw_px = self.st.best_bid * (1.0 - slip) if self.st.best_bid > 0.0 else 0.0
            hedge_px = _round_price_down(raw_px, tick)
        else:
            hedge_side = "b"
            raw_px = self.st.best_ask * (1.0 + slip) if self.st.best_ask > 0.0 else 0.0
            hedge_px = _round_price_up(raw_px, tick)

        hedge_sz = _round_size_down(residual_size, lot)
        if hedge_sz <= 0.0 or hedge_px <= 0.0:
            log.warning("Emergency hedge skipped: px=%.4f sz=%.4f", hedge_px, hedge_sz)
            return

        self.st.cloid_seq += 1
        cloid = f"wc-em-{self.st.cloid_seq}"
        now_ms = int(time.time() * 1000)

        try:
            result = await self._hedge_exchange.place_order(
                PlaceOrderParams(
                    orders=[UnitOrder(
                        instrument_id=self.st.instrument_id,
                        side=hedge_side,
                        position_side="BOTH",
                        price=_fmt_price(hedge_px, tick),
                        size=_fmt_size(hedge_sz, lot),
                        tif="IOC",
                        ro=False,
                        po=False,
                        cloid=cloid,
                    )],
                    expires_after=now_ms + 60_000,
                )
            )
            self.st.total_emergency += 1
            log.info("EMERGENCY #%d: Account 2 %s %.4f @ %.4f -> %s",
                     self.st.total_emergency, hedge_side, hedge_sz, hedge_px, result)
        except Exception as exc:
            log.error("EMERGENCY HEDGE FAILED: %s", exc)

    # ---------------------------------------------------------------
    # Order management
    # ---------------------------------------------------------------

    async def _cancel_cloids(self, cloids: List[str]):
        """Cancel old quotes by cloid. Never fall back to cancel_all."""
        if not cloids:
            return
        try:
            now_ms = int(time.time() * 1000)
            inst_id = self.st.instrument_id
            action_data = {
                "cancels": [
                    {"cloid": c, "instrumentId": inst_id} for c in cloids
                ],
                "expiresAfter": now_ms + 60_000,
                "nonce": now_ms,
            }
            sig = await _sign_action(
                wallet=self._exchange.wallet,
                action=action_data,
                tx_type=_OP_CODES["cancelByCloid"],
                is_testnet=False,
            )
            resp = await self._http.request(
                "exchange",
                {
                    "action": {
                        "data": action_data,
                        "type": str(_OP_CODES["cancelByCloid"]),
                    },
                    "signature": sig,
                    "nonce": action_data["nonce"],
                },
            )
            success = resp.get("success", False) if isinstance(resp, dict) else False
            if success:
                log.info("Cancelled %d old quotes by cloid", len(cloids))
            else:
                log.warning("Cancel by cloid response: %s", resp)
        except Exception as exc:
            log.warning("Cancel old cloids failed (expire naturally): %s", exc)

    async def _cancel_all(self):
        try:
            now_ms = int(time.time() * 1000)
            await self._exchange.cancel_all(
                CancelAllParams(expires_after=now_ms + 60_000)
            )
            self._active_cloids.clear()
        except Exception as exc:
            log.warning("Cancel all failed: %s", exc)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main():
    cfg = Config.from_env()
    bot = WickCatcher(cfg)

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _signal_handler():
        log.info("Interrupt received")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    bot_task = asyncio.create_task(bot.start())
    stop_task = asyncio.create_task(stop_event.wait())

    done, pending = await asyncio.wait(
        [bot_task, stop_task],
        return_when=asyncio.FIRST_COMPLETED,
    )

    await bot.stop()

    for task in pending:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    asyncio.run(main())
