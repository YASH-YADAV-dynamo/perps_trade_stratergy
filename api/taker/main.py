#!/usr/bin/env python3
"""
One-account taker worker.

Designed for Bot API-managed runs:
- receives market data via relay UDP when `--relay-port` is set
- opens when spread gate is met and closes by TP/timeout rules
- emits JSON metrics to stdout for API ingestion
"""

import asyncio
import json
import logging
import math
import os
import signal as os_signal
import socket
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from dotenv import load_dotenv
from eth_account import Account
from web3 import Web3

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
    OrderbookSubscriptionParams,
)

if os.getenv("BOT_STRATEGY_DISABLE_DOTENV", "").lower() not in ("1", "true", "yes"):
    load_dotenv()

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("dn_volume")


def _floor(val: float, step: float) -> float:
    return math.floor(val / step) * step if step > 0 else val

def _ceil(val: float, step: float) -> float:
    return math.ceil(val / step) * step if step > 0 else val

def _fmt(val: float, step: float) -> str:
    if step >= 1.0: return f"{val:.0f}"
    d = max(0, -math.floor(math.log10(step)))
    return f"{val:.{d}f}"


def _pos_entry_price(pos: dict) -> float:
    for key in (
        "entry_price",
        "entryPrice",
        "avg_entry_price",
        "average_entry_price",
        "avgEntryPrice",
        "average_price",
    ):
        try:
            v = float(pos.get(key, 0) or 0)
        except (TypeError, ValueError):
            v = 0.0
        if v > 0.0:
            return v
    try:
        sz = float(pos.get("size", 0) or 0)
    except (TypeError, ValueError):
        sz = 0.0
    try:
        pv = float(pos.get("position_value", 0) or 0)
    except (TypeError, ValueError):
        pv = 0.0
    if abs(sz) > 0.0 and abs(pv) > 0.0:
        return abs(pv) / abs(sz)
    return 0.0


_TAKER_MAX_BOOK_DEV_BPS = 500.0
_TAKER_MAX_ORDER_BN_DEV_BPS = 200.0


class L2Book:
    __slots__ = ("bid", "ask", "bid_sz", "ask_sz",
                 "_bids", "_asks", "_seq", "ready", "_event",
                 "_bn_ref")

    def __init__(self, event: asyncio.Event):
        self._bids: dict[float, float] = {}
        self._asks: dict[float, float] = {}
        self.bid = self.ask = self.bid_sz = self.ask_sz = 0.0
        self._seq = 0; self.ready = False; self._event = event
        self._bn_ref: float = 0.0

    def update_bn_ref(self, bn_mid: float):
        self._bn_ref = bn_mid

    def _reset_levels(self):
        self._bids.clear()
        self._asks.clear()
        self.bid = 0.0
        self.ask = 0.0
        self.bid_sz = 0.0
        self.ask_sz = 0.0
        self.ready = False

    def on_message(self, msg):
        data = msg.data if hasattr(msg, "data") else msg
        if not isinstance(data, dict):
            return
        books = data.get("books", data)
        if not isinstance(books, dict):
            return
        try:
            seq = int(books.get("sequence_number", 0) or 0)
        except (TypeError, ValueError):
            seq = 0

        def _px_sz(entry):
            if isinstance(entry, dict):
                p = entry.get("price", 0)
                s = entry.get("size", 0)
            elif isinstance(entry, (list, tuple)) and len(entry) >= 2:
                p = entry[0]
                s = entry[1]
            else:
                return 0.0, 0.0
            try:
                return float(p), float(s)
            except (TypeError, ValueError):
                return 0.0, 0.0

        ref = self._bn_ref
        max_dev = ref * _TAKER_MAX_BOOK_DEV_BPS / 10000.0 if ref > 0.0 else 0.0
        if data.get("update_type") == "snapshot":
            # Snapshot is authoritative: replace local cache and reset sequence.
            self._reset_levels()
            for e in books.get("bids", []):
                p, s = _px_sz(e)
                if p > 0.0 and s > 0.0:
                    if max_dev > 0.0 and abs(p - ref) > max_dev:
                        continue
                    self._bids[p] = s
            for e in books.get("asks", []):
                p, s = _px_sz(e)
                if p > 0.0 and s > 0.0:
                    if max_dev > 0.0 and abs(p - ref) > max_dev:
                        continue
                    self._asks[p] = s
            self._seq = seq
        else:
            if self._seq > 0 and seq > 0 and seq <= self._seq:
                return
            for e in books.get("bids", []):
                p, s = _px_sz(e)
                if p <= 0.0:
                    continue
                if s <= 0.0:
                    self._bids.pop(p, None)
                elif max_dev > 0.0 and abs(p - ref) > max_dev:
                    self._bids.pop(p, None)
                else:
                    self._bids[p] = s
            for e in books.get("asks", []):
                p, s = _px_sz(e)
                if p <= 0.0:
                    continue
                if s <= 0.0:
                    self._asks.pop(p, None)
                elif max_dev > 0.0 and abs(p - ref) > max_dev:
                    self._asks.pop(p, None)
                else:
                    self._asks[p] = s
            if seq > 0:
                self._seq = seq
        if self._bids:
            self.bid = max(self._bids)
            self.bid_sz = self._bids[self.bid]
        else:
            self.bid = 0.0
            self.bid_sz = 0.0
        if self._asks:
            self.ask = min(self._asks)
            self.ask_sz = self._asks[self.ask]
        else:
            self.ask = 0.0
            self.ask_sz = 0.0
        if self.bid > 0.0 and self.ask > 0.0:
            if self.ask <= self.bid:
                # Local incremental state drifted (missed/out-of-order UDP updates).
                # Drop cache and wait for fresh levels instead of emitting crossed spread.
                self._reset_levels()
                self._seq = 0
            else:
                self.ready = True
        else:
            self.ready = False
        self._event.set()

    @property
    def spread(self) -> float:
        return (self.ask - self.bid) if self.bid > 0 and self.ask > 0 else 0.0


@dataclass(frozen=True)
class TakerConfig:
    pk: str
    agent_address: str
    account_address: str
    symbol: str
    min_spread_usd: float
    min_spread_bps: float
    take_profit_bps: float
    close_bps: float
    close_timeout_ms: int
    order_size_usd: float
    target_exposure_x: float
    leverage: int
    enable_trading: bool
    cooldown_s: float
    max_loss_usd: float
    order_expiry_ms: int
    market_bias: float

    @classmethod
    def from_env(cls, symbol_override: str = "") -> "TakerConfig":
        def _env(name: str, fallback: str, default: str) -> str:
            return os.getenv(name, os.getenv(fallback, default))

        pk = os.getenv("HOTSTUFF_PRIVATE_KEY", "")
        agent = os.getenv("HOTSTUFF_AGENT_ADDRESS", "")
        account = os.getenv("HOTSTUFF_ACCOUNT_ADDRESS", agent)
        if not pk or not agent:
            log.error("Need HOTSTUFF_PRIVATE_KEY + HOTSTUFF_AGENT_ADDRESS")
            sys.exit(1)
        try:
            agent = Web3.to_checksum_address(agent)
            account = Web3.to_checksum_address(account)
        except Exception:
            log.error("HOTSTUFF_AGENT_ADDRESS / HOTSTUFF_ACCOUNT_ADDRESS invalid")
            sys.exit(1)
        return cls(
            pk=pk,
            agent_address=agent,
            account_address=account,
            symbol=symbol_override or _env("TAKER_SYMBOL", "DN_SYMBOL", "BTC-PERP"),
            min_spread_usd=float(_env("TAKER_MIN_SPREAD_USD", "DN_MIN_SPREAD_USD", "1.0")),
            min_spread_bps=float(_env("TAKER_MIN_SPREAD_BPS", "DN_MIN_SPREAD_BPS", "0.1")),
            take_profit_bps=float(_env("TAKER_TAKE_PROFIT_BPS", "DN_TAKE_PROFIT_BPS", "0.1")),
            close_bps=float(_env("TAKER_CLOSE_BPS", "DN_CLOSE_BPS", "0.2")),
            close_timeout_ms=int(_env("TAKER_CLOSE_TIMEOUT_MS", "DN_CLOSE_TIMEOUT_MS", "300")),
            order_size_usd=float(_env("TAKER_ORDER_SIZE_USD", "DN_ORDER_SIZE_USD", "0")),
            target_exposure_x=float(_env("TAKER_TARGET_EXPOSURE_X", "DN_TARGET_EXPOSURE_X", "2.0")),
            leverage=int(_env("TAKER_LEVERAGE", "DN_LEVERAGE", "20")),
            enable_trading=_env("TAKER_ENABLE_TRADING", "DN_ENABLE_TRADING", "false").lower() == "true",
            cooldown_s=float(_env("TAKER_COOLDOWN_S", "DN_COOLDOWN_S", "0.05")),
            max_loss_usd=float(_env("TAKER_MAX_LOSS_USD", "DN_MAX_LOSS_USD", "5.0")),
            order_expiry_ms=int(_env("TAKER_ORDER_EXPIRY_MS", "DN_ORDER_EXPIRY_MS", "15000")),
            market_bias=max(-1.0, min(1.0, float(_env("TAKER_MARKET_BIAS", "DN_MARKET_BIAS", "0.0")))),
        )


class OneAccountTaker:
    def __init__(self, cfg: TakerConfig, session_id: Optional[str] = None, relay_port: Optional[int] = None):
        self.cfg = cfg
        self._session_id = session_id
        self._relay_port = relay_port

        self._running = False
        self._stopped = False
        self._tasks: list[asyncio.Task] = []
        self._subs: list = []

        self._bbo_ev = asyncio.Event()
        self._book = L2Book(self._bbo_ev)
        self._bn_bid = 0.0
        self._bn_ask = 0.0

        self._inst_id = 0
        self._tick = 1.0
        self._lot = 0.00001
        self._min_not = 10.0

        self._info: Optional[InfoClient] = None
        self._ex: Optional[ExchangeClient] = None
        self._ws: Optional[WebSocketTransport] = None

        self._equity = 0.0
        self._start_eq = 0.0
        self._start_ts = 0.0

        self._open_side = 0          # +1 long, -1 short, 0 flat
        self._open_px = 0.0
        self._open_sz = 0.0
        self._open_ts = 0.0
        self._next_open_is_buy = True

        self._pnl = 0.0
        self._fills = 0
        self._vol = 0.0
        self._round_trips = 0
        self._margin_rejects = 0
        self._last_spread_bps = 0.0
        self._last_close_reason = "none"

    async def start(self):
        self._running = True
        self._start_ts = time.monotonic()
        mode = "LIVE" if self.cfg.enable_trading else "DRY-RUN"
        bias_str = "neutral" if self.cfg.market_bias == 0.0 else ("LONG %.1f" % self.cfg.market_bias if self.cfg.market_bias > 0 else "SHORT %.1f" % self.cfg.market_bias)
        log.info(
            "=== Taker [%s] symbol=%s spread>=%.4f USD (%.3f bps) tp=%.3f bps close=%.3f bps timeout=%dms bias=%s ===",
            mode,
            self.cfg.symbol,
            self.cfg.min_spread_usd,
            self.cfg.min_spread_bps,
            self.cfg.take_profit_bps,
            self.cfg.close_bps,
            self.cfg.close_timeout_ms,
            bias_str,
        )

        self._info = InfoClient(is_testnet=False)
        self._ex = ExchangeClient(wallet=Account.from_key(self.cfg.pk), is_testnet=False)
        await self._resolve_instrument()
        await self._sync_eq()
        await self._sync_position()
        self._start_eq = self._equity

        if self._relay_port:
            self._tasks.append(asyncio.create_task(self._run_relay_receiver()))
            log.info("Using relay on UDP port %d", self._relay_port)
        else:
            await self._start_direct_book_feed()

        if self.cfg.enable_trading:
            await self._cancel_all()

        self._tasks.append(asyncio.create_task(self._run_loop()))
        self._tasks.append(asyncio.create_task(self._run_eq_poll()))
        self._tasks.append(asyncio.create_task(self._run_metrics_emitter()))

        log.info("=== ready | equity=$%.2f ===", self._equity)
        try:
            await asyncio.gather(*self._tasks)
        except asyncio.CancelledError:
            pass

    async def stop(self):
        if self._stopped:
            return
        self._stopped = True
        self._running = False
        self._bbo_ev.set()

        if self.cfg.enable_trading and self._open_side != 0:
            await self._force_flatten()
        if self.cfg.enable_trading:
            await self._cancel_all()

        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            try:
                await t
            except asyncio.CancelledError:
                pass

        if self._ws:
            try:
                self._ws.disconnect()
            except Exception:
                pass

        await self._sync_eq()
        elapsed = time.monotonic() - self._start_ts if self._start_ts > 0 else 0.0
        rate = self._vol / (elapsed / 3600.0) if elapsed > 0 else 0.0
        log.info(
            "=== done: rt=%d fills=%d vol=$%.2f ($%.0f/hr) pnl=$%.4f eq=$%.2f ===",
            self._round_trips,
            self._fills,
            self._vol,
            rate,
            self._pnl,
            self._equity,
        )

    async def _start_direct_book_feed(self):
        try:
            self._ws = WebSocketTransport(WebSocketTransportOptions(
                is_testnet=False,
                timeout=15.0,
                keep_alive={"interval": 20.0, "timeout": 10.0},
                auto_connect=True,
                server={"mainnet": "wss://api.hotstuff.trade/ws/"},
            ))
            sc = SubscriptionClient(transport=self._ws)
            self._subs.append(await asyncio.to_thread(
                sc.orderbook,
                OrderbookSubscriptionParams(instrument_id=self.cfg.symbol),
                self._book.on_message,
            ))
            log.info("Direct HotStuff orderbook subscription active")
        except Exception as exc:
            log.warning("Direct orderbook sub failed (%s) -- REST ticker fallback", exc)
            self._tasks.append(asyncio.create_task(self._poll_bbo()))

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
                if msg.get("s") != self.cfg.symbol:
                    continue
                mtype = msg.get("t")
                if mtype == "hs_book":
                    try:
                        self._book.on_message(msg.get("data", msg))
                    except Exception as exc:
                        log.warning("hs_book parse: %s", exc)
                elif mtype == "bn_book":
                    try:
                        self._bn_bid = float(msg.get("b", 0) or 0)
                        self._bn_ask = float(msg.get("a", 0) or 0)
                        if self._bn_bid > 0.0 and self._bn_ask > 0.0:
                            self._book.update_bn_ref((self._bn_bid + self._bn_ask) * 0.5)
                    except (TypeError, ValueError):
                        pass
        finally:
            sock.close()

    async def _resolve_instrument(self):
        raw = await asyncio.to_thread(
            self._info.transport.request,
            "info",
            {"method": "instruments", "params": {"type": "perps"}},
        )
        for p in (raw.get("perps", []) if isinstance(raw, dict) else []):
            if p.get("name") == self.cfg.symbol:
                self._inst_id = int(p["id"])
                self._tick = float(p.get("tick_size", 1.0))
                self._lot = float(p.get("lot_size", 0.00001))
                self._min_not = float(p.get("min_notional_usd", 10.0))
                log.info(
                    "inst: %s id=%d tick=%s lot=%s",
                    self.cfg.symbol,
                    self._inst_id,
                    _fmt(self._tick, 0.000001),
                    _fmt(self._lot, 0.000001),
                )
                return
        log.error("%s not found", self.cfg.symbol)
        sys.exit(1)

    async def _sync_eq(self):
        try:
            raw = await asyncio.to_thread(
                self._info.transport.request,
                "info",
                {"method": "accountSummary", "params": {"user": self.cfg.account_address}},
            )
            if isinstance(raw, dict):
                eq = float(raw.get("total_account_equity", 0) or 0.0)
                if eq <= 0.0:
                    eq = float(raw.get("available_balance", 0) or 0.0)
                if eq <= 0.0:
                    eq = float(raw.get("margin_balance", 0) or 0.0)
                if eq <= 0.0:
                    eq = float(raw.get("derivative_account_equity", 0) or 0.0)
            else:
                eq = float(getattr(raw, "total_account_equity", 0) or 0.0)
                if eq <= 0.0:
                    eq = float(getattr(raw, "available_balance", 0) or 0.0)
            self._equity = eq
        except Exception:
            pass

    async def _sync_position(self):
        try:
            raw = await asyncio.to_thread(
                self._info.transport.request,
                "info",
                {"method": "positions", "params": {"user": self.cfg.account_address}},
            )
            if isinstance(raw, list):
                positions = raw
            elif isinstance(raw, dict):
                data = raw.get("data")
                if isinstance(data, list):
                    positions = data
                else:
                    positions = raw.get("positions") if isinstance(raw.get("positions"), list) else []
            else:
                positions = []
            pos = None
            for item in positions:
                if self.cfg.symbol in str(item.get("instrument", item.get("symbol", ""))):
                    pos = item
                    break
            if not pos:
                self._open_side = 0
                self._open_px = 0.0
                self._open_sz = 0.0
                self._open_ts = 0.0
                return

            sz = float(pos.get("size", 0) or 0.0)
            if abs(sz) <= self._lot:
                self._open_side = 0
                self._open_px = 0.0
                self._open_sz = 0.0
                self._open_ts = 0.0
                return

            self._open_side = 1 if sz > 0 else -1
            self._open_sz = abs(sz)
            entry = _pos_entry_price(pos)
            if entry > 0.0:
                self._open_px = entry
            if self._open_ts <= 0.0:
                self._open_ts = time.monotonic()
            # Keep alternating direction consistent after this synced open position is closed.
            self._next_open_is_buy = self._open_side < 0
        except Exception:
            pass

    async def _cancel_all(self):
        if not self._ex:
            return
        try:
            now = int(time.time() * 1000)
            await asyncio.to_thread(self._ex.cancel_all, CancelAllParams(expiresAfter=now + 60_000))
        except Exception:
            pass

    def _entry_trigger_usd(self, bid: float, ask: float) -> float:
        mid = (bid + ask) * 0.5 if bid > 0 and ask > 0 else 0.0
        bps_trigger = mid * (self.cfg.min_spread_bps / 10000.0) if mid > 0 else 0.0
        return max(self.cfg.min_spread_usd, bps_trigger)

    def _unrealized_pnl(self, bid: float, ask: float) -> float:
        if self._open_side == 0 or self._open_sz <= 0.0 or self._open_px <= 0.0:
            return 0.0
        if self._open_side > 0:
            if bid <= 0.0:
                return 0.0
            return (bid - self._open_px) * self._open_sz
        if ask <= 0.0:
            return 0.0
        return (self._open_px - ask) * self._open_sz

    def _close_price(self, bid: float, ask: float, bps_mult: float = 1.0) -> float:
        bps = max(self.cfg.close_bps * max(1.0, bps_mult), self.cfg.close_bps)
        if self._open_side > 0:
            ref = bid if bid > 0.0 else self._open_px
            return max(self._tick, ref * (1.0 - bps / 10000.0))
        ref = ask if ask > 0.0 else self._open_px
        return ref * (1.0 + bps / 10000.0)

    def _calc_order_size(self, ask: float) -> float:
        if ask <= 0:
            return 0.0
        # Low-equity profile: keep sizing deterministic and non-trivial for $2-$15 wallets.
        if 2.0 <= self._equity <= 15.0:
            notional = 12.0
        elif self.cfg.order_size_usd > 0:
            notional = self.cfg.order_size_usd
        else:
            notional = self._equity * self.cfg.target_exposure_x
        notional_cap = max(0.0, self._equity * self.cfg.leverage * 0.30)
        if notional_cap <= 0.0:
            return 0.0
        notional = min(notional, notional_cap)
        size = _floor(notional / ask, self._lot)
        size = min(size, self._book.bid_sz, self._book.ask_sz)
        size = _floor(size, self._lot)
        if size <= 0 or size * ask < self._min_not:
            return 0.0
        return size

    async def _ioc(self, side: str, price: float, size: float, tag: str, ro: bool = False) -> tuple[bool, float, float]:
        if size <= 0:
            return (False, 0.0, 0.0)
        if not self.cfg.enable_trading:
            return (True, price, size)
        cloid = f"taker-{tag}-{int(time.time() * 1000)}"
        try:
            now = int(time.time() * 1000)
            resp = await asyncio.to_thread(
                self._ex.place_order,
                PlaceOrderParams(
                    orders=[UnitOrder(
                        instrumentId=self._inst_id,
                        side="b" if side == "buy" else "s",
                        positionSide="BOTH",
                        price=_fmt(price, self._tick),
                        size=_fmt(size, self._lot),
                        tif="IOC",
                        ro=ro,
                        po=False,
                        cloid=cloid,
                    )],
                    expiresAfter=now + self.cfg.order_expiry_ms,
                ),
            )
            for s in (resp.get("data", {}).get("status", []) if isinstance(resp, dict) else []):
                if isinstance(s, dict) and "filled" in s:
                    fp = float(s["filled"].get("average_price", price))
                    fs = float(s["filled"].get("total_size", size))
                    return (True, fp, fs)
                if isinstance(s, dict) and "error" in s:
                    err = s["error"]
                    emsg = err.get("error", err) if isinstance(err, dict) else err
                    log.warning("%s rejected: %s", tag, emsg)
                    if "margin" in str(emsg).lower():
                        self._margin_rejects += 1
                    return (False, 0.0, 0.0)
            return (False, 0.0, 0.0)
        except Exception as exc:
            log.error("%s failed: %s", tag, exc)
            return (False, 0.0, 0.0)

    async def _open_position(self) -> bool:
        if self._open_side != 0:
            return False
        bid, ask = self._book.bid, self._book.ask
        if bid <= 0 or ask <= 0:
            return False
        bn_mid = (self._bn_bid + self._bn_ask) * 0.5 if self._bn_bid > 0 and self._bn_ask > 0 else 0.0
        if bn_mid > 0.0:
            hs_mid = (bid + ask) * 0.5
            if abs(hs_mid - bn_mid) / bn_mid * 10000.0 > _TAKER_MAX_ORDER_BN_DEV_BPS:
                return False
        spread = ask - bid
        if spread < self._entry_trigger_usd(bid, ask):
            return False
        size = self._calc_order_size(ask)
        if size <= 0:
            return False
        if self.cfg.market_bias > 0.0:
            side = "buy"
        elif self.cfg.market_bias < 0.0:
            side = "sell"
        else:
            side = "buy" if self._next_open_is_buy else "sell"
        px = ask if side == "buy" else bid
        ok, fp, fs = await self._ioc(side, px, size, "open", ro=False)
        if not ok or fs <= 0:
            return False
        self._open_side = 1 if side == "buy" else -1
        self._open_px = fp
        self._open_sz = fs
        self._open_ts = time.monotonic()
        self._fills += 1
        self._vol += fp * fs
        self._margin_rejects = 0
        log.info("OPEN %s %.6f @ %s spread=$%.4f", side, fs, _fmt(fp, self._tick), spread)
        return True

    def _close_signal(self) -> Optional[tuple[str, float]]:
        if self._open_side == 0:
            return None
        bid, ask = self._book.bid, self._book.ask
        if bid <= 0 or ask <= 0:
            return None
        elapsed_ms = (time.monotonic() - self._open_ts) * 1000.0
        unreal = self._unrealized_pnl(bid, ask)
        loss_cut_usd = max(0.25, self.cfg.max_loss_usd * 0.20)
        if unreal <= -loss_cut_usd:
            severity = min(12.0, max(2.0, abs(unreal) / max(loss_cut_usd, 1e-9)))
            return ("loss", self._close_price(bid, ask, bps_mult=severity))
        if self._open_side > 0:
            tp_px = self._open_px * (1.0 + self.cfg.take_profit_bps / 10000.0)
            if bid >= tp_px:
                return ("tp", bid)
            if elapsed_ms >= self.cfg.close_timeout_ms:
                return ("timeout", self._close_price(bid, ask, bps_mult=1.0))
            return None
        tp_px = self._open_px * (1.0 - self.cfg.take_profit_bps / 10000.0)
        if ask <= tp_px:
            return ("tp", ask)
        if elapsed_ms >= self.cfg.close_timeout_ms:
            return ("timeout", self._close_price(bid, ask, bps_mult=1.0))
        return None

    async def _close_position(self, reason: str, px: float) -> bool:
        if self._open_side == 0:
            return True
        bn_mid = (self._bn_bid + self._bn_ask) * 0.5 if self._bn_bid > 0 and self._bn_ask > 0 else 0.0
        if bn_mid > 0.0:
            max_dev = bn_mid * _TAKER_MAX_ORDER_BN_DEV_BPS / 10000.0
            px = max(bn_mid - max_dev, min(bn_mid + max_dev, px))
        close_side = "sell" if self._open_side > 0 else "buy"
        ok, fp, fs = await self._ioc(close_side, px, self._open_sz, f"close-{reason}", ro=True)
        if not ok or fs <= 0:
            return False

        filled = min(fs, self._open_sz)
        gross = ((fp - self._open_px) * filled) if self._open_side > 0 else ((self._open_px - fp) * filled)
        fees = (self._open_px * filled + fp * filled) * 0.00025
        self._pnl += (gross - fees)
        self._fills += 1
        self._vol += fp * filled

        rem = max(self._open_sz - filled, 0.0)
        if rem > self._lot:
            self._open_sz = rem
            return False

        self._open_side = 0
        self._open_px = 0.0
        self._open_sz = 0.0
        self._open_ts = 0.0
        self._round_trips += 1
        self._last_close_reason = reason
        self._next_open_is_buy = not self._next_open_is_buy
        log.info("CLOSE %s %.6f @ %s reason=%s pnl=$%.4f", close_side, filled, _fmt(fp, self._tick), reason, self._pnl)
        return True

    async def _force_flatten(self):
        for mult in (2.0, 4.0, 8.0):
            if self._open_side == 0:
                return
            bid, ask = self._book.bid, self._book.ask
            px = self._close_price(bid, ask, bps_mult=mult)
            await self._close_position(f"force{int(mult)}", px)
            await asyncio.sleep(0.15)
        if self._open_side != 0:
            log.warning("Force flatten incomplete; residual size=%.6f side=%d", self._open_sz, self._open_side)

    async def _run_loop(self):
        log.info("waiting for book...")
        while self._running and not self._book.ready:
            await asyncio.sleep(0.2)
        if not self._running:
            log.info("stopping before book became ready")
            return
        log.info("book ready")
        while self._running:
            try:
                self._bbo_ev.clear()
                try:
                    await asyncio.wait_for(self._bbo_ev.wait(), timeout=max(0.2, self.cfg.cooldown_s))
                except asyncio.TimeoutError:
                    pass
                if not self._running:
                    break
                bid, ask = self._book.bid, self._book.ask
                if bid <= 0 or ask <= 0:
                    continue
                mid = (bid + ask) * 0.5
                spr = ask - bid
                self._last_spread_bps = (spr / mid * 10000.0) if mid > 0 else 0.0

                unreal = self._unrealized_pnl(bid, ask)
                total_pnl = self._pnl + unreal
                if total_pnl <= -self.cfg.max_loss_usd:
                    if self._open_side != 0:
                        await self._close_position("loss_cap", self._close_price(bid, ask, bps_mult=8.0))
                    log.warning(
                        "loss cap reached: total_pnl=$%.4f (realized=$%.4f unrealized=$%.4f) <= -$%.4f -- stopping",
                        total_pnl,
                        self._pnl,
                        unreal,
                        self.cfg.max_loss_usd,
                    )
                    self._running = False
                    break

                if self._open_side == 0:
                    await self._open_position()
                else:
                    sig = self._close_signal()
                    if sig:
                        await self._close_position(sig[0], sig[1])
                await asyncio.sleep(self.cfg.cooldown_s)
            except asyncio.CancelledError:
                return
            except Exception as exc:
                log.error("loop: %s", exc, exc_info=True)
                await asyncio.sleep(1.0)

    async def _poll_bbo(self):
        while self._running:
            try:
                raw = await asyncio.to_thread(
                    self._info.transport.request,
                    "info",
                    {"method": "ticker", "params": {"symbol": self.cfg.symbol}},
                )
                ts = raw if isinstance(raw, list) else [raw] if raw else []
                if ts:
                    t = ts[0]
                    b = float(t.get("best_bid_price", 0))
                    a = float(t.get("best_ask_price", 0))
                    bsz = float(t.get("best_bid_size", 0))
                    asz = float(t.get("best_ask_size", 0))
                    if b > 0 and a > 0:
                        self._book.bid = b
                        self._book.ask = a
                        self._book.bid_sz = bsz if bsz > 0 else 1.0
                        self._book.ask_sz = asz if asz > 0 else 1.0
                        self._book.ready = True
                        self._bbo_ev.set()
                await asyncio.sleep(0.3)
            except asyncio.CancelledError:
                return
            except Exception:
                await asyncio.sleep(2.0)

    async def _run_eq_poll(self):
        while self._running:
            try:
                await asyncio.sleep(5.0)
                if self._running:
                    await self._sync_eq()
                    await self._sync_position()
            except asyncio.CancelledError:
                return
            except Exception:
                pass

    async def _run_metrics_emitter(self):
        while self._running:
            try:
                await asyncio.sleep(5.0)
                if not self._running:
                    break
                inventory = self._open_sz if self._open_side > 0 else (-self._open_sz if self._open_side < 0 else 0.0)
                hs_mid = (self._book.bid + self._book.ask) * 0.5 if self._book.bid > 0 and self._book.ask > 0 else 0.0
                bn_mid = (self._bn_bid + self._bn_ask) * 0.5 if self._bn_bid > 0 and self._bn_ask > 0 else 0.0
                mark_px = hs_mid if hs_mid > 0 else bn_mid
                unrealized_pnl = 0.0
                if self._open_side != 0 and self._open_sz > 0 and self._open_px > 0 and mark_px > 0:
                    if self._open_side > 0:
                        unrealized_pnl = (mark_px - self._open_px) * self._open_sz
                    else:
                        unrealized_pnl = (self._open_px - mark_px) * self._open_sz
                realized_pnl = self._pnl
                total_pnl = realized_pnl + unrealized_pnl
                payload = {
                    "session_id": self._session_id,
                    "symbol": self.cfg.symbol,
                    "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "pnl": round(total_pnl, 6),
                    "pnl_realized": round(realized_pnl, 6),
                    "pnl_unrealized": round(unrealized_pnl, 6),
                    "inventory": round(inventory, 6),
                    "inv_tier": 1 if abs(inventory) > 0 else 0,
                    "total_fills": self._fills,
                    "total_volume_usd": round(self._vol, 2),
                    "round_trips": self._round_trips,
                    "spread_bps": round(self._last_spread_bps, 2),
                    "vol_bps": 0.0,
                    "alpha": 0.0,
                    "toxic": 0.0,
                    "adverse_rate": 0.0,
                    "avg_markout_1s": 0.0,
                    "avg_markout_5s": 0.0,
                    "guard_interventions": 0,
                    "guard_halted": False,
                    "guard_spread_mult": 1.0,
                    "account_equity": round(self._equity, 2),
                    "fair_mid": round(hs_mid or bn_mid, 6),
                    "hs_mid": round(hs_mid, 6),
                    "bn_mid": round(bn_mid, 6),
                }
                sys.stdout.write(json.dumps(payload) + "\n")
                sys.stdout.flush()
            except asyncio.CancelledError:
                return
            except Exception:
                pass


async def main():
    session_id = None
    relay_port = None
    symbol_override = ""
    args = sys.argv[1:]
    i = 0
    while i < len(args):
        if args[i] == "--session-id" and i + 1 < len(args):
            session_id = args[i + 1]
            i += 2
        elif args[i] == "--relay-port" and i + 1 < len(args):
            relay_port = int(args[i + 1])
            i += 2
        elif args[i] == "--symbol" and i + 1 < len(args):
            symbol_override = args[i + 1]
            i += 2
        else:
            i += 1

    if session_id:
        root = logging.getLogger()
        for h in root.handlers[:]:
            root.removeHandler(h)
        logging.basicConfig(
            level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            stream=sys.stderr,
        )

    cfg = TakerConfig.from_env(symbol_override=symbol_override)
    bot = OneAccountTaker(cfg, session_id=session_id, relay_port=relay_port)
    stop_event = asyncio.Event()

    def _signal_handler():
        if not stop_event.is_set():
            stop_event.set()
            asyncio.create_task(bot.stop())

    loop = asyncio.get_running_loop()
    for sig in (os_signal.SIGINT, os_signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)
    try:
        await bot.start()
    except KeyboardInterrupt:
        pass
    finally:
        if not stop_event.is_set():
            await bot.stop()
        await asyncio.sleep(1.0)


if __name__ == "__main__":
    asyncio.run(main())
