
"""
Two-account spread-gated middle execution strategy.

Purpose:
  - Only acts when live spread is inside configured bounds.
  - Uses two independent accounts to place opposite-side orders at a
    middle price inside spread.
  - Dry-run by default; live trading requires explicit enable flag.

Important:
  - Hotstuff self-trade prevention blocks same-address crossing, so two
    accounts are required for this flow.
"""

import asyncio
import logging
import math
import os
import signal as os_signal
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

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
from hotstuff.methods.subscription.channels import BBOSubscriptionParams


if os.getenv("BOT_STRATEGY_DISABLE_DOTENV", "").lower() not in ("1", "true", "yes"):
    load_dotenv()


logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("mid_wash")


def _to_bool(raw: str, default: bool = False) -> bool:
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "y", "on")


def _round_dn(val: float, step: float) -> float:
    if step <= 0.0:
        return val
    return math.floor(val / step) * step


def _fmt(val: float, step: float) -> str:
    if step <= 0.0 or step >= 1.0:
        return f"{val:.0f}"
    d = max(0, -math.floor(math.log10(step)))
    return f"{val:.{d}f}"


def _sf(obj: Any, key: str, default: float = 0.0) -> float:
    try:
        if isinstance(obj, dict):
            return float(obj.get(key, default))
        return float(getattr(obj, key, default))
    except (TypeError, ValueError):
        return default


def _status_ok(item: Any) -> Tuple[bool, str]:
    if not isinstance(item, dict):
        return False, f"status type={type(item).__name__}"
    if "error" in item:
        err = item.get("error")
        if isinstance(err, dict):
            return False, str(err.get("error") or err.get("message") or err)
        return False, str(err)
    if "resting" in item or "filled" in item or "success" in item:
        return True, ""
    accepted = item.get("accepted")
    if isinstance(accepted, bool):
        return accepted, "" if accepted else "rejected by venue"
    return False, f"unknown status keys={','.join(str(k) for k in item.keys())}"


def _status_from_response(resp: Any) -> Tuple[bool, str]:
    if not isinstance(resp, dict):
        return False, "non-dict response"
    data = resp.get("data")
    if not isinstance(data, dict):
        return False, "missing data object"
    status = data.get("status")
    if not isinstance(status, list) or not status:
        return False, "missing status list"
    ok, msg = _status_ok(status[0])
    return ok, msg if msg else "accepted"


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
            value = float(pos.get(key, 0) or 0)
        except (TypeError, ValueError):
            value = 0.0
        if value > 0.0:
            return value
    return 0.0


@dataclass(frozen=True, slots=True)
class Config:
    symbol: str
    min_spread_bps: float
    max_spread_bps: float
    target_notional_usd: float
    cycle_interval_s: float
    maker_rest_ms: int
    max_book_stale_s: float
    order_expiry_ms: int
    maker_post_only: bool
    cancel_after_cycle: bool
    enable_trading: bool
    run_seconds: float
    simulate_book: bool
    sim_mid: float
    sim_spread_bps: float
    sim_spread_jitter_bps: float
    # account A
    pk1: str
    addr1: str
    # account B
    pk2: str
    addr2: str

    @classmethod
    def from_env(
        cls,
        symbol_override: str = "",
        run_seconds_override: Optional[float] = None,
        simulate_override: Optional[bool] = None,
    ) -> "Config":
        enable_trading = _to_bool(os.getenv("WT_ENABLE_TRADING", "false"))
        simulate_book = _to_bool(os.getenv("WT_SIMULATE_BOOK", "false"))
        if simulate_override is not None:
            simulate_book = simulate_override

        symbol = symbol_override or os.getenv("WT_SYMBOL", "HYPE-PERP")
        run_seconds = float(os.getenv("WT_RUN_SECONDS", "0"))
        if run_seconds_override is not None:
            run_seconds = run_seconds_override

        pk1 = os.getenv("HOTSTUFF_PRIVATE_KEY", "")
        addr1 = os.getenv("HOTSTUFF_AGENT_ADDRESS", "")
        pk2 = os.getenv("HEDGE_PRIVATE_KEY", "")
        addr2 = os.getenv("HEDGE_AGENT_ADDRESS", "")

        if enable_trading:
            if not pk1 or not addr1:
                log.error("Need HOTSTUFF_PRIVATE_KEY + HOTSTUFF_AGENT_ADDRESS for live mode")
                sys.exit(1)
            if not pk2 or not addr2:
                log.error("Need HEDGE_PRIVATE_KEY + HEDGE_AGENT_ADDRESS for live mode")
                sys.exit(1)

        return cls(
            symbol=symbol,
            min_spread_bps=float(os.getenv("WT_MIN_SPREAD_BPS", "1.0")),
            max_spread_bps=float(os.getenv("WT_MAX_SPREAD_BPS", "35.0")),
            target_notional_usd=float(os.getenv("WT_TARGET_NOTIONAL_USD", "25.0")),
            cycle_interval_s=float(os.getenv("WT_CYCLE_INTERVAL_S", "0.35")),
            maker_rest_ms=int(os.getenv("WT_MAKER_REST_MS", "120")),
            max_book_stale_s=float(os.getenv("WT_MAX_BOOK_STALE_S", "2.0")),
            order_expiry_ms=int(os.getenv("WT_ORDER_EXPIRY_MS", "30000")),
            maker_post_only=_to_bool(os.getenv("WT_MAKER_POST_ONLY", "true"), True),
            cancel_after_cycle=_to_bool(os.getenv("WT_CANCEL_AFTER_CYCLE", "true"), True),
            enable_trading=enable_trading,
            run_seconds=run_seconds,
            simulate_book=simulate_book,
            sim_mid=float(os.getenv("WT_SIM_MID", "100.0")),
            sim_spread_bps=float(os.getenv("WT_SIM_SPREAD_BPS", "4.0")),
            sim_spread_jitter_bps=float(os.getenv("WT_SIM_SPREAD_JITTER_BPS", "0.8")),
            pk1=pk1,
            addr1=addr1,
            pk2=pk2,
            addr2=addr2,
        )


@dataclass(slots=True)
class BookState:
    bid: float = 0.0
    ask: float = 0.0
    mid: float = 0.0
    spread_bps: float = 0.0
    last_ts: float = 0.0


@dataclass(slots=True)
class AccountState:
    label: str
    address: str
    equity: float = 0.0
    start_equity: float = 0.0
    session_pnl: float = 0.0
    position: float = 0.0
    avg_entry: float = 0.0
    est_volume_usd: float = 0.0
    cycles: int = 0


class MidWashBot:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._running = False
        self._stopped = False
        self._tasks: list[asyncio.Task] = []
        self._subs: list[Any] = []
        self._book_event = asyncio.Event()
        self._book = BookState()

        self._ws: Optional[WebSocketTransport] = None
        self._info: Optional[InfoClient] = None
        self._sub_client: Optional[SubscriptionClient] = None
        self._ex1: Optional[ExchangeClient] = None
        self._ex2: Optional[ExchangeClient] = None

        self._inst_id = 0
        self._tick = 0.01
        self._lot = 0.01
        self._min_notional = 10.0

        self._cycle_seq = 0
        self._eligible_cycles = 0
        self._executed_cycles = 0
        self._skipped_cycles = 0
        self._next_a1_buy = True
        self._accounts: Dict[str, AccountState] = {}

    async def start(self):
        self._running = True
        mode = "LIVE" if self.cfg.enable_trading else "DRY-RUN"
        log.info("=== Mid Wash starting ===")
        log.info("Mode            : %s", mode)
        log.info("Symbol          : %s", self.cfg.symbol)
        log.info("Spread gate     : %.2f..%.2f bps", self.cfg.min_spread_bps, self.cfg.max_spread_bps)
        log.info("Target notional : $%.2f", self.cfg.target_notional_usd)
        log.info("Cycle interval  : %.3fs", self.cfg.cycle_interval_s)
        log.info("Maker rest      : %dms", self.cfg.maker_rest_ms)
        log.info("Book source     : %s", "SIMULATED" if self.cfg.simulate_book else "LIVE BBO")
        if self.cfg.run_seconds > 0:
            log.info("Auto-stop       : %.1fs", self.cfg.run_seconds)

        self._info = InfoClient(is_testnet=False)
        await self._resolve_instrument()
        self._init_account_states()
        await self._sync_account_states(set_start=True)

        if self.cfg.enable_trading:
            w1 = Account.from_key(self.cfg.pk1)
            w2 = Account.from_key(self.cfg.pk2)
            self._ex1 = ExchangeClient(wallet=w1, is_testnet=False)
            self._ex2 = ExchangeClient(wallet=w2, is_testnet=False)
            await self._cancel_all(self._ex1, "A1")
            await self._cancel_all(self._ex2, "A2")

        if self.cfg.simulate_book:
            self._tasks.append(asyncio.create_task(self._run_simulated_book()))
        else:
            await self._start_bbo_subscription()

        self._tasks.append(asyncio.create_task(self._run_cycle_loop()))
        self._tasks.append(asyncio.create_task(self._run_status_loop()))
        if self.cfg.run_seconds > 0:
            self._tasks.append(asyncio.create_task(self._run_auto_stop()))

        try:
            await asyncio.gather(*self._tasks)
        except asyncio.CancelledError:
            pass

    async def stop(self):
        if self._stopped:
            return
        self._stopped = True
        self._running = False
        self._book_event.set()

        if self.cfg.enable_trading:
            if self._ex1:
                await self._cancel_all(self._ex1, "A1")
            if self._ex2:
                await self._cancel_all(self._ex2, "A2")

        current = asyncio.current_task()
        for task in self._tasks:
            if task is current:
                continue
            task.cancel()
        for task in self._tasks:
            if task is current:
                continue
            try:
                await task
            except asyncio.CancelledError:
                pass

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

        await self._sync_account_states(set_start=False)
        for client in (self._info, self._ex1, self._ex2):
            if client and hasattr(client, "transport"):
                try:
                    client.transport.close()
                except Exception:
                    pass

        log.info(
            "=== Mid Wash stopped | cycles=%d eligible=%d executed=%d skipped=%d ===",
            self._cycle_seq, self._eligible_cycles, self._executed_cycles, self._skipped_cycles,
        )
        self._log_account_summary(prefix="FINAL")

    async def _resolve_instrument(self):
        raw = await asyncio.to_thread(
            self._info.transport.request,
            "info",
            {"method": "instruments", "params": {"type": "perps"}},
        )
        perps = raw.get("perps", []) if isinstance(raw, dict) else []
        for p in perps:
            if p.get("name") == self.cfg.symbol:
                self._inst_id = int(p.get("id", 0))
                self._tick = float(p.get("tick_size", 0.01))
                self._lot = float(p.get("lot_size", 0.01))
                self._min_notional = float(p.get("min_notional_usd", 10.0))
                log.info(
                    "Instrument meta : id=%d tick=%s lot=%s min_notional=$%.2f",
                    self._inst_id,
                    _fmt(self._tick, 0.000001),
                    _fmt(self._lot, 0.000001),
                    self._min_notional,
                )
                return
        log.error("Instrument not found: %s", self.cfg.symbol)
        sys.exit(1)

    def _init_account_states(self):
        self._accounts.clear()
        if self.cfg.addr1:
            self._accounts["A1"] = AccountState(label="A1", address=self.cfg.addr1)
        if self.cfg.addr2:
            self._accounts["A2"] = AccountState(label="A2", address=self.cfg.addr2)
        if self._accounts:
            log.info(
                "Accounts        : %s",
                " | ".join(f"{st.label}:{st.address}" for st in self._accounts.values()),
            )

    @staticmethod
    def _extract_account_equity(raw: Any) -> float:
        if not isinstance(raw, dict):
            return 0.0
        for key in (
            "total_account_equity",
            "available_balance",
            "margin_balance",
            "derivative_account_equity",
        ):
            value = _sf(raw, key, 0.0)
            if value > 0.0:
                return value
        return 0.0

    def _extract_symbol_position(self, raw: Any) -> Tuple[float, float]:
        if isinstance(raw, list):
            positions = raw
        elif isinstance(raw, dict):
            data = raw.get("data")
            if isinstance(data, list):
                positions = data
            else:
                p = raw.get("positions")
                positions = p if isinstance(p, list) else []
        else:
            positions = []

        net = 0.0
        entry = 0.0
        for pos in positions:
            if not isinstance(pos, dict):
                continue
            inst_raw = str(pos.get("instrument", pos.get("symbol", "")))
            inst_id = 0
            try:
                inst_id = int(float(pos.get("instrument_id", 0) or 0))
            except (TypeError, ValueError):
                inst_id = 0
            if inst_raw and inst_raw != self.cfg.symbol:
                if inst_id != self._inst_id:
                    continue
            elif not inst_raw and inst_id != self._inst_id:
                continue

            size = _sf(pos, "size", 0.0)
            if abs(size) <= 0.0:
                legs = pos.get("legs")
                if isinstance(legs, list):
                    for leg in legs:
                        size += _sf(leg, "size", 0.0)
            net += size
            if entry <= 0.0:
                ep = _pos_entry_price(pos)
                if ep > 0.0:
                    entry = ep
        return net, entry

    async def _sync_account_states(self, set_start: bool):
        if not self._info or not self._accounts:
            return
        for st in self._accounts.values():
            try:
                raw_eq = await asyncio.to_thread(
                    self._info.transport.request,
                    "info",
                    {"method": "accountSummary", "params": {"user": st.address}},
                )
                eq = self._extract_account_equity(raw_eq)
                if eq > 0.0:
                    st.equity = eq
                    if set_start and st.start_equity <= 0.0:
                        st.start_equity = eq
                    if st.start_equity > 0.0:
                        st.session_pnl = st.equity - st.start_equity
            except Exception as exc:
                log.debug("%s equity sync: %s", st.label, exc)

            try:
                raw_pos = await asyncio.to_thread(
                    self._info.transport.request,
                    "info",
                    {"method": "positions", "params": {"user": st.address}},
                )
                net, entry = self._extract_symbol_position(raw_pos)
                st.position = net
                if entry > 0.0:
                    st.avg_entry = entry
                elif abs(st.position) <= self._lot * 0.5:
                    st.avg_entry = 0.0
            except Exception as exc:
                log.debug("%s position sync: %s", st.label, exc)

    def _record_cycle_execution(self, price: float, size: float):
        notional = price * size
        if notional <= 0.0 or not self._accounts:
            return
        for st in self._accounts.values():
            st.est_volume_usd += notional
            st.cycles += 1

    def _log_account_summary(self, prefix: str):
        if not self._accounts:
            return
        total_eq = 0.0
        total_pnl = 0.0
        total_vol = 0.0
        total_pos_abs = 0.0

        for key in ("A1", "A2"):
            st = self._accounts.get(key)
            if not st:
                continue
            total_eq += st.equity
            total_pnl += st.session_pnl
            total_vol += st.est_volume_usd
            total_pos_abs += abs(st.position)
            avg_entry = _fmt(st.avg_entry, self._tick) if st.avg_entry > 0.0 else "-"
            log.info(
                "%s %s eq=$%.2f start=$%.2f pnl=$%.2f pos=%s avg=%s vol=$%.2f cycles=%d",
                prefix,
                st.label,
                st.equity,
                st.start_equity,
                st.session_pnl,
                _fmt(st.position, self._lot),
                avg_entry,
                st.est_volume_usd,
                st.cycles,
            )

        log.info(
            "%s COMBINED eq=$%.2f pnl=$%.2f vol=$%.2f abs_pos=%s",
            prefix,
            total_eq,
            total_pnl,
            total_vol,
            _fmt(total_pos_abs, self._lot),
        )

    async def _start_bbo_subscription(self):
        ws_server = {"mainnet": "wss://api.hotstuff.trade/ws/"}
        self._ws = WebSocketTransport(WebSocketTransportOptions(
            is_testnet=False,
            timeout=15.0,
            keep_alive={"interval": 20.0, "timeout": 10.0},
            auto_connect=True,
            server=ws_server,
        ))
        self._sub_client = SubscriptionClient(transport=self._ws)
        sub = await asyncio.to_thread(
            self._sub_client.bbo,
            BBOSubscriptionParams(symbol=self.cfg.symbol),
            self._on_bbo,
        )
        self._subs.append(sub)
        log.info("Subscribed BBO for %s", self.cfg.symbol)

    def _on_bbo(self, msg):
        try:
            data = msg.data if hasattr(msg, "data") else msg
            bid = _sf(data, "best_bid_price") or _sf(data, "bestBidPrice")
            ask = _sf(data, "best_ask_price") or _sf(data, "bestAskPrice")
            if bid <= 0.0 or ask <= 0.0 or ask <= bid:
                return
            mid = (bid + ask) * 0.5
            spread_bps = (ask - bid) / mid * 10000.0 if mid > 0.0 else 0.0
            now = time.monotonic()
            self._book.bid = bid
            self._book.ask = ask
            self._book.mid = mid
            self._book.spread_bps = spread_bps
            self._book.last_ts = now
            self._book_event.set()
        except Exception:
            return

    async def _run_simulated_book(self):
        phase = 0.0
        mid = max(self.cfg.sim_mid, self._tick * 10.0)
        while self._running:
            phase += 0.17
            mid *= 1.0 + math.sin(phase * 0.2) * 0.00005
            spread_bps = self.cfg.sim_spread_bps + math.sin(phase) * self.cfg.sim_spread_jitter_bps
            if spread_bps < 0.2:
                spread_bps = 0.2
            half_spread = mid * spread_bps / 20000.0
            bid = _round_dn(mid - half_spread, self._tick)
            ask = _round_dn(mid + half_spread + self._tick, self._tick)
            if ask <= bid:
                ask = bid + self._tick
            sim_mid = (bid + ask) * 0.5
            sim_spread_bps = (ask - bid) / sim_mid * 10000.0 if sim_mid > 0.0 else 0.0
            self._book.bid = bid
            self._book.ask = ask
            self._book.mid = sim_mid
            self._book.spread_bps = sim_spread_bps
            self._book.last_ts = time.monotonic()
            self._book_event.set()
            await asyncio.sleep(0.10)

    async def _run_cycle_loop(self):
        if not self.cfg.simulate_book:
            await self._wait_for_first_book()
        while self._running:
            try:
                await asyncio.sleep(self.cfg.cycle_interval_s)
                if not self._running:
                    break
                self._cycle_seq += 1
                await self._do_cycle()
            except asyncio.CancelledError:
                return
            except Exception as exc:
                log.error("Cycle loop error: %s", exc)
                await asyncio.sleep(0.5)

    async def _wait_for_first_book(self):
        log.info("Waiting for first BBO...")
        deadline = time.monotonic() + 20.0
        while self._running:
            if self._book.last_ts > 0.0:
                log.info(
                    "Book ready: bid=%s ask=%s spread=%.2fbps",
                    _fmt(self._book.bid, self._tick),
                    _fmt(self._book.ask, self._tick),
                    self._book.spread_bps,
                )
                return
            if time.monotonic() >= deadline:
                log.error("No BBO received in 20s")
                self._running = False
                return
            self._book_event.clear()
            try:
                await asyncio.wait_for(self._book_event.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                pass

    async def _do_cycle(self):
        age = time.monotonic() - self._book.last_ts
        if self._book.last_ts <= 0.0 or age > self.cfg.max_book_stale_s:
            self._skipped_cycles += 1
            return
        spread_bps = self._book.spread_bps
        if spread_bps < self.cfg.min_spread_bps or spread_bps > self.cfg.max_spread_bps:
            self._skipped_cycles += 1
            return

        trade_px = self._pick_middle_price(self._book.bid, self._book.ask)
        if trade_px <= 0.0:
            self._skipped_cycles += 1
            return
        size = self._compute_size(trade_px)
        if size <= 0.0:
            self._skipped_cycles += 1
            return

        self._eligible_cycles += 1
        a1_side = "buy" if self._next_a1_buy else "sell"
        a2_side = "sell" if self._next_a1_buy else "buy"

        if not self.cfg.enable_trading:
            log.info(
                "DRY cycle=%d spread=%.2fbps px=%s sz=%s | maker=%s:%s taker=%s:%s",
                self._cycle_seq,
                spread_bps,
                _fmt(trade_px, self._tick),
                _fmt(size, self._lot),
                "A1",
                a1_side,
                "A2",
                a2_side,
            )
            self._executed_cycles += 1
            self._record_cycle_execution(trade_px, size)
            self._next_a1_buy = not self._next_a1_buy
            return

        ok = await self._run_live_cycle(trade_px, size, a1_side, a2_side, spread_bps)
        if ok:
            self._executed_cycles += 1
            self._record_cycle_execution(trade_px, size)
            self._next_a1_buy = not self._next_a1_buy

    async def _run_live_cycle(self, px: float, size: float, a1_side: str, a2_side: str, spread_bps: float) -> bool:
        maker_label = "A1"
        taker_label = "A2"
        maker_ex = self._ex1
        taker_ex = self._ex2
        maker_side = a1_side
        taker_side = a2_side

        maker_cloid = f"mw-m-{self._cycle_seq}-{int(time.time() * 1000)}"
        taker_cloid = f"mw-t-{self._cycle_seq}-{int(time.time() * 1000)}"

        log.info(
            "LIVE cycle=%d spread=%.2fbps px=%s sz=%s | maker=%s:%s taker=%s:%s",
            self._cycle_seq,
            spread_bps,
            _fmt(px, self._tick),
            _fmt(size, self._lot),
            maker_label,
            maker_side,
            taker_label,
            taker_side,
        )

        maker_resp = await self._place_order(
            maker_ex,
            side=maker_side,
            price=px,
            size=size,
            tif="GTC",
            post_only=self.cfg.maker_post_only,
            cloid=maker_cloid,
        )
        maker_ok, maker_msg = _status_from_response(maker_resp)
        if not maker_ok:
            log.warning("Maker rejected (%s): %s", maker_cloid, maker_msg)
            return False

        await asyncio.sleep(self.cfg.maker_rest_ms / 1000.0)

        taker_resp = await self._place_order(
            taker_ex,
            side=taker_side,
            price=px,
            size=size,
            tif="IOC",
            post_only=False,
            cloid=taker_cloid,
        )
        taker_ok, taker_msg = _status_from_response(taker_resp)
        if not taker_ok:
            log.warning("Taker rejected (%s): %s", taker_cloid, taker_msg)

        if self.cfg.cancel_after_cycle:
            await self._cancel_all(maker_ex, maker_label)
            await self._cancel_all(taker_ex, taker_label)

        return taker_ok

    async def _place_order(
        self,
        ex: ExchangeClient,
        side: str,
        price: float,
        size: float,
        tif: str,
        post_only: bool,
        cloid: str,
    ) -> Any:
        now_ms = int(time.time() * 1000)
        return await asyncio.to_thread(
            ex.place_order,
            PlaceOrderParams(
                orders=[
                    UnitOrder(
                        instrumentId=self._inst_id,
                        side="b" if side == "buy" else "s",
                        positionSide="BOTH",
                        price=_fmt(price, self._tick),
                        size=_fmt(size, self._lot),
                        tif=tif,
                        ro=False,
                        po=post_only,
                        cloid=cloid,
                    )
                ],
                expiresAfter=now_ms + self.cfg.order_expiry_ms,
            ),
        )

    async def _cancel_all(self, ex: Optional[ExchangeClient], label: str):
        if not ex:
            return
        try:
            now_ms = int(time.time() * 1000)
            await asyncio.to_thread(
                ex.cancel_all,
                CancelAllParams(expiresAfter=now_ms + 60_000),
            )
        except Exception as exc:
            log.warning("cancel_all %s: %s", label, exc)

    def _pick_middle_price(self, bid: float, ask: float) -> float:
        if bid <= 0.0 or ask <= 0.0 or ask <= bid or self._tick <= 0.0:
            return 0.0
        bid_i = int(math.floor((bid / self._tick) + 1e-9))
        ask_i = int(math.ceil((ask / self._tick) - 1e-9))
        lo_i = bid_i + 1
        hi_i = ask_i - 1
        if hi_i < lo_i:
            return 0.0
        mid_i = int(round(((bid + ask) * 0.5) / self._tick))
        if mid_i < lo_i:
            mid_i = lo_i
        elif mid_i > hi_i:
            mid_i = hi_i
        px = mid_i * self._tick
        if px <= bid or px >= ask:
            return 0.0
        return px

    def _compute_size(self, price: float) -> float:
        if price <= 0.0 or self._lot <= 0.0:
            return 0.0
        size = _round_dn(self.cfg.target_notional_usd / price, self._lot)
        if size <= 0.0:
            return 0.0
        if size * price < self._min_notional:
            return 0.0
        return size

    async def _run_status_loop(self):
        while self._running:
            try:
                await asyncio.sleep(5.0)
                if not self._running:
                    return
                await self._sync_account_states(set_start=False)
                age = time.monotonic() - self._book.last_ts if self._book.last_ts > 0.0 else -1.0
                log.info(
                    "STATUS cycles=%d eligible=%d executed=%d skipped=%d | spread=%.2fbps age=%.2fs",
                    self._cycle_seq,
                    self._eligible_cycles,
                    self._executed_cycles,
                    self._skipped_cycles,
                    self._book.spread_bps,
                    age,
                )
                self._log_account_summary(prefix="STATUS")
            except asyncio.CancelledError:
                return
            except Exception:
                return

    async def _run_auto_stop(self):
        await asyncio.sleep(self.cfg.run_seconds)
        if self._running:
            log.info("Auto-stop reached (%.1fs)", self.cfg.run_seconds)
            await self.stop()


def _parse_args() -> Tuple[str, Optional[float], Optional[bool]]:
    symbol = ""
    run_seconds: Optional[float] = None
    simulate: Optional[bool] = None
    args = sys.argv[1:]
    i = 0
    while i < len(args):
        a = args[i]
        if a == "--symbol" and i + 1 < len(args):
            symbol = args[i + 1]
            i += 2
            continue
        if a == "--run-seconds" and i + 1 < len(args):
            try:
                run_seconds = float(args[i + 1])
            except ValueError:
                run_seconds = None
            i += 2
            continue
        if a == "--simulate-book":
            simulate = True
            i += 1
            continue
        if a == "--no-simulate-book":
            simulate = False
            i += 1
            continue
        i += 1
    return symbol, run_seconds, simulate


async def main():
    symbol_override, run_seconds_override, simulate_override = _parse_args()
    cfg = Config.from_env(
        symbol_override=symbol_override,
        run_seconds_override=run_seconds_override,
        simulate_override=simulate_override,
    )
    bot = MidWashBot(cfg)
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
        await bot.stop()
        await asyncio.sleep(0.0)


if __name__ == "__main__":
    asyncio.run(main())
