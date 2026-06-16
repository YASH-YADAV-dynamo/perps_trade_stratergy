import asyncio
import logging
import os
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from hotstuff import (
    ExchangeClient, PlaceOrderParams, UnitOrder, CancelAllParams,
    CancelByCloidParams, UnitCancelByClOrderId, BrokerConfig,
)

from quoter import Quote, _fmt

log = logging.getLogger("aggressive_mm")

_EXEC_MAX_DEV_BPS = float(os.getenv("MM_EXEC_MAX_DEV_BPS", "10.0"))


@dataclass
class ActiveOrder:
    cloid: str
    instrument_id: int
    side: str
    price: float
    size: float


class OrderManager:
    """Multi-instrument diff-based order management.

    Tracks active orders per instrument_id. On each requote cycle:
      1) stage_quotes(instrument_id, quotes) -- per-symbol diff
      2) flush() -- single batched place + cancel across all instruments
    """

    def __init__(self, exchange: ExchangeClient, tick_map: Dict[int, float],
                 lot_map: Dict[int, float], post_only: bool, order_expiry_ms: int,
                 broker_address: str = "", broker_fee: str = ""):
        self._ex = exchange
        self._tick_map = tick_map
        self._lot_map = lot_map
        self._post_only = post_only
        self._order_expiry_ms = order_expiry_ms
        self._broker_cfg = (
            BrokerConfig(broker=broker_address, fee=broker_fee)
            if broker_address else None
        )
        self._active: Dict[int, List[ActiveOrder]] = {}
        self._cloid_seq = 0
        self._ref_prices: Dict[int, float] = {}

        self._pending_place: List[Tuple[int, Quote]] = []
        self._pending_cancel: List[ActiveOrder] = []
        self._pending_keep: Dict[int, List[ActiveOrder]] = {}

    def _next_cloid(self) -> str:
        self._cloid_seq += 1
        return f"mm-{int(time.time())}-{self._cloid_seq}"

    def update_ref_price(self, instrument_id: int, ref_price: float):
        if ref_price > 0.0:
            self._ref_prices[instrument_id] = ref_price

    def _order_matches_quote(self, order: ActiveOrder, quote: Quote, inst_id: int) -> bool:
        tick = self._tick_map.get(inst_id, 0.001)
        lot = self._lot_map.get(inst_id, 0.01)
        return (
            order.side == quote.side
            and abs(order.price - quote.price) < tick * 0.5
            and abs(order.size - quote.size) < lot * 0.5
        )

    @staticmethod
    def _is_accepted_status(item) -> Tuple[bool, str]:
        if not isinstance(item, dict):
            return False, f"unexpected status type={type(item).__name__}"
        if "error" in item:
            err = item.get("error")
            if isinstance(err, dict):
                msg = err.get("error") or err.get("message") or str(err)
            else:
                msg = str(err)
            return False, msg
        if (
            "resting" in item
            or "filled" in item
            or "success" in item
            or "open" in item
        ):
            return True, ""
        accepted = item.get("accepted")
        if isinstance(accepted, bool):
            if accepted:
                return True, ""
            return False, "rejected by venue"
        keys = ",".join(str(k) for k in item.keys())
        return False, f"unrecognized status keys={keys}"

    def stage_quotes(self, instrument_id: int, new_quotes: List[Quote]) -> bool:
        """Diff new quotes against active orders for one instrument.
        Stages places/cancels for the next flush(). Returns True if anything changed."""
        current = self._active.get(instrument_id, [])
        kept: List[ActiveOrder] = []
        to_cancel: List[ActiveOrder] = []
        to_place: List[Quote] = []

        matched_indices = set()
        for quote in new_quotes:
            match = None
            for idx, order in enumerate(current):
                if idx not in matched_indices and self._order_matches_quote(order, quote, instrument_id):
                    match = order
                    matched_indices.add(idx)
                    break
            if match:
                kept.append(match)
            else:
                to_place.append(quote)

        for idx, order in enumerate(current):
            if idx not in matched_indices:
                to_cancel.append(order)

        if not to_cancel and not to_place:
            self._pending_keep[instrument_id] = current
            return False

        if to_cancel:
            self._pending_cancel.extend(to_cancel)
            kept.clear()

        for q in to_place:
            self._pending_place.append((instrument_id, q))

        self._pending_keep[instrument_id] = kept
        return True

    async def flush(self) -> bool:
        """Execute all staged cancels and places in batched API calls.
        Returns True if any orders were changed."""
        has_changes = bool(self._pending_cancel) or bool(self._pending_place)

        cancel_failed: List[ActiveOrder] = []
        if self._pending_cancel:
            cancel_failed = await self._cancel_orders(self._pending_cancel)

        placed: Dict[int, List[ActiveOrder]] = {}
        if self._pending_place:
            placed = await self._place_orders(self._pending_place)

        fail_by_inst: Dict[int, List[ActiveOrder]] = {}
        for o in cancel_failed:
            if o.instrument_id not in fail_by_inst:
                fail_by_inst[o.instrument_id] = []
            fail_by_inst[o.instrument_id].append(o)

        for inst_id, kept in self._pending_keep.items():
            final = list(kept)
            if inst_id in fail_by_inst:
                final.extend(fail_by_inst[inst_id])
            if inst_id in placed:
                final.extend(placed[inst_id])
            self._active[inst_id] = final

        for inst_id in placed:
            if inst_id not in self._pending_keep:
                extra = fail_by_inst.get(inst_id, [])
                self._active[inst_id] = extra + placed[inst_id]

        self._pending_place.clear()
        self._pending_cancel.clear()
        self._pending_keep.clear()
        return has_changes

    async def cancel_all(self) -> bool:
        """Cancel everything on the account. Only clears local state on success."""
        ok = False
        try:
            now_ms = int(time.time() * 1000)
            await asyncio.to_thread(
                self._ex.cancel_all,
                CancelAllParams(expiresAfter=now_ms + 60_000),
            )
            ok = True
        except Exception as exc:
            log.warning("cancel_all FAILED (local state kept): %s", exc)
        if ok:
            self._active.clear()
        self._pending_place.clear()
        self._pending_cancel.clear()
        self._pending_keep.clear()
        return ok

    async def force_close_ioc(
        self,
        instrument_id: int,
        side: str,
        price: float,
        size: float,
        note: str = "",
        max_dev_bps: Optional[float] = None,
    ) -> bool:
        """Submit one reduce-only IOC close order for emergency flattening."""
        if instrument_id <= 0 or side not in ("b", "s") or price <= 0.0 or size <= 0.0:
            return False
        ref = self._ref_prices.get(instrument_id, 0.0)
        dev_limit = max_dev_bps if (max_dev_bps is not None and max_dev_bps > 0.0) else _EXEC_MAX_DEV_BPS
        if ref > 0.0:
            dev_bps = abs(price - ref) / ref * 10000.0
            if dev_bps > dev_limit:
                log.error(
                    "EXEC FIREWALL: blocked IOC inst=%d side=%s px=%.6f "
                    "ref=%.6f dev=%.1fbps lim=%.1fbps note=%s",
                    instrument_id, side, price, ref, dev_bps, dev_limit, note,
                )
                return False
        tick = self._tick_map.get(instrument_id, 0.001)
        lot = self._lot_map.get(instrument_id, 0.01)
        cloid = self._next_cloid()
        unit = UnitOrder(
            instrumentId=instrument_id,
            side=side,
            positionSide="BOTH",
            price=_fmt(price, tick),
            size=_fmt(size, lot),
            tif="IOC",
            ro=True,
            po=False,
            cloid=cloid,
        )
        resp = None
        try:
            now_ms = int(time.time() * 1000)
            ttl_ms = max(5000, min(60000, int(self._order_expiry_ms)))
            resp = await asyncio.to_thread(
                self._ex.place_order,
                PlaceOrderParams(
                    orders=[unit],
                    expiresAfter=now_ms + ttl_ms,
                    brokerConfig=self._broker_cfg,
                ),
            )
        except Exception as exc:
            log.warning(
                "force_close_ioc failed inst=%s side=%s px=%s sz=%s: %s",
                instrument_id, side, _fmt(price, tick), _fmt(size, lot), exc,
            )
            return False

        status_item = None
        if isinstance(resp, dict):
            data = resp.get("data")
            if isinstance(data, dict):
                st = data.get("status")
                if isinstance(st, list) and st:
                    status_item = st[0]
        if status_item is None:
            log.warning(
                "force_close_ioc submitted inst=%s side=%s px=%s sz=%s note=%s (status unknown)",
                instrument_id, side, _fmt(price, tick), _fmt(size, lot), note or "-",
            )
            return True

        ok, msg = self._is_accepted_status(status_item)
        if not ok:
            log.warning(
                "force_close_ioc rejected inst=%s side=%s px=%s sz=%s note=%s: %s",
                instrument_id, side, _fmt(price, tick), _fmt(size, lot), note or "-", msg,
            )
            return False
        log.warning(
            "force_close_ioc accepted inst=%s side=%s px=%s sz=%s note=%s",
            instrument_id, side, _fmt(price, tick), _fmt(size, lot), note or "-",
        )
        return True

    def reconcile_from_exchange(self, remote_cloids: Dict[int, set[str]]) -> Tuple[int, List[Tuple[str, int]]]:
        """Sync local state with exchange openOrders snapshot.
        Returns (local_removed_count, list_of_(cloid, instrument_id) orphans to cancel)."""
        removed = 0
        local_cloids_all: set = set()
        for inst_id, local_orders in list(self._active.items()):
            for o in local_orders:
                local_cloids_all.add(o.cloid)
            remote = remote_cloids.get(inst_id)
            if not remote:
                removed += len(local_orders)
                self._active.pop(inst_id, None)
                self._pending_keep.pop(inst_id, None)
                continue
            kept = [o for o in local_orders if o.cloid in remote]
            dropped = len(local_orders) - len(kept)
            if dropped > 0:
                removed += dropped
                if kept:
                    self._active[inst_id] = kept
                else:
                    self._active.pop(inst_id, None)
                    self._pending_keep.pop(inst_id, None)

        orphans: List[Tuple[str, int]] = []
        for inst_id, cloid_set in remote_cloids.items():
            for cloid in cloid_set:
                if cloid and cloid.startswith("mm-") and cloid not in local_cloids_all:
                    orphans.append((cloid, inst_id))
        return removed, orphans

    async def cancel_orphans(self, orphans: List[Tuple[str, int]]):
        """Cancel remote orders that exist on exchange but not in local state."""
        if not orphans:
            return
        cancels = [
            UnitCancelByClOrderId(cloid=c, instrumentId=iid)
            for c, iid in orphans
        ]
        try:
            now_ms = int(time.time() * 1000)
            await asyncio.to_thread(
                self._ex.cancel_by_cloid,
                CancelByCloidParams(cancels=cancels, expiresAfter=now_ms + 60_000),
            )
            log.warning("Cancelled %d orphan remote orders", len(orphans))
        except Exception as exc:
            log.warning("cancel_orphans (%d orders): %s", len(orphans), exc)

    async def _cancel_orders(self, orders: List[ActiveOrder]) -> List[ActiveOrder]:
        """Cancel orders by cloid. Returns list of orders whose cancel FAILED."""
        if not orders:
            return []
        cancels = [
            UnitCancelByClOrderId(cloid=o.cloid, instrumentId=o.instrument_id)
            for o in orders
        ]
        resp = None
        try:
            now_ms = int(time.time() * 1000)
            resp = await asyncio.to_thread(
                self._ex.cancel_by_cloid,
                CancelByCloidParams(cancels=cancels, expiresAfter=now_ms + 60_000),
            )
        except Exception as exc:
            log.warning(
                "cancel_by_cloid FAILED (%d orders kept in local state): %s",
                len(cancels), exc,
            )
            return list(orders)

        statuses = None
        if isinstance(resp, dict):
            data = resp.get("data")
            if isinstance(data, dict):
                st = data.get("status")
                if isinstance(st, list):
                    statuses = st

        if statuses is None:
            return []

        _GONE = ("not found", "already cancelled", "already filled", "not open")
        failed: List[ActiveOrder] = []
        limit = min(len(statuses), len(orders))
        for i in range(limit):
            ok, msg = self._is_accepted_status(statuses[i])
            if not ok:
                msg_lower = msg.lower()
                if any(p in msg_lower for p in _GONE):
                    continue
                failed.append(orders[i])
                log.warning(
                    "cancel_by_cloid rejected cloid=%s inst=%s: %s",
                    orders[i].cloid, orders[i].instrument_id, msg,
                )
        if failed:
            log.warning(
                "cancel_by_cloid: %d/%d failed (kept in local state)",
                len(failed), len(orders),
            )
        return failed

    async def _place_orders(self, items: List[Tuple[int, Quote]]) -> Dict[int, List[ActiveOrder]]:
        if not items:
            return {}

        safe_items: List[Tuple[int, Quote]] = []
        for inst_id, q in items:
            ref = self._ref_prices.get(inst_id, 0.0)
            if ref > 0.0:
                dev_bps = abs(q.price - ref) / ref * 10000.0
                if dev_bps > _EXEC_MAX_DEV_BPS:
                    log.error(
                        "EXEC FIREWALL: blocked GTC inst=%d side=%s "
                        "px=%.6f ref=%.6f dev=%.1fbps",
                        inst_id, q.side, q.price, ref, dev_bps,
                    )
                    continue
            safe_items.append((inst_id, q))
        if not safe_items:
            return {}

        units = []
        pending: List[Tuple[int, ActiveOrder]] = []

        for inst_id, q in safe_items:
            cloid = self._next_cloid()
            tick = self._tick_map.get(inst_id, 0.001)
            lot = self._lot_map.get(inst_id, 0.01)
            units.append(UnitOrder(
                instrumentId=inst_id,
                side=q.side,
                positionSide="BOTH",
                price=_fmt(q.price, tick),
                size=_fmt(q.size, lot),
                tif="GTC",
                ro=False,
                po=self._post_only,
                cloid=cloid,
            ))
            pending.append((inst_id, ActiveOrder(
                cloid=cloid, instrument_id=inst_id,
                side=q.side, price=q.price, size=q.size,
            )))

        resp = None
        try:
            now_ms = int(time.time() * 1000)
            resp = await asyncio.to_thread(
                self._ex.place_order,
                PlaceOrderParams(
                    orders=units,
                    expiresAfter=now_ms + self._order_expiry_ms,
                    brokerConfig=self._broker_cfg,
                ),
            )
            log.debug("Placed %d orders across instruments", len(units))
        except Exception as exc:
            log.error("Place orders failed: %s", exc)
            return {}

        statuses = None
        if isinstance(resp, dict):
            data = resp.get("data")
            if isinstance(data, dict):
                st = data.get("status")
                if isinstance(st, list):
                    statuses = st

        accepted_mask = None
        if statuses is not None:
            accepted_mask = [True] * len(pending)
            limit = min(len(statuses), len(pending))
            if len(statuses) != len(pending):
                log.warning(
                    "place_order status mismatch: got=%d expected=%d",
                    len(statuses), len(pending),
                )
            rejected = 0
            for i in range(limit):
                ok, msg = self._is_accepted_status(statuses[i])
                if ok:
                    continue
                rejected += 1
                accepted_mask[i] = False
                inst_id, order = pending[i]
                log.warning(
                    "place_order rejected inst=%s cloid=%s: %s",
                    inst_id, order.cloid, msg,
                )
            if rejected or len(statuses) != len(pending):
                accepted = sum(1 for flag in accepted_mask if flag)
                log.warning(
                    "place_order status summary: accepted=%d rejected=%d",
                    accepted, rejected,
                )

        result: Dict[int, List[ActiveOrder]] = {}
        for i, (inst_id, order) in enumerate(pending):
            if accepted_mask is not None and not accepted_mask[i]:
                continue
            if inst_id not in result:
                result[inst_id] = []
            result[inst_id].append(order)
        return result

    def active_count(self, instrument_id: int = None) -> int:
        if instrument_id is not None:
            return len(self._active.get(instrument_id, []))
        return sum(len(v) for v in self._active.values())

    def resting_size_by_side(self, instrument_id: int) -> Tuple[float, float]:
        """Return (bid_size, ask_size) total resting for an instrument."""
        bid_sz = 0.0
        ask_sz = 0.0
        for o in self._active.get(instrument_id, []):
            if o.side == "b":
                bid_sz += o.size
            else:
                ask_sz += o.size
        return bid_sz, ask_sz
