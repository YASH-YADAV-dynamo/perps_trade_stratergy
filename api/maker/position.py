import logging
import time
from dataclasses import dataclass, field
from typing import List, Tuple

log = logging.getLogger("aggressive_mm")


@dataclass
class PositionState:
    size_base: float = 0.0
    avg_entry: float = 0.0
    entry_cost: float = 0.0
    entry_size: float = 0.0
    is_close_mode: bool = False
    session_pnl: float = 0.0
    round_trips: int = 0
    total_fills: int = 0
    total_volume_usd: float = 0.0
    last_fill_ts: float = 0.0


class PositionTracker:
    """Graduated position management with 4 tiers.

    Tier 0 (0% - skew_start%):  quote both sides normally
    Tier 1 (skew_start% - skip_open%): skew center toward reducing side
    Tier 2 (skip_open% - 100%): skip opening side entirely
    Tier 3 (>= close_threshold_usd or max_inv): close mode, reducing side only, tight spread
    """

    def __init__(
        self,
        close_threshold_usd: float,
        max_inventory: float,
        lot_size: float,
        skew_start_pct: float,
        skip_open_pct: float,
    ):
        self._close_threshold_usd = close_threshold_usd
        self._max_inventory_usd = max_inventory
        self._max_inventory = 0.0
        self._lot_size = lot_size
        self._skew_start = skew_start_pct / 100.0
        self._skip_open = skip_open_pct / 100.0
        self.state = PositionState()

    def update_fair_price(self, fair_price: float):
        """Recompute base-unit max_inventory from USD notional and current fair price."""
        if fair_price > 0.0:
            self._max_inventory = self._max_inventory_usd / fair_price

    def apply_fill(self, is_buy: bool, size: float, price: float) -> float:
        """Optimistic fill tracking. Returns round-trip PnL (0.0 if no RT)."""
        self.state.total_fills += 1
        self.state.total_volume_usd += size * price
        self.state.last_fill_ts = time.monotonic()

        old_inv = self.state.size_base
        st = self.state
        rt_pnl = 0.0

        if old_inv == 0.0 or (old_inv > 0.0 and is_buy) or (old_inv < 0.0 and not is_buy):
            # Adding to position
            st.entry_cost += price * size
            st.entry_size += size
            st.avg_entry = st.entry_cost / st.entry_size if st.entry_size > 0 else price
        else:
            # Reducing position
            close_size = min(size, abs(old_inv))
            if st.avg_entry > 0.0:
                if old_inv > 0.0:
                    rt_pnl = (price - st.avg_entry) * close_size
                else:
                    rt_pnl = (st.avg_entry - price) * close_size
                st.session_pnl += rt_pnl
                st.round_trips += 1
                log.info(
                    "RT PnL: $%.4f (entry=%.4f exit=%.4f sz=%.4f) | session=$%.4f",
                    rt_pnl, st.avg_entry, price, close_size, st.session_pnl,
                )

            remaining = size - close_size
            if abs(old_inv) - close_size < self._lot_size:
                st.entry_cost = 0.0
                st.entry_size = 0.0
                st.avg_entry = 0.0
            elif remaining > 0.0:
                st.entry_cost = price * remaining
                st.entry_size = remaining
                st.avg_entry = price

        if is_buy:
            st.size_base += size
        else:
            st.size_base -= size

        return rt_pnl

    def reconcile(self, server_size: float):
        """Correct local position from REST poll."""
        drift = abs(self.state.size_base - server_size)
        if drift > self._lot_size:
            log.info(
                "POS RECONCILE: local=%.6f server=%.6f drift=%.6f",
                self.state.size_base, server_size, drift,
            )
            self.state.size_base = server_size

    def inventory_utilization(self) -> float:
        """Current inventory as fraction of max [0, 1+]."""
        if self._max_inventory <= 0.0:
            return 0.0
        return abs(self.state.size_base) / self._max_inventory

    def get_inventory_tier(self, fair_price: float) -> int:
        """Returns the current inventory management tier (0-3).

        0 = normal, both sides
        1 = skew center toward reducing side
        2 = skip opening side
        3 = close mode (reducing side only, tight spread)
        """
        inv = self.state.size_base
        pos_usd = abs(inv * fair_price)
        util = self.inventory_utilization()

        # Tier 3: close mode
        if pos_usd >= self._close_threshold_usd or util >= 1.0:
            return 3

        # Tier 2: skip opening side
        if util >= self._skip_open:
            return 2

        # Tier 1: skew
        if util >= self._skew_start:
            return 1

        # Tier 0: normal
        return 0

    def get_allowed_sides(self, fair_price: float) -> Tuple[bool, bool]:
        """Returns (allow_bids, allow_asks) based on graduated inventory tiers."""
        inv = self.state.size_base
        tier = self.get_inventory_tier(fair_price)
        self.state.is_close_mode = tier >= 3

        if tier >= 2:
            # Skip opening side (tier 2 and 3)
            if inv > 0:
                return False, True
            elif inv < 0:
                return True, False
            else:
                return True, True

        # Tier 0 and 1: both sides allowed (skew is applied in quoter)
        if abs(inv) >= self._max_inventory:
            if inv > 0:
                return False, True
            else:
                return True, False

        return True, True

    def get_inventory_skew_factor(self) -> float:
        """Returns skew factor [-1, 1] for quote center adjustment.

        Positive = long inventory, shift center down (make asks more aggressive).
        Negative = short inventory, shift center up (make bids more aggressive).
        """
        if self._max_inventory <= 0.0:
            return 0.0
        return self.state.size_base / self._max_inventory

    def get_close_size(self) -> float:
        """In close mode, order size = exact position size."""
        return abs(self.state.size_base)

    @property
    def inventory(self) -> float:
        return self.state.size_base

    @property
    def is_close_mode(self) -> bool:
        return self.state.is_close_mode
