import logging
import math
import random
from typing import List, Optional, Tuple

log = logging.getLogger("aggressive_mm")


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


class Quote:
    __slots__ = ("side", "price", "size")

    def __init__(self, side: str, price: float, size: float):
        self.side = side
        self.price = price
        self.size = size


class Quoter:
    """Generates bid/ask quotes with vol-dynamic spread + laddered levels.

    Spread model (replaces Avellaneda-Stoikov):
      half_spread = max(min_spread_bps, vol_bps * spread_vol_mult) / 10000 * fair / 2

    Then layered:
      - inventory skew: shifts center by skew_factor * skew_bps
      - alpha shift: directional forecast from signals
      - noise: gaussian randomization
      - toxic widening: 3-tier graduated response
      - fast move widening: adverse selection protection
      - guard spread mult: from Guard risk layer

    Laddered orders:
      - Level 0 (tightest): base_size
      - Level i: base_size * scale^i at spacing * i offset
      - Outer levels catch wicks, inner levels earn tight spread

    Close mode: reducing side only, tight close_spread_bps, size = position.
    """

    def __init__(
        self,
        tick: float,
        lot: float,
        min_notional: float,
    ):
        self._tick = tick
        self._lot = lot
        self._min_notional = min_notional

    def generate(
        self,
        fair: float,
        vol_bps: float,
        min_spread_bps: float,
        spread_vol_mult: float,
        close_spread_bps: float,
        inventory_skew_factor: float,
        inventory_skew_bps: float,
        alpha_shift: float,
        noise_bps: float,
        allow_bids: bool,
        allow_asks: bool,
        order_size_usd: float,
        close_size: float,
        is_close_mode: bool,
        hs_bid: float,
        hs_ask: float,
        levels: int,
        level_spacing_bps: float,
        level_size_scale: float,
        spread_mult: float,
        toxic_score: float,
        toxic_threshold: float,
        guard_spread_mult: float,
    ) -> List[Quote]:
        """Build laddered quote levels. Returns list of Quote objects."""

        if fair <= 0.0:
            return []

        # Close mode is deterministic and aggressive: quote around fair, no alpha/noise.
        if is_close_mode:
            half_spread = close_spread_bps / 10000.0 * fair / 2.0
            if guard_spread_mult < 1.0:
                half_spread *= max(0.05, guard_spread_mult)
            center = fair
        else:
            # -- Vol-dynamic base spread --
            dynamic_bps = max(min_spread_bps, vol_bps * spread_vol_mult)
            half_spread = dynamic_bps / 10000.0 * fair / 2.0

            # -- Noise --
            noise_shift = 0.0
            if noise_bps > 0.0:
                noise_shift = random.gauss(0.0, 1.0) * noise_bps / 10000.0 * fair

            # -- Inventory skew: shift center away from heavy side --
            skew_shift = 0.0
            if abs(inventory_skew_factor) > 0.01:
                skew_shift = -inventory_skew_factor * inventory_skew_bps / 10000.0 * fair

            center = fair + alpha_shift + noise_shift + skew_shift

            # -- Adverse selection: widen on fast moves --
            half_spread *= spread_mult

            # -- Guard layer spread widening --
            half_spread *= guard_spread_mult

            # -- Toxic flow graduated widening --
            if toxic_score >= 0.95:
                allow_bids = False
                allow_asks = False
            elif toxic_score >= 0.85:
                half_spread *= 2.5
            elif toxic_score >= toxic_threshold:
                half_spread *= 1.5

        bid_base = center - half_spread
        ask_base = center + half_spread

        # -- Clamp to HS BBO (prevent crossing the exchange book) --
        if hs_ask > 0.0 and bid_base >= hs_ask:
            bid_base = hs_ask - self._tick
        if hs_bid > 0.0 and ask_base <= hs_bid:
            ask_base = hs_bid + self._tick

        # -- Build laddered levels --
        quotes: List[Quote] = []
        spacing = level_spacing_bps / 10000.0 * fair
        levels_to_quote = 1 if is_close_mode else max(1, levels)

        for i in range(levels_to_quote):
            offset = spacing * i

            if is_close_mode:
                sz = _round_dn(close_size, self._lot)
            else:
                # Size scaling: inner = base, outer = base * scale^i
                base_sz = order_size_usd / fair if fair > 0.0 else 0.0
                raw_sz = base_sz * (level_size_scale ** i)
                sz = _round_dn(raw_sz, self._lot)
                min_sz = _round_up(self._min_notional / fair, self._lot) if fair > 0.0 else 0.0
                if min_sz > 0.0 and (sz <= 0.0 or sz < min_sz):
                    sz = min_sz

            if sz <= 0.0 or sz * fair < self._min_notional:
                continue

            if allow_bids:
                bp = _round_dn(bid_base - offset, self._tick)
                if bp > 0.0:
                    quotes.append(Quote("b", bp, sz))

            if allow_asks:
                ap = _round_up(ask_base + offset, self._tick)
                if ap > 0.0:
                    quotes.append(Quote("s", ap, sz))

        # -- Crossed quote guard --
        bids = [q for q in quotes if q.side == "b"]
        asks = [q for q in quotes if q.side == "s"]
        if bids and asks:
            best_bid = max(q.price for q in bids)
            best_ask = min(q.price for q in asks)
            if best_bid >= best_ask:
                log.warning(
                    "CROSSED blocked: bid=%.4f >= ask=%.4f", best_bid, best_ask,
                )
                return []

        return quotes

    @property
    def tick(self) -> float:
        return self._tick

    @property
    def lot(self) -> float:
        return self._lot
