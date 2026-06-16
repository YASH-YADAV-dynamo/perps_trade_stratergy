import csv
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Deque, List, Optional

log = logging.getLogger("aggressive_mm")


@dataclass
class FillRecord:
    """Single fill event for markout tracking."""
    __slots__ = (
        "ts", "side", "price", "size", "mid_at_fill",
        "markouts", "markout_complete",
    )
    ts: float              # monotonic timestamp
    side: str              # "b" or "s"
    price: float
    size: float
    mid_at_fill: float     # exchange mid at fill time
    markouts: list         # [(horizon_s, mid_at_horizon, markout_bps)]
    markout_complete: bool

    def __init__(self, ts: float, side: str, price: float, size: float, mid: float):
        self.ts = ts
        self.side = side
        self.price = price
        self.size = size
        self.mid_at_fill = mid
        self.markouts = []
        self.markout_complete = False


# Markout horizons in seconds
_HORIZONS = (1.0, 5.0, 10.0, 30.0, 60.0)
_HORIZON_COUNT = len(_HORIZONS)
_MAX_HORIZON = _HORIZONS[-1]


class Reflect:
    """Markout-based fill quality tracker.

    For every fill, tracks how the mid-price moves at 1s, 5s, 10s, 30s, 60s
    after the fill. Negative markout = adverse selection (you bought and price
    dropped, or sold and price rose).

    Provides aggregated metrics:
      - adverse_rate: fraction of fills with negative 5s markout
      - avg_markout_bps at each horizon
      - fill_quality_score: composite metric

    Feeds back into Guard to modulate risk.
    """

    def __init__(self, max_fills: int = 500):
        self._pending: Deque[FillRecord] = deque()
        self._completed: Deque[FillRecord] = deque(maxlen=max_fills)
        self._max_fills = max_fills
        # Rolling stats (avoid recomputing over full history)
        self._recent_adverse_count: int = 0
        self._recent_total_count: int = 0
        self._recent_window: Deque[bool] = deque(maxlen=50)  # True = adverse

    def record_fill(self, side: str, price: float, size: float, mid: float):
        """Record a new fill for markout tracking."""
        now = time.monotonic()
        rec = FillRecord(ts=now, side=side, price=price, size=size, mid=mid)
        self._pending.append(rec)

    def tick(self, current_mid: float):
        """Called on each pricing update. Checks pending fills for markout horizons."""
        if not self._pending:
            return

        now = time.monotonic()
        completed_indices = []

        for i, rec in enumerate(self._pending):
            age = now - rec.ts
            filled_new = False

            for h in _HORIZONS:
                # Check if this horizon is already recorded
                already = False
                for existing in rec.markouts:
                    if abs(existing[0] - h) < 0.1:
                        already = True
                        break
                if already:
                    continue

                if age >= h:
                    # Compute markout in bps
                    if rec.side == "b":
                        # Bought: positive markout if mid went up
                        markout_bps = (current_mid - rec.price) / rec.price * 10000.0
                    else:
                        # Sold: positive markout if mid went down
                        markout_bps = (rec.price - current_mid) / rec.price * 10000.0
                    rec.markouts.append((h, current_mid, markout_bps))
                    filled_new = True

            if len(rec.markouts) >= _HORIZON_COUNT:
                rec.markout_complete = True
                completed_indices.append(i)

            # Expire fills older than max horizon + buffer
            if age > _MAX_HORIZON + 10.0 and not rec.markout_complete:
                rec.markout_complete = True
                completed_indices.append(i)

        # Move completed to history (iterate in reverse to pop correctly)
        for i in sorted(completed_indices, reverse=True):
            rec = self._pending[i]
            # Remove from pending
            del self._pending[i]
            self._completed.append(rec)
            # Update rolling adverse tracking (5s markout)
            is_adverse = self._is_adverse(rec)
            self._recent_window.append(is_adverse)

    def _is_adverse(self, rec: FillRecord) -> bool:
        """Check if 5s markout is negative (adverse selection)."""
        for h, mid, bps in rec.markouts:
            if abs(h - 5.0) < 0.5:
                return bps < 0.0
        # Fallback: check first available markout
        if rec.markouts:
            return rec.markouts[0][2] < 0.0
        return False

    @property
    def adverse_rate(self) -> float:
        """Fraction of recent fills with adverse 5s markout."""
        if not self._recent_window:
            return 0.0
        adverse = sum(1 for a in self._recent_window if a)
        return adverse / len(self._recent_window)

    @property
    def avg_markout_bps(self) -> dict:
        """Average markout in bps at each horizon over completed fills."""
        if not self._completed:
            return {}
        sums = {}
        counts = {}
        for rec in self._completed:
            for h, mid, bps in rec.markouts:
                h_key = f"{h:.0f}s"
                sums[h_key] = sums.get(h_key, 0.0) + bps
                counts[h_key] = counts.get(h_key, 0) + 1
        result = {}
        for k in sums:
            result[k] = sums[k] / counts[k] if counts[k] > 0 else 0.0
        return result

    @property
    def fill_count(self) -> int:
        return len(self._completed)

    @property
    def pending_count(self) -> int:
        return len(self._pending)

    def summary_line(self) -> str:
        """One-line summary for logging."""
        adv = self.adverse_rate
        markouts = self.avg_markout_bps
        m5 = markouts.get("5s", 0.0)
        m30 = markouts.get("30s", 0.0)
        m60 = markouts.get("60s", 0.0)
        return (
            f"fills={self.fill_count} adverse={adv:.1%} "
            f"m5={m5:+.2f}bps m30={m30:+.2f}bps m60={m60:+.2f}bps"
        )

    def write_csv_row(self, writer, ts_str: str):
        """Write current aggregate metrics to CSV."""
        if not writer:
            return
        adv = self.adverse_rate
        markouts = self.avg_markout_bps
        try:
            writer.writerow([
                ts_str,
                self.fill_count,
                f"{adv:.4f}",
                f"{markouts.get('1s', 0.0):.4f}",
                f"{markouts.get('5s', 0.0):.4f}",
                f"{markouts.get('10s', 0.0):.4f}",
                f"{markouts.get('30s', 0.0):.4f}",
                f"{markouts.get('60s', 0.0):.4f}",
                self.pending_count,
            ])
        except Exception:
            pass

    @staticmethod
    def csv_header() -> list:
        return [
            "timestamp", "total_fills", "adverse_rate",
            "markout_1s_bps", "markout_5s_bps", "markout_10s_bps",
            "markout_30s_bps", "markout_60s_bps", "pending_fills",
        ]
