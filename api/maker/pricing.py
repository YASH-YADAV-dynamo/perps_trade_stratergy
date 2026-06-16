import math
import time
import os
from collections import deque
from typing import Optional

_MAX_OFFSET_DEV_BPS = float(os.getenv("MM_MAX_OFFSET_DEV_BPS", "10.0"))


class VolTracker:
    """Rolling realized volatility from price ticks."""
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

    def recent_move_bps(self, window_s: float) -> float:
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

    def vol_bps(self) -> float:
        n = len(self._prices)
        if n < 10:
            return 0.0
        sum_sq = 0.0
        for i in range(1, n):
            ret = (self._prices[i] - self._prices[i - 1]) / self._prices[i - 1]
            sum_sq += ret * ret
        return math.sqrt(sum_sq / (n - 1)) * 10000.0

    @property
    def count(self) -> int:
        return len(self._prices)


class MedianOffsetFairPrice:
    """Fair price = binance_mid + median(hs_mid - binance_mid).

    Collects one offset sample per second into a circular buffer, then
    takes the median over a configurable window.  Robust to outliers and
    captures the structural basis between Hotstuff and Binance.
    """

    def __init__(self, window_s: float = 300.0, min_samples: int = 10):
        self._window_s = window_s
        self._min_samples = min_samples
        self._max_samples = int(window_s) + 60
        self._offsets: deque = deque()
        self._timestamps: deque = deque()
        self._last_sample_ts: float = 0.0

    def add_sample(self, hs_mid: float, binance_mid: float):
        """Record one offset sample (max once per second)."""
        now = time.monotonic()
        if now - self._last_sample_ts < 1.0:
            return
        offset = hs_mid - binance_mid
        if binance_mid > 0.0 and len(self._offsets) >= self._min_samples:
            med = self.get_median_offset()
            if med is not None:
                dev_bps = abs(offset - med) / binance_mid * 10000.0
                if dev_bps > _MAX_OFFSET_DEV_BPS:
                    return
        self._last_sample_ts = now
        self._offsets.append(offset)
        self._timestamps.append(now)
        while len(self._offsets) > self._max_samples:
            self._offsets.popleft()
            self._timestamps.popleft()

    def flush(self):
        """Clear all offset samples, forcing re-warmup."""
        self._offsets.clear()
        self._timestamps.clear()
        self._last_sample_ts = 0.0

    def _valid_offsets(self) -> list:
        now = time.monotonic()
        cutoff = now - self._window_s
        return [o for o, t in zip(self._offsets, self._timestamps) if t > cutoff]

    def get_median_offset(self) -> Optional[float]:
        valid = self._valid_offsets()
        if len(valid) < self._min_samples:
            return None
        valid.sort()
        mid = len(valid) // 2
        if len(valid) % 2 == 0:
            return (valid[mid - 1] + valid[mid]) * 0.5
        return valid[mid]

    def get_fair_price(self, binance_mid: float) -> Optional[float]:
        offset = self.get_median_offset()
        if offset is None:
            return None
        return binance_mid + offset

    @property
    def sample_count(self) -> int:
        return len(self._valid_offsets())

    @property
    def is_ready(self) -> bool:
        return self.sample_count >= self._min_samples
