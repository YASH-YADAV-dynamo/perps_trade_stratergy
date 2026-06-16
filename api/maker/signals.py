import math
from collections import deque


def _ema_alpha(span: int) -> float:
    return 2.0 / (span + 1) if span > 0 else 1.0


class OBITracker:
    """Contrarian Order Book Imbalance (arxiv 2502.18625).

    When OBI is extreme but momentum is fading (imbalance decelerating /
    reversing), quote AGAINST the imbalance.
    """
    __slots__ = (
        "_obi_raw", "_obi_ema", "_obi_prev", "_obi_momentum",
        "_alpha", "_extreme_threshold", "_initialized",
    )

    def __init__(self, ema_span: int, extreme_threshold: float):
        self._obi_raw = 0.0
        self._obi_ema = 0.0
        self._obi_prev = 0.0
        self._obi_momentum = 0.0
        self._alpha = _ema_alpha(ema_span)
        self._extreme_threshold = extreme_threshold
        self._initialized = False

    def update(self, bid_qty: float, ask_qty: float):
        total = bid_qty + ask_qty
        if total <= 0.0:
            return
        raw = (bid_qty - ask_qty) / total
        self._obi_raw = raw
        if not self._initialized:
            self._obi_ema = raw
            self._initialized = True
        else:
            self._obi_ema += self._alpha * (raw - self._obi_ema)
        self._obi_momentum = raw - self._obi_prev
        self._obi_prev = raw

    @property
    def contrarian_signal(self) -> float:
        """[-1, 1]. Positive = bullish contrarian (ask-heavy fading -> buy)."""
        obi = self._obi_ema
        mom = self._obi_momentum
        if abs(obi) < self._extreme_threshold:
            return 0.0
        if obi > 0.0 and mom < 0.0:
            strength = min(1.0, abs(obi) * (1.0 + min(abs(mom) * 5.0, 2.0)))
            return -strength
        if obi < 0.0 and mom > 0.0:
            strength = min(1.0, abs(obi) * (1.0 + min(abs(mom) * 5.0, 2.0)))
            return strength
        return 0.0

    @property
    def obi_raw(self) -> float:
        return self._obi_raw

    @property
    def obi_ema(self) -> float:
        return self._obi_ema


class BarPortionTracker:
    """Candlestick alpha (SSRN 5066176).

    BP = (close - low) / (high - low) on completed 1m candles.
    """
    __slots__ = ("_bp_ema", "_alpha", "_bp_raw", "_has_data")

    def __init__(self, ema_span: int):
        self._bp_ema = 0.5
        self._bp_raw = 0.5
        self._alpha = _ema_alpha(ema_span)
        self._has_data = False

    def on_kline(self, o: float, h: float, low: float, c: float, closed: bool):
        rng = h - low
        bp = (c - low) / rng if rng > 0.0 else 0.5
        if closed:
            self._bp_ema += self._alpha * (bp - self._bp_ema)
            self._has_data = True
        self._bp_raw = bp

    @property
    def signal(self) -> float:
        """Centered [-1, 1]. Positive = bullish."""
        if not self._has_data:
            return 0.0
        return (self._bp_ema - 0.5) * 2.0

    @property
    def bp_raw(self) -> float:
        return self._bp_raw


class TradeImbalanceTracker:
    """Net buy/sell aggressor volume (arxiv 2602.00776)."""
    __slots__ = (
        "_events", "_window_s", "_buy_sum", "_sell_sum",
        "_imb_ema", "_alpha", "_initialized",
    )

    def __init__(self, window_s: float, ema_span: int):
        self._events: deque = deque()
        self._window_s = window_s
        self._buy_sum = 0.0
        self._sell_sum = 0.0
        self._imb_ema = 0.0
        self._alpha = _ema_alpha(ema_span)
        self._initialized = False

    def on_trade(self, qty: float, price: float, is_sell: bool, ts: float):
        notional = qty * price
        self._events.append((ts, notional, is_sell))
        if is_sell:
            self._sell_sum += notional
        else:
            self._buy_sum += notional
        cutoff = ts - self._window_s
        while self._events and self._events[0][0] < cutoff:
            old_ts, old_not, old_sell = self._events.popleft()
            if old_sell:
                self._sell_sum -= old_not
            else:
                self._buy_sum -= old_not
        total = self._buy_sum + self._sell_sum
        if total > 0.0:
            raw = (self._buy_sum - self._sell_sum) / total
            if not self._initialized:
                self._imb_ema = raw
                self._initialized = True
            else:
                self._imb_ema += self._alpha * (raw - self._imb_ema)

    @property
    def signal(self) -> float:
        """[-1, 1]. Positive = net buy pressure."""
        return max(-1.0, min(1.0, self._imb_ema))

    @property
    def raw_imbalance(self) -> float:
        return self._imb_ema


class RegimeDetector:
    """Toxic flow detection (arxiv 2508.16588, 2602.00776).

    Combines OBI flips + price velocity. Tiered response:
      0.70-0.85: widen 1.5x
      0.85-0.95: widen 2.5x + skip opening side
      >= 0.95:   pull all quotes
    """
    __slots__ = ("_obi_flip_thresh", "_vol_spike_mult", "_prev_obi", "_toxic_score")

    _OBI_ONLY_WEIGHT = 0.30
    _VOL_ONLY_WEIGHT = 0.60

    def __init__(self, obi_flip_threshold: float, vol_spike_mult: float):
        self._obi_flip_thresh = obi_flip_threshold
        self._vol_spike_mult = vol_spike_mult
        self._prev_obi = 0.0
        self._toxic_score = 0.0

    def update(self, obi: float, recent_move_bps: float, baseline_move_bps: float):
        obi_score = 0.0
        vol_score = 0.0

        obi_change = abs(obi - self._prev_obi)
        if obi_change > self._obi_flip_thresh:
            obi_score = min(1.0, obi_change / (self._obi_flip_thresh * 2.0))
        self._prev_obi = obi

        if baseline_move_bps > 0.5:
            ratio = recent_move_bps / baseline_move_bps
            if ratio > self._vol_spike_mult:
                vol_score = min(1.0, (ratio - 1.0) / max(1.0, self._vol_spike_mult - 1.0))

        if obi_score > 0.0 and vol_score > 0.0:
            combined = max(obi_score, vol_score)
        elif obi_score > 0.0:
            combined = obi_score * self._OBI_ONLY_WEIGHT
        elif vol_score > 0.0:
            combined = vol_score * self._VOL_ONLY_WEIGHT
        else:
            combined = 0.0

        if combined > 0.0:
            self._toxic_score = combined
        else:
            self._toxic_score = max(0.0, self._toxic_score * 0.7)

    @property
    def toxic_score(self) -> float:
        return self._toxic_score


class CVDTracker:
    """Cumulative Volume Delta from Binance aggTrades.

    Accumulates real buy/sell volume per 1m candle, then detects divergence
    between price trend and buying pressure trend over a lookback window.

    Bearish divergence: price higher high + CVD lower high (buying weakening)
    Bullish divergence: price lower low + CVD higher low (selling exhausting)
    """

    def __init__(self, lookback: int = 14):
        self._buy_vol: float = 0.0
        self._sell_vol: float = 0.0
        self._cvd: float = 0.0
        self._lookback = lookback
        self._cvd_history: deque = deque(maxlen=lookback * 3)
        self._close_history: deque = deque(maxlen=lookback * 3)
        self._divergence: float = 0.0

    def on_trade(self, qty: float, price: float, is_sell: bool):
        notional = qty * price
        if is_sell:
            self._sell_vol += notional
        else:
            self._buy_vol += notional

    def on_candle_close(self, close: float):
        delta = self._buy_vol - self._sell_vol
        self._cvd += delta
        self._cvd_history.append(self._cvd)
        self._close_history.append(close)
        self._buy_vol = 0.0
        self._sell_vol = 0.0
        self._detect_divergence()

    def _detect_divergence(self):
        n = len(self._cvd_history)
        lb = self._lookback
        if n < lb * 2:
            return

        rc = list(self._close_history)
        rv = list(self._cvd_history)
        recent_c = rc[-lb:]
        prev_c = rc[-lb * 2:-lb]
        recent_v = rv[-lb:]
        prev_v = rv[-lb * 2:-lb]

        price_hh = max(recent_c) > max(prev_c)
        cvd_lh = max(recent_v) < max(prev_v)
        price_ll = min(recent_c) < min(prev_c)
        cvd_hl = min(recent_v) > min(prev_v)

        if price_hh and cvd_lh:
            self._divergence = -1.0
        elif price_ll and cvd_hl:
            self._divergence = 1.0
        else:
            self._divergence *= 0.8

    @property
    def signal(self) -> float:
        """[-1, 1]. Negative = bearish divergence, positive = bullish."""
        return max(-1.0, min(1.0, self._divergence))

    @property
    def cvd_value(self) -> float:
        return self._cvd


class RSITracker:
    """Wilder RSI from Binance kline closes with divergence detection.

    Computes standard 14-period RSI using Wilder's smoothing (not EMA).
    Detects divergence between price extremes and RSI extremes over a
    lookback window.

    Bearish: price higher high + RSI lower high (momentum fading)
    Bullish: price lower low + RSI higher low (selling exhaustion)
    """

    def __init__(self, period: int = 14, div_lookback: int = 14):
        self._period = period
        self._div_lookback = div_lookback
        self._prev_close: float = 0.0
        self._avg_gain: float = 0.0
        self._avg_loss: float = 0.0
        self._rsi: float = 50.0
        self._count: int = 0
        self._rsi_history: deque = deque(maxlen=div_lookback * 3)
        self._close_history: deque = deque(maxlen=div_lookback * 3)
        self._divergence: float = 0.0

    def on_candle_close(self, close: float):
        self._count += 1
        if self._count == 1:
            self._prev_close = close
            return

        delta = close - self._prev_close
        self._prev_close = close
        gain = max(delta, 0.0)
        loss = max(-delta, 0.0)

        if self._count <= self._period + 1:
            # Accumulation phase: store gains/losses for initial SMA
            if not hasattr(self, "_init_gains"):
                self._init_gains: list = []
                self._init_losses: list = []
            self._init_gains.append(gain)
            self._init_losses.append(loss)
            if self._count == self._period + 1:
                self._avg_gain = sum(self._init_gains) / self._period
                self._avg_loss = sum(self._init_losses) / self._period
                del self._init_gains
                del self._init_losses
            else:
                return
        else:
            self._avg_gain = (self._avg_gain * (self._period - 1) + gain) / self._period
            self._avg_loss = (self._avg_loss * (self._period - 1) + loss) / self._period

        if self._avg_loss > 0.0:
            rs = self._avg_gain / self._avg_loss
            self._rsi = 100.0 - (100.0 / (1.0 + rs))
        else:
            self._rsi = 100.0

        self._rsi_history.append(self._rsi)
        self._close_history.append(close)
        self._detect_divergence()

    def _detect_divergence(self):
        n = len(self._rsi_history)
        lb = self._div_lookback
        if n < lb * 2:
            return

        rc = list(self._close_history)
        rr = list(self._rsi_history)
        recent_c = rc[-lb:]
        prev_c = rc[-lb * 2:-lb]
        recent_r = rr[-lb:]
        prev_r = rr[-lb * 2:-lb]

        price_hh = max(recent_c) > max(prev_c)
        rsi_lh = max(recent_r) < max(prev_r)
        price_ll = min(recent_c) < min(prev_c)
        rsi_hl = min(recent_r) > min(prev_r)

        if price_hh and rsi_lh:
            self._divergence = -1.0
        elif price_ll and rsi_hl:
            self._divergence = 1.0
        else:
            self._divergence *= 0.8

    @property
    def rsi(self) -> float:
        return self._rsi

    @property
    def signal(self) -> float:
        """[-1, 1]. Negative = bearish divergence, positive = bullish."""
        return max(-1.0, min(1.0, self._divergence))


class ADXRegimeFilter:
    """ADX from Binance 1m klines -- spread regime adjustment.

    ADX < 20: ranging market, spread *= 0.85 (tighten -- MM sweet spot)
    ADX 20-30: transition, spread *= 1.0
    ADX > 30: trending, spread *= 1.0 + (adx-30)/100 (widen progressively)

    Uses Wilder's smoothing (same as TradingView ADX).
    """

    def __init__(self, period: int = 14):
        self._period = period
        self._count: int = 0
        self._prev_high: float = 0.0
        self._prev_low: float = 0.0
        self._prev_close: float = 0.0
        self._atr: float = 0.0
        self._plus_dm_s: float = 0.0
        self._minus_dm_s: float = 0.0
        self._adx: float = 0.0
        self._tr_accum: float = 0.0
        self._pdm_accum: float = 0.0
        self._mdm_accum: float = 0.0
        self._initialized: bool = False

    def on_candle_close(self, o: float, h: float, low: float, c: float):
        self._count += 1
        if self._count == 1:
            self._prev_high = h
            self._prev_low = low
            self._prev_close = c
            return

        tr = max(h - low, abs(h - self._prev_close), abs(low - self._prev_close))
        up_move = h - self._prev_high
        down_move = self._prev_low - low
        plus_dm = max(up_move, 0.0) if up_move > down_move else 0.0
        minus_dm = max(down_move, 0.0) if down_move > up_move else 0.0

        self._prev_high = h
        self._prev_low = low
        self._prev_close = c
        p = self._period

        if not self._initialized:
            self._tr_accum += tr
            self._pdm_accum += plus_dm
            self._mdm_accum += minus_dm
            if self._count == p + 1:
                self._atr = self._tr_accum / p
                self._plus_dm_s = self._pdm_accum / p
                self._minus_dm_s = self._mdm_accum / p
                if self._atr > 0:
                    pdi = 100.0 * self._plus_dm_s / self._atr
                    mdi = 100.0 * self._minus_dm_s / self._atr
                    di_sum = pdi + mdi
                    self._adx = abs(pdi - mdi) / di_sum * 100.0 if di_sum > 0 else 0.0
                self._initialized = True
            return

        self._atr = (self._atr * (p - 1) + tr) / p
        self._plus_dm_s = (self._plus_dm_s * (p - 1) + plus_dm) / p
        self._minus_dm_s = (self._minus_dm_s * (p - 1) + minus_dm) / p
        if self._atr > 0:
            pdi = 100.0 * self._plus_dm_s / self._atr
            mdi = 100.0 * self._minus_dm_s / self._atr
            di_sum = pdi + mdi
            dx = abs(pdi - mdi) / di_sum * 100.0 if di_sum > 0 else 0.0
            self._adx = (self._adx * (p - 1) + dx) / p

    @property
    def adx(self) -> float:
        return self._adx

    @property
    def spread_mult(self) -> float:
        if not self._initialized:
            return 1.0
        if self._adx < 20.0:
            return 0.85
        if self._adx > 30.0:
            return 1.0 + min((self._adx - 30.0) / 100.0, 0.5)
        return 1.0

    @property
    def is_trending(self) -> bool:
        return self._initialized and self._adx > 25.0


class SuperTrendFilter:
    """SuperTrend from Binance 1m klines -- alpha confirmation filter.

    Classic ATR-based trailing band. When alpha direction aligns with
    SuperTrend trend: boost alpha 1.3x. When they conflict: dampen 0.5x.
    """

    def __init__(self, atr_period: int = 10, multiplier: float = 3.0):
        self._atr_period = atr_period
        self._mult = multiplier
        self._count: int = 0
        self._prev_close: float = 0.0
        self._atr: float = 0.0
        self._final_upper: float = 0.0
        self._final_lower: float = 0.0
        self._trend: int = 0
        self._tr_sum: float = 0.0
        self._initialized: bool = False

    def on_candle_close(self, o: float, h: float, low: float, c: float):
        self._count += 1
        if self._count == 1:
            self._prev_close = c
            self._tr_sum = h - low
            return

        tr = max(h - low, abs(h - self._prev_close), abs(low - self._prev_close))

        if self._count <= self._atr_period:
            self._tr_sum += tr
            self._prev_close = c
            if self._count == self._atr_period:
                self._atr = self._tr_sum / self._atr_period
                hl2 = (h + low) * 0.5
                self._final_upper = hl2 + self._mult * self._atr
                self._final_lower = hl2 - self._mult * self._atr
                self._trend = 1 if c > (self._final_upper + self._final_lower) * 0.5 else -1
                self._initialized = True
            return

        p = self._atr_period
        self._atr = (self._atr * (p - 1) + tr) / p

        hl2 = (h + low) * 0.5
        basic_upper = hl2 + self._mult * self._atr
        basic_lower = hl2 - self._mult * self._atr

        if basic_upper < self._final_upper or self._prev_close > self._final_upper:
            final_upper = basic_upper
        else:
            final_upper = self._final_upper

        if basic_lower > self._final_lower or self._prev_close < self._final_lower:
            final_lower = basic_lower
        else:
            final_lower = self._final_lower

        if self._trend == 1:
            if c < final_lower:
                self._trend = -1
        elif self._trend == -1:
            if c > final_upper:
                self._trend = 1

        self._final_upper = final_upper
        self._final_lower = final_lower
        self._prev_close = c

    def confirm_alpha(self, alpha: float) -> float:
        """Boost alpha when aligned with trend, dampen when conflicting."""
        if not self._initialized or self._trend == 0:
            return alpha
        if (alpha > 0.0 and self._trend > 0) or (alpha < 0.0 and self._trend < 0):
            return alpha * 1.3
        if (alpha > 0.0 and self._trend < 0) or (alpha < 0.0 and self._trend > 0):
            return alpha * 0.5
        return alpha

    @property
    def trend(self) -> int:
        return self._trend

    @property
    def trend_str(self) -> str:
        if self._trend > 0:
            return "UP"
        if self._trend < 0:
            return "DOWN"
        return "FLAT"


class PivotTracker:
    """Daily pivot points from Binance price data.

    Standard floor pivots from previous day H/L/C:
      P  = (H + L + C) / 3
      R1 = 2P - L,  S1 = 2P - H
      R2 = P + (H-L),  S2 = P - (H-L)
      R3 = H + 2(P-L),  S3 = L - 2(H-P)

    Near pivot levels: tighten spread (more activity expected).
    Far from levels: normal spread.
    """

    def __init__(self):
        self._day_high: float = 0.0
        self._day_low: float = 1e18
        self._day_close: float = 0.0
        self._pivot: float = 0.0
        self._r1: float = 0.0
        self._r2: float = 0.0
        self._r3: float = 0.0
        self._s1: float = 0.0
        self._s2: float = 0.0
        self._s3: float = 0.0
        self._ready: bool = False

    def set_previous_day(self, h: float, low: float, c: float):
        """Load previous day H/L/C directly (e.g. from Binance REST on startup)."""
        self._compute_pivots(h, low, c)

    def on_candle_close(self, o: float, h: float, low: float, c: float):
        """Track intraday extremes from 1m candles."""
        if h > self._day_high:
            self._day_high = h
        if low < self._day_low:
            self._day_low = low
        self._day_close = c

    def on_day_boundary(self):
        """Call at UTC midnight to rotate: today's data becomes pivots for tomorrow."""
        if self._day_high > 0.0 and self._day_low < 1e18 and self._day_close > 0.0:
            self._compute_pivots(self._day_high, self._day_low, self._day_close)
        self._day_high = 0.0
        self._day_low = 1e18
        self._day_close = 0.0

    def _compute_pivots(self, h: float, low: float, c: float):
        p = (h + low + c) / 3.0
        self._pivot = p
        self._r1 = 2.0 * p - low
        self._s1 = 2.0 * p - h
        self._r2 = p + (h - low)
        self._s2 = p - (h - low)
        self._r3 = h + 2.0 * (p - low)
        self._s3 = low - 2.0 * (h - p)
        self._ready = True

    def nearest_level_distance_bps(self, price: float) -> float:
        if not self._ready or price <= 0.0:
            return 9999.0
        min_dist = 1e18
        for lvl in (self._s3, self._s2, self._s1, self._pivot,
                    self._r1, self._r2, self._r3):
            if lvl > 0.0:
                dist = abs(price - lvl) / price * 10000.0
                if dist < min_dist:
                    min_dist = dist
        return min_dist

    def spread_mult(self, price: float) -> float:
        """Tighter spread near pivot levels (more activity expected)."""
        dist = self.nearest_level_distance_bps(price)
        if dist < 5.0:
            return 0.8
        if dist < 15.0:
            return 0.9
        return 1.0

    @property
    def levels(self) -> dict:
        if not self._ready:
            return {}
        return {
            "S3": self._s3, "S2": self._s2, "S1": self._s1,
            "P": self._pivot,
            "R1": self._r1, "R2": self._r2, "R3": self._r3,
        }

    @property
    def ready(self) -> bool:
        return self._ready
