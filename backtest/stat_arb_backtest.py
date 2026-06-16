"""
Statistical Arbitrage Strategy - Backtest Version
===================================================

Profit-oriented HYPE/BTC pairs mean-reversion.
Fewer, higher-conviction trades; volume is secondary.

All signals use only data available at bar close (no lookahead).
"""

from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


@dataclass
class SignalConfig:
    """Profit-oriented defaults: selective entries, let mean reversion pay."""
    lookback: int = 120
    entry_zscore: float = 3.25
    exit_zscore: float = 0.28
    stop_zscore: float = 1.25           # cut when spread moves further against us
    rsi_period: int = 14
    ema_fast: int = 9
    ema_slow: int = 21

    # Z-score dominates; aux signals are confirmation only
    weight_zscore: float = 0.70
    weight_rsi: float = 0.15
    weight_ema: float = 0.10
    weight_momentum: float = 0.05

    min_correlation: float = 0.78
    min_half_life_bars: float = 5.0     # skip noise
    max_half_life_bars: float = 35.0    # skip slow/trending spreads
    stop_loss_bps: float = 75.0
    take_profit_bps: float = 45.0       # bank gains on pair PnL
    max_hold_bars: int = 30
    cooldown_bars: int = 96             # ~8h between trades
    equity_fraction: float = 0.60
    min_composite_confirm: float = 0.40
    rsi_confirm_long: float = 42.0      # ratio RSI must be oversold for long
    rsi_confirm_short: float = 58.0     # ratio RSI must be overbought for short


class StatArbStrategy:
    """
    HYPE/BTC ratio mean-reversion with beta-neutral sizing.
    Enters only on extreme z-scores with confirmation; exits on reversion or stop.
    """

    def __init__(self, config: SignalConfig = None):
        self.config = config or SignalConfig()
        self.max_hold_bars = self.config.max_hold_bars

        self.current_position = 0
        self.position_size = 0.0
        self.btc_hedge_size = 0.0
        self.bars_in_trade = 0
        self.bars_since_exit = 9999

        self.entry_ratio_z = 0.0
        self.entry_hype_price = 0.0
        self.entry_btc_price = 0.0
        self.entry_beta = 1.0

    # ------------------------------------------------------------------ indicators

    def calculate_zscore(self, series: pd.Series) -> float:
        if len(series) < self.config.lookback:
            return 0.0
        window = series.iloc[-self.config.lookback:]
        std = window.std()
        if std < 1e-12:
            return 0.0
        return float((series.iloc[-1] - window.mean()) / std)

    def calculate_rsi(self, series: pd.Series) -> float:
        period = self.config.rsi_period
        if len(series) < period + 1:
            return 50.0
        deltas = series.diff().iloc[1:]
        gains = deltas[deltas > 0].sum() / period
        losses = -deltas[deltas < 0].sum() / period
        if losses < 1e-12:
            return 100.0
        rs = gains / losses
        return 100.0 - (100.0 / (1.0 + rs))

    def calculate_ema_cross(self, series: pd.Series) -> float:
        if len(series) < self.config.ema_slow:
            return 0.0
        ema_fast = series.ewm(span=self.config.ema_fast, adjust=False).mean().iloc[-1]
        ema_slow = series.ewm(span=self.config.ema_slow, adjust=False).mean().iloc[-1]
        if ema_slow <= 0:
            return 0.0
        return float((ema_fast - ema_slow) / ema_slow * 100)

    def calculate_momentum(self, series: pd.Series, lookback: int = 8) -> float:
        if len(series) < lookback + 1:
            return 0.0
        prev = series.iloc[-lookback - 1]
        if prev <= 0:
            return 0.0
        return float((series.iloc[-1] - prev) / prev * 100)

    def estimate_beta(self, hype: pd.Series, btc: pd.Series) -> float:
        n = min(len(hype), len(btc), self.config.lookback)
        if n < 30:
            return 1.0
        h = hype.iloc[-n:].pct_change().dropna()
        b = btc.iloc[-n:].pct_change().dropna()
        m = min(len(h), len(b))
        if m < 20:
            return 1.0
        h, b = h.iloc[-m:], b.iloc[-m:]
        var_b = b.var()
        if var_b < 1e-15:
            return 1.0
        beta = float(np.cov(h, b)[0, 1] / var_b)
        return float(np.clip(beta, 0.25, 3.0))

    def estimate_correlation(self, hype: pd.Series, btc: pd.Series) -> float:
        n = min(len(hype), len(btc), self.config.lookback)
        if n < 30:
            return 0.0
        h = hype.iloc[-n:].pct_change().dropna()
        b = btc.iloc[-n:].pct_change().dropna()
        m = min(len(h), len(b))
        if m < 20:
            return 0.0
        return float(h.iloc[-m:].corr(b.iloc[-m:]))

    def estimate_half_life(self, spread: pd.Series) -> float:
        """Ornstein-Uhlenbeck half-life in bars (past data only)."""
        n = min(len(spread), self.config.lookback)
        if n < 30:
            return self.config.max_half_life_bars
        window = spread.iloc[-n:].values
        lag = window[:-1]
        delta = np.diff(window)
        if lag.var() < 1e-15:
            return self.config.max_half_life_bars
        beta = np.cov(lag, delta)[0, 1] / lag.var()
        if beta >= 0:
            return self.config.max_half_life_bars
        half_life = -np.log(2) / beta
        return float(np.clip(half_life, 1.0, 500.0))

    def pair_pnl_bps(self, hype_price: float, btc_price: float) -> float:
        if self.entry_hype_price <= 0 or self.entry_btc_price <= 0:
            return 0.0
        beta = max(0.1, self.entry_beta)
        if self.current_position > 0:
            hype_bps = (hype_price - self.entry_hype_price) / self.entry_hype_price * 10000
            btc_bps = (self.entry_btc_price - btc_price) / self.entry_btc_price * 10000 * beta
        else:
            hype_bps = (self.entry_hype_price - hype_price) / self.entry_hype_price * 10000
            btc_bps = (btc_price - self.entry_btc_price) / self.entry_btc_price * 10000 * beta
        return float((hype_bps + btc_bps) / 2.0)

    def generate_signal(
        self,
        hype_data: pd.DataFrame,
        btc_data: pd.DataFrame,
    ) -> Tuple[float, float, Dict]:
        """
        Returns (composite, raw_ratio_z, details).
        raw_ratio_z: positive = ratio rich (short HYPE), negative = ratio cheap (long HYPE)
        """
        if len(hype_data) < self.config.lookback or len(btc_data) < self.config.lookback:
            return 0.0, 0.0, {}

        ratio = hype_data["close"] / btc_data["close"]
        log_ratio = np.log(ratio.replace(0, np.nan)).dropna()
        if len(log_ratio) < self.config.lookback:
            return 0.0, 0.0, {}
        raw_z = self.calculate_zscore(log_ratio)
        trade_z = -raw_z  # cheap ratio -> positive signal (long HYPE)

        ratio_rsi = self.calculate_rsi(ratio)
        rsi_signal = np.clip((50.0 - ratio_rsi) / 25.0, -1, 1)
        ema_signal = np.clip(self.calculate_ema_cross(ratio) * 8, -1, 1)
        hype_mom = self.calculate_momentum(hype_data["close"])
        btc_mom = self.calculate_momentum(btc_data["close"])
        mom_diff = np.clip((hype_mom - btc_mom) / 6, -1, 1)

        composite = np.clip(
            trade_z * self.config.weight_zscore
            + rsi_signal * self.config.weight_rsi
            + ema_signal * self.config.weight_ema
            + mom_diff * self.config.weight_momentum,
            -1,
            1,
        )

        corr = self.estimate_correlation(hype_data["close"], btc_data["close"])
        half_life = self.estimate_half_life(log_ratio)
        beta = self.estimate_beta(hype_data["close"], btc_data["close"])

        details = {
            "raw_z": raw_z,
            "trade_z": trade_z,
            "composite": composite,
            "ratio_rsi": ratio_rsi,
            "correlation": corr,
            "half_life": half_life,
            "beta": beta,
        }
        return float(composite), float(raw_z), details

    def _close_actions(self, symbols: List[str], reason: str) -> List[Dict]:
        actions = []
        if self.current_position > 0:
            actions.append({"symbol": symbols[0], "side": "SELL", "size": self.position_size, "reason": reason})
            actions.append({"symbol": symbols[1], "side": "FLAT", "size": 0, "reason": reason})
        elif self.current_position < 0:
            actions.append({"symbol": symbols[0], "side": "BUY", "size": self.position_size, "reason": reason})
            actions.append({"symbol": symbols[1], "side": "FLAT", "size": 0, "reason": reason})
        self.current_position = 0
        self.position_size = 0.0
        self.btc_hedge_size = 0.0
        self.bars_in_trade = 0
        self.bars_since_exit = 0
        return actions

    def _size_pair(self, equity: float, hype_price: float, btc_price: float, beta: float) -> Tuple[float, float]:
        deploy = equity * self.config.equity_fraction
        total_notional = deploy * 1.5  # modest leverage on the pair
        hype_notional = total_notional / (1.0 + beta)
        btc_notional = total_notional * beta / (1.0 + beta)
        hype_size = hype_notional / hype_price if hype_price > 0 else 0.0
        btc_size = btc_notional / btc_price if btc_price > 0 else 0.0
        return hype_size, btc_size

    def _passes_entry_filters(self, raw_z: float, composite: float, details: Dict) -> Optional[int]:
        """Return +1 long HYPE, -1 short HYPE, or None."""
        if details.get("correlation", 0) < self.config.min_correlation:
            return None
        hl = details.get("half_life", 999)
        if hl < self.config.min_half_life_bars or hl > self.config.max_half_life_bars:
            return None
        if self.bars_since_exit < self.config.cooldown_bars:
            return None

        rsi = details.get("ratio_rsi", 50)

        # Long HYPE: log-ratio statistically cheap + oversold
        if raw_z <= -self.config.entry_zscore:
            if composite >= self.config.min_composite_confirm and rsi <= self.config.rsi_confirm_long:
                return 1
        # Short HYPE: log-ratio rich + overbought
        elif raw_z >= self.config.entry_zscore:
            if composite <= -self.config.min_composite_confirm and rsi >= self.config.rsi_confirm_short:
                return -1
        return None

    def __call__(
        self,
        bar_index: int,
        get_data_fn: Callable,
        state,
        symbols: List[str],
        equity: float,
    ) -> List[Dict]:
        actions: List[Dict] = []
        self.bars_since_exit += 1

        hype_data = get_data_fn(symbols[0], self.config.lookback + 30)
        btc_data = get_data_fn(symbols[1], self.config.lookback + 30)
        if len(hype_data) < self.config.lookback:
            return actions

        composite, raw_z, details = self.generate_signal(hype_data, btc_data)
        hype_price = float(hype_data["close"].iloc[-1])
        btc_price = float(btc_data["close"].iloc[-1])
        beta = details.get("beta", 1.0)

        if self.current_position != 0:
            self.bars_in_trade += 1
            pnl_bps = self.pair_pnl_bps(hype_price, btc_price)
            should_exit = False
            reason = ""

            # Profit take: spread reverted OR pair hit target
            if abs(raw_z) <= self.config.exit_zscore or pnl_bps >= self.config.take_profit_bps:
                should_exit = True
                reason = "take_profit"
            # Stop: spread extended further against us
            elif self.current_position > 0 and raw_z <= self.entry_ratio_z - self.config.stop_zscore:
                should_exit = True
                reason = "stop_zscore"
            elif self.current_position < 0 and raw_z >= self.entry_ratio_z + self.config.stop_zscore:
                should_exit = True
                reason = "stop_zscore"
            elif pnl_bps <= -self.config.stop_loss_bps:
                should_exit = True
                reason = "stop_loss"
            elif self.bars_in_trade >= self.max_hold_bars:
                should_exit = True
                reason = "max_hold"

            if should_exit:
                actions.extend(self._close_actions(symbols, reason))
        else:
            direction = self._passes_entry_filters(raw_z, composite, details)
            if direction is not None:
                hype_size, btc_size = self._size_pair(equity, hype_price, btc_price, beta)
                if hype_size <= 0 or btc_size <= 0:
                    return actions

                self.current_position = direction
                self.position_size = hype_size
                self.btc_hedge_size = btc_size
                self.entry_ratio_z = raw_z
                self.entry_hype_price = hype_price
                self.entry_btc_price = btc_price
                self.entry_beta = beta
                self.bars_in_trade = 0

                if direction > 0:
                    actions.append({"symbol": symbols[0], "side": "BUY", "size": hype_size, "reason": "entry_long"})
                    actions.append({"symbol": symbols[1], "side": "SELL", "size": btc_size, "reason": "entry_long"})
                else:
                    actions.append({"symbol": symbols[0], "side": "SELL", "size": hype_size, "reason": "entry_short"})
                    actions.append({"symbol": symbols[1], "side": "BUY", "size": btc_size, "reason": "entry_short"})

        return actions


class AdaptiveStatArb(StatArbStrategy):
    """Regime-aware variant — stays selective in high vol, slightly tighter in calm markets."""

    def __init__(self):
        super().__init__()
        self.regime = "normal"

    def _detect_regime(self, hype_data: pd.DataFrame) -> str:
        if len(hype_data) < 40:
            return "normal"
        vol = hype_data["close"].pct_change().iloc[-40:].std()
        if vol > 0.012:
            return "high_vol"
        if vol < 0.004:
            return "low_vol"
        return "normal"

    def __call__(self, bar_index, get_data_fn, state, symbols, equity):
        hype_data = get_data_fn(symbols[0], 45)
        regime = self._detect_regime(hype_data)
        if regime != self.regime:
            self.regime = regime
            if regime == "high_vol":
                self.config.entry_zscore = 3.0
                self.config.exit_zscore = 0.45
                self.config.stop_loss_bps = 100.0
            elif regime == "low_vol":
                self.config.entry_zscore = 2.5
                self.config.exit_zscore = 0.25
                self.config.stop_loss_bps = 80.0
            else:
                self.config.entry_zscore = 2.75
                self.config.exit_zscore = 0.35
                self.config.stop_loss_bps = 120.0
        return super().__call__(bar_index, get_data_fn, state, symbols, equity)
