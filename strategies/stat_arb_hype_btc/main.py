"""
Statistical Arbitrage: HYPE/BTC Pair Trading
============================================

A pairs-trading strategy that exploits mean-reversion in the HYPE/BTC ratio.
Uses z-score analysis combined with technical indicators (RSI, MACD, EMA),
orderbook microstructure (OBI, depth skew, CVD), and cross-exchange signals
(basis, funding differentials).

Architecture:
- Functional core with explicit state passing
- Pure signal calculation functions
- Async I/O at the edges
- Dollar-neutral execution on HotStuff DEX
"""

import asyncio
import json
import logging
import math
import os
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, NamedTuple, Optional, Tuple

import aiohttp
from dotenv import load_dotenv
from eth_account import Account

# HotStuff SDK imports
from hotstuff import (
    ExchangeClient,
    HttpTransport,
    HttpTransportOptions,
    InfoClient,
    PlaceOrderParams,
    SubscriptionClient,
    UnitOrder,
    WebSocketTransport,
    WebSocketTransportOptions,
)
from hotstuff.utils.signing import sign_action

# Load environment unless explicitly disabled
if os.getenv("BOT_STRATEGY_DISABLE_DOTENV", "").lower() not in ("1", "true", "yes"):
    load_dotenv()

# -----------------------------------------------------------------------------
# Logging Setup
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("stat_arb")

# -----------------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------------
BINANCE_WS_ENDPOINT = "wss://fstream.binance.com/stream"
BINANCE_REST_ENDPOINT = "https://fapi.binance.com/fapi/v1"

SIGNAL_NAMES = [
    "zscore",
    "ratio_rsi",
    "ratio_stoch_rsi",
    "ratio_macd",
    "ratio_ema_cross",
    "ratio_ema_trend",
    "ratio_vwap_dev",
    "obi_diff",
    "cvd_diff",
    "depth_skew_diff",
    "basis_diff",
    "funding_diff",
]

DEFAULT_WEIGHT_CONFIG = {
    "zscore": 0.20,
    "ratio_rsi": 0.08,
    "ratio_stoch_rsi": 0.07,
    "ratio_macd": 0.10,
    "ratio_ema_cross": 0.08,
    "ratio_ema_trend": 0.05,
    "ratio_vwap_dev": 0.07,
    "obi_diff": 0.10,
    "cvd_diff": 0.08,
    "depth_skew_diff": 0.07,
    "basis_diff": 0.05,
    "funding_diff": 0.05,
}

REGIME_MEAN_REVERT = "MEAN_REVERT"
REGIME_MIXED = "MIXED"
REGIME_TRENDING = "TRENDING"

# -----------------------------------------------------------------------------
# Data Structures
# -----------------------------------------------------------------------------

class Candle(NamedTuple):
    timestamp: float
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: float


@dataclass
class OrderbookSnapshot:
    bids: List[List[float]] = field(default_factory=list)
    asks: List[List[float]] = field(default_factory=list)
    last_update: float = 0.0
    sequence: int = 0

    def apply_delta(self, bid_changes: List[Dict], ask_changes: List[Dict]):
        """Apply orderbook delta updates."""
        for change in bid_changes:
            price = float(change["price"])
            size = float(change.get("size", 0))
            self._update_level(self.bids, price, size, is_bid=True)
        for change in ask_changes:
            price = float(change["price"])
            size = float(change.get("size", 0))
            self._update_level(self.asks, price, size, is_bid=False)
        self.bids.sort(key=lambda x: -x[0])
        self.asks.sort(key=lambda x: x[0])

    def _update_level(self, levels: List[List[float]], price: float, size: float, is_bid: bool):
        found = False
        for i, level in enumerate(levels):
            if abs(level[0] - price) < 1e-12:
                if size <= 0:
                    levels.pop(i)
                else:
                    level[1] = size
                found = True
                break
        if not found and size > 0:
            levels.append([price, size])

    def is_fresh(self, max_age_sec: float = 10.0) -> bool:
        return self.last_update > 0 and (time.monotonic() - self.last_update) < max_age_sec


@dataclass
class MarketDataState:
    # Candle history
    hype_candles: List[Candle] = field(default_factory=list)
    btc_candles: List[Candle] = field(default_factory=list)
    
    # Ratio series
    ratio_prices: List[float] = field(default_factory=list)
    log_spread_series: List[float] = field(default_factory=list)
    
    # HotStuff realtime prices
    hype_hs_bid: float = 0.0
    hype_hs_ask: float = 0.0
    hype_hs_mid: float = 0.0
    btc_hs_bid: float = 0.0
    btc_hs_ask: float = 0.0
    btc_hs_mid: float = 0.0
    
    # Binance reference prices
    hype_binance_price: float = 0.0
    btc_binance_price: float = 0.0
    
    # Orderbooks
    hype_orderbook: OrderbookSnapshot = field(default_factory=OrderbookSnapshot)
    btc_orderbook: OrderbookSnapshot = field(default_factory=OrderbookSnapshot)
    
    # CVD tracking
    hype_cvd_current: float = 0.0
    btc_cvd_current: float = 0.0
    hype_volume_current: float = 0.0
    btc_volume_current: float = 0.0
    
    # VWAP tracking
    vwap_cumulative_rpv: float = 0.0
    vwap_cumulative_vol: float = 0.0
    vwap_date: str = ""
    
    # Funding rates
    hype_funding_rate: float = 0.0
    btc_funding_rate: float = 0.0
    
    # Computed statistics
    beta_estimate: float = 1.0
    correlation_estimate: float = 0.0
    half_life_estimate: float = float("inf")
    
    # Position tracking
    account_equity: float = 0.0
    net_hype_position: float = 0.0
    net_btc_position: float = 0.0
    entry_signal_value: float = 0.0
    entry_hype_mid: float = 0.0
    entry_btc_mid: float = 0.0
    bars_since_entry: int = 0
    realized_pnl: float = 0.0
    total_trades: int = 0
    total_signals: int = 0
    bar_count: int = 0
    
    # Instrument specs
    hype_instrument_id: int = 0
    hype_tick_size: float = 0.01
    hype_lot_size: float = 0.01
    btc_instrument_id: int = 0
    btc_tick_size: float = 1.0
    btc_lot_size: float = 0.0001


@dataclass(frozen=True)
class StrategyParameters:
    private_key: str
    agent_address: str
    hype_symbol: str
    btc_symbol: str
    weights: Dict[str, float]
    
    # Z-score config
    zscore_lookback: int
    zscore_entry_threshold: float
    zscore_exit_threshold: float
    
    # Technical indicator periods
    rsi_period: int
    macd_fast: int
    macd_slow: int
    macd_signal: int
    ema_fast_period: int
    ema_slow_period: int
    ema_trend_period: int
    stoch_rsi_period: int
    stoch_rsi_k: int
    
    # Trading parameters
    equity_multiplier: float
    leverage: int
    max_loss_percent: float
    max_hold_bars: int
    signal_entry_threshold: float
    signal_exit_threshold: float
    
    # Buffer sizes
    candle_buffer_size: int
    warmup_candle_count: int
    
    # Execution
    enable_live_trading: bool
    slippage_bps: float
    half_life_gate_threshold: float
    long_bias: float
    stop_loss_bps: float
    signal_zscore_lookback: int

    @classmethod
    def from_environment(cls) -> "StrategyParameters":
        pk = os.getenv("HOTSTUFF_PRIVATE_KEY", "")
        if not pk:
            logger.error("HOTSTUFF_PRIVATE_KEY not set")
            sys.exit(1)
        addr = os.getenv("HOTSTUFF_AGENT_ADDRESS", "")
        if not addr:
            logger.error("HOTSTUFF_AGENT_ADDRESS not set")
            sys.exit(1)

        # Load and normalize weights
        weights = {}
        weight_sum = 0.0
        for name in SIGNAL_NAMES:
            env_key = f"SA_W_{name.upper()}"
            value = float(os.getenv(env_key, str(DEFAULT_WEIGHT_CONFIG[name])))
            weights[name] = value
            weight_sum += value
        
        if abs(weight_sum - 1.0) > 0.01 and weight_sum > 0:
            weights = {k: v / weight_sum for k, v in weights.items()}

        return cls(
            private_key=pk,
            agent_address=addr,
            hype_symbol=os.getenv("SA_HYPE_SYMBOL", "HYPE-PERP"),
            btc_symbol=os.getenv("SA_BTC_SYMBOL", "BTC-PERP"),
            weights=weights,
            zscore_lookback=int(os.getenv("SA_ZSCORE_LOOKBACK", "100")),
            zscore_entry_threshold=float(os.getenv("SA_ZSCORE_ENTRY", "2.0")),
            zscore_exit_threshold=float(os.getenv("SA_ZSCORE_EXIT", "0.5")),
            rsi_period=int(os.getenv("SA_RSI_PERIOD", "14")),
            macd_fast=int(os.getenv("SA_MACD_FAST", "12")),
            macd_slow=int(os.getenv("SA_MACD_SLOW", "26")),
            macd_signal=int(os.getenv("SA_MACD_SIGNAL", "9")),
            ema_fast_period=int(os.getenv("SA_EMA_FAST", "9")),
            ema_slow_period=int(os.getenv("SA_EMA_SLOW", "21")),
            ema_trend_period=int(os.getenv("SA_EMA_TREND", "55")),
            stoch_rsi_period=int(os.getenv("SA_STOCH_RSI_PERIOD", "14")),
            stoch_rsi_k=int(os.getenv("SA_STOCH_RSI_K", "3")),
            equity_multiplier=float(os.getenv("SA_EQUITY_MULT", "2.0")),
            leverage=int(os.getenv("SA_LEVERAGE", "5")),
            max_loss_percent=float(os.getenv("SA_MAX_LOSS_PCT", "3.0")),
            max_hold_bars=int(os.getenv("SA_MAX_HOLD_BARS", "24")),
            signal_entry_threshold=float(os.getenv("SA_SIGNAL_ENTRY", "0.30")),
            signal_exit_threshold=float(os.getenv("SA_SIGNAL_EXIT", "0.10")),
            candle_buffer_size=int(os.getenv("SA_CANDLE_BUFFER", "300")),
            warmup_candle_count=int(os.getenv("SA_WARMUP_CANDLES", "60")),
            enable_live_trading=os.getenv("SA_ENABLE_TRADING", "false").lower() == "true",
            slippage_bps=float(os.getenv("SA_SLIPPAGE_BPS", "15")),
            half_life_gate_threshold=float(os.getenv("SA_HL_GATE_THRESHOLD", "30.0")),
            long_bias=float(os.getenv("SA_LONG_BIAS", "0.0")),
            stop_loss_bps=float(os.getenv("SA_STOP_LOSS_BPS", "0.0")),
            signal_zscore_lookback=int(os.getenv("SA_SIGNAL_ZSCORE_LOOKBACK", "0")),
        )


@dataclass
class SignalResult:
    raw_values: Dict[str, float] = field(default_factory=dict)
    normalized_signals: Dict[str, float] = field(default_factory=dict)


@dataclass
class RegimeState:
    half_life_ema: float = 20.0
    current_regime: str = REGIME_MIXED
    mean_revert_threshold: float = 15.0
    trend_threshold: float = 40.0

    def update(self, half_life: float) -> str:
        capped_hl = min(half_life, 500.0)
        alpha = 0.3
        self.half_life_ema = alpha * capped_hl + (1.0 - alpha) * self.half_life_ema
        
        if self.half_life_ema <= self.mean_revert_threshold:
            self.current_regime = REGIME_MEAN_REVERT
        elif self.half_life_ema >= self.trend_threshold:
            self.current_regime = REGIME_TRENDING
        else:
            self.current_regime = REGIME_MIXED
        return self.current_regime


# -----------------------------------------------------------------------------
# Pure Functions - Technical Analysis
# -----------------------------------------------------------------------------

def clip_value(value: float, lower: float = -1.0, upper: float = 1.0) -> float:
    return max(lower, min(upper, value))


def ema_calculation(prices: List[float], period: int) -> float:
    """Calculate EMA of the last value in the series."""
    if not prices or period < 1:
        return 0.0
    alpha = 2.0 / (period + 1)
    result = prices[0]
    for i in range(1, len(prices)):
        result = prices[i] * alpha + result * (1 - alpha)
    return result


def rsi_calculation(prices: List[float], period: int = 14) -> float:
    """Calculate RSI using Wilder's smoothing method."""
    n = len(prices)
    if n < 2:
        return 50.0
    
    gains_sum = 0.0
    losses_sum = 0.0
    avg_gain = 0.0
    avg_loss = 0.0
    
    for i in range(1, n):
        delta = prices[i] - prices[i - 1]
        gain = max(delta, 0.0)
        loss = max(-delta, 0.0)
        
        if i < period:
            gains_sum += gain
            losses_sum += loss
        elif i == period:
            avg_gain = (gains_sum + gain) / period
            avg_loss = (losses_sum + loss) / period
        else:
            avg_gain = (avg_gain * (period - 1) + gain) / period
            avg_loss = (avg_loss * (period - 1) + loss) / period
    
    if avg_loss < 1e-15:
        return 99.0
    rs = avg_gain / avg_loss
    return 100.0 - 100.0 / (1.0 + rs)


def stochastic_calculation(values: List[float], period: int = 14) -> float:
    """Stochastic oscillator: (current - min) / (max - min) * 100."""
    if len(values) < period:
        return 50.0
    window = values[-period:]
    lowest = min(window)
    highest = max(window)
    range_val = highest - lowest
    if range_val < 1e-15:
        return 50.0
    return (values[-1] - lowest) / range_val * 100.0


def macd_histogram(prices: List[float], fast: int = 12, slow: int = 26, signal: int = 9) -> float:
    """Returns MACD histogram value."""
    if len(prices) < slow:
        return 0.0
    ema_fast_val = ema_calculation(prices, fast)
    ema_slow_val = ema_calculation(prices, slow)
    macd_line = ema_fast_val - ema_slow_val
    signal_line = ema_calculation(prices[-signal*2:] if len(prices) >= signal*2 else prices, signal)
    return macd_line - signal_line


def zscore_calculation(values: List[float], lookback: int) -> float:
    """Calculate z-score of the latest value."""
    if len(values) < lookback or lookback < 2:
        return 0.0
    window = values[-lookback:]
    mean_val = sum(window) / lookback
    variance = sum((v - mean_val) ** 2 for v in window) / lookback
    std_dev = variance ** 0.5
    if std_dev < 1e-15:
        return 0.0
    return (values[-1] - mean_val) / std_dev


def beta_calculation(hype_prices: List[float], btc_prices: List[float], lookback: int) -> float:
    """OLS hedge ratio: cov(HYPE,BTC) / var(BTC)."""
    n = min(len(hype_prices), len(btc_prices))
    if n < lookback + 1 or lookback < 10:
        return 1.0
    
    hype_returns = []
    btc_returns = []
    start = n - lookback
    for i in range(start, n):
        hp = hype_prices[i - 1]
        bp = btc_prices[i - 1]
        if hp > 0 and bp > 0:
            hype_returns.append(hype_prices[i] / hp - 1.0)
            btc_returns.append(btc_prices[i] / bp - 1.0)
    
    m = len(hype_returns)
    if m < 10:
        return 1.0
    
    mean_h = sum(hype_returns) / m
    mean_b = sum(btc_returns) / m
    
    cov = sum((h - mean_h) * (b - mean_b) for h, b in zip(hype_returns, btc_returns)) / m
    var_b = sum((b - mean_b) ** 2 for b in btc_returns) / m
    
    if var_b < 1e-20:
        return 1.0
    return cov / var_b


def correlation_calculation(hype_prices: List[float], btc_prices: List[float], lookback: int) -> float:
    """Pearson correlation coefficient."""
    n = min(len(hype_prices), len(btc_prices))
    if n < lookback + 1 or lookback < 10:
        return 0.0
    
    hype_returns = []
    btc_returns = []
    start = n - lookback
    for i in range(start, n):
        hp = hype_prices[i - 1]
        bp = btc_prices[i - 1]
        if hp > 0 and bp > 0:
            hype_returns.append(hype_prices[i] / hp - 1.0)
            btc_returns.append(btc_prices[i] / bp - 1.0)
    
    m = len(hype_returns)
    if m < 10:
        return 0.0
    
    mean_h = sum(hype_returns) / m
    mean_b = sum(btc_returns) / m
    
    cov = sum((h - mean_h) * (b - mean_b) for h, b in zip(hype_returns, btc_returns)) / m
    var_h = sum((h - mean_h) ** 2 for h in hype_returns) / m
    var_b = sum((b - mean_b) ** 2 for b in btc_returns) / m
    
    denom = (var_h * var_b) ** 0.5
    if denom < 1e-20:
        return 0.0
    return cov / denom


def half_life_calculation(spread_series: List[float], lookback: int) -> float:
    """Half-life of mean reversion via OLS on lagged spread."""
    if len(spread_series) < lookback + 1:
        return float("inf")
    
    window = spread_series[-(lookback + 1):]
    n = len(window) - 1
    
    y = [window[i + 1] - window[i] for i in range(n)]
    x = window[:-1]
    
    mean_x = sum(x) / n
    mean_y = sum(y) / n
    
    cov_xy = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, y)) / n
    var_x = sum((xi - mean_x) ** 2 for xi in x) / n
    
    if var_x < 1e-20:
        return float("inf")
    
    beta = cov_xy / var_x
    if beta >= 0:
        return float("inf")
    return -0.693147 / beta


def orderbook_imbalance(bids: List[List[float]], asks: List[List[float]], levels: int = 10) -> float:
    """Calculate orderbook imbalance: (bid_vol - ask_vol) / total_vol."""
    bid_vol = sum(b[1] for b in bids[:levels])
    ask_vol = sum(a[1] for a in asks[:levels])
    total = bid_vol + ask_vol
    if total < 1e-10:
        return 0.0
    return (bid_vol - ask_vol) / total


def depth_skew(bids: List[List[float]], asks: List[List[float]], mid: float, levels: int = 20) -> float:
    """Weighted depth skew within 2% of mid."""
    if mid <= 0 or not bids or not asks:
        return 0.0
    
    max_distance = 0.02
    bid_weight = 0.0
    ask_weight = 0.0
    
    for b in bids[:levels]:
        dist = (mid - b[0]) / mid
        if dist > max_distance:
            break
        bid_weight += b[1] * (1.0 - dist / max_distance)
    
    for a in asks[:levels]:
        dist = (a[0] - mid) / mid
        if dist > max_distance:
            break
        ask_weight += a[1] * (1.0 - dist / max_distance)
    
    total = bid_weight + ask_weight
    if total < 1e-10:
        return 0.0
    return (bid_weight - ask_weight) / total


# -----------------------------------------------------------------------------
# Signal Generation
# -----------------------------------------------------------------------------

def compute_all_signals(
    state: MarketDataState,
    params: StrategyParameters,
    signal_history: Dict[str, List[float]],
    enable_signal_zscore: bool,
) -> SignalResult:
    """Compute all 12 signals from market state."""
    result = SignalResult()
    ratios = state.ratio_prices
    
    if not ratios:
        return result
    
    # 1. Z-score of ratio
    z = zscore_calculation(ratios, params.zscore_lookback)
    result.raw_values["zscore"] = z
    z_signal = clip_value(-z / params.zscore_entry_threshold)
    
    # Gate by half-life
    hl = state.half_life_estimate
    if params.half_life_gate_threshold > 0 and hl > params.half_life_gate_threshold:
        z_signal *= max(0.1, params.half_life_gate_threshold / hl)
    result.normalized_signals["zscore"] = z_signal
    
    # 2. RSI of ratio
    rsi_val = rsi_calculation(ratios, params.rsi_period) if len(ratios) >= params.rsi_period + 1 else 50.0
    result.raw_values["ratio_rsi"] = rsi_val
    result.normalized_signals["ratio_rsi"] = clip_value((50.0 - rsi_val) / 30.0)
    
    # 3. Stochastic RSI
    if len(ratios) >= params.rsi_period + params.stoch_rsi_period:
        rsi_series = [rsi_calculation(ratios[:i+1], params.rsi_period) for i in range(params.rsi_period, len(ratios))]
        stoch_val = stochastic_calculation(rsi_series, params.stoch_rsi_period)
    else:
        stoch_val = 50.0
    result.raw_values["ratio_stoch_rsi"] = stoch_val
    result.normalized_signals["ratio_stoch_rsi"] = clip_value((50.0 - stoch_val) / 30.0)
    
    # 4. MACD histogram
    hist = macd_histogram(ratios, params.macd_fast, params.macd_slow, params.macd_signal)
    result.raw_values["ratio_macd"] = hist
    recent_hist = ratios[-50:] if len(ratios) >= 50 else ratios
    max_hist = max(abs(h) for h in recent_hist) if recent_hist else 1e-15
    if max_hist < 1e-15:
        max_hist = 1e-15
    result.normalized_signals["ratio_macd"] = clip_value(hist / max_hist)
    
    # 5. EMA cross
    ema_fast_val = ema_calculation(ratios, params.ema_fast_period)
    ema_slow_val = ema_calculation(ratios, params.ema_slow_period)
    cross_pct = ((ema_fast_val - ema_slow_val) / ema_slow_val * 100.0) if ema_slow_val > 1e-15 else 0.0
    result.raw_values["ratio_ema_cross"] = cross_pct
    result.normalized_signals["ratio_ema_cross"] = clip_value(cross_pct / 1.0)
    
    # 6. EMA trend
    ema_trend_val = ema_calculation(ratios, params.ema_trend_period)
    trend_pct = ((ema_slow_val - ema_trend_val) / ema_trend_val * 100.0) if ema_trend_val > 1e-15 else 0.0
    result.raw_values["ratio_ema_trend"] = trend_pct
    result.normalized_signals["ratio_ema_trend"] = clip_value(trend_pct / 2.0)
    
    # 7. VWAP deviation
    vwap_dev = 0.0
    if state.vwap_cumulative_vol > 0 and ratios:
        vwap = state.vwap_cumulative_rpv / state.vwap_cumulative_vol
        if vwap > 1e-15:
            vwap_dev = (ratios[-1] - vwap) / vwap * 100.0
    result.raw_values["ratio_vwap_dev"] = vwap_dev
    result.normalized_signals["ratio_vwap_dev"] = clip_value(-vwap_dev / 0.5)
    
    # 8. OBI differential
    hype_obi = orderbook_imbalance(state.hype_orderbook.bids, state.hype_orderbook.asks) if state.hype_orderbook.is_fresh() else 0.0
    btc_obi = orderbook_imbalance(state.btc_orderbook.bids, state.btc_orderbook.asks) if state.btc_orderbook.is_fresh() else 0.0
    obi_diff = hype_obi - btc_obi
    result.raw_values["obi_diff"] = obi_diff
    result.normalized_signals["obi_diff"] = clip_value(obi_diff / 0.5)
    
    # 9. CVD differential
    hype_cvd_norm = state.hype_cvd_current / state.hype_volume_current if state.hype_volume_current > 0 else 0.0
    btc_cvd_norm = state.btc_cvd_current / state.btc_volume_current if state.btc_volume_current > 0 else 0.0
    cvd_diff = hype_cvd_norm - btc_cvd_norm
    result.raw_values["cvd_diff"] = cvd_diff
    result.normalized_signals["cvd_diff"] = clip_value(cvd_diff / 0.3)
    
    # 10. Depth skew differential
    hype_skew = depth_skew(state.hype_orderbook.bids, state.hype_orderbook.asks, state.hype_hs_mid) if state.hype_orderbook.is_fresh() and state.hype_hs_mid > 0 else 0.0
    btc_skew = depth_skew(state.btc_orderbook.bids, state.btc_orderbook.asks, state.btc_hs_mid) if state.btc_orderbook.is_fresh() and state.btc_hs_mid > 0 else 0.0
    skew_diff = hype_skew - btc_skew
    result.raw_values["depth_skew_diff"] = skew_diff
    result.normalized_signals["depth_skew_diff"] = clip_value(skew_diff / 0.5)
    
    # 11. Cross-exchange basis differential
    hype_basis = 0.0
    if state.hype_hs_mid > 0 and state.hype_binance_price > 0:
        hype_basis = (state.hype_hs_mid - state.hype_binance_price) / state.hype_binance_price * 10000.0
    btc_basis = 0.0
    if state.btc_hs_mid > 0 and state.btc_binance_price > 0:
        btc_basis = (state.btc_hs_mid - state.btc_binance_price) / state.btc_binance_price * 10000.0
    basis_diff = hype_basis - btc_basis
    result.raw_values["basis_diff"] = basis_diff
    result.normalized_signals["basis_diff"] = clip_value(-basis_diff / 50.0)
    
    # 12. Funding differential
    funding_diff = state.hype_funding_rate - state.btc_funding_rate
    result.raw_values["funding_diff"] = funding_diff
    result.normalized_signals["funding_diff"] = clip_value(-funding_diff / 0.001)
    
    # Optional: variance-normalize signals
    if enable_signal_zscore:
        for name in SIGNAL_NAMES:
            signal_history[name].append(result.normalized_signals[name])
            hist = signal_history[name]
            if len(hist) > params.signal_zscore_lookback:
                hist.pop(0)
            if len(hist) >= params.signal_zscore_lookback:
                m = sum(hist) / len(hist)
                v = sum((x - m) ** 2 for x in hist) / len(hist)
                s = math.sqrt(v)
                if s > 1e-12:
                    result.normalized_signals[name] = clip_value((result.normalized_signals[name] - m) / s, -2.0, 2.0)
                else:
                    result.normalized_signals[name] = 0.0
    
    return result


def aggregate_signal_weighted(signals: Dict[str, float], weights: Dict[str, float], bias: float) -> float:
    """Compute weighted sum of signals."""
    total = sum(signals.get(name, 0.0) * weights.get(name, 0.0) for name in SIGNAL_NAMES)
    return total + bias


# -----------------------------------------------------------------------------
# Decision Logic
# -----------------------------------------------------------------------------

def decide_trade_action(
    weighted_sum: float,
    state: MarketDataState,
    params: StrategyParameters,
) -> str:
    """Determine trading action based on weighted signal sum."""
    has_position = abs(state.net_hype_position) > 1e-8 or abs(state.net_btc_position) > 1e-8
    
    if has_position:
        state.bars_since_entry += 1
        
        # Check stop-loss
        if params.stop_loss_bps > 0:
            unrealized = calculate_unrealized_pnl_bps(state)
            if unrealized < -params.stop_loss_bps:
                logger.warning("Stop-loss triggered: unrealized=%.1f bps", unrealized)
                return "EXIT"
        
        # Exit conditions
        if abs(weighted_sum) < params.signal_exit_threshold:
            return "EXIT"
        if state.bars_since_entry >= params.max_hold_bars:
            return "EXIT"
        if weighted_sum > 0 and state.entry_signal_value < 0:
            return "EXIT"
        if weighted_sum < 0 and state.entry_signal_value > 0:
            return "EXIT"
        
        return "HOLD_LONG_HYPE" if state.entry_signal_value > 0 else "HOLD_SHORT_HYPE"
    
    # Entry conditions
    if weighted_sum > params.signal_entry_threshold:
        return "LONG_HYPE_SHORT_BTC"
    if weighted_sum < -params.signal_entry_threshold:
        return "SHORT_HYPE_LONG_BTC"
    
    return "FLAT"


def calculate_unrealized_pnl_bps(state: MarketDataState) -> float:
    """Calculate unrealized PnL in basis points."""
    eh = state.entry_hype_mid
    eb = state.entry_btc_mid
    if eh <= 0 or eb <= 0:
        return 0.0
    
    ch = state.hype_hs_mid
    cb = state.btc_hs_mid
    if ch <= 0 or cb <= 0:
        return 0.0
    
    beta = max(0.1, abs(state.beta_estimate))
    
    if state.entry_signal_value > 0:  # Long HYPE / Short BTC
        hype_pnl = (ch - eh) / eh * 10000.0
        btc_pnl = (eb - cb) / cb * 10000.0 * beta
    else:  # Short HYPE / Long BTC
        hype_pnl = (eh - ch) / eh * 10000.0
        btc_pnl = (cb - eb) / cb * 10000.0 * beta
    
    return hype_pnl + btc_pnl


# -----------------------------------------------------------------------------
# Execution
# -----------------------------------------------------------------------------

def round_to_tick(price: float, tick: float, round_up: bool) -> float:
    """Round price to tick size."""
    if tick <= 0:
        return price
    if round_up:
        return math.ceil(price / tick) * tick
    return math.floor(price / tick) * tick


async def execute_entry(
    action: str,
    weighted_sum: float,
    state: MarketDataState,
    params: StrategyParameters,
    exchange: ExchangeClient,
) -> bool:
    """Execute entry trade. Returns True if successful."""
    if action not in ("LONG_HYPE_SHORT_BTC", "SHORT_HYPE_LONG_BTC"):
        return False
    
    equity = state.account_equity
    if equity <= 0:
        logger.warning("Cannot enter: equity=0")
        return False
    
    hype_side = "BUY" if action == "LONG_HYPE_SHORT_BTC" else "SELL"
    btc_side = "SELL" if action == "LONG_HYPE_SHORT_BTC" else "BUY"
    
    total_notional = equity * params.equity_multiplier
    beta = max(0.1, abs(state.beta_estimate))
    hype_notional = total_notional / (1.0 + beta)
    btc_notional = total_notional * beta / (1.0 + beta)
    
    hype_mid = state.hype_hs_mid
    btc_mid = state.btc_hs_mid
    if hype_mid <= 0 or btc_mid <= 0:
        logger.warning("Cannot enter: invalid prices")
        return False
    
    hype_size = math.floor(hype_notional / hype_mid / state.hype_lot_size) * state.hype_lot_size
    btc_size = math.floor(btc_notional / btc_mid / state.btc_lot_size) * state.btc_lot_size
    
    if hype_size * hype_mid < 10 or btc_size * btc_mid < 10:
        logger.warning("Below min notional")
        return False
    
    # Apply slippage
    slip = params.slippage_bps / 10000.0
    hype_price = hype_mid * (1.0 + slip) if hype_side == "BUY" else hype_mid * (1.0 - slip)
    btc_price = btc_mid * (1.0 + slip) if btc_side == "BUY" else btc_mid * (1.0 - slip)
    
    hype_price = round_to_tick(hype_price, state.hype_tick_size, hype_side == "BUY")
    btc_price = round_to_tick(btc_price, state.btc_tick_size, btc_side == "BUY")
    
    logger.info("ENTERING: %s HYPE %.4f@%.4f | %s BTC %.6f@%.2f",
                hype_side, hype_size, hype_price, btc_side, btc_size, btc_price)
    
    try:
        # Note: Exchange execution would go here
        # Update state on success
        if hype_side == "BUY":
            state.net_hype_position += hype_size
            state.net_btc_position -= btc_size
        else:
            state.net_hype_position -= hype_size
            state.net_btc_position += btc_size
        
        state.entry_signal_value = weighted_sum
        state.entry_hype_mid = state.hype_hs_mid
        state.entry_btc_mid = state.btc_hs_mid
        state.bars_since_entry = 0
        state.total_trades += 1
        return True
    except Exception as e:
        logger.error("Entry failed: %s", e)
        return False


async def execute_exit(
    state: MarketDataState,
    params: StrategyParameters,
    exchange: ExchangeClient,
) -> bool:
    """Exit all positions. Returns True if successful."""
    logger.info("EXITING: HYPE=%.4f BTC=%.6f", state.net_hype_position, state.net_btc_position)
    
    tasks = []
    
    if abs(state.net_hype_position) > 1e-8:
        is_buy = state.net_hype_position < 0
        # Note: Order construction would go here
        state.net_hype_position = 0.0
    
    if abs(state.net_btc_position) > 1e-8:
        is_buy = state.net_btc_position < 0
        # Note: Order construction would go here
        state.net_btc_position = 0.0
    
    state.entry_signal_value = 0.0
    state.entry_hype_mid = 0.0
    state.entry_btc_mid = 0.0
    state.bars_since_entry = 0
    state.total_trades += 1
    return True


# -----------------------------------------------------------------------------
# Data Logging
# -----------------------------------------------------------------------------

class DataLogger:
    def __init__(self, data_dir: str):
        self._dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
        self._file = None
        self._current_date = ""
    
    def _ensure_file(self, dt: datetime):
        date_str = dt.strftime("%Y-%m-%d")
        if date_str != self._current_date:
            if self._file:
                self._file.close()
            path = os.path.join(self._dir, f"signals_{date_str}.csv")
            exists = os.path.exists(path)
            self._file = open(path, "a")
            self._current_date = date_str
            if not exists:
                header = ["ts", "dt"] + [f"raw_{n}" for n in SIGNAL_NAMES] + [f"sig_{n}" for n in SIGNAL_NAMES]
                header += ["weighted_sum", "action", "hype_mid", "btc_mid"]
                self._file.write(",".join(header) + "\n")
    
    def log(self, timestamp: float, state: MarketDataState, result: SignalResult, wsum: float, action: str):
        dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        self._ensure_file(dt)
        
        vals = [
            f"{timestamp:.0f}",
            dt.strftime("%Y-%m-%d %H:%M:%S"),
        ]
        vals += [f"{result.raw_values.get(n, 0.0):.6f}" for n in SIGNAL_NAMES]
        vals += [f"{result.normalized_signals.get(n, 0.0):.4f}" for n in SIGNAL_NAMES]
        vals += [f"{wsum:.4f}", action, f"{state.hype_hs_mid:.4f}", f"{state.btc_hs_mid:.2f}"]
        
        self._file.write(",".join(vals) + "\n")
        self._file.flush()
    
    def close(self):
        if self._file:
            self._file.close()


# -----------------------------------------------------------------------------
# Async I/O - HotStuff Integration
# -----------------------------------------------------------------------------

async def resolve_instruments(http: HttpTransport, state: MarketDataState, params: StrategyParameters):
    """Resolve instrument IDs and specs from HotStuff."""
    raw = await http.request("info", {"method": "instruments", "params": {"type": "perps"}})
    perps = raw.get("perps", []) if isinstance(raw, dict) else []
    
    found = 0
    for p in perps:
        name = p.get("name", "")
        if name == params.hype_symbol:
            state.hype_instrument_id = int(p["id"])
            state.hype_tick_size = float(p.get("tick_size", 0.01))
            state.hype_lot_size = float(p.get("lot_size", 0.01))
            logger.info("HYPE: %s id=%d tick=%.6f lot=%.6f",
                       name, state.hype_instrument_id, state.hype_tick_size, state.hype_lot_size)
            found += 1
        elif name == params.btc_symbol:
            state.btc_instrument_id = int(p["id"])
            state.btc_tick_size = float(p.get("tick_size", 1.0))
            state.btc_lot_size = float(p.get("lot_size", 0.0001))
            logger.info("BTC:  %s id=%d tick=%.6f lot=%.6f",
                       name, state.btc_instrument_id, state.btc_tick_size, state.btc_lot_size)
            found += 1
    
    if found < 2:
        logger.error("Could not resolve both instruments (found %d/2)", found)
        sys.exit(1)


async def sync_account_equity(http: HttpTransport, state: MarketDataState, params: StrategyParameters):
    """Sync account equity from HotStuff."""
    try:
        raw = await http.request(
            "info",
            {"method": "accountSummary", "params": {"user": params.agent_address}},
        )
        eq = float(raw.get("total_account_equity", 0))
        if eq > 0:
            state.account_equity = eq
            logger.info("Account equity: $%.2f", eq)
        else:
            avail = float(raw.get("available_balance", 0))
            if avail > 0:
                state.account_equity = avail
                logger.info("Account equity (avail): $%.2f", avail)
    except Exception as e:
        logger.warning("Equity sync failed: %s", e)


async def fetch_binance_history(
    session: aiohttp.ClientSession,
    state: MarketDataState,
    params: StrategyParameters,
):
    """Fetch historical candles from Binance."""
    limit = params.candle_buffer_size
    
    for symbol, candle_list, label in [
        ("HYPEUSDT", state.hype_candles, "HYPE"),
        ("BTCUSDT", state.btc_candles, "BTC"),
    ]:
        try:
            url = f"{BINANCE_REST_ENDPOINT}/klines?symbol={symbol}&interval=5m&limit={limit}"
            async with session.get(url) as resp:
                if resp.status != 200:
                    logger.warning("Binance %s HTTP %d", symbol, resp.status)
                    continue
                data = await resp.json()
                for k in data:
                    candle_list.append(Candle(
                        timestamp=k[0] / 1000.0,
                        open_price=float(k[1]),
                        high_price=float(k[2]),
                        low_price=float(k[3]),
                        close_price=float(k[4]),
                        volume=float(k[5]),
                    ))
                if candle_list:
                    if label == "HYPE":
                        state.hype_binance_price = candle_list[-1].close_price
                    else:
                        state.btc_binance_price = candle_list[-1].close_price
                logger.info("Binance %s: %d candles loaded", label, len(candle_list))
        except Exception as e:
            logger.warning("Binance history %s failed: %s", label, e)


def rebuild_ratio_series(state: MarketDataState, params: StrategyParameters):
    """Rebuild ratio price series from candles."""
    hc = state.hype_candles
    bc = state.btc_candles
    n = min(len(hc), len(bc))
    
    state.ratio_prices.clear()
    state.log_spread_series.clear()
    
    for i in range(n):
        hi = len(hc) - n + i
        bi = len(bc) - n + i
        btc_c = bc[bi].close_price
        hype_c = hc[hi].close_price
        if btc_c > 0 and hype_c > 0:
            state.ratio_prices.append(hype_c / btc_c)
            state.log_spread_series.append(math.log(hype_c) - math.log(btc_c))
    
    lb = params.zscore_lookback
    if len(state.ratio_prices) >= lb:
        fetch_n = lb + 10
        hype_prices = [c.close_price for c in hc[-fetch_n:]]
        btc_prices = [c.close_price for c in bc[-fetch_n:]]
        state.beta_estimate = beta_calculation(hype_prices, btc_prices, lb)
        state.correlation_estimate = correlation_calculation(hype_prices, btc_prices, lb)
        state.half_life_estimate = half_life_calculation(state.log_spread_series, lb)


# -----------------------------------------------------------------------------
# Main Strategy Runner
# -----------------------------------------------------------------------------

class StatArbRunner:
    def __init__(self, params: StrategyParameters):
        self.params = params
        self.state = MarketDataState()
        self.regime = RegimeState()
        self.data_logger: Optional[DataLogger] = None
        self.signal_history: Dict[str, List[float]] = {n: [] for n in SIGNAL_NAMES}
        self._running = False
        self._candle_event = asyncio.Event()
        
        # HotStuff clients
        self._http: Optional[HttpTransport] = None
        self._ws: Optional[WebSocketTransport] = None
        self._exchange: Optional[ExchangeClient] = None
        
        # Binance
        self._binance_session: Optional[aiohttp.ClientSession] = None
    
    async def start(self):
        logger.info("=== Stat-Arb HYPE/BTC starting ===")
        logger.info("Mode: %s", "LIVE" if self.params.enable_live_trading else "DRY-RUN")
        
        # Initialize logging
        data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
        self.data_logger = DataLogger(data_dir)
        
        # Initialize HotStuff connections
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
            is_testnet=False, timeout=15.0,
            keep_alive={"interval": 20.0, "timeout": 10.0},
            auto_connect=True, server=ws_server,
        ))
        
        wallet = Account.from_key(self.params.private_key)
        self._exchange = ExchangeClient(transport=self._http, wallet=wallet)
        
        # Resolve instruments and sync state
        await resolve_instruments(self._http, self.state, self.params)
        await sync_account_equity(self._http, self.state, self.params)
        
        # Load historical data
        self._binance_session = aiohttp.ClientSession()
        await fetch_binance_history(self._binance_session, self.state, self.params)
        rebuild_ratio_series(self.state, self.params)
        
        self._running = True
        logger.info("=== Ready. %d candles, %d ratio points ===",
                   len(self.state.hype_candles), len(self.state.ratio_prices))
        
        # Start main loop
        await self._main_loop()
    
    async def stop(self):
        logger.info("Shutting down...")
        self._running = False
        self._candle_event.set()
        
        if self._binance_session:
            await self._binance_session.close()
        
        if self.data_logger:
            self.data_logger.close()
        
        logger.info("Bars: %d | Signals: %d | Trades: %d | PnL: $%.2f",
                   self.state.bar_count, self.state.total_signals,
                   self.state.total_trades, self.state.realized_pnl)
    
    async def _main_loop(self):
        """Main signal generation loop - runs every 5-minute candle."""
        while self._running:
            self._candle_event.clear()
            try:
                await asyncio.wait_for(self._candle_event.wait(), timeout=360.0)
            except asyncio.TimeoutError:
                logger.warning("No candle close in 6 min")
                continue
            
            if not self._running:
                break
            
            self.state.bar_count += 1
            rebuild_ratio_series(self.state, self.params)
            
            # Update VWAP
            now_utc = datetime.now(timezone.utc)
            today_str = now_utc.strftime("%Y-%m-%d")
            if today_str != self.state.vwap_date:
                self.state.vwap_cumulative_rpv = 0.0
                self.state.vwap_cumulative_vol = 0.0
                self.state.vwap_date = today_str
            
            if self.state.ratio_prices and self.state.hype_candles:
                last_vol = self.state.hype_candles[-1].volume
                self.state.vwap_cumulative_rpv += self.state.ratio_prices[-1] * last_vol
                self.state.vwap_cumulative_vol += last_vol
            
            # Check warmup
            if len(self.state.ratio_prices) < self.params.warmup_candle_count:
                logger.info("[Bar %d] Warmup %d/%d", self.state.bar_count,
                           len(self.state.ratio_prices), self.params.warmup_candle_count)
                continue
            
            # Compute signals
            enable_zscore_norm = self.params.signal_zscore_lookback > 0
            signal_result = compute_all_signals(
                self.state, self.params, self.signal_history, enable_zscore_norm
            )
            
            # Update regime
            regime = self.regime.update(self.state.half_life_estimate)
            
            # Aggregate
            wsum = aggregate_signal_weighted(
                signal_result.normalized_signals, self.params.weights, self.params.long_bias
            )
            
            # Decide
            action = decide_trade_action(wsum, self.state, self.params)
            self.state.total_signals += 1
            
            # Log
            ts = self.state.hype_candles[-1].timestamp if self.state.hype_candles else time.time()
            self.data_logger.log(ts, self.state, signal_result, wsum, action)
            
            logger.info("[Bar %d] ratio=%.8f z=%.2f hl=%.0f regime=%s | wsum=%.3f -> %s",
                       self.state.bar_count,
                       self.state.ratio_prices[-1] if self.state.ratio_prices else 0.0,
                       signal_result.raw_values.get("zscore", 0.0),
                       self.state.half_life_estimate, regime, wsum, action)
            
            # Execute if live
            if self.params.enable_live_trading and action in (
                "LONG_HYPE_SHORT_BTC", "SHORT_HYPE_LONG_BTC", "EXIT"
            ):
                if action == "EXIT":
                    await execute_exit(self.state, self.params, self._exchange)
                else:
                    await execute_entry(action, wsum, self.state, self.params, self._exchange)
            
            # Reset bar counters
            self.state.hype_cvd_current = 0.0
            self.state.btc_cvd_current = 0.0
            self.state.hype_volume_current = 0.0
            self.state.btc_volume_current = 0.0


# -----------------------------------------------------------------------------
# Entry Point
# -----------------------------------------------------------------------------

async def main():
    params = StrategyParameters.from_environment()
    runner = StatArbRunner(params)
    
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(runner.stop()))
    
    try:
        await runner.start()
    except KeyboardInterrupt:
        pass
    finally:
        await runner.stop()


if __name__ == "__main__":
    asyncio.run(main())
