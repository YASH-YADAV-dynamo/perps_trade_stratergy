"""
HYPE/BTC stat-arb — tuned for Hotstuff (profit-oriented, validated on Mar–May 2026).
"""

from typing import Dict, List

from fee_model import DEFAULT_SLIPPAGE_BPS, taker_rate
from stat_arb_backtest import SignalConfig, StatArbStrategy

HYPE = "HYPE-PERP"
BTC = "BTC-PERP"

HYPE_BTC_ARB_CONFIG = SignalConfig(
    lookback=120,
    entry_zscore=3.25,
    exit_zscore=0.28,
    stop_zscore=1.25,
    take_profit_bps=42.0,
    stop_loss_bps=72.0,
    min_correlation=0.78,
    min_half_life_bars=5.0,
    max_half_life_bars=35.0,
    max_hold_bars=30,
    cooldown_bars=96,
    equity_fraction=0.58,
    min_composite_confirm=0.40,
    rsi_confirm_long=42.0,
    rsi_confirm_short=58.0,
)


def annotate_arb_actions(actions: List[Dict]) -> List[Dict]:
    for a in actions:
        sym = a.get("symbol", HYPE)
        is_entry = a.get("reason", "").startswith("entry")
        a["price_field"] = "open" if is_entry else "close"
        a["fee_rate"] = taker_rate(sym)
        a["slippage_bps"] = DEFAULT_SLIPPAGE_BPS
    return actions


class HypeBtcStatArbStrategy(StatArbStrategy):
    def __init__(self):
        super().__init__(HYPE_BTC_ARB_CONFIG)

    def __call__(self, bar_index, get_data_fn, state, symbols, equity, **kwargs):
        actions = super().__call__(bar_index, get_data_fn, state, [HYPE, BTC], equity)
        return annotate_arb_actions(actions)
