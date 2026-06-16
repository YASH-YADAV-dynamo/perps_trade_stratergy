"""
USA500 stat-arb backtest config (USA500 / USA100 pair).
"""

from typing import Dict, List

from fee_model import DEFAULT_SLIPPAGE_BPS, taker_rate
from stat_arb_backtest import SignalConfig, StatArbStrategy

USA500 = "USA500-PERP"
USA100 = "USA100-PERP"

USA500_ARB_CONFIG = SignalConfig(
    lookback=120,
    entry_zscore=3.05,
    exit_zscore=0.20,
    stop_zscore=0.95,
    take_profit_bps=11.0,
    stop_loss_bps=48.0,
    min_correlation=0.86,
    min_half_life_bars=5.0,
    max_half_life_bars=38.0,
    max_hold_bars=24,
    cooldown_bars=84,
    equity_fraction=0.50,
    min_composite_confirm=0.38,
    rsi_confirm_long=43.0,
    rsi_confirm_short=57.0,
)


def annotate_arb_actions(actions: List[Dict]) -> List[Dict]:
    for a in actions:
        sym = a.get("symbol", USA500)
        is_entry = a.get("reason", "").startswith("entry")
        a["price_field"] = "open" if is_entry else "close"
        a["fee_rate"] = taker_rate(sym)
        a["slippage_bps"] = DEFAULT_SLIPPAGE_BPS
    return actions


class USA500StatArbStrategy(StatArbStrategy):
    def __init__(self):
        super().__init__(USA500_ARB_CONFIG)

    def __call__(self, bar_index, get_data_fn, state, symbols, equity, **kwargs):
        actions = super().__call__(bar_index, get_data_fn, state, [USA500, USA100], equity)
        return annotate_arb_actions(actions)
