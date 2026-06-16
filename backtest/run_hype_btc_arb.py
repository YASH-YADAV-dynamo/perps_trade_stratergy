"""HYPE/BTC stat-arb backtest. See ../assumptions.md."""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from backtest_engine import BacktestEngine
from config import DEFAULT_END, DEFAULT_START, INITIAL_EQUITY, RESOLUTION, WARMUP_STAT_ARB
from fee_model import DEFAULT_SLIPPAGE_BPS, taker_rate
from hype_btc_stat_arb import BTC, HYPE, HypeBtcStatArbStrategy
from io_utils import DATA_DIR, fetch_hotstuff, load_hotstuff
from visualize import BacktestVisualizer

RESULTS = Path(__file__).parent / "results" / "hype_btc_stat_arb"


def run(data, initial_equity, warmup, strategy_type="standard"):
    from stat_arb_backtest import AdaptiveStatArb
    strategy = AdaptiveStatArb() if strategy_type == "adaptive" else HypeBtcStatArbStrategy()
    engine = BacktestEngine(initial_equity, taker_rate(HYPE), DEFAULT_SLIPPAGE_BPS)
    for sym, df in data.items():
        engine.load_data(sym, df)
    engine.reset()
    symbols = [HYPE, BTC]
    n = min(len(data[s]) for s in symbols)
    for bar in range(warmup, n):
        engine.current_bar = bar
        ts = data[symbols[0]].iloc[bar]["datetime"]
        for action in strategy(bar, engine.get_lookback_data, engine.state, symbols, engine.state.equity):
            kw = {k: action.get(k) for k in ("price_field", "fee_rate", "slippage_bps")}
            sym, side, size = action["symbol"], action["side"], action.get("size", 0)
            if side == "FLAT" and sym in engine.state.positions:
                p = engine.state.positions[sym]
                engine.execute_trade(sym, "SELL" if p.size > 0 else "BUY", abs(p.size), ts, action.get("reason", ""), **kw)
            elif side in ("BUY", "SELL") and size > 0:
                engine.execute_trade(sym, side, size, ts, action.get("reason", ""), **kw)
        engine.update_equity(ts)
    engine._print_summary()
    RESULTS.mkdir(parents=True, exist_ok=True)
    engine.save_results(RESULTS)
    return RESULTS


def main():
    p = argparse.ArgumentParser(description="HYPE/BTC stat-arb backtest")
    p.add_argument("--fetch", action="store_true")
    p.add_argument("--start-date", default=DEFAULT_START)
    p.add_argument("--end-date", default=DEFAULT_END)
    p.add_argument("--initial-equity", type=float, default=INITIAL_EQUITY)
    p.add_argument("--warmup", type=int, default=WARMUP_STAT_ARB)
    p.add_argument("--strategy", default="standard", choices=["standard", "adaptive"])
    p.add_argument("--no-charts", action="store_true")
    args = p.parse_args()

    if args.fetch:
        for s in (HYPE, BTC):
            fetch_hotstuff(s, args.start_date, args.end_date, RESOLUTION)

    print("Loading data...")
    data = load_hotstuff([HYPE, BTC], RESOLUTION)
    for s, df in data.items():
        print(f"  {s}: {len(df)} bars")

    run(data, args.initial_equity, args.warmup, args.strategy)
    if not args.no_charts:
        BacktestVisualizer(RESULTS).create_all_charts(str(RESULTS / "charts"))
    print(f"Results: {RESULTS}")


if __name__ == "__main__":
    main()
