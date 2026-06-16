"""Latency arb backtest (Hotstuff vs Binance). See ../assumptions.md."""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from backtest_engine import BacktestEngine
from config import DEFAULT_END, DEFAULT_START, INITIAL_EQUITY, RESOLUTION, WARMUP_LATENCY_ARB
from fee_model import DEFAULT_SLIPPAGE_BPS, taker_rate
from io_utils import HS_TO_BINANCE, align, fetch_binance, fetch_hotstuff, load_merged_hs_bn
from latency_arb_backtest import LatencyArbConfig, LatencyArbStrategy
from visualize import BacktestVisualizer

LATENCY_CFG = LatencyArbConfig()
RESULTS = Path(__file__).parent / "results" / "latency_arb"


def main():
    p = argparse.ArgumentParser(description="Latency arb backtest")
    p.add_argument("--fetch", action="store_true")
    p.add_argument("--symbols", default="HYPE-PERP")
    p.add_argument("--start-date", default=DEFAULT_START)
    p.add_argument("--end-date", default=DEFAULT_END)
    p.add_argument("--initial-equity", type=float, default=INITIAL_EQUITY)
    p.add_argument("--warmup", type=int, default=WARMUP_LATENCY_ARB)
    p.add_argument("--no-charts", action="store_true")
    args = p.parse_args()

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    for hs in symbols:
        if hs not in HS_TO_BINANCE:
            sys.exit(f"No Binance map for {hs}. See io_utils.HS_TO_BINANCE")
        bn = HS_TO_BINANCE[hs]
        if args.fetch:
            fetch_hotstuff(hs, args.start_date, args.end_date, RESOLUTION)
            fetch_binance(bn, args.start_date, args.end_date, RESOLUTION)

    data = {hs: load_merged_hs_bn(hs, HS_TO_BINANCE[hs], RESOLUTION) for hs in symbols}
    data = align(data)
    for hs, df in data.items():
        print(f"  {hs}: {len(df)} bars")

    strategy = LatencyArbStrategy(LATENCY_CFG)
    engine = BacktestEngine(args.initial_equity, taker_rate(symbols[0]), DEFAULT_SLIPPAGE_BPS)
    for sym, df in data.items():
        engine.load_data(sym, df)
    engine.reset()
    n = min(len(data[s]) for s in symbols)

    for bar in range(args.warmup, n):
        engine.current_bar = bar
        ts = data[symbols[0]].iloc[bar]["datetime"]
        for action in strategy(bar, engine.get_lookback_data, engine.state, symbols, engine.state.equity, engine.get_current_bar):
            kw = {k: action.get(k) for k in ("price_field", "fee_rate", "slippage_bps")}
            if action["side"] in ("BUY", "SELL") and action.get("size", 0) > 0:
                engine.execute_trade(action["symbol"], action["side"], action["size"], ts, action.get("reason", ""), **kw)
        strategy.sync_entry_prices(engine.state)
        engine.update_equity(ts)

    engine._print_summary()
    RESULTS.mkdir(parents=True, exist_ok=True)
    engine.save_results(RESULTS)
    if not args.no_charts:
        BacktestVisualizer(RESULTS).create_all_charts(str(RESULTS / "charts"))
    print(f"Results: {RESULTS}")


if __name__ == "__main__":
    main()
