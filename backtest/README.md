# Backtest

Event-driven backtests on **real Hotstuff data**. No lookahead.

**Assumptions & rules:** [../assumptions.md](../assumptions.md)  
**Exchange fees/symbols:** [../api/models.py](../api/models.py) → mirrored in [fee_model.py](fee_model.py)  
**Default capital:** $500 → [config.py](config.py)

## Setup

```bash
cd backtest
pip install -r requirements.txt
```

## Run

```bash
# Stat-arb HYPE/BTC
python run_hype_btc_arb.py --fetch

# Latency arb HYPE vs Binance
python run_latency_arb.py --fetch

# Stat-arb USA500/USA100
python run_usa500_arb.py --fetch
```

Add `--no-charts` to skip plots. Override capital: `--initial-equity 500`.

## Layout

```
backtest/
├── config.py              # INITIAL_EQUITY, dates, warmup
├── fee_model.py           # taker bps per symbol (from api/models.py)
├── io_utils.py            # data load, fetch, HS↔Binance map
├── backtest_engine.py     # event loop, no lookahead
├── stat_arb_backtest.py   # core stat-arb logic
├── latency_arb_backtest.py
├── hype_btc_stat_arb.py   # tuned HYPE/BTC params
├── usa500_stat_arb.py     # tuned USA500 params
├── fetch_data.py          # Hotstuff candles
├── fetch_binance_data.py  # Binance klines
├── visualize.py
├── run_hype_btc_arb.py
├── run_latency_arb.py
├── run_usa500_arb.py
├── data/                  # parquet (gitignored)
└── results/
    ├── hype_btc_stat_arb/
    ├── latency_arb/
    └── usa500_stat_arb/
```

## Results (@ $500, Mar–May 2026)

### HYPE/BTC Stat-Arb
Log-ratio mean reversion. Long when HYPE cheap vs BTC, short when expensive.  
[`results/hype_btc_stat_arb/`](results/hype_btc_stat_arb/) — +0.42%, 4 trades | [`summary.json`](results/hype_btc_stat_arb/summary.json) | [`trades.csv`](results/hype_btc_stat_arb/trades.csv)

### Latency Arb (HYPE vs Binance)
Basis divergence on Hotstuff vs Binance futures. Entry when ≥11 bps signal, take-profit at 9 bps.  
[`results/latency_arb/`](results/latency_arb/) — +0.29%, 98 trades | [`summary.json`](results/latency_arb/summary.json) | [`trades.csv`](results/latency_arb/trades.csv)

### USA500/USA100 Stat-Arb
Log-ratio mean reversion on index futures pair. Long when USA500 cheap, short when expensive.  
[`results/usa500_stat_arb/`](results/usa500_stat_arb/) — +0.05%, 4 trades | [`summary.json`](results/usa500_stat_arb/summary.json) | [`trades.csv`](results/usa500_stat_arb/trades.csv)

Run with `--no-charts` to skip plots. Default runners generate equity curve + trade analysis charts.
