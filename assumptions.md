# Backtest Assumptions

**Capital:** $500 default (`backtest/config.py`). Position sizes scale with equity.

## Data
- Hotstuff: `chart` API, `chart_type: mark` → `data/*.parquet`
- Binance: USD-M futures klines → `data/BN_*.parquet`
- 5m candles. Real data only.

## No Lookahead
- Signals: bars `0 … t-1` only (`get_lookback_data`).
- Entry: bar `t` open. Exit: bar `t` close.

## Fees & slippage
- Rates: `backtest/fee_model.py` (mirrors `api/models.py`)
- Crypto taker: **2.5 bps**/side. USA500/USA100: **1.5 bps**/side.
- Slippage: **3 bps** entry; **4.5 bps** latency exit.

## Strategies (tuned Mar–May 2026)
| Runner | Pair | Key params |
|--------|------|------------|
| `run_hype_btc_arb.py` | HYPE/BTC | z≥3.25, TP 42 bps, cooldown 96 |
| `run_latency_arb.py` | HYPE vs Binance | signal≥11 bps, TP 9 bps, cooldown 42 |
| `run_usa500_arb.py` | USA500/USA100 | z≥3.05, TP 11 bps, cooldown 84 |

## Not modeled
Latency, partial fills, funding, orderbook. Latency arb uses 5m proxy not tick data.

## Exchange constants
- Symbols & fees: `api/models.py` → `INSTRUMENT_DEFAULTS`, `BASE_DEFAULTS`
- Backtest copy: `backtest/fee_model.py`, `backtest/io_utils.py` (Binance map)
