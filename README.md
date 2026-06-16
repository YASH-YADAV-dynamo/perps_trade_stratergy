# Hotstuff Trading Strategies

Live strategies under `strategies/` and `api/`. Backtests under `backtest/`.

## Backtest Results ($500 capital, Mar–May 2026)

### HYPE/BTC Stat-Arb
Log-ratio mean reversion. Long when HYPE cheap vs BTC, short when expensive.
- **Return:** +0.42% | **Final Equity:** $502.10 | **Trades:** 4 (1W/1L) | **Max Drawdown:** 0.16% | **Fees:** $0.22

### Latency Arb (HYPE vs Binance)
Basis divergence on Hotstuff vs Binance futures. Entry when ≥11 bps signal, take-profit at 9 bps.
- **Return:** +0.29% | **Final Equity:** $501.43 | **Trades:** 98 (26W/23L) | **Max Drawdown:** 0.64% | **Fees:** $2.45

### USA500/USA100 Stat-Arb
Log-ratio mean reversion on index futures pair. Long when USA500 cheap, short when expensive.
- **Return:** +0.05% | **Final Equity:** $500.24 | **Trades:** 4 (2W/0L) | **Max Drawdown:** 0.07% | **Fees:** $0.11

## Setup & Run

- **Rules & params:** [assumptions.md](assumptions.md) — no lookahead, $500 capital, fees, slippage
- **Exchange fees & symbols:** [api/models.py](api/models.py) (`INSTRUMENT_DEFAULTS`, `BASE_DEFAULTS`)

```bash
cd backtest && pip install -r requirements.txt
python run_hype_btc_arb.py
python run_latency_arb.py --symbols HYPE-PERP
python run_usa500_arb.py
```
