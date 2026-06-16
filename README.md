# Hotstuff Trading Strategies

Live strategies under `strategies/` and `api/`. Backtests under `backtest/`.

## Backtest

- **Rules & params:** [assumptions.md](assumptions.md) — no lookahead, $500 capital, fees, slippage
- **Exchange fees & symbols:** [api/models.py](api/models.py) (`INSTRUMENT_DEFAULTS`, `BASE_DEFAULTS`)
- **How to run:** [backtest/README.md](backtest/README.md)

```bash
cd backtest && pip install -r requirements.txt
python run_hype_btc_arb.py
python run_latency_arb.py --symbols HYPE-PERP
python run_usa500_arb.py
```
