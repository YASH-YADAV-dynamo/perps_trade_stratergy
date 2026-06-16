"""
Hotstuff fee rates for backtesting.

Source of truth: ../api/models.py
  - BASE_DEFAULTS["taker_fee_bps"]  → crypto default 2.5
  - INSTRUMENT_DEFAULTS[symbol]["taker_fee_bps"]  → per-instrument overrides
  - USA500-PERP, USA100-PERP, GOLD-PERP, etc. → 1.5 bps
"""

from typing import Dict

# Mirror api/models.py INSTRUMENT_DEFAULTS taker_fee_bps
TAKER_FEE_BPS: Dict[str, float] = {
    "USA500-PERP": 1.5,
    "USA100-PERP": 1.5,
    "GOLD-PERP": 1.5,
    "SILVER-PERP": 1.5,
    "BTC-PERP": 2.5,
    "ETH-PERP": 2.5,
    "HYPE-PERP": 2.5,
    "SOL-PERP": 2.5,
    "XRP-PERP": 2.5,
    "ZEC-PERP": 2.5,
}

MAKER_FEE_BPS: Dict[str, float] = {}

DEFAULT_TAKER_BPS = 2.5  # BASE_DEFAULTS in api/models.py
DEFAULT_SLIPPAGE_BPS = 3.0


def taker_rate(symbol: str) -> float:
    return TAKER_FEE_BPS.get(symbol, DEFAULT_TAKER_BPS) / 10_000.0


def maker_rate(symbol: str) -> float:
    return MAKER_FEE_BPS.get(symbol, 0.0) / 10_000.0


def slippage_rate(symbol: str) -> float:
    return DEFAULT_SLIPPAGE_BPS / 10_000.0


def stat_arb_round_trip_bps(leg_a: str, leg_b: str) -> float:
    return (TAKER_FEE_BPS.get(leg_a, DEFAULT_TAKER_BPS) * 2
            + TAKER_FEE_BPS.get(leg_b, DEFAULT_TAKER_BPS) * 2)
