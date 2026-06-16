"""
Config defaults, merge logic, and request validation.
No Pydantic -- plain dicts only.
"""

BASE_DEFAULTS = {
    "order_size_usd": 0,
    "target_exposure_x": 2.5,
    "levels": 5,
    "level_spacing_bps": 1.0,
    "level_size_scale": 1.4,
    "min_spread_bps": 0.1,
    "spread_vol_mult": 1.8,
    "close_spread_bps": 0.1,
    "use_alpha": True,
    "alpha_bps": 15.0,
    "inventory_skew_bps": 3.0,
    "max_inventory": 1000.0,
    "leverage": 20,
    "noise_bps": 0.0,
    "close_threshold_usd": 10.0,
    "inv_skew_start_pct": 30,
    "inv_skip_open_pct": 60,
    "toxic_threshold": 0.7,
    "adx_regime_enabled": True,
    "supertrend_enabled": True,
    "pivot_enabled": True,
    "max_loss_pct": 5.0,
    "guard_max_session_loss_usd": 5.0,
    "guard_max_drawdown_pct": 3.0,
    "guard_cooldown_s": 30,
    "guard_loss_streak_trigger": 3,
    "fixed_tp_enabled": False,
    "fixed_tp_bps": 8.0,
    "taker_fee_bps": 2.5,
    "market_bias": 0.0,
}

INSTRUMENT_DEFAULTS = {
    "HYPE-PERP":   {"min_spread_bps": 0.1, "spread_vol_mult": 1.8, "alpha_bps": 8.0, "max_inventory": 1000.0},
    "BTC-PERP":    {"min_spread_bps": 0.1, "spread_vol_mult": 1.5, "alpha_bps": 4.0, "max_inventory": 2000.0},
    "ETH-PERP":    {"min_spread_bps": 0.1, "spread_vol_mult": 1.5, "alpha_bps": 4.0, "max_inventory": 2000.0},
    "SOL-PERP":    {"min_spread_bps": 0.1, "spread_vol_mult": 1.8, "alpha_bps": 6.0, "max_inventory": 1000.0},
    "XRP-PERP":    {"min_spread_bps": 0.1, "spread_vol_mult": 2.0, "alpha_bps": 6.0, "max_inventory": 1000.0},
    "ZEC-PERP":    {"min_spread_bps": 0.1, "spread_vol_mult": 2.5, "alpha_bps": 8.0, "max_inventory": 1000.0},
    "BNB-PERP":    {"min_spread_bps": 0.1, "spread_vol_mult": 1.5, "alpha_bps": 4.0, "max_inventory": 1500.0},
    "AAPL-PERP":   {"min_spread_bps": 0.1, "spread_vol_mult": 1.6, "alpha_bps": 5.0, "max_inventory": 1200.0},
    "AMZN-PERP":   {"min_spread_bps": 0.1, "spread_vol_mult": 1.6, "alpha_bps": 5.0, "max_inventory": 1200.0},
    "GOOGL-PERP":  {"min_spread_bps": 0.1, "spread_vol_mult": 1.6, "alpha_bps": 5.0, "max_inventory": 1200.0},
    "META-PERP":   {"min_spread_bps": 0.1, "spread_vol_mult": 1.6, "alpha_bps": 5.0, "max_inventory": 1200.0},
    "MSFT-PERP":   {"min_spread_bps": 0.1, "spread_vol_mult": 1.6, "alpha_bps": 5.0, "max_inventory": 1200.0},
    "EWJ-PERP":    {"min_spread_bps": 0.1, "spread_vol_mult": 1.5, "alpha_bps": 4.0, "max_inventory": 1000.0},
    "EWY-PERP":    {"min_spread_bps": 0.1, "spread_vol_mult": 1.5, "alpha_bps": 4.0, "max_inventory": 1000.0},
    "TSLA-PERP":   {"min_spread_bps": 0.1, "spread_vol_mult": 2.0, "alpha_bps": 8.0, "max_inventory": 800.0},
    "PLTR-PERP":   {"min_spread_bps": 0.1, "spread_vol_mult": 2.0, "alpha_bps": 8.0, "max_inventory": 800.0},
    "NVDA-PERP":   {"min_spread_bps": 0.1, "spread_vol_mult": 1.7, "alpha_bps": 5.5, "max_inventory": 1200.0},
    "GOLD-PERP":   {"min_spread_bps": 0.1, "spread_vol_mult": 1.5, "alpha_bps": 1.0, "max_inventory": 2000.0, "close_threshold_usd": 5000.0, "taker_fee_bps": 1.5},
    "SILVER-PERP": {"min_spread_bps": 0.1, "spread_vol_mult": 1.5, "alpha_bps": 1.0, "max_inventory": 2000.0, "close_threshold_usd": 5000.0, "taker_fee_bps": 1.5},
    "WTIOIL-PERP": {"min_spread_bps": 0.1, "spread_vol_mult": 1.5, "alpha_bps": 1.0, "max_inventory": 2000.0, "close_threshold_usd": 5000.0, "taker_fee_bps": 1.5},
    "BRENTOIL-PERP": {"min_spread_bps": 0.1, "spread_vol_mult": 1.5, "alpha_bps": 1.0, "max_inventory": 2000.0, "close_threshold_usd": 5000.0, "taker_fee_bps": 1.5},
    "NATGAS-PERP": {"min_spread_bps": 0.1, "spread_vol_mult": 1.5, "alpha_bps": 1.0, "max_inventory": 2000.0, "close_threshold_usd": 5000.0, "taker_fee_bps": 1.5},
    "EURUSD-PERP": {"min_spread_bps": 0.1, "spread_vol_mult": 1.5, "alpha_bps": 1.0, "max_inventory": 2000.0, "close_threshold_usd": 5000.0, "taker_fee_bps": 1.5},
    "USDJPY-PERP": {"min_spread_bps": 0.1, "spread_vol_mult": 1.5, "alpha_bps": 1.0, "max_inventory": 2000.0, "close_threshold_usd": 5000.0, "taker_fee_bps": 1.5},
    "USA500-PERP": {"min_spread_bps": 0.1, "spread_vol_mult": 1.5, "alpha_bps": 1.0, "max_inventory": 2000.0, "close_threshold_usd": 5000.0, "taker_fee_bps": 1.5},
    "USA100-PERP": {"min_spread_bps": 0.1, "spread_vol_mult": 1.5, "alpha_bps": 1.0, "max_inventory": 2000.0, "close_threshold_usd": 5000.0, "taker_fee_bps": 1.5},
    "SPACEX-PERP": {"min_spread_bps": 0.1, "spread_vol_mult": 2.5, "alpha_bps": 12.0, "max_inventory": 400.0},
    "X-PERP":      {"min_spread_bps": 0.1, "spread_vol_mult": 1.5, "alpha_bps": 5.0, "max_inventory": 500.0},
}

VALID_SYMBOLS = set(INSTRUMENT_DEFAULTS.keys())
VALID_STRATEGIES = frozenset({"maker", "taker"})

ALLOWED_CONFIG_KEYS = frozenset(BASE_DEFAULTS.keys())
TAKER_CONFIG_DEFAULTS = {
    "min_spread_usd": 1.0,
    "min_spread_bps": 0.1,
    "take_profit_bps": 0.1,
    "close_bps": 0.2,
    "close_timeout_ms": 300,
    "order_size_usd": 0.0,
    "target_exposure_x": 50.0,
    "leverage": 20,
    "cooldown_s": 0.05,
    "max_loss_usd": 5.0,
    "order_expiry_ms": 15000,
    "market_bias": 0.0,
}
TAKER_INSTRUMENT_DEFAULTS = {
    "BTC-PERP": {"min_spread_usd": 1.0, "min_spread_bps": 0.1, "take_profit_bps": 0.1, "close_bps": 0.2},
    "ETH-PERP": {"min_spread_usd": 0.4, "min_spread_bps": 0.15, "take_profit_bps": 0.12, "close_bps": 0.25},
    "SOL-PERP": {"min_spread_usd": 0.08, "min_spread_bps": 0.2, "take_profit_bps": 0.15, "close_bps": 0.3},
    "HYPE-PERP": {"min_spread_usd": 0.03, "min_spread_bps": 0.25, "take_profit_bps": 0.18, "close_bps": 0.35},
    "XRP-PERP": {"min_spread_usd": 0.004, "min_spread_bps": 0.25, "take_profit_bps": 0.2, "close_bps": 0.4},
    "ZEC-PERP": {"min_spread_usd": 0.15, "min_spread_bps": 0.2, "take_profit_bps": 0.15, "close_bps": 0.3},
    "BNB-PERP": {"min_spread_usd": 0.05, "min_spread_bps": 0.15, "take_profit_bps": 0.12, "close_bps": 0.25},
    "AAPL-PERP": {"min_spread_usd": 0.08, "min_spread_bps": 0.15, "take_profit_bps": 0.12, "close_bps": 0.25},
    "AMZN-PERP": {"min_spread_usd": 0.08, "min_spread_bps": 0.15, "take_profit_bps": 0.12, "close_bps": 0.25},
    "GOOGL-PERP": {"min_spread_usd": 0.08, "min_spread_bps": 0.15, "take_profit_bps": 0.12, "close_bps": 0.25},
    "META-PERP": {"min_spread_usd": 0.08, "min_spread_bps": 0.15, "take_profit_bps": 0.12, "close_bps": 0.25},
    "MSFT-PERP": {"min_spread_usd": 0.08, "min_spread_bps": 0.15, "take_profit_bps": 0.12, "close_bps": 0.25},
    "EWJ-PERP": {"min_spread_usd": 0.02, "min_spread_bps": 0.2, "take_profit_bps": 0.15, "close_bps": 0.3},
    "EWY-PERP": {"min_spread_usd": 0.02, "min_spread_bps": 0.2, "take_profit_bps": 0.15, "close_bps": 0.3},
    "TSLA-PERP": {"min_spread_usd": 0.15, "min_spread_bps": 0.2, "take_profit_bps": 0.15, "close_bps": 0.3},
    "PLTR-PERP": {"min_spread_usd": 0.12, "min_spread_bps": 0.2, "take_profit_bps": 0.15, "close_bps": 0.3},
    "NVDA-PERP": {"min_spread_usd": 0.1, "min_spread_bps": 0.15, "take_profit_bps": 0.12, "close_bps": 0.25},
    "GOLD-PERP": {"min_spread_usd": 0.08, "min_spread_bps": 0.1, "take_profit_bps": 0.1, "close_bps": 0.2},
    "SILVER-PERP": {"min_spread_usd": 0.03, "min_spread_bps": 0.1, "take_profit_bps": 0.12, "close_bps": 0.25},
    "WTIOIL-PERP": {"min_spread_usd": 0.05, "min_spread_bps": 0.1, "take_profit_bps": 0.1, "close_bps": 0.2},
    "BRENTOIL-PERP": {"min_spread_usd": 0.05, "min_spread_bps": 0.1, "take_profit_bps": 0.1, "close_bps": 0.2},
    "NATGAS-PERP": {"min_spread_usd": 0.03, "min_spread_bps": 0.1, "take_profit_bps": 0.12, "close_bps": 0.25},
    "EURUSD-PERP": {"min_spread_usd": 0.0001, "min_spread_bps": 0.1, "take_profit_bps": 0.1, "close_bps": 0.2},
    "USDJPY-PERP": {"min_spread_usd": 0.02, "min_spread_bps": 0.1, "take_profit_bps": 0.1, "close_bps": 0.2},
    "USA500-PERP": {"min_spread_usd": 0.1, "min_spread_bps": 0.1, "take_profit_bps": 0.1, "close_bps": 0.2},
    "USA100-PERP": {"min_spread_usd": 0.1, "min_spread_bps": 0.1, "take_profit_bps": 0.1, "close_bps": 0.2},
    "SPACEX-PERP": {"min_spread_usd": 0.05, "min_spread_bps": 0.25, "take_profit_bps": 0.18, "close_bps": 0.35},
    "X-PERP": {"min_spread_usd": 0.01, "min_spread_bps": 0.25, "take_profit_bps": 0.18, "close_bps": 0.35},
}
TAKER_ALLOWED_CONFIG_KEYS = frozenset(TAKER_CONFIG_DEFAULTS.keys())

_TYPE_MAP = {k: type(v) for k, v in BASE_DEFAULTS.items()}
_TAKER_TYPE_MAP = {k: type(v) for k, v in TAKER_CONFIG_DEFAULTS.items()}


def _coerce(key: str, value):
    """Cast value to the same type as the default for that key."""
    target = _TYPE_MAP.get(key)
    if target is None:
        return value
    if target is bool:
        if isinstance(value, str):
            return value.lower() in ("true", "1", "yes")
        return bool(value)
    if target is int:
        return int(value)
    if target is float:
        return round(float(value), 1) if "bps" in key else float(value)
    return value


def build_config(symbol: str, user_overrides: dict = None) -> dict:
    """Merge BASE_DEFAULTS -> INSTRUMENT_DEFAULTS[symbol] -> user overrides."""
    cfg = dict(BASE_DEFAULTS)
    inst = INSTRUMENT_DEFAULTS.get(symbol)
    if inst:
        cfg.update(inst)
    if user_overrides:
        for k, v in user_overrides.items():
            if k in ALLOWED_CONFIG_KEYS:
                cfg[k] = _coerce(k, v)
    return cfg


def _coerce_taker(key: str, value):
    target = _TAKER_TYPE_MAP.get(key)
    if target is None:
        return value
    if target is bool:
        if isinstance(value, str):
            return value.lower() in ("true", "1", "yes")
        return bool(value)
    if target is int:
        return int(value)
    if target is float:
        return float(value)
    return value


def build_taker_config(symbol: str, user_overrides: dict = None) -> dict:
    cfg = dict(TAKER_CONFIG_DEFAULTS)
    inst = TAKER_INSTRUMENT_DEFAULTS.get(symbol)
    if inst:
        cfg.update(inst)
    if user_overrides:
        for k, v in user_overrides.items():
            if k in TAKER_ALLOWED_CONFIG_KEYS:
                cfg[k] = _coerce_taker(k, v)
    return cfg


def validate_session_request(body: dict) -> str:
    """Return error string if invalid, empty string if OK."""
    strategy = str(body.get("strategy") or "maker").lower()
    if strategy not in VALID_STRATEGIES:
        return f"Invalid strategy: {strategy}. Must be one of: {sorted(VALID_STRATEGIES)}"
    if not body.get("agent_address"):
        return "agent_address is required"
    if not body.get("agent_private_key"):
        return "agent_private_key is required"
    symbols = body.get("symbols")
    if not symbols or not isinstance(symbols, list) or len(symbols) == 0:
        return "symbols is required (list of instrument names)"
    bad = [s for s in symbols if s not in VALID_SYMBOLS]
    if bad:
        return f"Invalid symbols: {bad}. Must be one of: {sorted(VALID_SYMBOLS)}"
    if len(symbols) != len(set(symbols)):
        return "Duplicate symbols in list"

    if strategy == "taker":
        if len(symbols) != 1:
            return "taker strategy requires exactly one symbol"
        taker_cfg = body.get("taker_config") or {}
        if not isinstance(taker_cfg, dict):
            return "taker_config must be an object"
        unknown = set(taker_cfg.keys()) - TAKER_ALLOWED_CONFIG_KEYS
        if unknown:
            return f"Unknown taker_config keys: {sorted(unknown)}"

    return ""


def validate_config_update(body: dict) -> str:
    """Return error string if invalid config keys, empty string if OK."""
    unknown = set(body.keys()) - ALLOWED_CONFIG_KEYS
    if unknown:
        return f"Unknown config keys: {sorted(unknown)}"
    return ""


def validate_taker_config_update(body: dict) -> str:
    unknown = set(body.keys()) - TAKER_ALLOWED_CONFIG_KEYS
    if unknown:
        return f"Unknown taker config keys: {sorted(unknown)}"
    return ""
