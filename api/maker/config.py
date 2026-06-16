import logging
import os
import sys
from dataclasses import dataclass
from web3 import Web3

log = logging.getLogger("aggressive_mm")


@dataclass(frozen=True)
class Config:
    private_key: str
    agent_address: str
    account_address: str
    symbol: str
    # Fair price
    fair_price_window_s: float
    fair_price_warmup_s: float
    # Spread -- base spread is now vol-adaptive: max(min_spread_bps, vol_bps * spread_vol_mult)
    min_spread_bps: float
    spread_vol_mult: float
    close_spread_bps: float
    # Inventory skew: shift quote center by skew_bps * (inv / max_inv)
    inventory_skew_bps: float
    # Alpha signals
    use_alpha: bool
    w_contrarian_obi: float
    w_bar_portion: float
    w_trade_imbalance: float
    w_cvd_divergence: float
    w_rsi_divergence: float
    alpha_bps: float
    signal_ema_span: int
    bp_lookback: int
    trade_imb_window_s: float
    cvd_lookback: int
    rsi_period: int
    rsi_div_lookback: int
    # Regime / toxic flow
    toxic_threshold: float
    obi_extreme: float
    obi_flip_threshold: float
    vol_spike_mult: float
    # Regime indicators (Binance-derived)
    adx_regime_enabled: bool
    adx_period: int
    supertrend_enabled: bool
    supertrend_atr_period: int
    supertrend_multiplier: float
    pivot_enabled: bool
    # Adverse selection
    fast_move_bps: float
    fast_move_window_s: float
    fast_move_widen: float
    # Quoting -- laddered orders
    levels: int
    level_spacing_bps: float
    order_size_usd: float
    target_exposure_x: float
    level_size_scale: float
    post_only: bool
    order_expiry_ms: int
    # Requoting
    requote_interval_s: float
    requote_threshold_bps: float
    update_throttle_ms: float
    # Noise
    noise_bps: float
    # Position / inventory -- graduated (max_inventory is USD notional, converted to base at runtime)
    close_threshold_usd: float
    max_inventory: float
    inv_skew_start_pct: float
    inv_skip_open_pct: float
    # Guard risk layer
    guard_max_session_loss_usd: float
    guard_max_drawdown_pct: float
    guard_cooldown_s: float
    guard_loss_streak_trigger: int
    guard_adverse_rate_threshold: float
    guard_adverse_rate_widen: float
    guard_inventory_decay_s: float
    guard_inventory_stale_mult: float
    guard_adverse_alpha_threshold: float
    # Risk
    max_loss_pct: float
    leverage: int
    # Volatility
    vol_window_s: float
    # Fixed TP (volume farming mode)
    fixed_tp_enabled: bool
    fixed_tp_bps: float
    taker_fee_bps: float
    # Market bias: -1.0 = full short, 0.0 = neutral, 1.0 = full long
    market_bias: float
    # Broker
    broker_address: str
    broker_fee: str
    # Execution
    enable_trading: bool

    @classmethod
    def from_env(cls, symbol_override: str = "") -> "Config":
        pk = os.getenv("HOTSTUFF_PRIVATE_KEY", "")
        agent_addr = os.getenv("HOTSTUFF_AGENT_ADDRESS", "")
        account_addr = os.getenv("HOTSTUFF_ACCOUNT_ADDRESS", "")
        if not pk or not agent_addr:
            log.error("HOTSTUFF_PRIVATE_KEY / HOTSTUFF_AGENT_ADDRESS not set")
            sys.exit(1)
        if not account_addr:
            account_addr = agent_addr
        try:
            agent_addr = Web3.to_checksum_address(agent_addr)
            account_addr = Web3.to_checksum_address(account_addr)
        except Exception:
            log.error("HOTSTUFF_AGENT_ADDRESS / HOTSTUFF_ACCOUNT_ADDRESS invalid")
            sys.exit(1)

        w_obi = float(os.getenv("MM_W_CONTRARIAN_OBI", "0.25"))
        w_bp = float(os.getenv("MM_W_BAR_PORTION", "0.20"))
        w_timb = float(os.getenv("MM_W_TRADE_IMBALANCE", "0.20"))
        w_cvd = float(os.getenv("MM_W_CVD_DIVERGENCE", "0.20"))
        w_rsi = float(os.getenv("MM_W_RSI_DIVERGENCE", "0.15"))
        w_total = w_obi + w_bp + w_timb + w_cvd + w_rsi
        if w_total > 0.0:
            w_obi /= w_total
            w_bp /= w_total
            w_timb /= w_total
            w_cvd /= w_total
            w_rsi /= w_total

        return cls(
            private_key=pk,
            agent_address=agent_addr,
            account_address=account_addr,
            symbol=symbol_override or os.getenv("MM_SYMBOL", "HYPE-PERP"),
            fair_price_window_s=float(os.getenv("MM_FAIR_PRICE_WINDOW_S", "300")),
            fair_price_warmup_s=float(os.getenv("MM_FAIR_PRICE_WARMUP_S", "10")),
            # Vol-adaptive spread: half_spread = max(min_spread, vol_bps * mult) / 2
            min_spread_bps=float(os.getenv("MM_MIN_SPREAD_BPS", "0.1")),
            spread_vol_mult=float(os.getenv("MM_SPREAD_VOL_MULT", "1.8")),
            close_spread_bps=float(os.getenv("MM_CLOSE_SPREAD_BPS", "0.1")),
            # Inventory skew
            inventory_skew_bps=float(os.getenv("MM_INVENTORY_SKEW_BPS", "3.0")),
            # Alpha
            use_alpha=os.getenv("MM_USE_ALPHA", "true").lower() == "true",
            w_contrarian_obi=w_obi,
            w_bar_portion=w_bp,
            w_trade_imbalance=w_timb,
            w_cvd_divergence=w_cvd,
            w_rsi_divergence=w_rsi,
            alpha_bps=float(os.getenv("MM_ALPHA_BPS", "15")),
            signal_ema_span=int(os.getenv("MM_SIGNAL_EMA_SPAN", "10")),
            bp_lookback=int(os.getenv("MM_BP_LOOKBACK", "10")),
            trade_imb_window_s=float(os.getenv("MM_TRADE_IMB_WINDOW_S", "60")),
            cvd_lookback=int(os.getenv("MM_CVD_LOOKBACK", "14")),
            rsi_period=int(os.getenv("MM_RSI_PERIOD", "14")),
            rsi_div_lookback=int(os.getenv("MM_RSI_DIV_LOOKBACK", "14")),
            # Regime / toxic
            toxic_threshold=float(os.getenv("MM_TOXIC_THRESHOLD", "0.7")),
            obi_extreme=float(os.getenv("MM_OBI_EXTREME", "0.4")),
            obi_flip_threshold=float(os.getenv("MM_OBI_FLIP_THRESHOLD", "0.5")),
            vol_spike_mult=float(os.getenv("MM_VOL_SPIKE_MULT", "3.0")),
            # Regime indicators
            adx_regime_enabled=os.getenv("MM_ADX_REGIME_ENABLED", "true").lower() == "true",
            adx_period=int(os.getenv("MM_ADX_PERIOD", "14")),
            supertrend_enabled=os.getenv("MM_SUPERTREND_ENABLED", "true").lower() == "true",
            supertrend_atr_period=int(os.getenv("MM_SUPERTREND_ATR_PERIOD", "10")),
            supertrend_multiplier=float(os.getenv("MM_SUPERTREND_MULTIPLIER", "3.0")),
            pivot_enabled=os.getenv("MM_PIVOT_ENABLED", "true").lower() == "true",
            # Adverse selection
            fast_move_bps=float(os.getenv("MM_FAST_MOVE_BPS", "20")),
            fast_move_window_s=float(os.getenv("MM_FAST_MOVE_WINDOW_S", "10")),
            fast_move_widen=float(os.getenv("MM_FAST_MOVE_WIDEN", "2.0")),
            # Laddered quoting
            levels=int(os.getenv("MM_LEVELS", "5")),
            level_spacing_bps=float(os.getenv("MM_LEVEL_SPACING_BPS", "1.0")),
            order_size_usd=float(os.getenv("MM_ORDER_SIZE_USD", "0")),
            target_exposure_x=float(os.getenv("MM_TARGET_EXPOSURE_X", "2.5")),
            level_size_scale=float(os.getenv("MM_LEVEL_SIZE_SCALE", "1.4")),
            post_only=os.getenv("MM_POST_ONLY", "true").lower() == "true",
            order_expiry_ms=int(os.getenv("MM_ORDER_EXPIRY_MS", "300000")),
            # Requoting
            requote_interval_s=float(os.getenv("MM_REQUOTE_INTERVAL_S", "0.2")),
            requote_threshold_bps=float(os.getenv("MM_REQUOTE_THRESHOLD_BPS", "1.0")),
            update_throttle_ms=float(os.getenv("MM_UPDATE_THROTTLE_MS", "200")),
            noise_bps=float(os.getenv("MM_NOISE_BPS", "0.0")),
            # Position -- graduated inventory management
            close_threshold_usd=float(os.getenv("MM_CLOSE_THRESHOLD_USD", "10.0")),
            max_inventory=float(os.getenv("MM_MAX_INVENTORY", "1000.0")),
            inv_skew_start_pct=float(os.getenv("MM_INV_SKEW_START_PCT", "30")),
            inv_skip_open_pct=float(os.getenv("MM_INV_SKIP_OPEN_PCT", "60")),
            # Guard risk layer
            guard_max_session_loss_usd=float(os.getenv("MM_GUARD_MAX_SESSION_LOSS_USD", "5.0")),
            guard_max_drawdown_pct=float(os.getenv("MM_GUARD_MAX_DRAWDOWN_PCT", "3.0")),
            guard_cooldown_s=float(os.getenv("MM_GUARD_COOLDOWN_S", "30")),
            guard_loss_streak_trigger=int(os.getenv("MM_GUARD_LOSS_STREAK_TRIGGER", "3")),
            guard_adverse_rate_threshold=float(os.getenv("MM_GUARD_ADVERSE_RATE_THRESHOLD", "0.6")),
            guard_adverse_rate_widen=float(os.getenv("MM_GUARD_ADVERSE_RATE_WIDEN", "2.0")),
            guard_inventory_decay_s=float(os.getenv("MM_GUARD_INVENTORY_DECAY_S", "300")),
            guard_inventory_stale_mult=float(os.getenv("MM_GUARD_INVENTORY_STALE_MULT", "0.5")),
            guard_adverse_alpha_threshold=float(os.getenv("MM_GUARD_ADVERSE_ALPHA_THRESHOLD", "0.25")),
            # Risk
            max_loss_pct=float(os.getenv("MM_MAX_LOSS_PCT", "5.0")),
            leverage=int(os.getenv("MM_LEVERAGE", "20")),
            vol_window_s=float(os.getenv("MM_VOL_WINDOW_S", "300")),
            fixed_tp_enabled=os.getenv("MM_FIXED_TP_ENABLED", "false").lower() == "true",
            fixed_tp_bps=float(os.getenv("MM_FIXED_TP_BPS", "8.0")),
            taker_fee_bps=float(os.getenv("MM_TAKER_FEE_BPS", "2.5")),
            market_bias=max(-1.0, min(1.0, float(os.getenv("MM_MARKET_BIAS", "0.0")))),
            broker_address=os.getenv("DXD_BROKER_ADDRESS", ""),
            broker_fee=os.getenv("DXD_BROKER_FEE", "0.00001"),
            enable_trading=os.getenv("MM_ENABLE_TRADING", "false").lower() == "true",
        )
