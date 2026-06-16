"""
Latency Arb — bar-level backtest proxy (Hotstuff vs Binance divergence).

Mirrors live BasisTracker logic: enter on deviation spike, exit on convergence.
Signal from bar t-1; entry at open t; exit at close t. No lookahead.
"""

from dataclasses import dataclass
from typing import Callable, Dict, List, Optional

from fee_model import DEFAULT_SLIPPAGE_BPS, taker_rate


class BasisTracker:
    """EMA baseline of structural HS/BN divergence (same idea as live bot)."""

    def __init__(self, ema_half_life: int = 200):
        self.alpha = 2.0 / (ema_half_life + 1)
        self.ema = 0.0
        self.initialized = False
        self.tick_count = 0
        self.warmup = ema_half_life

    def update(self, divergence_bps: float) -> float:
        self.tick_count += 1
        if not self.initialized:
            self.ema = divergence_bps
            self.initialized = True
            return 0.0
        self.ema += self.alpha * (divergence_bps - self.ema)
        return divergence_bps - self.ema

    @property
    def warmed_up(self) -> bool:
        return self.tick_count >= self.warmup


@dataclass
class LatencyArbConfig:
    entry_threshold_bps: float = 11.0
    exit_threshold_bps: float = 0.5
    velocity_min_bps_s: float = 0.0
    min_edge_after_fees_bps: float = 14.0
    take_profit_bps: float = 9.0
    min_raw_div_bps: float = 3.5
    stop_loss_bps: float = 28.0
    max_hold_bars: int = 2
    cooldown_bars: int = 42
    equity_fraction: float = 0.20
    ema_half_life: int = 200
    bar_seconds: float = 300.0
    exit_slippage_bps: float = 4.5


@dataclass
class _InstState:
    basis: BasisTracker
    position: int = 0
    size: float = 0.0
    entry_price: float = 0.0
    bars_in_trade: int = 0
    bars_since_exit: int = 9999
    last_signal_bps: float = 0.0


class LatencyArbStrategy:
    """Multi-instrument latency arb; merged HS+BN columns in each symbol dataframe."""

    BN_COL = "bn_close"

    def __init__(self, config: LatencyArbConfig = None):
        self.config = config or LatencyArbConfig()
        self._states: Dict[str, _InstState] = {}

    def _state(self, symbol: str) -> _InstState:
        if symbol not in self._states:
            self._states[symbol] = _InstState(
                basis=BasisTracker(self.config.ema_half_life)
            )
        return self._states[symbol]

    @staticmethod
    def _divergence_bps(hs: float, bn: float) -> float:
        if hs <= 0:
            return 0.0
        return (bn - hs) / hs * 10_000.0

    def _velocity_bps_s(self, df) -> float:
        if len(df) < 3:
            return 0.0
        bn0 = float(df[self.BN_COL].iloc[-3])
        bn1 = float(df[self.BN_COL].iloc[-1])
        if bn0 <= 0:
            return 0.0
        move = abs(bn1 - bn0) / bn0 * 10_000.0
        return move / (2 * self.config.bar_seconds)

    def _signal_from_history(self, hist) -> Optional[Dict]:
        if len(hist) < 2 or self.BN_COL not in hist.columns:
            return None
        row = hist.iloc[-1]
        hs = float(row["close"])
        bn = float(row[self.BN_COL])
        raw = self._divergence_bps(hs, bn)
        return {"hs": hs, "bn": bn, "raw_div_bps": raw}

    def probe_entry(self, get_data_fn: Callable, symbol: str) -> Optional[Dict]:
        st = self._state(symbol)
        if st.position != 0 or st.bars_since_exit < self.config.cooldown_bars:
            return None

        hist = get_data_fn(symbol, self.config.ema_half_life + 5)
        sig = self._signal_from_history(hist)
        if sig is None:
            return None

        signal_bps = st.basis.update(sig["raw_div_bps"])
        if not st.basis.warmed_up:
            return None

        if self.config.velocity_min_bps_s > 0:
            if self._velocity_bps_s(hist) < self.config.velocity_min_bps_s:
                return None

        fee_rt = taker_rate(symbol) * 10_000 * 2 + self.config.exit_slippage_bps
        edge = abs(signal_bps) - fee_rt
        if edge < self.config.min_edge_after_fees_bps:
            return None

        direction = 0
        if signal_bps > self.config.entry_threshold_bps:
            direction = 1
        elif signal_bps < -self.config.entry_threshold_bps:
            direction = -1
        if direction == 0:
            return None

        return {"direction": direction, "edge_bps": edge, "signal_bps": signal_bps}

    def __call__(
        self,
        bar_index: int,
        get_data_fn: Callable,
        state,
        symbols: List[str],
        equity: float,
        get_exec_bar=None,
    ) -> List[Dict]:
        actions: List[Dict] = []
        per_sym_equity = equity / max(len(symbols), 1)

        for symbol in symbols:
            st = self._state(symbol)
            st.bars_since_exit += 1
            hist = get_data_fn(symbol, self.config.ema_half_life + 5)

            signal_bps = st.last_signal_bps
            raw_div = 0.0
            if len(hist) >= 1 and self.BN_COL in hist.columns:
                sig = self._signal_from_history(hist)
                if sig:
                    raw_div = sig["raw_div_bps"]
                    signal_bps = st.basis.update(raw_div)
                    st.last_signal_bps = signal_bps

            if st.position != 0:
                st.bars_in_trade += 1
                row = get_exec_bar(symbol) if get_exec_bar else None
                mark = float(row["close"]) if row is not None else 0.0
                if mark <= 0:
                    continue

                pnl_bps = (
                    (mark - st.entry_price) / st.entry_price * 10_000
                    if st.position > 0
                    else (st.entry_price - mark) / st.entry_price * 10_000
                )
                reason = ""
                if pnl_bps >= self.config.take_profit_bps:
                    reason = "take_profit"
                elif pnl_bps <= -self.config.stop_loss_bps:
                    reason = "stop_loss"
                elif st.position > 0 and signal_bps <= self.config.exit_threshold_bps:
                    reason = "exit_converge"
                elif st.position < 0 and signal_bps >= -self.config.exit_threshold_bps:
                    reason = "exit_converge"
                elif st.bars_in_trade >= self.config.max_hold_bars:
                    reason = "max_hold"

                if reason:
                    side = "SELL" if st.position > 0 else "BUY"
                    actions.append({
                        "symbol": symbol,
                        "side": side,
                        "size": st.size,
                        "reason": reason,
                        "price_field": "close",
                        "fee_rate": taker_rate(symbol),
                        "slippage_bps": self.config.exit_slippage_bps,
                    })
                    st.position = 0
                    st.size = 0.0
                    st.entry_price = 0.0
                    st.bars_in_trade = 0
                    st.bars_since_exit = 0
                continue

            if st.bars_since_exit < self.config.cooldown_bars or not st.basis.warmed_up:
                continue

            if self.config.velocity_min_bps_s > 0:
                if self._velocity_bps_s(hist) < self.config.velocity_min_bps_s:
                    continue

            fee_rt = taker_rate(symbol) * 10_000 * 2 + self.config.exit_slippage_bps
            edge = abs(signal_bps) - fee_rt
            if edge < self.config.min_edge_after_fees_bps:
                continue

            direction = 0
            if signal_bps > self.config.entry_threshold_bps and raw_div >= self.config.min_raw_div_bps:
                direction = 1
            elif signal_bps < -self.config.entry_threshold_bps and raw_div <= -self.config.min_raw_div_bps:
                direction = -1
            if direction == 0:
                continue

            ref = hist.iloc[-1]
            hs = float(ref["close"])
            if hs <= 0:
                continue

            notional = per_sym_equity * self.config.equity_fraction
            size = notional / hs
            st.position = direction
            st.size = size
            st.bars_in_trade = 0

            side = "BUY" if direction > 0 else "SELL"
            actions.append({
                "symbol": symbol,
                "side": side,
                "size": size,
                "reason": "latency_entry",
                "price_field": "open",
                "fee_rate": taker_rate(symbol),
                "slippage_bps": DEFAULT_SLIPPAGE_BPS,
            })

        return actions

    def sync_entry_prices(self, state) -> None:
        for sym, st in self._states.items():
            if st.position == 0 or st.entry_price > 0:
                continue
            pos = state.positions.get(sym)
            if pos:
                st.entry_price = pos.entry_price
