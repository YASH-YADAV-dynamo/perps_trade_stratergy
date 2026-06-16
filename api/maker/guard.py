import logging
import time
from collections import deque
from dataclasses import dataclass
from typing import List, Optional, Tuple

log = logging.getLogger("aggressive_mm")
_INTERVENTION_LOG_INTERVAL_S = 30.0


@dataclass
class GuardState:
    """Mutable state tracked by the Guard risk layer."""
    # Session high-water mark for trailing drawdown
    hwm_pnl: float = 0.0
    # Cooldown: timestamp when cooldown expires (0 = not cooling)
    cooldown_until: float = 0.0
    # Recent round-trip outcomes for loss streak detection
    recent_rt_pnls: "deque" = None  # type: ignore
    # Adverse fill tracking (from REFLECT feedback)
    recent_adverse_rate: float = 0.0
    # Last guard action for logging
    last_action: str = ""
    # Cumulative times guard intervened
    interventions: int = 0
    # Inventory age: monotonic timestamp of last fill
    last_fill_ts: float = 0.0
    # Current halt state from latest guard evaluation
    halt_active: bool = False
    # Throttled intervention logging state
    last_intervention_reason: str = ""
    last_intervention_log_ts: float = 0.0

    def __post_init__(self):
        if self.recent_rt_pnls is None:
            self.recent_rt_pnls = deque(maxlen=20)


class GuardDecision:
    """Output of the Guard layer -- what the quoter/execution should do."""
    __slots__ = (
        "allow_bids", "allow_asks", "force_close",
        "spread_mult", "halt", "reason",
    )

    def __init__(self):
        self.allow_bids: bool = True
        self.allow_asks: bool = True
        self.force_close: bool = False
        self.spread_mult: float = 1.0
        self.halt: bool = False
        self.reason: str = ""


class Guard:
    """Standalone risk layer sitting between quoter and execution.

    Inspired by Nunchi agent-cli Guard pattern. Runs independently from
    the quoting logic. Answers: "should we allow these quotes through,
    modify them, or kill everything?"

    Checks (in priority order):
      1. Session drawdown circuit breaker (hard stop)
      2. Trailing drawdown from high-water mark
      3. Cooldown after bad round-trip streak
      4. Adverse fill rate gate (from REFLECT markout feedback)
      5. Inventory age decay (stale position -> force close)
      6. Adverse alpha skip (positioned + alpha against us)
      7. Quote-widening under elevated risk

    All thresholds are configurable via Config.
    """

    def __init__(
        self,
        max_session_loss_usd: float,
        max_drawdown_pct: float,
        cooldown_after_loss_s: float,
        loss_streak_trigger: int,
        adverse_rate_threshold: float,
        adverse_rate_widen: float,
        inventory_decay_s: float,
        inventory_stale_mult: float,
        adverse_alpha_threshold: float,
        lot_size: float,
    ):
        self._max_session_loss = max_session_loss_usd
        self._max_drawdown_pct = max_drawdown_pct
        self._cooldown_s = cooldown_after_loss_s
        self._loss_streak_trigger = loss_streak_trigger
        self._adverse_rate_thresh = adverse_rate_threshold
        self._adverse_rate_widen = adverse_rate_widen
        self._inventory_decay_s = inventory_decay_s
        self._inventory_stale_mult = inventory_stale_mult
        self._adverse_alpha_thresh = adverse_alpha_threshold
        self._lot = lot_size
        self.state = GuardState()

    def on_round_trip(self, pnl: float):
        """Called by position tracker after each completed round-trip."""
        self.state.recent_rt_pnls.append(pnl)
        # Update high-water mark
        # (session_pnl is passed in evaluate, but we track RT-level streaks here)

    def on_fill(self):
        """Record fill timestamp for inventory age tracking."""
        self.state.last_fill_ts = time.monotonic()

    def update_adverse_rate(self, rate: float):
        """Called by REFLECT with recent adverse selection rate [0,1]."""
        self.state.recent_adverse_rate = rate

    def evaluate(
        self,
        session_pnl: float,
        account_equity: float,
        inventory: float,
        max_inventory: float,
        alpha_combined: float,
        toxic_score: float,
    ) -> GuardDecision:
        """Run all guard checks. Returns a GuardDecision."""
        dec = GuardDecision()
        now = time.monotonic()
        self.state.halt_active = False

        # -- 1. Session drawdown circuit breaker --
        if self._max_session_loss > 0.0 and session_pnl < -self._max_session_loss:
            dec.halt = True
            dec.allow_bids = False
            dec.allow_asks = False
            dec.reason = f"SESSION_LOSS: pnl=${session_pnl:.4f} < -${self._max_session_loss:.2f}"
            self._log_intervention(dec.reason)
            self.state.halt_active = True
            return dec

        # -- 2. Trailing drawdown from high-water mark --
        if session_pnl > self.state.hwm_pnl:
            self.state.hwm_pnl = session_pnl
        if self._max_drawdown_pct > 0.0 and account_equity > 0.0:
            drawdown = self.state.hwm_pnl - session_pnl
            drawdown_pct = drawdown / account_equity * 100.0
            if drawdown_pct >= self._max_drawdown_pct:
                dec.halt = True
                dec.allow_bids = False
                dec.allow_asks = False
                dec.reason = (
                    f"DRAWDOWN: {drawdown_pct:.1f}% from hwm "
                    f"(hwm=${self.state.hwm_pnl:.4f} now=${session_pnl:.4f})"
                )
                self._log_intervention(dec.reason)
                self.state.halt_active = True
                return dec

        # -- 3. Cooldown after loss streak --
        if now < self.state.cooldown_until:
            remaining = self.state.cooldown_until - now
            dec.halt = True
            dec.allow_bids = False
            dec.allow_asks = False
            dec.reason = f"COOLDOWN: {remaining:.0f}s remaining after loss streak"
            self.state.halt_active = True
            return dec

        # Check if we should enter cooldown
        if self._loss_streak_trigger > 0 and len(self.state.recent_rt_pnls) >= self._loss_streak_trigger:
            recent = list(self.state.recent_rt_pnls)
            tail = recent[-self._loss_streak_trigger:]
            if all(p < 0.0 for p in tail):
                self.state.cooldown_until = now + self._cooldown_s
                dec.halt = True
                dec.allow_bids = False
                dec.allow_asks = False
                dec.reason = (
                    f"LOSS_STREAK: {self._loss_streak_trigger} consecutive losses, "
                    f"cooling {self._cooldown_s:.0f}s"
                )
                self._log_intervention(dec.reason)
                # Clear streak so we don't re-trigger immediately after cooldown
                self.state.recent_rt_pnls.clear()
                self.state.halt_active = True
                return dec

        # -- 4. Adverse fill rate gate --
        if (
            self._adverse_rate_thresh > 0.0
            and self.state.recent_adverse_rate > self._adverse_rate_thresh
        ):
            dec.spread_mult = max(dec.spread_mult, self._adverse_rate_widen)
            dec.reason = (
                f"ADVERSE_FILLS: rate={self.state.recent_adverse_rate:.1%} "
                f"> {self._adverse_rate_thresh:.1%}, widen x{self._adverse_rate_widen:.1f}"
            )

        # -- 5. Inventory age decay (stale position -> force tighter close) --
        inv_abs = abs(inventory)
        if inv_abs > self._lot and self.state.last_fill_ts > 0.0:
            age = now - self.state.last_fill_ts
            if age > self._inventory_decay_s:
                staleness = min(3.0, age / self._inventory_decay_s)
                dec.force_close = True
                # Tighten close spread on stale inventory to improve exit fill odds.
                close_mult = max(0.05, min(1.0, self._inventory_stale_mult))
                dec.spread_mult = min(dec.spread_mult, close_mult)
                dec.reason = (
                    f"STALE_INV: age={age:.0f}s > {self._inventory_decay_s:.0f}s "
                    f"(staleness={staleness:.1f}x, close_mult={close_mult:.2f})"
                )

        # -- 6. Adverse alpha skip --
        if self._adverse_alpha_thresh > 0.0 and inv_abs > self._lot:
            if inventory > 0.0 and alpha_combined < -self._adverse_alpha_thresh:
                dec.allow_bids = False
                if not dec.reason:
                    dec.reason = (
                        f"ADVERSE_ALPHA: long inv={inventory:.4f} "
                        f"alpha={alpha_combined:.3f} < -{self._adverse_alpha_thresh:.2f}"
                    )
            elif inventory < 0.0 and alpha_combined > self._adverse_alpha_thresh:
                dec.allow_asks = False
                if not dec.reason:
                    dec.reason = (
                        f"ADVERSE_ALPHA: short inv={inventory:.4f} "
                        f"alpha={alpha_combined:.3f} > {self._adverse_alpha_thresh:.2f}"
                    )

        # -- 7. Elevated risk spread widening --
        # Compound: toxic + adverse fills can stack
        if toxic_score >= 0.5 and self.state.recent_adverse_rate > 0.3:
            extra = 1.0 + toxic_score * 0.5
            dec.spread_mult = max(dec.spread_mult, extra)

        self.state.halt_active = dec.halt
        self.state.last_action = dec.reason if dec.reason else "OK"
        return dec

    def _log_intervention(self, reason: str):
        now = time.monotonic()
        same_reason = reason == self.state.last_intervention_reason
        if same_reason and (now - self.state.last_intervention_log_ts) < _INTERVENTION_LOG_INTERVAL_S:
            return
        self.state.interventions += 1
        self.state.last_intervention_reason = reason
        self.state.last_intervention_log_ts = now
        log.warning("GUARD [#%d]: %s", self.state.interventions, reason)

    @property
    def is_halted(self) -> bool:
        return self.state.halt_active

    @property
    def interventions(self) -> int:
        return self.state.interventions
