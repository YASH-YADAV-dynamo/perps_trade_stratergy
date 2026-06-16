"""
Backtest Engine for Stat-Arb Strategy
======================================

Features:
- Event-driven backtesting (no lookahead bias)
- Tracks equity, positions, P&L
- Simulates slippage and fees
- Auto-parameter adjustment based on recent performance
- Outputs trade history and equity curve

Usage:
    from backtest_engine import BacktestEngine
    engine = BacktestEngine(initial_equity=500, fee_rate=0.0005)
    results = engine.run(strategy, data)
"""

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


@dataclass
class Position:
    symbol: str
    size: float  # positive = long, negative = short
    entry_price: float
    entry_time: datetime
    
    def unrealized_pnl(self, current_price: float) -> float:
        return self.size * (current_price - self.entry_price)
    
    def market_value(self, current_price: float) -> float:
        return abs(self.size) * current_price


@dataclass
class Trade:
    symbol: str
    side: str  # "BUY" or "SELL"
    size: float
    price: float
    timestamp: datetime
    fee: float
    pnl_realized: float = 0.0
    exit_reason: str = ""


@dataclass
class BacktestState:
    """Tracks the backtest state at any point in time."""
    equity: float
    cash: float
    positions: Dict[str, Position] = field(default_factory=dict)
    trades: List[Trade] = field(default_factory=list)
    equity_curve: List[Tuple[datetime, float]] = field(default_factory=list)
    timestamp: Optional[datetime] = None
    
    # Statistics
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    total_fees: float = 0.0
    gross_pnl: float = 0.0
    max_drawdown: float = 0.0
    peak_equity: float = 0.0


class BacktestEngine:
    """
    Event-driven backtest engine.
    
    Processes data bar-by-bar, calling the strategy function
    with only historical data available up to that point.
    """
    
    def __init__(
        self,
        initial_equity: float = 500.0,
        fee_rate: float = 0.0005,  # 5 bps per trade (maker/taker avg)
        slippage_bps: float = 5.0,  # 5 bps slippage on entry/exit
        perps: bool = True,  # Perpetual futures mode (no expiry)
    ):
        self.initial_equity = initial_equity
        self.fee_rate = fee_rate
        self.slippage_bps = slippage_bps / 10000.0
        self.perps = perps
        
        self.state: Optional[BacktestState] = None
        self.data_history: Dict[str, pd.DataFrame] = {}
        self.current_bar: int = 0
        
    def reset(self):
        """Reset the engine state."""
        self.state = BacktestState(
            equity=self.initial_equity,
            cash=self.initial_equity,
            equity_curve=[(datetime.min, self.initial_equity)],
            peak_equity=self.initial_equity,
        )
        self.current_bar = 0
    
    def load_data(self, symbol: str, df: pd.DataFrame):
        """Load historical data for a symbol."""
        df = df.sort_values("timestamp").reset_index(drop=True)
        self.data_history[symbol] = df.copy()
        
    def get_lookback_data(self, symbol: str, lookback_bars: int) -> pd.DataFrame:
        """
        Get historical data up to current bar (NO LOOKAHEAD).
        This is the key function that prevents lookahead bias.
        """
        if symbol not in self.data_history:
            return pd.DataFrame()
        
        df = self.data_history[symbol]
        end_idx = self.current_bar
        start_idx = max(0, end_idx - lookback_bars)
        
        return df.iloc[start_idx:end_idx].copy()
    
    def get_current_bar(self, symbol: str):
        """Current bar OHLC (execution only — not for signal generation)."""
        if symbol not in self.data_history:
            return None
        df = self.data_history[symbol]
        if self.current_bar >= len(df):
            return None
        return df.iloc[self.current_bar]

    def get_bar_price(self, symbol: str, field: str = "close") -> Optional[float]:
        row = self.get_current_bar(symbol)
        if row is None or field not in row:
            return None
        return float(row[field])

    def get_current_price(self, symbol: str) -> Optional[float]:
        return self.get_bar_price(symbol, "close")
    
    def calculate_position_value(self) -> float:
        """Calculate total value of all positions at current prices."""
        total = 0.0
        for sym, pos in self.state.positions.items():
            price = self.get_current_price(sym)
            if price:
                total += pos.market_value(price)
        return total
    
    def calculate_unrealized_pnl(self) -> float:
        """Calculate unrealized P&L of all positions."""
        total = 0.0
        for sym, pos in self.state.positions.items():
            price = self.get_current_price(sym)
            if price:
                total += pos.unrealized_pnl(price)
        return total
    
    def update_equity(self, timestamp: datetime):
        """Mark-to-market equity (no double-counting of position notional)."""
        unrealized = self.calculate_unrealized_pnl()
        self.state.equity = self.initial_equity + self.state.gross_pnl + unrealized - self.state.total_fees
        self.state.equity_curve.append((timestamp, self.state.equity))
        
        # Track drawdown
        if self.state.equity > self.state.peak_equity:
            self.state.peak_equity = self.state.equity
        drawdown = (self.state.peak_equity - self.state.equity) / self.state.peak_equity
        self.state.max_drawdown = max(self.state.max_drawdown, drawdown)
    
    def execute_trade(
        self,
        symbol: str,
        side: str,  # "BUY" or "SELL"
        size: float,
        timestamp: datetime,
        exit_reason: str = "",
        price_field: str = "close",
        fee_rate: Optional[float] = None,
        slippage_bps: Optional[float] = None,
    ) -> bool:
        """
        Execute a trade with slippage and fees.
        Returns True if successful.
        """
        base_price = self.get_bar_price(symbol, price_field)
        if base_price is None:
            return False

        slip = (slippage_bps / 10_000.0) if slippage_bps is not None else self.slippage_bps
        fee_r = fee_rate if fee_rate is not None else self.fee_rate

        slippage_factor = 1 + slip if side == "BUY" else 1 - slip
        exec_price = base_price * slippage_factor

        notional = size * exec_price
        fee = notional * fee_r
        
        # Check if we have enough equity (allowing up to 10x leverage)
        max_position = self.state.equity * 10
        current_exposure = self.calculate_position_value()
        if current_exposure + notional > max_position:
            # Reduce size to fit within limits
            available = max_position - current_exposure
            if available <= 0:
                return False
            size = available / exec_price
            notional = size * exec_price
            fee = notional * self.fee_rate
        
        # Handle position updates
        existing_pos = self.state.positions.get(symbol)
        realized_pnl = 0.0
        
        if existing_pos:
            if (existing_pos.size > 0 and side == "SELL") or (existing_pos.size < 0 and side == "BUY"):
                # Reducing position
                close_size = min(abs(existing_pos.size), size)
                realized_pnl = existing_pos.size / abs(existing_pos.size) * close_size * (exec_price - existing_pos.entry_price)
                
                # Update position
                if abs(existing_pos.size) <= size:
                    # Full close
                    del self.state.positions[symbol]
                else:
                    # Partial close - reduce position
                    remaining = abs(existing_pos.size) - size
                    direction = 1 if existing_pos.size > 0 else -1
                    existing_pos.size = direction * remaining
            else:
                # Adding to position - update average entry
                total_size = abs(existing_pos.size) + size
                avg_price = (abs(existing_pos.size) * existing_pos.entry_price + size * exec_price) / total_size
                direction = 1 if existing_pos.size > 0 else -1
                existing_pos.size = direction * total_size
                existing_pos.entry_price = avg_price
        else:
            # New position
            direction = 1 if side == "BUY" else -1
            self.state.positions[symbol] = Position(
                symbol=symbol,
                size=direction * size,
                entry_price=exec_price,
                entry_time=timestamp,
            )
        
        # Update cash and fees
        cash_change = notional if side == "SELL" else -notional
        self.state.cash += cash_change - fee
        self.state.total_fees += fee
        self.state.gross_pnl += realized_pnl
        
        # Track statistics
        self.state.total_trades += 1
        if realized_pnl > 0:
            self.state.winning_trades += 1
        elif realized_pnl < 0:
            self.state.losing_trades += 1
        
        # Record trade
        trade = Trade(
            symbol=symbol,
            side=side,
            size=size,
            price=exec_price,
            timestamp=timestamp,
            fee=fee,
            pnl_realized=realized_pnl,
            exit_reason=exit_reason,
        )
        self.state.trades.append(trade)
        
        return True
    
    def run(
        self,
        strategy_fn: Callable,
        symbols: List[str],
        warmup_bars: int = 100,
        progress_interval: int = 1000,
    ) -> BacktestState:
        """
        Run the backtest.
        
        Args:
            strategy_fn: Function(bar_index, get_data_fn, state) -> actions
            symbols: List of symbols to trade
            warmup_bars: Bars to skip for strategy initialization
            progress_interval: Print progress every N bars
        """
        self.reset()
        
        # Find minimum data length
        min_bars = min(len(self.data_history[s]) for s in symbols if s in self.data_history)
        
        print(f"Running backtest: {min_bars} bars, {warmup_bars} warmup")
        print(f"Initial equity: ${self.initial_equity:.2f}")
        
        for bar in range(warmup_bars, min_bars):
            self.current_bar = bar
            
            # Get current timestamp from first symbol
            ts = self.data_history[symbols[0]].iloc[bar]["datetime"]
            self.state.timestamp = ts
            
            # Call strategy function
            actions = strategy_fn(
                bar_index=bar,
                get_data_fn=self.get_lookback_data,
                state=self.state,
                symbols=symbols,
                equity=self.state.equity,
            )
            
            # Execute actions
            for action in actions:
                symbol = action.get("symbol")
                side = action.get("side")  # "BUY", "SELL", or "FLAT"
                size = action.get("size", 0)
                exit_reason = action.get("reason", "")
                kw = {
                    "price_field": action.get("price_field", "close"),
                    "fee_rate": action.get("fee_rate"),
                    "slippage_bps": action.get("slippage_bps"),
                }

                if side == "FLAT" and symbol in self.state.positions:
                    pos = self.state.positions[symbol]
                    close_side = "SELL" if pos.size > 0 else "BUY"
                    self.execute_trade(
                        symbol, close_side, abs(pos.size), ts, exit_reason, **kw
                    )
                elif side in ("BUY", "SELL") and size > 0:
                    self.execute_trade(symbol, side, size, ts, exit_reason, **kw)
            
            # Update equity marking
            self.update_equity(ts)
            
            # Progress
            if bar % progress_interval == 0:
                print(f"  Bar {bar}/{min_bars}: Equity=${self.state.equity:.2f}")
        
        # Final stats
        self._print_summary()
        return self.state
    
    def _print_summary(self):
        """Print backtest summary."""
        print("\n" + "="*60)
        print("BACKTEST SUMMARY")
        print("="*60)
        print(f"Initial Equity:    ${self.initial_equity:,.2f}")
        print(f"Final Equity:      ${self.state.equity:,.2f}")
        print(f"Total Return:      {(self.state.equity / self.initial_equity - 1) * 100:.2f}%")
        print(f"Max Drawdown:      {self.state.max_drawdown * 100:.2f}%")
        print(f"Total Trades:      {self.state.total_trades}")
        print(f"Winning Trades:    {self.state.winning_trades} ({self.state.winning_trades/max(1,self.state.total_trades)*100:.1f}%)")
        print(f"Losing Trades:     {self.state.losing_trades} ({self.state.losing_trades/max(1,self.state.total_trades)*100:.1f}%)")
        print(f"Total Fees Paid:   ${self.state.total_fees:,.2f}")
        print(f"Gross P&L:         ${self.state.gross_pnl:,.2f}")
        print("="*60)
    
    def save_results(self, output_path: Path):
        """Save backtest results to files."""
        output_path = Path(output_path)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Save equity curve
        equity_df = pd.DataFrame(self.state.equity_curve, columns=["timestamp", "equity"])
        equity_df.to_csv(output_path / "equity_curve.csv", index=False)
        
        # Save trades
        trades_data = []
        for t in self.state.trades:
            trades_data.append({
                "timestamp": t.timestamp,
                "symbol": t.symbol,
                "side": t.side,
                "size": t.size,
                "price": t.price,
                "fee": t.fee,
                "pnl_realized": t.pnl_realized,
                "exit_reason": t.exit_reason,
            })
        trades_df = pd.DataFrame(trades_data)
        trades_df.to_csv(output_path / "trades.csv", index=False)
        
        # Save summary JSON
        summary = {
            "initial_equity": self.initial_equity,
            "final_equity": self.state.equity,
            "total_return_pct": (self.state.equity / self.initial_equity - 1) * 100,
            "max_drawdown_pct": self.state.max_drawdown * 100,
            "total_trades": self.state.total_trades,
            "winning_trades": self.state.winning_trades,
            "losing_trades": self.state.losing_trades,
            "win_rate": self.state.winning_trades / max(1, self.state.total_trades),
            "total_fees": self.state.total_fees,
            "gross_pnl": self.state.gross_pnl,
        }
        with open(output_path / "summary.json", "w") as f:
            json.dump(summary, f, indent=2)
        
        print(f"\nResults saved to: {output_path}")
