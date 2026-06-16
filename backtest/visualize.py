"""
Backtest Visualization Module
=============================

Generates charts and visualizations for backtest results:
- Equity curve with drawdowns
- Trade P&L distribution
- Monthly returns heatmap
- Drawdown periods
- Trade entry/exit points on price

Usage:
    from visualize import BacktestVisualizer
    viz = BacktestVisualizer(results_path)
    viz.create_all_charts(output_dir="./charts")
"""

import json
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas as pd
import seaborn as sns
from matplotlib.gridspec import GridSpec

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 8)
plt.rcParams['font.size'] = 10


class BacktestVisualizer:
    """Creates visualizations from backtest results."""
    
    def __init__(self, results_dir: str):
        self.results_dir = Path(results_dir)
        
        # Load data
        equity_raw = pd.read_csv(self.results_dir / "equity_curve.csv")
        equity_raw["timestamp"] = pd.to_datetime(equity_raw["timestamp"], utc=True, errors="coerce")
        self.equity_df = equity_raw.dropna(subset=["timestamp"]).reset_index(drop=True)
        # Downsample for plotting performance
        if len(self.equity_df) > 3000:
            step = max(1, len(self.equity_df) // 3000)
            self.equity_df = self.equity_df.iloc[::step].reset_index(drop=True)

        trades_path = self.results_dir / "trades.csv"
        if trades_path.exists() and trades_path.stat().st_size > 0:
            self.trades_df = pd.read_csv(trades_path, parse_dates=["timestamp"])
        else:
            self.trades_df = pd.DataFrame()
        
        with open(self.results_dir / "summary.json") as f:
            self.summary = json.load(f)
    
    def plot_equity_curve(self, save_path: Optional[str] = None, show: bool = False):
        """Plot equity curve with drawdown shading."""
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10), 
                                        gridspec_kw={'height_ratios': [3, 1]})
        
        df = self.equity_df.copy()
        
        # Calculate drawdown
        df['peak'] = df['equity'].cummax()
        df['drawdown'] = (df['equity'] - df['peak']) / df['peak'] * 100
        
        # Equity curve
        ax1.plot(df['timestamp'], df['equity'], linewidth=1.5, color='#2E86AB', label='Equity')
        ax1.fill_between(df['timestamp'], df['equity'], alpha=0.3, color='#2E86AB')
        
        # Initial equity line
        initial = self.summary['initial_equity']
        ax1.axhline(y=initial, color='gray', linestyle='--', alpha=0.5, label=f'Initial (${initial:,.0f})')
        
        # Formatting
        ax1.set_title('Equity Curve', fontsize=14, fontweight='bold')
        ax1.set_ylabel('Equity ($)', fontsize=12)
        ax1.legend(loc='upper left')
        ax1.grid(True, alpha=0.3)
        
        # Format y-axis as currency
        ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
        
        # Drawdown
        ax2.fill_between(df['timestamp'], df['drawdown'], 0, 
                         color='#E74C3C', alpha=0.4, label='Drawdown')
        ax2.plot(df['timestamp'], df['drawdown'], color='#C0392B', linewidth=1)
        ax2.set_title('Drawdown (%)', fontsize=12)
        ax2.set_ylabel('Drawdown %', fontsize=11)
        ax2.set_xlabel('Date', fontsize=12)
        ax2.grid(True, alpha=0.3)
        
        # Format dates
        ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
        plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=150, bbox_inches='tight')
            print(f"Saved: {save_path}")
        
        if show:
            plt.show()
        else:
            plt.close()
    
    def plot_trade_analysis(self, save_path: Optional[str] = None, show: bool = False):
        """Plot trade analysis charts."""
        fig = plt.figure(figsize=(16, 12))
        gs = GridSpec(3, 2, figure=fig)
        
        # Filter to trades with realized P&L
        closed_trades = self.trades_df[self.trades_df['pnl_realized'] != 0].copy()
        
        if len(closed_trades) == 0:
            print("No closed trades to analyze")
            return
        
        # 1. P&L Distribution
        ax1 = fig.add_subplot(gs[0, 0])
        colors = ['#27AE60' if pnl > 0 else '#E74C3C' for pnl in closed_trades['pnl_realized']]
        ax1.bar(range(len(closed_trades)), closed_trades['pnl_realized'], color=colors, alpha=0.7)
        ax1.axhline(y=0, color='black', linewidth=0.5)
        ax1.set_title('Trade P&L', fontsize=12, fontweight='bold')
        ax1.set_ylabel('P&L ($)')
        ax1.grid(True, alpha=0.3)
        
        # 2. Cumulative P&L
        ax2 = fig.add_subplot(gs[0, 1])
        closed_trades['cumulative_pnl'] = closed_trades['pnl_realized'].cumsum()
        ax2.plot(range(len(closed_trades)), closed_trades['cumulative_pnl'], 
                color='#2E86AB', linewidth=2)
        ax2.fill_between(range(len(closed_trades)), closed_trades['cumulative_pnl'], 
                        alpha=0.3, color='#2E86AB')
        ax2.set_title('Cumulative Trade P&L', fontsize=12, fontweight='bold')
        ax2.set_ylabel('Cumulative P&L ($)')
        ax2.grid(True, alpha=0.3)
        
        # 3. Win/Loss Distribution
        ax3 = fig.add_subplot(gs[1, 0])
        wins = closed_trades[closed_trades['pnl_realized'] > 0]['pnl_realized']
        losses = closed_trades[closed_trades['pnl_realized'] < 0]['pnl_realized']
        
        ax3.hist(wins, bins=20, alpha=0.6, color='#27AE60', label=f'Wins (n={len(wins)})')
        ax3.hist(losses, bins=20, alpha=0.6, color='#E74C3C', label=f'Losses (n={len(losses)})')
        ax3.axvline(x=0, color='black', linestyle='--', linewidth=1)
        ax3.set_title('P&L Distribution', fontsize=12, fontweight='bold')
        ax3.set_xlabel('P&L ($)')
        ax3.set_ylabel('Frequency')
        ax3.legend()
        ax3.grid(True, alpha=0.3)
        
        # 4. Trade Duration
        ax4 = fig.add_subplot(gs[1, 1])
        # Calculate duration (simplified - assuming consecutive trades)
        durations = self._estimate_trade_durations()
        if durations:
            ax4.hist(durations, bins=15, color='#9B59B6', alpha=0.7, edgecolor='black')
            ax4.set_title('Trade Duration Distribution', fontsize=12, fontweight='bold')
            ax4.set_xlabel('Duration (bars)')
            ax4.set_ylabel('Frequency')
            ax4.grid(True, alpha=0.3)
        
        # 5. Exit Reason Analysis
        ax5 = fig.add_subplot(gs[2, :])
        exit_reasons = closed_trades['exit_reason'].value_counts()
        colors_exit = plt.cm.Set3(np.linspace(0, 1, len(exit_reasons)))
        bars = ax5.bar(exit_reasons.index, exit_reasons.values, color=colors_exit, edgecolor='black')
        ax5.set_title('Exit Reasons', fontsize=12, fontweight='bold')
        ax5.set_ylabel('Count')
        
        # Add value labels on bars
        for bar in bars:
            height = bar.get_height()
            ax5.text(bar.get_x() + bar.get_width()/2., height,
                    f'{int(height)}',
                    ha='center', va='bottom', fontsize=10)
        
        ax5.grid(True, alpha=0.3, axis='y')
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=150, bbox_inches='tight')
            print(f"Saved: {save_path}")
        
        if show:
            plt.show()
        else:
            plt.close()
    
    def _estimate_trade_durations(self) -> List[int]:
        """Estimate trade durations from entry/exit sequences."""
        durations = []
        entry_bar = None
        
        for i, trade in self.trades_df.iterrows():
            if trade['exit_reason'] in ('entry_long', 'entry_short'):
                entry_bar = i
            elif entry_bar is not None and trade['exit_reason'] not in ('entry_long', 'entry_short'):
                duration = i - entry_bar
                durations.append(duration)
                entry_bar = None
        
        return durations
    
    def plot_monthly_returns(self, save_path: Optional[str] = None, show: bool = False):
        """Plot monthly returns heatmap."""
        df = self.equity_df.copy()
        df['month'] = df['timestamp'].dt.to_period('M')
        df['daily_return'] = df['equity'].pct_change()
        
        # Calculate monthly returns
        monthly = df.groupby('month').agg({
            'equity': ['first', 'last'],
            'daily_return': 'sum'
        })
        monthly.columns = ['first_equity', 'last_equity', 'sum_return']
        monthly['monthly_return'] = (monthly['last_equity'] - monthly['first_equity']) / monthly['first_equity'] * 100
        
        # Reshape for heatmap
        monthly['year'] = monthly.index.year
        monthly['month_num'] = monthly.index.month
        
        pivot = monthly.pivot(index='year', columns='month_num', values='monthly_return')
        
        # Plot
        fig, ax = plt.subplots(figsize=(14, 6))
        
        cmap = sns.diverging_palette(10, 133, s=85, l=55, as_cmap=True)
        sns.heatmap(pivot, annot=True, fmt='.1f', cmap=cmap, center=0,
                   cbar_kws={'label': 'Return %'}, linewidths=0.5, ax=ax)
        
        ax.set_title('Monthly Returns (%)', fontsize=14, fontweight='bold')
        ax.set_xlabel('Month', fontsize=12)
        ax.set_ylabel('Year', fontsize=12)

        month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                      'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        tick_labels = [month_names[int(m) - 1] for m in pivot.columns]
        ax.set_xticklabels(tick_labels)
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=150, bbox_inches='tight')
            print(f"Saved: {save_path}")
        
        if show:
            plt.show()
        else:
            plt.close()
    
    def plot_rolling_statistics(self, save_path: Optional[str] = None, show: bool = False):
        """Plot rolling Sharpe ratio and win rate."""
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))
        
        df = self.equity_df.copy()
        df['returns'] = df['equity'].pct_change()
        
        # Rolling Sharpe (21-bar window)
        window = 21
        rolling_mean = df['returns'].rolling(window=window).mean()
        rolling_std = df['returns'].rolling(window=window).std()
        df['rolling_sharpe'] = rolling_mean / rolling_std * np.sqrt(window)
        
        ax1.plot(df['timestamp'], df['rolling_sharpe'], color='#2E86AB', linewidth=1.5)
        ax1.axhline(y=0, color='black', linestyle='--', linewidth=0.5)
        ax1.axhline(y=1, color='green', linestyle='--', linewidth=0.5, alpha=0.5)
        ax1.fill_between(df['timestamp'], df['rolling_sharpe'], 0, 
                        where=(df['rolling_sharpe'] > 0), color='green', alpha=0.2)
        ax1.fill_between(df['timestamp'], df['rolling_sharpe'], 0, 
                        where=(df['rolling_sharpe'] < 0), color='red', alpha=0.2)
        ax1.set_title('Rolling Sharpe Ratio (21-bar)', fontsize=12, fontweight='bold')
        ax1.set_ylabel('Sharpe Ratio')
        ax1.grid(True, alpha=0.3)
        
        # Trade win rate over time
        if len(self.trades_df) > 10:
            trades = self.trades_df.copy()
            trades['is_win'] = trades['pnl_realized'] > 0
            trades['cumulative_wins'] = trades['is_win'].cumsum()
            trades['cumulative_trades'] = range(1, len(trades) + 1)
            trades['win_rate'] = trades['cumulative_wins'] / trades['cumulative_trades'] * 100
            
            ax2.plot(trades['timestamp'], trades['win_rate'], 
                    color='#9B59B6', linewidth=2, label='Cumulative Win Rate')
            ax2.axhline(y=50, color='gray', linestyle='--', alpha=0.5)
            ax2.fill_between(trades['timestamp'], trades['win_rate'], 50,
                            where=(trades['win_rate'] > 50), color='green', alpha=0.2)
            ax2.fill_between(trades['timestamp'], trades['win_rate'], 50,
                            where=(trades['win_rate'] < 50), color='red', alpha=0.2)
            ax2.set_title('Cumulative Win Rate', fontsize=12, fontweight='bold')
            ax2.set_ylabel('Win Rate %')
            ax2.set_xlabel('Date')
            ax2.legend()
            ax2.grid(True, alpha=0.3)
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=150, bbox_inches='tight')
            print(f"Saved: {save_path}")
        
        if show:
            plt.show()
        else:
            plt.close()
    
    def create_summary_report(self, save_path: Optional[str] = None):
        """Create text summary report."""
        lines = []
        lines.append("=" * 60)
        lines.append("BACKTEST SUMMARY REPORT")
        lines.append("=" * 60)
        lines.append("")
        
        # Key metrics
        lines.append("PERFORMANCE METRICS")
        lines.append("-" * 40)
        lines.append(f"Initial Equity:        ${self.summary['initial_equity']:,.2f}")
        lines.append(f"Final Equity:          ${self.summary['final_equity']:,.2f}")
        lines.append(f"Total Return:          {self.summary['total_return_pct']:.2f}%")
        lines.append(f"Max Drawdown:          {self.summary['max_drawdown_pct']:.2f}%")
        lines.append("")
        
        lines.append("TRADE STATISTICS")
        lines.append("-" * 40)
        lines.append(f"Total Trades:          {self.summary['total_trades']}")
        lines.append(f"Winning Trades:        {self.summary['winning_trades']}")
        lines.append(f"Losing Trades:         {self.summary['losing_trades']}")
        lines.append(f"Win Rate:              {self.summary['win_rate']*100:.1f}%")
        lines.append(f"Total Fees:            ${self.summary['total_fees']:,.2f}")
        lines.append(f"Gross P&L:             ${self.summary['gross_pnl']:,.2f}")
        lines.append("")
        
        # Calculate additional metrics
        df = self.equity_df.copy()
        df['returns'] = df['equity'].pct_change().dropna()
        
        if len(df) > 1:
            lines.append("RISK METRICS")
            lines.append("-" * 40)
            
            returns = df['returns'].dropna()
            
            # Sharpe (simplified, assuming daily)
            if len(returns) > 21:
                sharpe = returns.mean() / returns.std() * np.sqrt(252)
                lines.append(f"Sharpe Ratio:          {sharpe:.2f}")
            
            # Calmar
            calmar = (self.summary['total_return_pct'] / 100) / max(0.001, self.summary['max_drawdown_pct'] / 100)
            lines.append(f"Calmar Ratio:          {calmar:.2f}")
            
            # Volatility
            volatility = returns.std() * np.sqrt(252) * 100
            lines.append(f"Annualized Volatility: {volatility:.1f}%")
            
            lines.append("")
        
        lines.append("=" * 60)
        
        report = "\n".join(lines)
        
        print(report)
        
        if save_path:
            with open(save_path, 'w') as f:
                f.write(report)
            print(f"\nSaved report: {save_path}")
        
        return report
    
    def create_all_charts(self, output_dir: str = "./charts"):
        """Generate all visualization charts."""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        print("\nGenerating charts...")
        print("-" * 40)
        
        self.plot_equity_curve(save_path=output_path / "equity_curve.png")
        self.plot_trade_analysis(save_path=output_path / "trade_analysis.png")
        self.plot_monthly_returns(save_path=output_path / "monthly_returns.png")
        self.plot_rolling_statistics(save_path=output_path / "rolling_stats.png")
        self.create_summary_report(save_path=output_path / "summary_report.txt")
        
        print("-" * 40)
        print(f"All charts saved to: {output_path}")


def main():
    """CLI entry point for visualization."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Visualize backtest results")
    parser.add_argument("--results-dir", required=True, help="Directory with backtest results")
    parser.add_argument("--output-dir", default="./charts", help="Output directory for charts")
    parser.add_argument("--show", action="store_true", help="Show charts interactively")
    
    args = parser.parse_args()
    
    viz = BacktestVisualizer(args.results_dir)
    
    if args.show:
        viz.plot_equity_curve(show=True)
        viz.plot_trade_analysis(show=True)
        viz.plot_monthly_returns(show=True)
        viz.plot_rolling_statistics(show=True)
    else:
        viz.create_all_charts(args.output_dir)


if __name__ == "__main__":
    main()
