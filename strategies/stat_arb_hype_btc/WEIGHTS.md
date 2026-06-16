# Stat Arb HYPE/BTC -- Signal & Weight Reference

## Strategy Overview

Pair-trades HYPE-PERP vs BTC-PERP on Hyperliquid (Hotstuff).
Long HYPE + Short BTC when signals say HYPE is undervalued relative to BTC,
Short HYPE + Long BTC when signals say HYPE is overvalued.

The 12 normalized signals are combined into a single `weighted_sum` (wsum).
If wsum > entry_threshold (+0.30), go LONG HYPE / SHORT BTC.
If wsum < -entry_threshold (-0.30), go SHORT HYPE / LONG BTC.
Exit when |wsum| < exit_threshold (0.10) or protections trigger.

All signals are clipped to [-1, +1] range. Positive = bullish HYPE vs BTC.

---

## Methodology Deep Dive: Why Weights May Underpredict

### Current Pipeline (What We Do)

1. **Raw signals** are computed in different units: z-score (std devs), RSI (0-100), funding (rate), OBI (-1 to 1), basis (bps), etc.
2. **Normalization** maps each to roughly [-1, 1] via a **fixed divisor** per signal:
   - RSI: (50 - rsi) / 30
   - Funding: -funding_diff / 0.001
   - OBI: obi_diff / 0.5
   - MACD: hist / max(|hist| over last 50 bars)
   - etc.
3. **Aggregation**: wsum = sum(sig_i * w_i) + bias, with sum(w_i) = 1.

### Problems (Why Prediction Fails)

**1. Inconsistent effective variance**

Each signal is clipped to [-1, 1] but the **typical in-sample variance** differs a lot. Funding might sit in [-0.2, 0.2] most of the time (raw rates small), so after clip it has low variance. RSI can swing full [-1, 1]. So a weight of 0.20 on funding adds less **movement** to wsum than 0.10 on RSI. Weights are then "scale-contaminated": they mix importance with the arbitrary divisor. So we are not really "weighting importance" we are "weighting (importance x typical range)".

**2. Fixed divisors = regime-blind**

Divisors (0.5, 0.3, 30, 0.001, 50) are constants. In low-vol regimes, the same raw move produces a larger normalized move; in high-vol regimes, normalized signal compresses. So the same raw information is scaled differently across time. Entry threshold 0.30 is then hit more or less often depending on regime, not on signal quality.

**3. No variance normalization before combining**

Proper combo is: put every signal on a **common scale** (e.g. mean 0, std 1 over a lookback), then weight. Then w_i truly means "how much to trust signal i" and the contribution to wsum variance is proportional to w_i. Right now we do not do that, so high-variance signals dominate wsum even with smaller weights.

**4. Double-counting correlated signals**

RSI, StochRSI, MACD, EMA cross are all momentum. We add them with weights 0.06, 0.08, 0.08, 0.05. The **effective** momentum weight is the sum when they agree. So we overweight momentum relative to what we think (single "momentum" block). No decorrelation or PCA.

**5. Z-score is already statistical; others are not**

Z-score is "number of standard deviations". The rest are "level / magic number". We mix statistical units with ad hoc units, so the weighted sum has no clear interpretation (e.g. "wsum = 0.3" does not mean "0.3 sigma expected return").

**6. Weights sum to 1 dilutes good signals**

Forcing sum(w) = 1 spreads weight across 12 signals. If only 3 have IC > 0, we still give the other 9 a total of e.g. 0.50. So the good signals are diluted and wsum is noisier.

### What Would Fix It (Quant Best Practice)

1. **Variance-normalize before aggregate:** For each bar, replace sig_i by its **rolling z-score** over the last N bars (e.g. N=60). Then each signal has mean 0 and std 1 in the window, so weights = relative importance and wsum scale is stable. Optional: SA_SIGNAL_ZSCORE_LOOKBACK=60.
2. **Rolling divisors:** Replace fixed 0.5, 0.3, etc. by rolling std or IQR of the raw signal so normalized signal has consistent scale across regimes.
3. **IC- or risk-parity weights:** Set w_i proportional to max(0, IC_i) and normalize; or equal risk contribution so each signal contributes equally to wsum variance.
4. **Fewer, decorrelated signals:** Combine momentum into one block (e.g. first principal component of RSI, StochRSI, MACD, EMA) and use that + OBI + funding + zscore. Reduces double-counting.
5. **Predict target directly:** Train a small model (e.g. next-bar ratio return in bps) on the 12 raws and use that score for entry/exit instead of ad hoc weighted sum.

The code now supports (1) via optional rolling z-score of the 12 signals before aggregation (SA_SIGNAL_ZSCORE_LOOKBACK).

---

## Protection Parameters (from .env)

| Parameter | Value | Effect |
|-----------|-------|--------|
| SA_LONG_BIAS | 0.05 | Added to wsum every bar. Makes longs easier (+0.25 effective), shorts harder (-0.35 effective) |
| SA_HL_GATE_THRESHOLD | 30.0 | Attenuates zscore when half-life > 30 (no mean-reversion) |
| SA_STOP_LOSS_BPS | 35.0 | Exit if unrealized pair-trade PnL < -35 bps |
| SA_MAX_HOLD_BARS | 8 | Force exit after 8 bars (40 minutes) |
| SA_SIGNAL_ENTRY | 0.30 | wsum must exceed this to enter a trade |
| SA_SIGNAL_EXIT | 0.10 | Exit when |wsum| drops below this |
| SA_SLIPPAGE_BPS | 15.0 | Estimated round-trip slippage per trade |

---

## Signal Definitions

### 1. zscore -- Z-score of HYPE/BTC price ratio

**What it does:** Measures how far the HYPE/BTC ratio is from its rolling mean
in standard deviations. Core mean-reversion signal.

**Direction:** Negative z = ratio below mean = HYPE cheap = LONG HYPE signal (+)

**Code:**
```python
z = compute_zscore(rc, self.cfg.zscore_lookback)  # lookback=60 bars
z_sig = _clip(-z / self.cfg.zscore_entry)          # zscore_entry=2.0
# Gate: attenuate when half-life is too long (market not mean-reverting)
if hl_thresh > 0.0 and hl > hl_thresh:
    z_sig *= max(0.1, hl_thresh / hl)
```

**IC Analysis (21h):** Inconsistent across sessions. Good in mean-reverting regimes,
harmful in trending. The HL gate helps but does not fully fix it.

---

### 2. ratio_rsi -- RSI(14) of HYPE/BTC ratio

**What it does:** Relative Strength Index of the ratio. Classic momentum oscillator.

**Direction:** RSI < 30 = oversold ratio = HYPE cheap = LONG HYPE signal (+)

**Code:**
```python
rsi_val = rsi_series(rc, self.cfg.rsi_period)[-1]  # period=14
sig["ratio_rsi"] = _clip((50.0 - rsi_val) / 30.0)
```

**IC Analysis:** Weak and inconsistent. Often contradicts actual price direction
because it measures ratio momentum, not absolute level.

---

### 3. ratio_stoch_rsi -- Stochastic RSI of ratio

**What it does:** Stochastic oscillator applied to RSI values. More sensitive
than raw RSI, catches overbought/oversold extremes faster.

**Direction:** Low stochRSI = oversold momentum = LONG HYPE signal (+)

**Code:**
```python
rsi_vals = rsi_series(rc, self.cfg.rsi_period)
stoch_val = stoch_of_series(rsi_vals, self.cfg.stoch_rsi_period)  # period=14
sig["ratio_stoch_rsi"] = _clip((50.0 - stoch_val) / 30.0)
```

**IC Analysis:** Consistently positive IC (+0.090 avg). One of the 3 reliable signals.
Works because it fires at extremes where mean-reversion is statistically likely.

---

### 4. ratio_macd -- MACD histogram of ratio

**What it does:** MACD(12,26,9) histogram on the HYPE/BTC ratio. Measures momentum
acceleration -- positive histogram = ratio momentum accelerating up.

**Direction:** Positive histogram = HYPE strengthening = LONG HYPE signal (+)

**Code:**
```python
hist_all = macd_histogram_series(rc, fast=12, slow=26, signal=9)
hist = hist_all[-1]
recent = hist_all[-50:]
max_h = max(abs(h) for h in recent)  # dynamic normalization
sig["ratio_macd"] = _clip(hist / max_h)
```

**IC Analysis:** Mixed. Shows Simpson's Paradox -- looks good in aggregate but
can be misleading within individual sessions. Lagging indicator.

---

### 5. ratio_ema_cross -- EMA fast/slow cross on ratio

**What it does:** Percentage difference between fast EMA(8) and slow EMA(21)
of the ratio. Classic trend-following crossover signal.

**Direction:** Fast above slow = ratio trending up = LONG HYPE signal (+)

**Code:**
```python
ema_f = ema_last(rc, self.cfg.ema_fast)    # fast=8
ema_s = ema_last(rc, self.cfg.ema_slow)    # slow=21
cross_pct = (ema_f - ema_s) / ema_s * 100.0
sig["ratio_ema_cross"] = _clip(cross_pct / 1.0)
```

**IC Analysis:** Mixed. Trend-following signal in a mean-reversion strategy
creates tension. Can generate false signals in choppy markets.

---

### 6. ratio_ema_trend -- EMA slow/long-term trend

**What it does:** Percentage difference between slow EMA(21) and long-term EMA(50)
of the ratio. Measures the broader trend direction.

**Direction:** Slow above long-term = broader uptrend = LONG HYPE signal (+)

**Code:**
```python
ema_t = ema_last(rc, self.cfg.ema_trend)   # trend=50
trend_pct = (ema_s - ema_t) / ema_t * 100.0
sig["ratio_ema_trend"] = _clip(trend_pct / 2.0)
```

**IC Analysis:** Very slow-moving. Low signal-to-noise for 5-minute bar trading.
Useful as a regime filter but poor as a standalone alpha source.

---

### 7. ratio_vwap_dev -- VWAP deviation of ratio

**What it does:** How far the current ratio is from its Volume-Weighted Average Price.
Measures deviation from the fair value anchored by volume.

**Direction:** Below VWAP = HYPE cheap relative to volume-fair = LONG HYPE (+)

**Code:**
```python
vwap = st.vwap_cum_rpv / st.vwap_cum_vol  # cumulative ratio*vol / cumvol
vwap_dev = (rc[-1] - vwap) / vwap * 100.0
sig["ratio_vwap_dev"] = _clip(-vwap_dev / 0.5)  # note: inverted
```

**IC Analysis:** NEGATIVE IC in 2 of 3 sessions. The inversion causes it to
fight the trend. When ratio trends away from VWAP, this signal keeps calling
for mean-reversion that does not arrive.

---

### 8. obi_diff -- Order Book Imbalance differential

**What it does:** Compares bid vs ask depth imbalance on HYPE vs BTC orderbooks.
Uses top 10 levels. Pure microstructure signal.

**Direction:** HYPE bids stronger than BTC bids = buying pressure = LONG HYPE (+)

**Code:**
```python
hype_obi = compute_obi(st.hype_ob.bids, st.hype_ob.asks, 10)
btc_obi = compute_obi(st.btc_ob.bids, st.btc_ob.asks, 10)
obi_diff = hype_obi - btc_obi
sig["obi_diff"] = _clip(obi_diff / 0.5)
```

**IC Analysis:** Consistently positive IC (+0.106 avg). Second most reliable signal.
Real-time orderbook data has genuine predictive power for short-term direction.

---

### 9. cvd_diff -- Cumulative Volume Delta rate differential

**What it does:** Compares normalized buy/sell aggression between HYPE and BTC
within each 5-min bar. CVD = buy_volume - sell_volume, normalized by total volume.

**Direction:** HYPE net buying > BTC net buying = LONG HYPE (+)

**Code:**
```python
hype_cvd_n = st.hype_cvd_bar / st.hype_vol_bar  # normalized to [-1, 1]
btc_cvd_n = st.btc_cvd_bar / st.btc_vol_bar
cvd_diff = hype_cvd_n - btc_cvd_n
sig["cvd_diff"] = _clip(cvd_diff / 0.3)
```

**IC Analysis:** NEGATIVE IC in all 3 sessions tested. CVD on Hyperliquid appears
noisy -- taker flow does not predict next-bar direction reliably for pair trades.
Needs implementation fix (better aggTrade normalization).

---

### 10. depth_skew_diff -- Depth skew differential

**What it does:** Measures asymmetry of orderbook depth around mid-price
(how much more depth is on bid side vs ask side) for HYPE vs BTC.

**Direction:** HYPE depth skewed to bids (more support) = LONG HYPE (+)

**Code:**
```python
hype_skew = compute_depth_skew(st.hype_ob.bids, st.hype_ob.asks, hype_mid)
btc_skew = compute_depth_skew(st.btc_ob.bids, st.btc_ob.asks, btc_mid)
skew_diff = hype_skew - btc_skew
sig["depth_skew_diff"] = _clip(skew_diff / 0.5)
```

**IC Analysis:** Mildly positive. Captures institutional order placement patterns.
Less predictive than OBI but adds diversification.

---

### 11. basis_diff -- Cross-exchange basis differential

**What it does:** Compares Hyperliquid vs Binance price premium for both HYPE
and BTC. Positive basis = Hyperliquid price higher than Binance (taker demand).

**Direction:** HYPE premium > BTC premium = HYPE overpriced = SHORT HYPE (inverted)

**Code:**
```python
hype_basis = (st.hype_hs_mid - st.hype_bn_price) / st.hype_bn_price * 10000  # in bps
btc_basis = (st.btc_hs_mid - st.btc_bn_price) / st.btc_bn_price * 10000
basis_diff = hype_basis - btc_basis
sig["basis_diff"] = _clip(-basis_diff / 50.0)  # inverted: high premium = short
```

**IC Analysis:** Weak and noisy. Cross-exchange basis can persist for long periods
(structural difference in fee structure) rather than mean-reverting quickly.

---

### 12. funding_diff -- Funding rate differential

**What it does:** Compares perpetual funding rates between HYPE and BTC.
High funding = crowded positioning on that side.

**Direction:** HYPE funding > BTC funding = crowded HYPE longs = SHORT HYPE (inverted)

**Code:**
```python
funding_diff = st.hype_funding - st.btc_funding
sig["funding_diff"] = _clip(-funding_diff / 0.001)  # inverted
```

**IC Analysis:** BEST signal. IC = +0.262 average. Consistently positive across
all sessions. Funding rate carries real information about positioning extremes
and mean-reverts reliably over 1-3 bar horizons.

---

## Weight Configurations

### PREVIOUS (Main .env -- currently deployed)

Diversified weights across all 12 signals with +0.05 long bias.
All protections active (stop-loss, max-hold, hl-gate).

| Signal | Weight | Rationale |
|--------|--------|-----------|
| zscore | 0.08 | Core mean-reversion, gated by half-life |
| ratio_rsi | 0.06 | Supplementary momentum oscillator |
| ratio_stoch_rsi | 0.08 | More responsive RSI variant |
| ratio_macd | 0.08 | Momentum acceleration |
| ratio_ema_cross | 0.05 | Trend-following crossover |
| ratio_ema_trend | 0.04 | Broader trend context |
| ratio_vwap_dev | 0.07 | VWAP mean-reversion |
| obi_diff | 0.16 | Orderbook microstructure (high IC) |
| cvd_diff | 0.13 | Volume delta (needs fix -- negative IC) |
| depth_skew_diff | 0.12 | Orderbook asymmetry |
| basis_diff | 0.07 | Cross-exchange premium |
| funding_diff | 0.06 | Funding rate (underweighted vs IC) |
| **Long Bias** | **+0.05** | Blocks weak shorts, favors structural HYPE demand |
| **TOTAL** | **1.00** | |

---

### D BALANCED (scenario paper-trade, no bias, no protections)

Zeros out noisy/negative-IC signals. Concentrates on momentum + microstructure.

| Signal | Weight | Rationale |
|--------|--------|-----------|
| zscore | 0.00 | Zeroed -- unreliable without mean-reversion |
| ratio_rsi | 0.00 | Zeroed -- weak IC |
| ratio_stoch_rsi | 0.10 | Kept -- consistent positive IC |
| ratio_macd | 0.15 | Momentum acceleration |
| ratio_ema_cross | 0.20 | Strongest trend signal |
| ratio_ema_trend | 0.12 | Broader trend |
| ratio_vwap_dev | 0.00 | Zeroed -- negative IC |
| obi_diff | 0.13 | Orderbook imbalance |
| cvd_diff | 0.00 | Zeroed -- negative IC |
| depth_skew_diff | 0.10 | Orderbook skew |
| basis_diff | 0.00 | Zeroed -- weak IC |
| funding_diff | 0.20 | Strong IC, well weighted |
| **Long Bias** | **0.00** | None |
| **TOTAL** | **1.00** | |

---

### E FLIP (scenario paper-trade, no bias, no protections)

Contrarian: negative-IC signals get NEGATIVE weights (bet against them).
If a signal consistently predicts wrong, inverting it should predict right.

| Signal | Weight | Rationale |
|--------|--------|-----------|
| zscore | -0.06 | Inverted -- unreliable z-score becomes contrarian |
| ratio_rsi | -0.04 | Inverted -- weak RSI flipped |
| ratio_stoch_rsi | 0.06 | Kept positive (good IC) |
| ratio_macd | 0.10 | Momentum |
| ratio_ema_cross | 0.18 | Trend following |
| ratio_ema_trend | 0.12 | Broader trend |
| ratio_vwap_dev | -0.08 | Inverted -- VWAP deviation flipped |
| obi_diff | 0.08 | Orderbook |
| cvd_diff | -0.06 | Inverted -- CVD flipped (negative IC) |
| depth_skew_diff | 0.06 | Orderbook skew |
| basis_diff | -0.04 | Inverted -- basis flipped |
| funding_diff | 0.18 | Strong IC |
| **Long Bias** | **0.00** | None |

---

### R REGIME (scenario paper-trade, MIXED regime weights, no bias, no protections)

Moderate approach: all signals kept but noisy ones sharply downweighted.
Uses the MIXED regime set (half-life between 15-40 bars).

| Signal | Weight | Rationale |
|--------|--------|-----------|
| zscore | 0.04 | Reduced from 0.08 -- less reliance on mean-reversion |
| ratio_rsi | 0.02 | Minimal |
| ratio_stoch_rsi | 0.09 | Moderate |
| ratio_macd | 0.13 | Momentum |
| ratio_ema_cross | 0.16 | Strong trend weight |
| ratio_ema_trend | 0.10 | Trend context |
| ratio_vwap_dev | 0.02 | Minimal -- negative IC awareness |
| obi_diff | 0.12 | Microstructure |
| cvd_diff | 0.02 | Minimal -- negative IC awareness |
| depth_skew_diff | 0.09 | Moderate |
| basis_diff | 0.02 | Minimal |
| funding_diff | 0.17 | High -- best IC signal |
| **Long Bias** | **0.00** | None |
| **TOTAL** | **1.00** | |

---

### T1 TIER1 (scenario paper-trade, no bias, no protections)

Maximum concentration on the 3 signals with consistently positive IC.
Most aggressive signal concentration -- only Funding, OBI, StochRSI get weight.

| Signal | Weight | Rationale |
|--------|--------|-----------|
| zscore | 0.00 | Zeroed |
| ratio_rsi | 0.00 | Zeroed |
| ratio_stoch_rsi | 0.20 | 3rd best IC (+0.090) |
| ratio_macd | 0.00 | Zeroed |
| ratio_ema_cross | 0.00 | Zeroed |
| ratio_ema_trend | 0.00 | Zeroed |
| ratio_vwap_dev | 0.00 | Zeroed |
| obi_diff | 0.30 | 2nd best IC (+0.106) |
| cvd_diff | 0.00 | Zeroed |
| depth_skew_diff | 0.05 | Small tiebreaker |
| basis_diff | 0.00 | Zeroed |
| funding_diff | 0.45 | BEST IC (+0.262) |
| **Long Bias** | **0.00** | None |
| **TOTAL** | **1.00** | |

---

## Performance Comparison (268 bars, Feb 16 18:45 - Feb 17 17:00, PAIR-TRADE PnL)

### By Slippage Level

| Config | 0bp slip | 5bp slip | 10bp slip | 15bp slip | Trades | Bias |
|--------|:--------:|:--------:|:---------:|:---------:|:------:|:----:|
| PREVIOUS | +67.6 | -7.4 | -82.4 | -157.4 | 15 | 0.05 |
| D_BALANCED | +179.8 | +119.8 | +59.8 | -0.2 | 12 | 0.00 |
| **E_FLIP** | **+331.2** | **+241.2** | **+151.2** | **+61.2** | 18 | 0.00 |
| T1_TIER1 | +184.6 | +19.6 | -145.4 | -310.4 | 33 | 0.00 |
| PREV NoBias | -101.8 | -191.8 | -281.8 | -371.8 | 18 | 0.00 |

### Night (18:00-06:00 UTC) vs Day (06:00-18:00 UTC) at 0bp slippage

| Config | Night | Day | NET |
|--------|------:|----:|----:|
| PREVIOUS | -2.4 | +69.9 | +67.6 |
| D_BALANCED | +32.6 | +147.2 | +179.8 |
| **E_FLIP** | **+226.7** | **+104.5** | **+331.2** |
| T1_TIER1 | -72.1 | +256.7 | +184.6 |

### Outlier Analysis (at 0bp slippage, threshold 30 bps)

| Config | NET | excl outliers | Outlier-dependent? |
|--------|----:|:-------------:|:------------------:|
| PREVIOUS | +67.6 | +68.5 | No |
| D_BALANCED | +179.8 | +69.7 | Partially (1 big +92 win) |
| **E_FLIP** | **+331.2** | **+17.8** | Yes (5 big wins, 5 big losses) |
| T1_TIER1 | +184.6 | +129.0 | Partially |

---

## Exit Criteria Behavior

### How Each Exit Type Works

1. **FADE** -- |wsum| drops below 0.10. Signal lost conviction. Most common exit.
   Speed: fast (1-4 bars typically). Locks in small wins or cuts losses early.

2. **FLIP** -- wsum crosses zero (signal reverses direction).
   Speed: fast (1-2 bars). Aggressive cut when market regime changes.

3. **STOP** -- Unrealized pair-trade PnL < -35 bps.
   Speed: immediate once threshold hit. Hard risk management.

4. **MAXHOLD** -- Position held for 8 bars (40 minutes).
   Speed: slow by definition. Forces exit on stale trades.

### Exit Speed by Config (average bars to exit)

| Config | Avg bars (win) | Avg bars (loss) | Most common exit |
|--------|:--------------:|:---------------:|:----------------:|
| PREVIOUS | 2.0 | 1.7 | FADE (fast in/out) |
| D_BALANCED | 5.4 | 3.7 | FADE (holds longer) |
| E_FLIP | 5.9 | 4.7 | FADE + MAXHOLD (holds longest) |
| T1_TIER1 | 3.4 | 4.0 | FADE + FLIP |

### Profit Booking vs Loss Cutting

- **PREVIOUS**: Cuts both fast. Small wins (+9 avg), small losses (-20 avg). Conservative.
- **D_BALANCED**: Holds winners longer (5.4 bars). Bigger avg win (+26) but also bigger losses (-19).
- **E_FLIP**: Holds longest. Lets winners run to +55 avg. But losses also run to -29 avg.
  High conviction -- trusts the signal and waits for MAXHOLD or FADE.
- **T1_TIER1**: Many trades (33). Moderate hold times. FLIP exits work well (+19 bps from 7 flips).
  Active trader that repositions frequently.

---

## Key Findings

1. **E_FLIP is the most robust config** -- only one profitable at all slippage levels.
   The contrarian approach (negative weights on bad signals) works because it
   converts consistently wrong signals into consistently right ones.

2. **D_BALANCED is second best** -- breaks even at 15bp slippage, profitable at lower.
   Clean signal set with no negative-IC signals dragging it down.

3. **PREVIOUS depends heavily on long bias** -- without bias it is deeply negative.
   The bias blocks bad shorts (which is valuable) but masks poor signal quality.

4. **T1_TIER1 over-trades** -- 33 trades in 22h means high slippage cost.
   Signals are good but the concentrated weights trigger too many entries.

5. **CVD_DIFF needs fixing** -- negative IC in all sessions. Currently hurting PREVIOUS
   (weight 0.13) and being correctly zeroed in D_BALANCED and T1_TIER1.

6. **FUNDING_DIFF is the best signal** -- IC=+0.262, consistent across all sessions.
   Underweighted in PREVIOUS (0.06). All newer configs give it 0.18-0.45.

---

## Simulation Bug Note (2026-02-17)

An earlier simulation showed PREVIOUS at +56.5 bps with 62% WR. This used
HYPE-only PnL (single leg, ignoring BTC position) and zero slippage.
Correct pair-trade PnL with realistic slippage shows lower returns for all configs.
All numbers in this document use the corrected pair-trade PnL model.
