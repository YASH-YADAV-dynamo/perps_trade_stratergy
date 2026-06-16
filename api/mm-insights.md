# Market Making Insights for Hotstuff Negative-Rebate Mode

This note documents the exact maker-first strategy that should be shared with other MM bots for Hotstuff. It is based on the current `dxd_api/maker` and `dxd_api/taker` code, plus the Hotstuff fee model where perp maker fees are always negative at the normal tier.

Assumed target environment from the strategy request:

- Maker rebate target: about `0.2 bps` negative fee conservatively. Hotstuff docs currently show standard perps maker fee around `-0.002%` (`-0.2 bps`) and taker fee around `0.025%` (`2.5 bps`), with higher maker rebate tiers possible.
- Quote mode: `1` level.
- TP mode: fixed take profit around `1 bps`.
- Exposure mode: size from `target_exposure_x`, not fixed notional unless explicitly configured.

The main conclusion: Hotstuff is a maker-rebate venue first. The working strategy is not a taker spread bot. It is a one-level post-only maker that quotes from an external fair value, keeps inventory tiny, flips into close-only TP mode around `1 bps`, and only uses taker/IOC when inventory risk is worse than paying the fee.

## Shareable Strategy In One Page

This is the strategy worth sharing:

```text
Run one-level post-only maker quotes around Binance-adjusted fair.
Quote both sides only while inventory is small.
When one side fills, stop increasing that side early through inventory skew.
When mark-to-fair profit reaches about 1 bps, switch to close-only mode.
Try to close passively first, because maker close earns rebate too.
Use IOC only for stale inventory, guard halt, or profit greater than taker fee.
Measure 1s/5s markout. If fills are toxic, widen or stop that symbol.
```

Why this works on Hotstuff:

- Maker fills are paid a rebate instead of charged a fee.
- Post-only orders are cancelled if they would immediately take liquidity, so accidental taker fees are avoided.
- A `1 bps` gross TP can be enough only when most exits are maker exits or close near fair.
- The edge dies if inventory is held too long, so TP mode and early skew matter more than raw quote count.
- The bot should optimize for clean round trips, not maximum volume.

The simplest profitable shape is:

```text
maker entry rebate
+ maker or near-maker exit
+ 0.5 to 1.5 bps favorable fair move
- adverse markout
- inventory carry
= edge
```

The losing shape is:

```text
maker entry rebate
+ tiny TP target
- taker close fee
- stale inventory slippage
= churn loss
```

## Exact Working Maker Profile

Use this as the baseline profile when the goal is Hotstuff negative maker fee farming with `1` level and `1 bps` TP:

```json
{
  "levels": 1,
  "level_spacing_bps": 1.0,
  "level_size_scale": 1.0,
  "min_spread_bps": 0.1,
  "spread_vol_mult": 1.5,
  "close_spread_bps": 0.1,
  "fixed_tp_enabled": true,
  "fixed_tp_bps": 1.0,
  "taker_fee_bps": 2.5,
  "target_exposure_x": 5.0,
  "inventory_skew_bps": 3.0,
  "inv_skew_start_pct": 20,
  "inv_skip_open_pct": 45,
  "guard_inventory_decay_s": 180,
  "guard_adverse_rate_threshold": 0.55,
  "guard_adverse_rate_widen": 2.0,
  "toxic_threshold": 0.7,
  "requote_interval_s": 0.2,
  "requote_threshold_bps": 0.3,
  "update_throttle_ms": 150,
  "use_alpha": true,
  "alpha_bps": 4.0,
  "noise_bps": 0.0
}
```

For the tested Hotstuff maker profile, starting around `target_exposure_x = 5.0` is fine. When markouts stay clean and close-mode exits are fast, scaling toward `10.0` can work. The important limit is not the raw leverage number; it is whether the symbol can flatten inventory without repeated taker exits or tier-3 close-mode stalls.

The safest version for sharing publicly:

```json
{
  "levels": 1,
  "min_spread_bps": 0.2,
  "spread_vol_mult": 1.8,
  "fixed_tp_enabled": true,
  "fixed_tp_bps": 1.0,
  "target_exposure_x": 5.0,
  "inv_skew_start_pct": 20,
  "inv_skip_open_pct": 45,
  "guard_inventory_decay_s": 180,
  "guard_adverse_rate_threshold": 0.50,
  "taker_fee_bps": 2.5
}
```

The more aggressive version after live validation:

```json
{
  "levels": 1,
  "min_spread_bps": 0.1,
  "spread_vol_mult": 1.3,
  "fixed_tp_enabled": true,
  "fixed_tp_bps": 1.0,
  "target_exposure_x": 10.0,
  "inv_skew_start_pct": 15,
  "inv_skip_open_pct": 40,
  "guard_inventory_decay_s": 120,
  "guard_adverse_rate_threshold": 0.55,
  "taker_fee_bps": 2.5
}
```

Do not use the aggressive version on a symbol until the conservative version shows:

- `5s` average markout near `0 bps` or positive.
- adverse fill rate below `55%`.
- close-mode duration usually below `30s`.
- no frequent guard halts.
- inventory tier 3 is rare.

## Exact Runtime Loop

The maker loop should behave like this:

```text
1. Read Binance bookTicker, aggTrade, and 1m kline.
2. Read Hotstuff orderbook.
3. Validate Hotstuff BBO against Binance.
4. Build fair = Binance mid + median Hotstuff/Binance basis.
5. Compute vol, OBI, trade imbalance, regime, and markout feedback.
6. If flat or low inventory, place one post-only bid and one post-only ask.
7. If inventory grows, skew center against the heavy side.
8. If inventory is too high, stop opening the heavy side.
9. If profit reaches fixed TP, quote only the reducing side.
10. If close mode gets stale, tighten close spread.
11. If stale too long or guard halts, use reduce-only IOC to flatten.
12. Emit metrics and judge the symbol by net PnL plus markout, not volume.
```

For Hotstuff specifically, the highest priority rules are:

- Every opening quote should be post-only.
- Every close quote should prefer post-only unless risk is stale.
- IOC is a risk tool, not the base profit engine.
- If a post-only order gets rejected for crossing, that is fine. Missing one fill is better than paying taker fee by accident.
- If Binance and Hotstuff disagree too much, do not quote.

## Exact Quote Placement

The working quote should be built from fair, not from Hotstuff mid alone:

```text
fair = binance_mid + median(hotstuff_mid - binance_mid)
```

Then the center is shifted:

```text
center = fair + alpha_shift + inventory_skew_shift
```

The spread is:

```text
dynamic_spread_bps = max(min_spread_bps, vol_bps * spread_vol_mult)
half_spread_price = fair * dynamic_spread_bps / 10000 / 2
```

For `levels = 1`:

```text
bid = round_down(center - half_spread_price, tick)
ask = round_up(center + half_spread_price, tick)
```

Then clamp against Hotstuff BBO:

```text
if bid >= hotstuff_ask:
    bid = hotstuff_ask - tick

if ask <= hotstuff_bid:
    ask = hotstuff_bid + tick
```

The goal is to be near the touch without accidentally crossing. Hotstuff post-only cancellation protects the account, but repeated rejections waste action budget and cause missed quote time.

## Exact Inventory Logic

This bot is not using Avellaneda-Stoikov to maintain inventory. There is no reservation-price formula, no gamma risk-aversion parameter, no model-derived optimal spread, and no stochastic control loop. Inventory is managed with a simple deterministic system:

```text
1. Convert max inventory from USD into base units using current fair price.
2. Compute inventory utilization = abs(current_base_position) / max_inventory_base.
3. Put inventory into one of four tiers.
4. In tier 1, shift the quote center linearly away from the heavy side.
5. In tier 2, stop quoting the side that would make inventory worse.
6. In tier 3, enter close-only mode and quote only the reducing side.
```

That is the whole inventory engine. It is intentionally simple because the edge on Hotstuff comes from negative maker fees plus clean passive recycling, not from solving a theoretical optimal-control model.

The core state is:

```text
max_inventory_base = max_inventory_usd / fair_price
inventory_utilization = abs(position_base) / max_inventory_base
inventory_skew_factor = position_base / max_inventory_base
```

`inventory_skew_factor` is signed:

```text
positive = long inventory
negative = short inventory
zero = flat
```

Then the quoter applies a linear center shift:

```text
skew_shift = -inventory_skew_factor * inventory_skew_bps / 10000 * fair
center = fair + alpha_shift + noise_shift + skew_shift
```

This is how skew is introduced:

- If inventory is long, `inventory_skew_factor` is positive, so `skew_shift` is negative. The quote center moves down. The ask becomes cheaper and easier to fill, while the bid becomes less attractive. That encourages sells and discourages more buys.
- If inventory is short, `inventory_skew_factor` is negative, so `skew_shift` is positive. The quote center moves up. The bid becomes more aggressive and easier to fill, while the ask becomes less attractive. That encourages buys and discourages more sells.
- If inventory is flat, `inventory_skew_factor` is near zero, so no inventory skew is applied.

Example with `inventory_skew_bps = 3.0`:

```text
fair = 100.00
max_inventory_base = 10
current_inventory = +5 long
inventory_skew_factor = +0.5
skew_shift = -0.5 * 3 / 10000 * 100 = -0.015
center = fair - 0.015 = 99.985
```

That `1.5 bps` center move makes the ask easier to hit and makes the bid less competitive. It is not trying to predict price. It is trying to make the next fill more likely to reduce inventory.

Working thresholds:

```text
0% to 20% utilization: quote both sides normally.
20% to 45% utilization: keep both sides, but skew toward flattening.
45% to 100% utilization: stop opening the heavy side.
100% or close threshold hit: close-only mode.
```

For long inventory:

```text
tier 0: allow_bids = true,  allow_asks = true,  no meaningful skew
tier 1: allow_bids = true,  allow_asks = true,  center shifts lower
tier 2: allow_bids = false, allow_asks = true,  only reduce or hold
tier 3: allow_bids = false, allow_asks = true,  close-only with tight spread
```

For short inventory:

```text
tier 0: allow_bids = true, allow_asks = true,  no meaningful skew
tier 1: allow_bids = true, allow_asks = true,  center shifts higher
tier 2: allow_bids = true, allow_asks = false, only reduce or hold
tier 3: allow_bids = true, allow_asks = false, close-only with tight spread
```

The important detail is that tier 1 still quotes both sides. It does not panic. It just changes the fill probabilities. Tier 2 is where it stops adding to the bad side. Tier 3 is where it stops being a market maker on that symbol and becomes a passive closer.

This is why `inv_skew_start_pct` and `inv_skip_open_pct` are critical:

```text
inv_skew_start_pct = when center skew begins
inv_skip_open_pct = when the opening side is disabled
```

Recommended Hotstuff maker values:

```text
inv_skew_start_pct = 15 to 25
inv_skip_open_pct = 40 to 50
inventory_skew_bps = 2 to 5
```

For a very liquid, clean symbol, use softer skew:

```text
inv_skew_start_pct = 25
inv_skip_open_pct = 55
inventory_skew_bps = 2
```

For toxic or thin symbols, use harder skew:

```text
inv_skew_start_pct = 15
inv_skip_open_pct = 35
inventory_skew_bps = 5
```

The key is not to wait for maximum inventory. On a negative maker venue, the temptation is to keep quoting both sides because every fill pays rebate. That is wrong when one-sided flow appears. The bot should avoid earning tiny rebates while building a position that costs much more to exit.

## Exact TP Logic

The working TP logic is:

```text
if fixed_tp_enabled and position has avg_entry:
    profit_bps = mark_to_fair_profit_bps
    if profit_bps >= fixed_tp_bps:
        switch to close-only mode
```

Close-only mode means:

- if long, quote only asks;
- if short, quote only bids;
- disable alpha and noise;
- use tight `close_spread_bps`;
- decay close spread over time if TP is not filled.

For `1 bps` TP, the bot should try hard to exit as maker. A taker exit at `2.5 bps` fee can destroy the entire TP unless the mark-to-fair profit has grown above the taker cost.

Good IOC rule:

```text
only IOC close if profit_bps > taker_fee_bps + slippage_buffer_bps
```

Practical buffer:

```text
taker_fee_bps = 2.5
slippage_buffer_bps = 0.5 to 1.0
minimum_ioc_profit = 3.0 to 3.5 bps
```

So the shared strategy is:

```text
1 bps TP triggers close-only maker mode.
3 bps or higher profit allows emergency IOC close.
negative or flat profit never uses IOC unless guard risk says flatten now.
```

## Immediate Bugs And Risks

### 1. `1 bps` TP is too small for the taker worker

The taker worker opens with IOC and closes with reduce-only IOC. That means it removes liquidity on both legs in the normal case. In code, realized PnL subtracts:

```text
fees = (open_notional + close_notional) * 0.00025
```

`0.00025` is `2.5 bps` per side. A round trip is roughly `5 bps` before spread and slippage. A `1 bps` TP therefore loses money unless the entry captured a much larger stale spread and the exit is unusually favorable.

Recommendation: keep `TAKER_TAKE_PROFIT_BPS` above full round-trip taker cost plus slippage, or use taker only as emergency flattening / stale-spread capture. Do not treat taker as a rebate farming strategy.

### 2. Maker TP uses `taker_fee_bps` as a gate for late IOC close

Maker fixed TP enters close mode when mark-to-fair profit reaches `fixed_tp_bps`. After a 30 second decay, it may use IOC close only if `profit_bps > taker_fee_bps`. That protects against giving back all profit to taker fee, but if the configured `taker_fee_bps` is stale or too low, the bot can convert good maker inventory into bad taker exits.

Recommendation: set `MM_TAKER_FEE_BPS` to the real Hotstuff taker fee tier. In standard Hotstuff perps, use about `2.5`, not `0.1`.

### 3. Negative maker fee does not make every passive fill good

A `0.1 bps` to `0.2 bps` maker rebate is small compared with adverse selection. If average 5 second markout is `-1 bps`, the rebate cannot save the quote. The current maker bot has `Reflect` and `Guard` for this reason: it tracks adverse fill rate and widens or halts when fills become toxic.

Recommendation: optimize on net markout plus rebate, not fill count or volume alone.

## Current Maker Strategy

The maker worker is the stronger base for Hotstuff rebate mode. It is a multi-symbol, post-only GTC strategy with:

- Fair price from Binance mid plus a rolling median Hotstuff/Binance basis.
- Hotstuff book validation against Binance to reject bad or stale levels.
- Vol-adaptive spread: `max(min_spread_bps, vol_bps * spread_vol_mult)`.
- Inventory management is not Stoikov. It uses four deterministic inventory tiers plus a linear center skew from `position_base / max_inventory_base`.
- Inventory skew: quote center moves away from the side that increases current inventory, using `-inventory_skew_factor * inventory_skew_bps`.
- Alpha shift from OBI, bar portion, trade imbalance, CVD divergence, RSI divergence, SuperTrend, ADX, pivots, and Hotstuff top-book OBI.
- Post-only order placement through `po=True`.
- Diff-based order management to avoid cancel/place churn when quote prices and sizes did not change.
- Graduated inventory tiers: normal, skew, skip opening side, close-only.
- Fixed TP mode that converts inventory into close-only mode when mark-to-fair profit reaches `fixed_tp_bps`.
- Reflect/Guard feedback from markouts, adverse fill rate, loss streaks, drawdown, stale inventory, and toxic flow.

For a negative maker fee environment, this is the right architecture: earn rebate only when the fill is not toxic, widen when markouts get bad, and use TP close mode to avoid carrying inventory too long.

## Best Maker Setup For Always-Negative Maker, `1` Level, `1 bps` TP

Use `1` level when the objective is clean, low-inventory rebate/TP cycling. Five levels can farm more volume, but it also stacks inventory during a directional move. In a small rebate environment, the outer levels often add inventory risk faster than they add edge.

Suggested starting config:

```json
{
  "levels": 1,
  "level_spacing_bps": 1.0,
  "level_size_scale": 1.0,
  "min_spread_bps": 0.1,
  "spread_vol_mult": 1.5,
  "close_spread_bps": 0.1,
  "fixed_tp_enabled": true,
  "fixed_tp_bps": 1.0,
  "taker_fee_bps": 2.5,
  "target_exposure_x": 5.0,
  "inventory_skew_bps": 3.0,
  "inv_skew_start_pct": 20,
  "inv_skip_open_pct": 45,
  "guard_inventory_decay_s": 180,
  "guard_adverse_rate_threshold": 0.55,
  "guard_adverse_rate_widen": 2.0,
  "toxic_threshold": 0.7,
  "requote_interval_s": 0.2,
  "requote_threshold_bps": 0.3,
  "update_throttle_ms": 150
}
```

For larger accounts, raise `target_exposure_x` slowly. The sizing formula for maker auto-size is:

```text
order_size_per_level = (account_equity / symbol_count * target_exposure_x) / (2 * sum(level_size_scale ** i))
```

With `levels = 1`, this becomes:

```text
order_size_per_side = account_equity / symbol_count * target_exposure_x / 2
```

That is simple and predictable. Example: with `$100` equity, one symbol, and `target_exposure_x = 5`, the bot quotes about `$250` bid and `$250` ask. With `target_exposure_x = 10`, it quotes about `$500` per side. This is why the inventory tier rules and close-only mode matter: the exposure is intentionally aggressive, so the bot must skew early and stop adding to the heavy side quickly.

## Maker Logic Other Bots Should Copy

### 1. Quote from fair value, not raw Hotstuff mid

Hotstuff top-of-book can be thin or stale. The maker bot uses Binance as an external reference and learns the median Hotstuff/Binance basis. That is better than blindly quoting around Hotstuff mid.

Good rule:

```text
fair = binance_mid + median(hotstuff_mid - binance_mid)
```

Then clamp fair to a max basis band versus Binance.

### 2. Make rebate conditional on markout quality

The maker rebate is only real edge if post-fill markout is not worse than the rebate plus spread capture.

Track:

- `1s` markout for immediate pickoff.
- `5s` markout for toxic flow.
- `30s` and `60s` markout for inventory carry quality.
- adverse fill rate over the last 50 fills.

Operational rule:

```text
if average_5s_markout_bps + maker_rebate_bps < 0:
    widen or stop quoting that side
```

The current `Reflect` and `Guard` modules already implement the required feedback loop.

### 3. One level is better for TP farming than a deep ladder

Deep ladders are useful when the objective is wick capture, but the requested mode is `1 bps` TP. For that mode, one level keeps the inventory small and lets the bot recycle risk. If you add more levels, outer orders should be much wider and smaller, not simply larger.

Use laddering only when:

- the symbol has reliable mean reversion,
- markouts stay positive after fills,
- close-mode fill rate is high,
- inventory does not frequently hit tier 3.

### 4. Inventory skew must start early

Do not wait until max inventory to react. The current maker starts skewing at `inv_skew_start_pct` and stops opening the heavy side around `inv_skip_open_pct`. For negative rebate mode, start skew earlier because the rebate is small.

Recommended:

```text
inv_skew_start_pct = 20 to 30
inv_skip_open_pct  = 45 to 60
```

If inventory is long, the bot should make asks easier to fill and bids harder to fill. If inventory is short, it should make bids easier to fill and asks harder to fill.

### 5. TP close mode should stay mostly maker-first

The maker bot correctly enters close-only mode when profit reaches `fixed_tp_bps`. That is good because it turns inventory into an exit problem instead of continuing to open both sides.

Best behavior:

- First try close-only post-only quotes near fair.
- Tighten `close_spread_bps` over time.
- Use IOC only after enough time has passed or if inventory is stale.
- Never use IOC close if expected profit does not exceed taker fee plus slippage.

For `1 bps` TP, IOC close is usually not profitable at standard taker fees. The TP is mainly useful when the close can also be maker or when the fair price keeps moving favorably.

## Taker Strategy Role

The taker worker is not a classic market maker. It is a single-symbol IOC spread capture bot:

- Waits for Hotstuff spread to exceed `max(min_spread_usd, min_spread_bps)`.
- Opens long at ask or short at bid with IOC.
- Alternates direction unless `market_bias` is set.
- Closes with reduce-only IOC on TP, timeout, loss, or loss cap.
- Sizes from fixed `order_size_usd`, or `equity * target_exposure_x`, capped at `30%` of leveraged buying power.
- Limits size to current top-of-book bid/ask size.
- Filters Hotstuff book levels against Binance by max deviation.

This can work only when the visible spread is unusually wide and actually executable. It should be thought of as opportunistic spread capture or emergency flattening, not rebate farming.

## Best Taker Setup

For standard Hotstuff fees, taker TP must be much larger than maker TP.

Suggested conservative config:

```json
{
  "min_spread_bps": 6.0,
  "take_profit_bps": 6.0,
  "close_bps": 1.0,
  "close_timeout_ms": 500,
  "target_exposure_x": 1.0,
  "cooldown_s": 0.10,
  "max_loss_usd": 2.0,
  "order_expiry_ms": 5000,
  "market_bias": 0.0
}
```

If the goal is pure stale-spread capture, `min_spread_bps` should be at least:

```text
open_taker_fee_bps + close_taker_fee_bps + expected_slippage_bps + safety_margin_bps
```

At `2.5 bps` taker fee each side and even `0.5 bps` slippage, a realistic floor is around `6 bps` to `8 bps`. Anything below that is mostly volume churn unless the close leg is expected to be maker, which this taker worker does not do.

## Maker Versus Taker Decision

Use maker when:

- Hotstuff maker fee is negative or near zero.
- You can keep adverse markouts near flat or positive.
- The symbol has enough two-sided flow.
- You want repeatable `1 bps` TP inventory recycling.
- You can tolerate partial fills and passive queue risk.

Use taker when:

- The book is stale or unusually wide.
- You need immediate flattening.
- The visible spread is larger than full taker round-trip cost.
- You have a strong external signal and waiting in queue is worse than paying taker fees.

Do not use taker when:

- The target profit is `1 bps`.
- The spread is only slightly wide.
- The Hotstuff/Binance basis is unstable.
- Top-of-book size is thin and likely to disappear.

## Symbol Selection

For maker mode, prefer symbols with:

- tight Binance reference quality,
- frequent but not one-directional fills,
- stable Hotstuff/Binance basis,
- positive or near-flat `5s` markouts,
- low close-mode inventory duration,
- enough min-notional-friendly liquidity.

For taker mode, prefer symbols with:

- occasional wide Hotstuff spreads,
- enough top-of-book size for the configured order,
- reliable Binance basis filter,
- fast mean reversion after spread dislocation.

Avoid symbols where the bot frequently logs guard halts, stale inventory, toxic flow, or negative markouts. Those are not rebate farms; they are adverse selection traps.

## Exact Symbol Scoring

A shareable MM bot should not run every listed market with the same size. Score each symbol from live metrics and only increase exposure on symbols that behave cleanly.

Use this simple score:

```text
symbol_score =
    2.0 * clamp(avg_markout_5s_bps, -2, 2)
  + 1.0 * clamp(avg_markout_30s_bps, -2, 2)
  + 0.5 * round_trips_per_hour
  - 3.0 * adverse_rate
  - 1.0 * close_mode_minutes_per_hour
  - 2.0 * guard_halts_per_hour
```

Interpretation:

```text
score > 1.0: increase target_exposure_x slowly
score 0.0 to 1.0: keep size unchanged
score -1.0 to 0.0: reduce target_exposure_x
score < -1.0: stop the symbol
```

The fastest practical version:

```text
if adverse_rate > 0.60:
    stop or widen symbol

if avg_markout_5s_bps < -0.5:
    widen symbol

if close_mode_duration_p95 > 60s:
    lower exposure or raise TP

if guard_halts_per_hour > 0:
    lower exposure immediately
```

## Exact Kill Switches

These are the rules that protect the strategy from turning a rebate farm into an inventory bag:

```text
Kill symbol for 10 to 30 minutes if:
- total session PnL hits max loss,
- three round trips in a row are negative,
- Hotstuff/Binance basis is outside sanity band,
- Hotstuff BBO is crossed or stale,
- adverse fill rate is above 65% after at least 20 fills,
- close-only mode lasts longer than 120 seconds,
- markout_5s average is below -1 bps after at least 20 fills.
```

Reduce exposure by half if:

```text
- inventory reaches tier 3 more than twice in 15 minutes,
- TP triggers but does not close within 30 seconds,
- IOC close is used more than once per 10 round trips,
- spread needs to widen above 2x baseline to avoid toxic fills.
```

Increase exposure only if:

```text
- realized PnL is positive after fees,
- average 5s markout is flat or positive,
- close-mode duration is short,
- adverse fill rate is below 50%,
- no guard halt in the last hour.
```

## What Other Bots Should Copy Exactly

Copy these:

- Use post-only for all normal maker orders.
- Use one level first; add levels only after markouts prove the symbol is safe.
- Use external fair value, not raw venue mid.
- Use inventory skew before inventory is large.
- Use close-only TP mode instead of opening both sides forever.
- Track markouts and adverse fill rate.
- Treat taker IOC as an escape hatch, not an alpha engine.
- Use per-symbol config. BTC, ETH, HYPE, SOL, and thin markets should not share identical size.

Do not copy these bad patterns:

- Do not set `target_exposure_x = 50` for maker rebate farming.
- Do not use taker TP at `1 bps`.
- Do not keep quoting both sides when inventory is already heavy.
- Do not quote only from Hotstuff mid when Binance basis is available.
- Do not optimize for volume if markout is negative.
- Do not widen forever on a toxic symbol; stop it.

## Production Sharing Notes

If this strategy is shared with another MM bot, explain it as:

```text
This is a passive rebate capture and inventory recycling strategy.
It is not a directional prediction strategy.
It is not a taker scalper.
Its edge comes from combining negative maker fees, fair-value quoting,
small inventory, post-only exits, and markout-based risk control.
```

The minimum implementation required in another bot:

```text
1. Post-only maker order support.
2. Reduce-only close support.
3. IOC emergency close support.
4. External reference price.
5. Per-symbol inventory and average entry tracking.
6. Markout tracking at 1s and 5s.
7. Inventory skew and close-only mode.
8. Configurable taker fee gate before IOC close.
```

If another bot cannot track markouts, it should run with wider spreads and lower exposure:

```json
{
  "levels": 1,
  "min_spread_bps": 0.5,
  "spread_vol_mult": 2.0,
  "fixed_tp_bps": 1.5,
  "target_exposure_x": 0.3
}
```

## Why Always-Negative Maker Changes The Strategy

On most venues, maker has a small positive fee or zero fee. On Hotstuff, maker starts negative on perps, so every clean passive fill has an immediate fee edge. That changes the priority order:

```text
normal venue:
    spread capture > fees > inventory

Hotstuff negative-maker venue:
    avoid adverse selection > recycle inventory > collect rebate > spread capture
```

The rebate is valuable because it pays on every maker fill, but it is still smaller than a fast adverse move. So the exact working strategy is not "quote tighter forever." It is "quote tight only while flow is clean, then pull or skew fast."

When flow is clean:

```text
min_spread_bps can be 0.1 to 0.2
target_exposure_x can be 5.0 to 10.0
fixed_tp_bps can be 1.0
```

When flow is toxic:

```text
spread_vol_mult should rise
heavy-side opening should stop
target_exposure_x should fall
symbol should cooldown if markouts stay negative
```

## Metrics That Matter

Other MM bots should optimize these first:

- Net PnL after fees and rebates.
- Average `5s` markout.
- Adverse fill rate.
- Round trips per hour.
- Time spent in close mode.
- Inventory utilization percentile, not just max inventory.
- Fill-to-cancel ratio.
- TP trigger to close latency.
- Quote rejection rate from post-only crossing.
- Difference between Hotstuff mid, Binance mid, and fair mid.

Useful maker formula:

```text
expected_edge_bps =
    maker_rebate_bps
  + half_spread_captured_bps
  + average_markout_bps
  - inventory_carry_cost_bps
  - close_cost_bps
```

For `1 bps` TP mode, `close_cost_bps` is the deciding variable. If close is maker, the mode can work. If close is taker, the target needs to be larger.

## Practical Operating Playbook

Start maker with one symbol and one level at `target_exposure_x = 5.0`. Increase toward `10.0` only after the bot shows stable fill quality and closes inventory without repeated IOC.

Recommended progression:

1. Start with `levels = 1`, `target_exposure_x = 5.0`, `fixed_tp_bps = 1.0`.
2. Confirm post-only fills are not immediately adverse on `1s` and `5s` markouts.
3. If adverse rate is above `55%`, widen `spread_vol_mult` or reduce `alpha_bps`.
4. If inventory reaches tier 3 often, reduce `target_exposure_x` or start skew earlier.
5. If TP triggers but close does not fill, lower `close_spread_bps` or raise `fixed_tp_bps`.
6. Only add a second level after one-level mode is profitable after fees.
7. Keep taker worker separate from maker TP farming; use it only for wide-spread opportunity or emergency exits.

## Recommended Defaults By Objective

### Rebate And TP Farm

```json
{
  "levels": 1,
  "min_spread_bps": 0.1,
  "fixed_tp_enabled": true,
  "fixed_tp_bps": 1.0,
  "target_exposure_x": 5.0,
  "inv_skew_start_pct": 20,
  "inv_skip_open_pct": 45,
  "guard_inventory_decay_s": 180
}
```

### Safer Maker For Volatile Symbols

```json
{
  "levels": 1,
  "min_spread_bps": 0.5,
  "spread_vol_mult": 2.0,
  "fixed_tp_enabled": true,
  "fixed_tp_bps": 2.0,
  "target_exposure_x": 5.0,
  "guard_adverse_rate_threshold": 0.50
}
```

### Opportunistic Taker Only

```json
{
  "min_spread_bps": 6.0,
  "take_profit_bps": 6.0,
  "close_bps": 1.0,
  "target_exposure_x": 1.0,
  "cooldown_s": 0.10,
  "max_loss_usd": 2.0
}
```

## Final Guidance

For Hotstuff negative maker fee, the best improvement path is not "trade more"; it is "make every passive fill less toxic." The maker bot already has the right components: external fair value, post-only enforcement, inventory skew, TP close mode, markout feedback, and guard halts. The strongest near-term strategy is a one-level maker with `1 bps` fixed TP, conservative exposure, early inventory skew, and strict markout-based widening.

The taker bot is useful, but it is a different tool. It should not compete with maker mode for `1 bps` TP farming because taker fees dominate the target. Use taker when the spread is wide enough to beat both sides of taker fee, or when risk must be reduced immediately.
