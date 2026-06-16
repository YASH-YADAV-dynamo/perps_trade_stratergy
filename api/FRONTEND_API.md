# Bot API -- Frontend Reference

Base URL: `http://<host>:8199/v1`

Authenticated endpoints require `Authorization: Bearer <jwt_token>`.
All request/response bodies are JSON (`Content-Type: application/json`).

---

## Symbols

```
HYPE-PERP  BTC-PERP  ETH-PERP  SOL-PERP  XRP-PERP  ZEC-PERP  BNB-PERP
GOLD-PERP  SILVER-PERP  WTIOIL-PERP  BRENTOIL-PERP  NATGAS-PERP
EURUSD-PERP  USDJPY-PERP  USA500-PERP  USA100-PERP  X-PERP
```

---

## User Flow

```
1. POST /v1/auth/nonce                     -- get nonce for wallet signature
2. (user signs the nonce message with their HotStuff wallet)
3. POST /v1/auth/login                     -- verify signature, get JWT
4. GET  /v1/config/defaults                -- load maker+taker config defaults
5. POST /v1/sessions                       -- start strategy (maker, taker)
6. GET  /v1/sessions/{id}/metrics          -- poll dashboard (every 5-10s)
7. PATCH /v1/sessions/{id}/config          -- live config change (auto-restarts)
8. POST /v1/sessions/{id}/stop             -- stop MM
9. GET  /v1/sessions                       -- session history
10. GET /v1/sessions/{id}/metrics/history  -- historical metrics for charts
```

---

## Authentication (wallet-based)

Users authenticate by signing a message with their HotStuff wallet (the main user address, not the agent address). No passwords or API keys.

### `POST /v1/auth/nonce`

No auth. Creates user record if new, returns a nonce to sign.

**Request**
```json
{ "address": "0xYourHotStuffWalletAddress" }
```

**Response 200**
```json
{
  "nonce": "a1b2c3d4e5f6...",
  "message": "Sign in to Bot\nNonce: a1b2c3d4e5f6..."
}
```

The frontend should ask the user to sign the `message` string using EIP-191 personal_sign (standard wallet popup).

### `POST /v1/auth/login`

No auth. Verifies the signature and returns a JWT (24h expiry).

**Request**
```json
{
  "address": "0xYourHotStuffWalletAddress",
  "signature": "0x..."
}
```

**Response 200**
```json
{
  "token": "eyJhbGciOiJI...",
  "user_id": "d50d54075e4d4591",
  "wallet_address": "0xyour..."
}
```

Store `token` in memory or localStorage. Use as `Authorization: Bearer <token>` on all subsequent requests. When expired, repeat nonce + login (user signs again).

**401** -- unknown wallet (call `/v1/auth/nonce` first) or bad signature.

---

## Endpoints

### `GET /v1/health`

No auth.

```json
{ "status": "ok", "active_sessions": 3 }
```

---

### `GET /v1/config/defaults`

No auth. Returns maker defaults per symbol, plus taker defaults.

**Response 200**
```json
{
  "defaults": {
    "HYPE-PERP": { "min_spread_bps": 0.1, "levels": 5, "...full config..." },
    "ETH-PERP":  { "..." },
    "...": "..."
  },
  "allowed_keys": ["adx_regime_enabled", "alpha_bps", "..."],
  "taker_defaults": {
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
    "market_bias": 0.0
  },
  "taker_defaults_by_symbol": {
    "BTC-PERP": { "...symbol tuned defaults..." },
    "ETH-PERP": { "...symbol tuned defaults..." }
  },
  "taker_allowed_keys": ["close_bps", "close_timeout_ms", "cooldown_s", "leverage", "market_bias", "max_loss_usd", "min_spread_bps", "min_spread_usd", "order_expiry_ms", "order_size_usd", "take_profit_bps", "target_exposure_x"]
}
```

---

### `POST /v1/sessions`

Start a strategy session.  
`strategy="maker"` runs the maker worker (multi-symbol).  
`strategy="taker"` runs the one-account taker worker (single symbol).

**Maker request (default)**
```json
{
  "strategy": "maker",
  "agent_address": "0xYourHotStuffAgentAddress",
  "agent_private_key": "0xYourAgentPrivateKey",
  "symbols": ["HYPE-PERP", "ETH-PERP"],
  "config": { "levels": 3, "min_spread_bps": 2.0 },
  "symbol_config": {
    "ETH-PERP": { "min_spread_bps": 1.5, "use_alpha": false }
  }
}
```

**Taker request**
```json
{
  "strategy": "taker",
  "agent_address": "0xYourHotStuffAgentAddress",
  "agent_private_key": "0xYourAgentPrivateKey",
  "symbols": ["BTC-PERP"],
  "taker_config": {
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
    "market_bias": 0.0
  }
}
```

| Field | Required | Description |
|---|---|---|
| `strategy` | no | `maker` (default) or `taker` |
| `agent_address` | yes | HotStuff agent wallet address |
| `agent_private_key` | yes | Agent private key (encrypted at rest, never returned) |
| `symbols` | yes | Instruments to quote on |
| `config` | maker only | Global maker config overrides (all symbols) |
| `symbol_config` | maker only | Per-symbol maker overrides (layers on top of global) |
| `taker_config` | taker only | One-account taker overrides (`min_spread_usd`, `min_spread_bps`, `take_profit_bps`, `close_bps`, `close_timeout_ms`, `order_size_usd`, `target_exposure_x`, `leverage`, `cooldown_s`, `max_loss_usd`, `order_expiry_ms`, `market_bias`) |

Maker config merge: `BASE_DEFAULTS -> INSTRUMENT_DEFAULTS[symbol] -> config -> symbol_config[symbol]`
  
Taker requirements: `symbols` must contain exactly one symbol.

Taker runtime notes:
- For low-equity wallets (`$2-$15`), auto sizing uses a fixed `~$12` notional before leverage cap checks.
- Taker now applies loss-aversion exits: adverse open PnL can trigger faster reduce-only IOC close attempts before the session loss cap stops trading.

**Response 201**
```json
{
  "session_id": "4b4d4a7bb2954b42",
  "status": "running",
  "strategy": "maker",
  "symbols": ["HYPE-PERP", "ETH-PERP"],
  "agent_address": "0xYour...",
  "started_at": "2026-03-20T13:17:49Z",
  "config": {
    "HYPE-PERP": { "min_spread_bps": 2.0, "levels": 3, "..." },
    "ETH-PERP":  { "min_spread_bps": 1.5, "use_alpha": false, "..." }
  }
}
```

Taker response includes `strategy: "taker"` and `taker_config`.

**Errors:** 400 (validation), 409 (symbol conflict with another running session)

---

### `GET /v1/sessions`

List all sessions for the authenticated user (newest first).

```json
{
  "sessions": [
    { "session_id": "...", "status": "running", "strategy": "maker", "symbols": [...], "agent_address": "...",
      "started_at": "...", "stopped_at": null, "error": null },
    { "session_id": "...", "status": "stopped", "strategy": "taker", "..." }
  ]
}
```

Statuses: `starting`, `running`, `stopped`, `error`

---

### `GET /v1/sessions/{session_id}`

Single session detail. Same shape as one list item. **404** if not found.

---

### `GET /v1/sessions/{session_id}/config`

Returns config for the session strategy.

```json
{
  "session_id": "...",
  "strategy": "maker",
  "symbols": ["HYPE-PERP", "ETH-PERP"],
  "configs": { "HYPE-PERP": { "...full merged config..." }, "ETH-PERP": { "..." } }
}
```

For taker sessions:

```json
{
  "session_id": "...",
  "strategy": "taker",
  "symbols": ["BTC-PERP"],
  "config": {
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
    "market_bias": 0.0
  }
}
```

---

### `PATCH /v1/sessions/{session_id}/config`

Update config for a running session. Strategy subprocess auto-restarts (~2-3s gap).
  
For maker sessions, use maker keys (`min_spread_bps`, `levels`, etc).  
For taker sessions, patch only taker keys (`min_spread_usd`, `min_spread_bps`, `take_profit_bps`, `close_bps`, `close_timeout_ms`, `order_size_usd`, `target_exposure_x`, `leverage`, `cooldown_s`, `max_loss_usd`, `order_expiry_ms`, `market_bias`).

**Global update (all symbols):**
```json
{ "min_spread_bps": 3.0, "levels": 7 }
```

**Single-symbol update:**
```json
{ "symbol": "ETH-PERP", "min_spread_bps": 1.0 }
```

**Response 200** -- returns updated config for that strategy. **400** for unknown keys or stopped session.

---

### `POST /v1/sessions/{session_id}/stop`

Cancels orders, stops the process.

```json
{ "session_id": "...", "status": "stopped" }
```

**400** if already stopped.

---

### `GET /v1/sessions/{session_id}/metrics`

Live snapshot. Updated every ~5s while running.

| Param | Required | Description |
|---|---|---|
| `symbol` | no | Filter to one symbol |

```json
{
  "session_id": "...",
  "metrics": {
    "HYPE-PERP": {
      "ts": "2026-03-20T13:25:00Z", "pnl": 0.12, "pnl_realized": 0.05, "pnl_unrealized": 0.07, "inventory": -0.05,
      "inv_tier": 0, "total_fills": 48, "total_volume_usd": 1230.5,
      "round_trips": 22, "spread_bps": 2.3, "quote_mode": "normal", "vol_bps": 8.1,
      "alpha": 0.12, "toxic": 0.35, "adverse_rate": 0.15,
      "avg_markout_1s": -0.02, "avg_markout_5s": 0.01,
      "guard_interventions": 0, "guard_halted": false, "guard_spread_mult": 1.0,
      "account_equity": 500.0, "fair_mid": 24.12, "hs_mid": 24.11, "bn_mid": 24.13
    },
    "ETH-PERP": { "..." }
  }
}
```

**404** during warmup (10-30s after start).

---

### `GET /v1/sessions/{session_id}/metrics/history`

Historical metrics for charts. Rows ordered `ts DESC`.

| Param | Required | Default | Description |
|---|---|---|---|
| `symbol` | no | all | Filter to one symbol |
| `since` | no | - | ISO timestamp lower bound |
| `until` | no | - | ISO timestamp upper bound |
| `limit` | no | 500 | Max rows (max 2000) |

---

## Metrics Fields

| Field | Type | Description |
|---|---|---|
| `ts` | string | Snapshot timestamp |
| `pnl` | float | Session PnL (USD) |
| `pnl_realized` | float | Realized PnL component (USD) |
| `pnl_unrealized` | float | Mark-to-market PnL component (USD) |
| `inventory` | float | Position size (signed) |
| `inv_tier` | int | 0=normal, 1=skew, 2=skip-open, 3=close-only |
| `total_fills` | int | Total fills this session |
| `total_volume_usd` | float | Total volume (USD) |
| `round_trips` | int | Completed round trips |
| `spread_bps` | float | Effective spread (bps) |
| `quote_mode` | string | Quote state (`normal`, `close`, etc.) |
| `vol_bps` | float | Estimated volatility (bps) |
| `alpha` | float | Alpha signal (-1 to +1) |
| `toxic` | float | Toxic flow score (0 to 1) |
| `adverse_rate` | float | Fraction of adverse fills |
| `avg_markout_1s` | float | 1s markout (bps) |
| `avg_markout_5s` | float | 5s markout (bps) |
| `guard_interventions` | int | Guard interventions |
| `guard_halted` | bool | Trading halted by guard |
| `guard_spread_mult` | float | Guard spread multiplier |
| `account_equity` | float | Account equity (USD) |
| `fair_mid` | float | Computed fair mid |
| `hs_mid` | float | HotStuff mid |
| `bn_mid` | float | Binance mid |

---

## Config Parameters

### Simple (default UI)

| Key | Type | Default | Description |
|---|---|---|---|
| `min_spread_bps` | float | 0.1 | Minimum spread (bps) |
| `levels` | int | 5 | Order levels per side |
| `level_spacing_bps` | float | 1.0 | Distance between levels |
| `order_size_usd` | float | 0 | USD per level (0 = auto from equity) |
| `target_exposure_x` | float | 2.5 | Max exposure as equity multiple |
| `use_alpha` | bool | true | Enable alpha signals |
| `fixed_tp_enabled` | bool | false | Fixed take-profit mode |
| `fixed_tp_bps` | float | 3.0 | TP distance (bps) |
| `market_bias` | float | 0.0 | Directional bias: -1.0 = full short, 0.0 = neutral, 1.0 = full long |

### Advanced

| Key | Type | Default | Description |
|---|---|---|---|
| `spread_vol_mult` | float | 1.5-2.5 | Vol multiplier for dynamic spread |
| `close_spread_bps` | float | 0.1 | Close-only spread |
| `alpha_bps` | float | 15.0 | Max alpha shift |
| `inventory_skew_bps` | float | 3.0 | Quote skew with inventory |
| `max_inventory` | float | 1000.0 | Max position notional (USD) -- converted to base units at runtime using fair price |
| `leverage` | int | 20 | Account leverage |
| `level_size_scale` | float | 1.4 | Geometric size scaling |
| `noise_bps` | float | 2.0 | Random quote noise |
| `close_threshold_usd` | float | 10.0 | Flat-position threshold |
| `inv_skew_start_pct` | int | 30 | Inventory % to start skewing |
| `inv_skip_open_pct` | int | 60 | Inventory % to skip opens |
| `toxic_threshold` | float | 0.7 | Toxic flow defense trigger |
| `adx_regime_enabled` | bool | true | ADX trend filter |
| `supertrend_enabled` | bool | true | SuperTrend filter |
| `pivot_enabled` | bool | true | Daily pivot adjustment |
| `max_loss_pct` | float | 5.0 | Max session loss (% equity) |
| `guard_max_session_loss_usd` | float | 5.0 | Absolute USD loss limit |
| `guard_max_drawdown_pct` | float | 3.0 | Max drawdown from peak |
| `guard_cooldown_s` | int | 30 | Pause after loss streak (s) |
| `guard_loss_streak_trigger` | int | 3 | Consecutive losses for cooldown |

### Market Bias

`market_bias` is a float from `-1.0` to `1.0` (default `0.0` = neutral). Available on both maker and taker.

| Value | Meaning |
|---|---|
| `1.0` | Full long bias |
| `0.5` | Mild long bias |
| `0.0` | Neutral (default) |
| `-0.5` | Mild short bias |
| `-1.0` | Full short bias |

**Maker effects:**
- Adds persistent directional alpha shift (long bias = more aggressive bids, wider asks)
- Reduces inventory skew penalty in the bias direction (tolerates more inventory before tier escalation)
- Re-enables the bias-side quotes that inventory tiers would normally block (unless in close mode or at max inventory)

**Taker effects:**
- Locks entry direction: positive bias = only opens longs, negative bias = only opens shorts, zero = alternates buy/sell as before

### Execution Cadence Defaults (service-level)

These cadence knobs are strategy runtime defaults and are not exposed as user PATCH keys today:

- Requote loop interval: `200ms` (`MM_REQUOTE_INTERVAL_S=0.2`)
- Requote throttle floor: `200ms` (`MM_UPDATE_THROTTLE_MS=200`)
- Event trigger threshold: `0.1 bps` (`MM_REQUOTE_THRESHOLD_BPS=0.1`)
- Open-orders reconciliation interval: `1000ms` (internal loop)

### Per-Symbol Defaults

| Symbol | min_spread_bps | spread_vol_mult |
|---|---|---|
| HYPE-PERP | 0.1 | 1.8 |
| BTC-PERP | 0.1 | 1.5 |
| ETH-PERP | 0.1 | 1.5 |
| SOL-PERP | 0.1 | 1.8 |
| XRP-PERP | 0.1 | 2.0 |
| ZEC-PERP | 0.1 | 2.5 |
| BNB-PERP | 0.1 | 1.5 |
| GOLD-PERP | 0.1 | 1.5 |
| SILVER-PERP | 0.1 | 1.5 |
| WTIOIL-PERP | 0.1 | 1.5 |
| BRENTOIL-PERP | 0.1 | 1.5 |
| NATGAS-PERP | 0.1 | 1.5 |
| EURUSD-PERP | 0.1 | 1.5 |
| USDJPY-PERP | 0.1 | 1.5 |
| USA500-PERP | 0.1 | 1.5 |
| USA100-PERP | 0.1 | 1.5 |
| X-PERP | 0.1 | 1.5 |

---

## Auto-Sizing

When `order_size_usd = 0`:

```
per_symbol_equity = account_equity / num_symbols
scale_sum = sum(level_size_scale^i for i in 0..levels-1)
size_per_level = (per_symbol_equity * target_exposure_x) / (2 * scale_sum)
```

Low-equity maker behavior:

```
if account_equity < 10.0:
    size_per_level = max(size_per_level, 12.0)
```

---

## Error Responses

```json
{ "detail": "Human-readable message" }
```

| Status | Meaning |
|---|---|
| 400 | Validation / bad input |
| 401 | Missing or invalid auth |
| 404 | Not found |
| 409 | Symbol conflict |
| 500 | Internal error |

---

## Admin API (ops dashboard)

All routes below require `Authorization: Bearer <BOT_ADMIN_TOKEN>`.
Admin API is VM-local only and should be accessed via SSH tunnel.

### `GET /v1/admin-8888/summary`

Returns global ops state plus per-session admin rows.

```json
{
  "boot_time": "2026-03-25T18:22:10.120Z",
  "users_count": 15,
  "active_subprocesses": 2,
  "session_status": { "running": 2, "stopped": 4, "archived": 3, "error": 1 },
  "relay": { "active_feeds": 6, "feeds": [ { "symbol": "BTC-PERP", "...": "..." } ] },
  "sessions": [
    {
      "session_id": "...",
      "user_id": "...",
      "status": "running",
      "strategy": "maker",
      "symbols": ["HYPE-PERP"],
      "agent_address": "0xAgent...",
      "wallet_address": "0xMainWallet...",
      "live_equity": 9.31,
      "start_config": { "strategy": "maker", "global": {}, "per_symbol": {}, "effective": { "...": "..." } },
      "metrics": { "HYPE-PERP": { "...": "..." } },
      "last_metrics_ts": "2026-03-25T18:22:45Z",
      "log_lines": 150
    }
  ]
}
```

### `POST /v1/admin-8888/relay/restart`

Restart all relay feeds, or one feed with query param `?symbol=BTC-PERP`.

### `POST /v1/admin-8888/sessions/{session_id}/restart`

Restart a session from DB config and encrypted signer key.

### `POST /v1/admin-8888/sessions/{session_id}/stop`

Stop the strategy subprocess and mark session stopped.

### `DELETE /v1/admin-8888/sessions/{session_id}`

Soft delete by default (archives row, keeps metrics, restartable).  
Use `?hard=true` for irreversible delete + metrics wipe.

### `POST /v1/admin-8888/purge-stopped`

Soft archive all stopped/error by default.  
Use `?hard=true` for irreversible purge including metrics.

---

## Frontend Notes

- **Auth**: Users sign with their main HotStuff wallet (EIP-191). JWT expires in 24h; re-do nonce+login to refresh.
- **Two keys**: The wallet key proves identity (login). The agent private key is for order signing (sent only on session start, encrypted at rest, never returned).
- Poll `/metrics` every 5-10s while `running`. Metrics emit every ~5s.
- After start, metrics 404 for 10-30s during warmup. Show a loading state.
- `PATCH /config` restarts the process; ~2-3s metrics gap.
- Multiple sessions per user are allowed as long as symbols don't overlap.
- Conflict check is per-symbol: overlapping symbols across running sessions are blocked (409).
- Strategy source folders are canonical under `dxd_api/*` paths: `dxd_api/maker` and `dxd_api/taker`.
