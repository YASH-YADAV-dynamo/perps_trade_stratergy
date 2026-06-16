import asyncio
import json
import logging
import os
import secrets
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from hotstuff import InfoClient
from hotstuff.methods.info.account import AccountSummaryParams
from web3 import Web3

from . import db
from . import crypto
from . import session_manager as sm
from .relay import Relay
from .auth import (
    get_current_user,
    require_admin,
    verify_signature,
    create_jwt,
    SIGN_MESSAGE_PREFIX,
)
from .models import (
    BASE_DEFAULTS,
    VALID_SYMBOLS,
    build_config,
    build_taker_config,
    validate_session_request,
    validate_config_update,
    validate_taker_config_update,
)

log = logging.getLogger("bot-api")

_boot_time: Optional[str] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    db.get_conn()
    log.info("Database initialized")

    _relay = Relay()
    sm.relay = _relay
    log.info("Market data relay initialized")

    global _boot_time
    _boot_time = datetime.now(timezone.utc).isoformat()
    sm.start_flush_task()
    await sm.resume_running_sessions()
    log.info("DXD API ready")
    yield
    log.info("Shutting down -- stopping all sessions")
    sm.stop_flush_task()
    await sm.stop_all()
    await _relay.stop_all()
    sm.relay = None
    db.close()


app = FastAPI(title="DXD API", version="0.1.0", lifespan=lifespan)

_cors_origins = [s.strip() for s in os.getenv("DXD_CORS_ORIGINS", "*").split(",") if s.strip()]
if not _cors_origins:
    _cors_origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

_hs_info_client: Optional[InfoClient] = None
_ADMIN_UI_PATH = "/admin-8888"
_ADMIN_API_PREFIX = "/v1/admin-8888"


def _local_admin_hosts() -> frozenset[str]:
    # Loopback only by default. From Docker Desktop / Linux bridge, the peer
    # is often the host gateway (e.g. 172.17.0.1); set DXD_ADMIN_EXTRA_HOSTS.
    hosts = {"127.0.0.1", "::1"}
    extra = os.environ.get("DXD_ADMIN_EXTRA_HOSTS", "") or ""
    if extra.strip():
        for part in extra.split(","):
            p = part.strip()
            if p:
                hosts.add(p)
    return frozenset(hosts)


_LOCAL_ADMIN_HOSTS = _local_admin_hosts()


def _request_host(request: Request) -> str:
    host = request.client.host if request.client else ""
    # Normalize IPv4-mapped IPv6 loopback.
    if host.startswith("::ffff:"):
        host = host[7:]
    return host


def _assert_local_admin_request(request: Request) -> None:
    host = _request_host(request)
    if host not in _LOCAL_ADMIN_HOSTS:
        raise HTTPException(
            status_code=403,
            detail="Admin endpoints are local-only on VM. Use SSH tunnel to localhost:8888.",
        )


async def require_local_admin(
    request: Request,
    _: None = Depends(require_admin),
) -> None:
    _assert_local_admin_request(request)


async def require_local_admin_page(request: Request) -> None:
    _assert_local_admin_request(request)


# -- Health ----------------------------------------------------------------

@app.get("/v1/health")
async def health():
    return {"status": "ok", "active_sessions": sm.active_count()}


@app.get("/health")
async def health_legacy():
    # Legacy frontend compatibility path.
    return {"status": "ok", "active_sessions": sm.active_count()}


# -- Auth ------------------------------------------------------------------

@app.post("/v1/auth/nonce")
async def auth_nonce(request: Request):
    body = await request.json()
    address = (body.get("address") or "").strip()
    if not address or len(address) != 42 or not address.startswith("0x"):
        raise HTTPException(status_code=400, detail="Valid Ethereum address required")
    address = address.lower()

    nonce = secrets.token_hex(16)
    now = datetime.now(timezone.utc).isoformat()
    user = db.get_user_by_wallet(address)
    if user is None:
        user_id = uuid.uuid4().hex[:16]
        db.upsert_user(user_id, address, nonce, now)
    else:
        db.rotate_nonce(address, nonce)

    return {"nonce": nonce, "message": SIGN_MESSAGE_PREFIX + nonce}


@app.post("/auth/nonce")
async def auth_nonce_legacy(request: Request):
    return await auth_nonce(request)


@app.post("/v1/auth/login")
async def auth_login(request: Request):
    body = await request.json()
    address = (body.get("address") or "").strip().lower()
    signature = (body.get("signature") or "").strip()
    if not address or not signature:
        raise HTTPException(status_code=400, detail="address and signature required")

    user = db.get_user_by_wallet(address)
    if user is None:
        raise HTTPException(status_code=401, detail="Unknown wallet; call /v1/auth/nonce first")

    if not verify_signature(address, user["nonce"], signature):
        raise HTTPException(status_code=401, detail="Signature verification failed")

    db.rotate_nonce(address, secrets.token_hex(16))
    token = create_jwt(user["id"], address)
    return {
        "token": token,
        "user_id": user["id"],
        "wallet_address": address,
    }


@app.post("/auth/login")
async def auth_login_legacy(request: Request):
    return await auth_login(request)


# -- Config ----------------------------------------------------------------

@app.get("/v1/config/defaults")
async def get_config_defaults():
    result = {}
    for symbol in sorted(VALID_SYMBOLS):
        result[symbol] = build_config(symbol)
    taker_defaults = build_taker_config("BTC-PERP", {})
    taker_defaults_by_symbol = {
        symbol: build_taker_config(symbol, {})
        for symbol in sorted(VALID_SYMBOLS)
    }
    return {
        "defaults": result,
        "allowed_keys": sorted(BASE_DEFAULTS.keys()),
        "taker_defaults": taker_defaults,
        "taker_defaults_by_symbol": taker_defaults_by_symbol,
        "taker_allowed_keys": sorted(taker_defaults.keys()),
    }


# -- Sessions --------------------------------------------------------------

@app.post("/v1/sessions", status_code=201)
async def create_session(request: Request, user: dict = Depends(get_current_user)):
    body = await request.json()
    user_id = user["id"]

    err = validate_session_request(body)
    if err:
        raise HTTPException(status_code=400, detail=err)

    strategy = str(body.get("strategy") or "maker").strip().lower()
    symbols = body["symbols"]

    conflict = db.has_symbol_conflict(user_id, symbols)
    if conflict:
        raise HTTPException(
            status_code=409,
            detail=f"Symbols already running in session {conflict['session_id']}: {conflict['overlap']}",
        )

    try:
        await sm.validate_session_credentials(
            private_key=body["agent_private_key"],
            agent_address=body["agent_address"],
            account_address=user.get("wallet_address"),
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    session_id = uuid.uuid4().hex[:16]
    encrypted_pk = crypto.encrypt(body["agent_private_key"])
    now = datetime.now(timezone.utc).isoformat()
    per_symbol_cfgs: Optional[dict] = None
    strategy_options: Optional[dict] = None

    if strategy == "maker":
        user_config = body.get("config") or {}
        symbol_configs = body.get("symbol_config") or {}
        per_symbol_cfgs = {}
        for sym in symbols:
            merged = {**user_config, **symbol_configs.get(sym, {})}
            per_symbol_cfgs[sym] = build_config(sym, merged)
        config_json = json.dumps({
            "strategy": "maker",
            "global": user_config,
            "per_symbol": symbol_configs,
        })
    else:
        taker_cfg = build_taker_config(symbols[0], body.get("taker_config") or {})
        strategy_options = {
            "config": taker_cfg,
        }
        config_json = json.dumps({
            "strategy": "taker",
            "taker": {
                "symbol": symbols[0],
                "config": taker_cfg,
            },
        })

    db.create_session(
        session_id=session_id,
        user_id=user_id,
        agent_address=body["agent_address"],
        encrypted_pk=encrypted_pk,
        symbols=symbols,
        config_overrides=config_json,
        started_at=now,
    )

    try:
        await sm.start_session(
            session_id=session_id,
            private_key=body["agent_private_key"],
            agent_address=body["agent_address"],
            account_address=user.get("wallet_address"),
            symbols=symbols,
            symbol_configs=per_symbol_cfgs,
            strategy=strategy,
            strategy_options=strategy_options,
        )
    except Exception as exc:
        db.update_session_status(session_id, "error", error=str(exc))
        raise HTTPException(status_code=500, detail=f"Failed to start session: {exc}")

    resp = {
        "session_id": session_id,
        "status": "running",
        "strategy": strategy,
        "symbols": symbols,
        "agent_address": body["agent_address"],
        "started_at": now,
    }
    if strategy == "maker":
        resp["config"] = per_symbol_cfgs or {}
    else:
        resp["taker_config"] = strategy_options["config"] if strategy_options else {}
    return resp


@app.post("/v1/sessions/{session_id}/stop")
async def stop_session(session_id: str, user: dict = Depends(get_current_user)):
    row = db.get_session(session_id, user["id"])
    if not row:
        raise HTTPException(status_code=404, detail="Session not found")
    if row["status"] not in ("starting", "running"):
        raise HTTPException(status_code=400, detail=f"Session is already {row['status']}")

    await sm.stop_session(session_id)
    return {"session_id": session_id, "status": "stopped"}


@app.get("/v1/sessions")
async def list_sessions(user: dict = Depends(get_current_user)):
    rows = db.list_sessions(user["id"])
    return {"sessions": [_session_dict(r) for r in rows]}


@app.get("/v1/sessions/{session_id}")
async def get_session(session_id: str, user: dict = Depends(get_current_user)):
    row = db.get_session(session_id, user["id"])
    if not row:
        raise HTTPException(status_code=404, detail="Session not found")
    return _session_dict(row)


# -- Session config --------------------------------------------------------

@app.get("/v1/sessions/{session_id}/config")
async def get_session_config(session_id: str, user: dict = Depends(get_current_user)):
    row = db.get_session(session_id, user["id"])
    if not row:
        raise HTTPException(status_code=404, detail="Session not found")
    symbols = json.loads(row["symbols"])
    stored = _parse_session_overrides(row)
    strategy = _stored_strategy(stored)
    if strategy == "taker":
        taker = stored.get("taker") or {}
        symbol = symbols[0] if symbols else ""
        cfg = build_taker_config(symbol, taker.get("config") or {})
        return {
            "session_id": session_id,
            "strategy": "taker",
            "symbols": symbols,
            "config": cfg,
        }

    global_cfg = stored.get("global", {})
    per_sym = stored.get("per_symbol", {})
    configs = {}
    for sym in symbols:
        merged = {**global_cfg, **per_sym.get(sym, {})}
        configs[sym] = build_config(sym, merged)
    return {
        "session_id": session_id,
        "strategy": "maker",
        "symbols": symbols,
        "configs": configs,
    }


@app.patch("/v1/sessions/{session_id}/config")
async def update_session_config(
    session_id: str, request: Request, user: dict = Depends(get_current_user),
):
    row = db.get_session(session_id, user["id"])
    if not row:
        raise HTTPException(status_code=404, detail="Session not found")
    if row["status"] not in ("starting", "running"):
        raise HTTPException(status_code=400, detail=f"Session is {row['status']}, cannot reconfigure")

    body = await request.json()
    target_symbol = body.pop("symbol", None)
    symbols = json.loads(row["symbols"])
    stored = _parse_session_overrides(row)
    strategy = _stored_strategy(stored)

    if strategy == "taker":
        if target_symbol:
            raise HTTPException(status_code=400, detail="taker strategy uses single-symbol config only")
        err = validate_taker_config_update(body)
        if err:
            raise HTTPException(status_code=400, detail=err)
        taker = stored.get("taker") or {}
        cfg_old = taker.get("config") or {}
        symbol = symbols[0] if symbols else ""
        cfg_new = build_taker_config(symbol, {**cfg_old, **body})
        taker["config"] = cfg_new
        stored["strategy"] = "taker"
        stored["taker"] = taker
        db.update_session_config(session_id, json.dumps(stored))
        try:
            await sm.restart_session_from_db(session_id)
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Restart failed: {exc}")
        return {
            "session_id": session_id,
            "status": "running",
            "strategy": "taker",
            "config": cfg_new,
        }

    err = validate_config_update(body)
    if err:
        raise HTTPException(status_code=400, detail=err)

    global_cfg = stored.get("global", {})
    per_sym = stored.get("per_symbol", {})

    if target_symbol:
        if target_symbol not in symbols:
            raise HTTPException(status_code=404, detail=f"Symbol {target_symbol} not in session")
        old = per_sym.get(target_symbol, {})
        per_sym[target_symbol] = {**old, **body}
    else:
        global_cfg = {**global_cfg, **body}

    new_stored = json.dumps({
        "strategy": "maker",
        "global": global_cfg,
        "per_symbol": per_sym,
    })
    per_symbol_cfgs = {}
    for sym in symbols:
        merged = {**global_cfg, **per_sym.get(sym, {})}
        per_symbol_cfgs[sym] = build_config(sym, merged)

    try:
        await sm.restart_session(session_id, per_symbol_cfgs)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Restart failed: {exc}")

    db.update_session_config(session_id, new_stored)
    return {
        "session_id": session_id,
        "status": "running",
        "strategy": "maker",
        "configs": per_symbol_cfgs,
    }


# -- Metrics ---------------------------------------------------------------

@app.get("/v1/sessions/{session_id}/metrics")
async def get_metrics(
    session_id: str,
    symbol: Optional[str] = Query(None),
    user: dict = Depends(get_current_user),
):
    row = db.get_session(session_id, user["id"])
    if not row:
        raise HTTPException(status_code=404, detail="Session not found")

    symbols = json.loads(row["symbols"])
    if symbol and symbol not in symbols:
        raise HTTPException(status_code=404, detail=f"Symbol {symbol} not in session")

    target_syms = [symbol] if symbol else symbols
    metrics = {}
    db_latest = db.get_latest_metrics(session_id)
    db_rollup = db.get_metrics_cumulative(session_id)
    for sym in target_syms:
        snap = sm.get_snapshot(session_id, sym)
        db_row = db_latest.get(sym)
        db_metric = _db_metric_dict(db_row) if db_row else None
        db_metric_rollup = _db_metric_rollup_dict(db_rollup[sym]) if sym in db_rollup else None
        if snap:
            metrics[sym] = _merge_metric_counters(_snap_dict(snap), db_metric, db_metric_rollup)
            continue
        if db_metric:
            metrics[sym] = _merge_metric_counters(db_metric, db_metric, db_metric_rollup)

    if not metrics:
        raise HTTPException(status_code=404, detail="No metrics available yet")

    return {"session_id": session_id, "metrics": metrics}


@app.get("/v1/sessions/{session_id}/metrics/history")
async def get_metrics_history(
    session_id: str,
    symbol: Optional[str] = Query(None),
    since: Optional[str] = Query(None),
    until: Optional[str] = Query(None),
    limit: int = Query(500, le=2000),
    user: dict = Depends(get_current_user),
):
    row = db.get_session(session_id, user["id"])
    if not row:
        raise HTTPException(status_code=404, detail="Session not found")

    rows = db.get_metrics_history(session_id, symbol=symbol, since=since, until=until, limit=limit)
    return {
        "session_id": session_id,
        "rows": [_db_metric_dict(r) for r in rows],
    }


# -- Helpers ---------------------------------------------------------------

def _get_hs_info_client() -> InfoClient:
    global _hs_info_client
    if _hs_info_client is None:
        _hs_info_client = InfoClient(is_testnet=False)
    return _hs_info_client


def _checksum_address(addr: str) -> Optional[str]:
    if not addr:
        return None
    try:
        return Web3.to_checksum_address(addr)
    except Exception:
        return None


def _to_float(val, default: float = 0.0) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _user_wallet_map(rows: list[dict]) -> dict[str, str]:
    user_wallet: dict[str, str] = {}
    for row in rows:
        uid = str(row.get("user_id") or "")
        if not uid or uid in user_wallet:
            continue
        user = db.get_user_by_id(uid)
        wallet = str(user.get("wallet_address", "")) if user else ""
        user_wallet[uid] = wallet
    return user_wallet


async def _fetch_live_equity_by_user(user_wallet: dict[str, str]) -> dict[str, Optional[float]]:
    user_wallet_cs = {
        uid: _checksum_address(wallet)
        for uid, wallet in user_wallet.items()
    }

    eq_by_wallet: dict[str, Optional[float]] = {}
    info = _get_hs_info_client()
    for wallet in {w for w in user_wallet_cs.values() if w}:
        try:
            summary = await asyncio.to_thread(
                info.account_summary,
                AccountSummaryParams(user=wallet),
            )
            eq = _to_float(getattr(summary, "total_account_equity", 0.0), 0.0)
            # Keep legacy fallback for accounts where available balance is the only populated field.
            if eq <= 0.0:
                eq = _to_float(getattr(summary, "available_balance", 0.0), 0.0)
            eq_by_wallet[wallet] = eq
        except Exception:
            eq_by_wallet[wallet] = None

    return {
        uid: (eq_by_wallet.get(wallet) if wallet else None)
        for uid, wallet in user_wallet_cs.items()
    }


def _parse_session_overrides(row: dict) -> dict:
    raw = row.get("config_overrides")
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return {}


def _stored_strategy(stored: dict) -> str:
    strategy = str(stored.get("strategy") or "maker").strip().lower()
    return strategy if strategy in ("maker", "taker") else "maker"


def _session_dict(row: dict) -> dict:
    symbols = json.loads(row["symbols"]) if isinstance(row["symbols"], str) else row["symbols"]
    stored = _parse_session_overrides(row)
    return {
        "session_id": row["id"],
        "status": row["status"],
        "strategy": _stored_strategy(stored),
        "symbols": symbols,
        "agent_address": row["agent_address"],
        "started_at": row["started_at"],
        "stopped_at": row.get("stopped_at"),
        "error": row.get("error"),
    }


def _admin_start_config(symbols: list[str], stored: dict) -> dict:
    strategy = _stored_strategy(stored)
    if strategy == "taker":
        taker = stored.get("taker") if isinstance(stored.get("taker"), dict) else {}
        symbol = str(taker.get("symbol") or (symbols[0] if symbols else ""))
        return {
            "strategy": "taker",
            "symbol": symbol,
            "config": build_taker_config(symbol, taker.get("config") or {}),
        }

    global_cfg = stored.get("global") if isinstance(stored.get("global"), dict) else {}
    per_sym = stored.get("per_symbol") if isinstance(stored.get("per_symbol"), dict) else {}
    effective: dict[str, dict] = {}
    for sym in symbols:
        merged = {**global_cfg, **(per_sym.get(sym) or {})}
        effective[sym] = build_config(sym, merged)
    return {
        "strategy": "maker",
        "global": global_cfg,
        "per_symbol": per_sym,
        "effective": effective,
    }


def _snap_dict(snap: dict) -> dict:
    return {
        "ts": snap.get("ts", ""),
        "symbol": snap.get("symbol", ""),
        "pnl": snap.get("pnl", 0.0),
        "pnl_realized": snap.get("pnl_realized", 0.0),
        "pnl_unrealized": snap.get("pnl_unrealized", 0.0),
        "inventory": snap.get("inventory", 0.0),
        "inv_tier": snap.get("inv_tier", 0),
        "total_fills": snap.get("total_fills", 0),
        "total_volume_usd": snap.get("total_volume_usd", 0.0),
        "round_trips": snap.get("round_trips", 0),
        "spread_bps": snap.get("spread_bps", 0.0),
        "quote_mode": snap.get("quote_mode", ""),
        "vol_bps": snap.get("vol_bps", 0.0),
        "alpha": snap.get("alpha", 0.0),
        "toxic": snap.get("toxic", 0.0),
        "adverse_rate": snap.get("adverse_rate", 0.0),
        "avg_markout_1s": snap.get("avg_markout_1s", 0.0),
        "avg_markout_5s": snap.get("avg_markout_5s", 0.0),
        "guard_interventions": snap.get("guard_interventions", 0),
        "guard_halted": snap.get("guard_halted", False),
        "guard_spread_mult": snap.get("guard_spread_mult", 1.0),
        "account_equity": snap.get("account_equity", 0.0),
        "fair_mid": snap.get("fair_mid", 0.0),
        "hs_mid": snap.get("hs_mid", 0.0),
        "bn_mid": snap.get("bn_mid", 0.0),
    }


def _db_metric_dict(r: dict) -> dict:
    return {
        "session_id": r["session_id"],
        "symbol": r.get("symbol", ""),
        "ts": r["ts"],
        "pnl": r.get("pnl", 0.0) or 0.0,
        "pnl_realized": 0.0,
        "pnl_unrealized": 0.0,
        "inventory": r.get("inventory", 0.0) or 0.0,
        "inv_tier": r.get("inv_tier", 0) or 0,
        "total_fills": r.get("total_fills", 0) or 0,
        "total_volume_usd": r.get("total_volume", 0.0) or 0.0,
        "round_trips": r.get("round_trips", 0) or 0,
        "spread_bps": r.get("spread_bps", 0.0) or 0.0,
        "quote_mode": "",
        "vol_bps": r.get("vol_bps", 0.0) or 0.0,
        "alpha": r.get("alpha", 0.0) or 0.0,
        "toxic": r.get("toxic", 0.0) or 0.0,
        "adverse_rate": r.get("adverse_rate", 0.0) or 0.0,
        "avg_markout_1s": r.get("avg_markout_1s", 0.0) or 0.0,
        "avg_markout_5s": r.get("avg_markout_5s", 0.0) or 0.0,
        "guard_interventions": r.get("guard_interventions", 0) or 0,
        "guard_halted": bool(r.get("guard_halted", 0)),
        "guard_spread_mult": r.get("guard_spread_mult", 1.0) or 1.0,
        "account_equity": r.get("account_equity", 0.0) or 0.0,
        "fair_mid": r.get("fair_mid", 0.0) or 0.0,
        "hs_mid": r.get("hs_mid", 0.0) or 0.0,
        "bn_mid": r.get("bn_mid", 0.0) or 0.0,
    }


def _db_metric_rollup_dict(r: dict) -> dict:
    return {
        "symbol": r.get("symbol", ""),
        "total_fills": r.get("total_fills", 0) or 0,
        "total_volume_usd": r.get("total_volume", 0.0) or 0.0,
        "round_trips": r.get("round_trips", 0) or 0,
        "guard_interventions": r.get("guard_interventions", 0) or 0,
    }


def _merge_metric_counters(base: dict, latest: Optional[dict], rollup: Optional[dict]) -> dict:
    merged = dict(base)
    if latest and not merged.get("ts"):
        merged["ts"] = latest.get("ts", "")
    merged["total_fills"] = max(
        int(merged.get("total_fills", 0) or 0),
        int((latest or {}).get("total_fills", 0) or 0),
        int((rollup or {}).get("total_fills", 0) or 0),
    )
    merged["round_trips"] = max(
        int(merged.get("round_trips", 0) or 0),
        int((latest or {}).get("round_trips", 0) or 0),
        int((rollup or {}).get("round_trips", 0) or 0),
    )
    merged["total_volume_usd"] = max(
        float(merged.get("total_volume_usd", 0.0) or 0.0),
        float((latest or {}).get("total_volume_usd", 0.0) or 0.0),
        float((rollup or {}).get("total_volume_usd", 0.0) or 0.0),
    )
    merged["guard_interventions"] = max(
        int(merged.get("guard_interventions", 0) or 0),
        int((latest or {}).get("guard_interventions", 0) or 0),
        int((rollup or {}).get("guard_interventions", 0) or 0),
    )
    return merged


def _admin_session_summary(row: dict) -> dict:
    sid = row["id"]
    base = _session_dict(row)
    base["user_id"] = row["user_id"]
    base["pid"] = row.get("pid")
    base["started_at"] = row.get("started_at", "")
    base["stopped_at"] = row.get("stopped_at", "")
    symbols = base["symbols"]
    stored = _parse_session_overrides(row)
    base["start_config"] = _admin_start_config(symbols, stored)
    metrics: dict = {}
    latest_ts = None
    db_latest = db.get_latest_metrics(sid)
    db_rollup = db.get_metrics_cumulative(sid)
    for sym in symbols:
        snap = sm.get_snapshot(sid, sym)
        db_row = db_latest.get(sym)
        db_metric = _db_metric_dict(db_row) if db_row else None
        db_metric_rollup = _db_metric_rollup_dict(db_rollup[sym]) if sym in db_rollup else None
        if snap:
            live_metric = _snap_dict(snap)
            metrics[sym] = _merge_metric_counters(live_metric, db_metric, db_metric_rollup)
            ts = live_metric.get("ts")
        else:
            if db_metric:
                metrics[sym] = _merge_metric_counters(db_metric, db_metric, db_metric_rollup)
                ts = db_metric.get("ts")
            else:
                ts = None
        if ts and (latest_ts is None or ts > latest_ts):
            latest_ts = ts
    base["metrics"] = metrics
    base["last_metrics_ts"] = latest_ts
    base["log_lines"] = len(sm.get_logs(sid))
    return base



_ADMIN_DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>DXD Ops</title>
<style>
:root{--bg:#0c0f14;--card:#151a22;--bd:#2a3344;--tx:#e7ecf3;--muted:#8b98a8;--acc:#3d8bfd;--acc2:#22c55e;--danger:#ef4444;--warn:#f59e0b}
*{box-sizing:border-box}body{margin:0;font-family:ui-sans-serif,system-ui,-apple-system,sans-serif;background:var(--bg);color:var(--tx);min-height:100vh}
.top{padding:20px 24px;border-bottom:1px solid var(--bd);background:linear-gradient(180deg,#121820 0%,var(--bg) 100%)}
h1{margin:0 0 4px;font-size:1.35rem;font-weight:650;letter-spacing:-.02em}
.sub{margin:0;color:var(--muted);font-size:.82rem;line-height:1.45}
.toolbar{display:flex;flex-wrap:wrap;gap:10px;align-items:center;margin-top:14px}
.toolbar input[type=password]{background:var(--card);border:1px solid var(--bd);color:var(--tx);padding:8px 12px;border-radius:8px;min-width:240px;font-size:.9rem}
.btn{border:0;padding:7px 13px;border-radius:8px;font-weight:600;font-size:.8rem;cursor:pointer;color:#fff;white-space:nowrap}
.btn-acc{background:var(--acc)}.btn-sec{background:#374151}.btn-ok{background:var(--acc2);color:#052e16}
.btn-warn{background:var(--warn);color:#1a1408}.btn-dan{background:var(--danger)}.btn:disabled{opacity:.4;cursor:not-allowed}
.chk{display:flex;align-items:center;gap:6px;color:var(--muted);font-size:.82rem}
#stat{font-size:.78rem;color:var(--muted)}
#lastact{margin-top:8px;font-size:.78rem;min-height:1.1em}
.lastact-ok{color:var(--acc2)}.lastact-fail{color:var(--danger)}
.wrap{padding:20px 24px}
.cards{display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));gap:10px;margin-bottom:18px}
.card{background:var(--card);border:1px solid var(--bd);border-radius:12px;padding:12px 14px}
.card b{display:block;font-size:.65rem;text-transform:uppercase;letter-spacing:.06em;color:var(--muted);margin-bottom:3px}
.card span{font-size:1.3rem;font-weight:700;font-variant-numeric:tabular-nums}
.card small{display:block;margin-top:4px;color:var(--muted);font-weight:400;font-size:.62rem;line-height:1.3}
.tbl-wrap{overflow:auto;border:1px solid var(--bd);border-radius:12px;background:var(--card)}
table{width:100%;border-collapse:collapse;font-size:.72rem}
th,td{padding:9px 10px;text-align:left;border-bottom:1px solid var(--bd);vertical-align:top}
th{background:#1a222d;color:var(--muted);font-weight:600;text-transform:uppercase;font-size:.62rem;letter-spacing:.04em}
tr:hover td{background:rgba(61,139,253,.05)}
code{font-family:ui-monospace,SFMono-Regular,monospace;font-size:.68rem;background:#0c0f14;padding:2px 5px;border-radius:4px}
.met-toggle{cursor:pointer;color:var(--acc);font-size:.68rem;border:1px solid var(--bd);border-radius:6px;padding:3px 8px;background:var(--bg);display:inline-block}
.met-toggle:hover{background:#1a222d}
.met-detail{display:none;margin-top:6px}
.met-detail.open{display:block}
.sym-block{border-left:3px solid var(--acc);padding:6px 8px;margin:6px 0;background:#0c0f14;border-radius:0 6px 6px 0}
.sym-block b{color:var(--acc);font-size:.68rem}
.kv{display:grid;grid-template-columns:repeat(auto-fill,minmax(115px,1fr));gap:3px 10px;margin-top:4px;color:var(--muted);font-size:.64rem}
.kv span{color:var(--tx)}
pre.addr{margin:0;white-space:pre-wrap;word-break:break-all;font-size:.62rem;color:var(--muted);max-width:160px}
.err{color:var(--danger);font-size:.68rem;max-width:200px}
.ts{color:var(--muted);font-size:.62rem}
.actions{display:flex;flex-wrap:wrap;gap:5px}
.st-running{color:var(--acc2);font-weight:700}.st-error{color:var(--danger);font-weight:700}
.st-stopped{color:var(--muted)}.st-starting{color:var(--warn)}.st-archived{color:#9ca3af;font-weight:700}
.fresh{display:inline-block;font-size:.6rem;padding:2px 6px;border-radius:4px;font-weight:600;margin-bottom:4px}
.fresh-ok{background:#052e16;color:var(--acc2)}.fresh-warn{background:#1a1408;color:var(--warn)}.fresh-stale{background:#1c0a0a;color:var(--danger)}
.cfg-box{display:none;margin-top:8px;max-height:320px;overflow:auto;background:#0a0c10;border:1px solid var(--bd);border-radius:8px;padding:8px;font-family:ui-monospace,SFMono-Regular,monospace;font-size:.62rem;line-height:1.5;white-space:pre-wrap;word-break:break-all;color:var(--tx)}
.cfg-box.open{display:block}
.log-box{display:none;margin-top:8px;max-height:300px;overflow:auto;background:#0a0c10;border:1px solid var(--bd);border-radius:8px;padding:8px;font-family:ui-monospace,SFMono-Regular,monospace;font-size:.62rem;line-height:1.5;white-space:pre-wrap;word-break:break-all;color:var(--muted)}
.log-box.open{display:block}
.relay-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:10px;margin-bottom:18px}
.relay-card{background:var(--card);border:1px solid var(--bd);border-radius:10px;padding:12px 14px;font-size:.72rem}
.relay-card h3{margin:0 0 8px;font-size:.8rem;font-weight:600;color:var(--tx)}
.relay-row{display:flex;justify-content:space-between;padding:3px 0;border-bottom:1px solid #1a222d}
.relay-row:last-child{border-bottom:0}
.relay-label{color:var(--muted)}.relay-val{font-weight:600;font-variant-numeric:tabular-nums}
.dot{display:inline-block;width:7px;height:7px;border-radius:50%;margin-right:5px;vertical-align:middle}
.dot-ok{background:var(--acc2)}.dot-down{background:var(--danger)}.dot-idle{background:var(--muted)}
</style>
</head>
<body>
<div class="top">
<h1>DXD backend</h1>
<p class="sub"><strong>Stop</strong> halts worker, keeps row. <strong>Archive</strong> keeps metrics and restartability. <strong>Hard delete/purge</strong> irreversibly removes sessions + metrics.</p>
<div class="toolbar">
<input id="tok" type="password" autocomplete="off" placeholder="admin token"/>
<button class="btn btn-sec" id="save">Save</button>
<button class="btn btn-sec" id="clr">Clear</button>
<button class="btn btn-acc" id="go">Refresh</button>
<button class="btn btn-dan" id="purge">Archive stopped</button>
<button class="btn btn-dan" id="purge-hard">Hard purge</button>
<button class="btn btn-warn" id="relay-restart">Restart Relay</button>
<label class="chk"><input type="checkbox" id="auto"/> Auto 10s</label>
<span id="stat"></span>
</div>
<p id="lastact"></p>
</div>
<div class="wrap">
<div class="cards" id="cards"></div>
<div class="relay-grid" id="relay"></div>
<div class="tbl-wrap">
<table id="tbl"><thead><tr>
<th>Session</th><th>User</th><th>Status</th><th>PID</th><th>Symbols</th><th>Started / Stopped</th><th>Agent</th><th>Main</th><th>Live Equity</th><th>Strategy</th><th>Metrics</th><th>Error</th><th>Actions</th>
</tr></thead><tbody></tbody></table>
</div>
</div>
<script>
(function(){
var $=function(id){return document.getElementById(id);};
var tok=$('tok'),stat=$('stat'),lastact=$('lastact'),cards=$('cards');
var tbody=document.querySelector('#tbl tbody');
var ADMIN_API='/v1/admin-8888';
try{tok.value=sessionStorage.getItem('dxd_admin_t')||'';}catch(e){}
$('save').onclick=function(){try{sessionStorage.setItem('dxd_admin_t',tok.value);stat.textContent='Saved.';}catch(e){}};
$('clr').onclick=function(){tok.value='';try{sessionStorage.removeItem('dxd_admin_t');}catch(e){}stat.textContent='Cleared.';};
function esc(s){return s==null?'':String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}
function card(t,v,sub){return '<div class="card"><b>'+t+'</b><span>'+v+'</span>'+(sub?'<small>'+sub+'</small>':'')+'</div>';}
function fmtts(s){if(!s)return'-';try{return new Date(s).toLocaleString();}catch(e){return esc(s);}}
function g(x,d){return x==null||x===''?d:x;}
function liveEqCell(v){
if(v==null||v==='')return '<span class="ts">--</span>';
var n=Number(v);
if(!isFinite(n))return '<span class="ts">--</span>';
return '$'+n.toFixed(4).replace(/\\.?0+$/,'');
}
function fmtm(m){
if(!m)return'';
var f=['ts','pnl','inventory','inv_tier','total_fills','total_volume_usd','round_trips','spread_bps','vol_bps',
'pnl_realized','pnl_unrealized','quote_mode','alpha','toxic','adverse_rate','avg_markout_1s','avg_markout_5s','guard_interventions','guard_halted','guard_spread_mult',
'account_equity','fair_mid','hs_mid','bn_mid'];
var lb={ts:'ts',pnl:'pnl',inventory:'inv',inv_tier:'tier',total_fills:'fills',total_volume_usd:'vol$',
round_trips:'rt',spread_bps:'spr',vol_bps:'vol',pnl_realized:'pnl.r',pnl_unrealized:'pnl.u',quote_mode:'q.mode',alpha:'alpha',toxic:'toxic',adverse_rate:'adv%',
avg_markout_1s:'mo1s',avg_markout_5s:'mo5s',guard_interventions:'g.int',guard_halted:'g.halt',
guard_spread_mult:'g.mult',account_equity:'equity',fair_mid:'fair',hs_mid:'hs',bn_mid:'ref'};
var h='<div class="kv">';
for(var i=0;i<f.length;i++){var k=f[i];h+='<div>'+(lb[k]||k)+' <span>'+esc(g(m[k],0))+'</span></div>';}
return h+'</div>';
}
function freshBadge(lastTs,status){
if(!lastTs)return status==='running'?'<span class="fresh fresh-stale">no data</span>':'';
var age=Math.round((Date.now()-new Date(lastTs).getTime())/1000);
var cls=age<15?'fresh-ok':age<60?'fresh-warn':'fresh-stale';
var label=age<60?age+'s ago':Math.round(age/60)+'m ago';
return '<span class="fresh '+cls+'">'+label+'</span>';
}
function metricCell(metrics,symbols,lastTs,status){
var badge=freshBadge(lastTs,status);
var hasAny=false;
for(var k in metrics){if(Object.prototype.hasOwnProperty.call(metrics,k)){hasAny=true;break;}}
if(!hasAny)return badge+'<span class="err">No metrics</span>';
var summary='';
for(var i=0;i<symbols.length;i++){
var sym=symbols[i],m=metrics[sym];
if(m){summary+=esc(sym)+': pnl='+esc(g(m.pnl,0))+' eq='+esc(g(m.account_equity,0))+' ';}
}
var detail='';
for(var k in metrics){if(!Object.prototype.hasOwnProperty.call(metrics,k))continue;
detail+='<div class="sym-block"><b>'+esc(k)+'</b>'+fmtm(metrics[k])+'</div>';}
return badge+'<br/><span class="met-toggle" data-toggle="met">'+esc(summary||'view')+'</span>'+
'<div class="met-detail">'+detail+'</div>';
}
function cfgText(v){
try{return JSON.stringify(v||{},null,2);}
catch(e){return String(v||'');}
}
async function apicall(path,method,body){
var t=(tok.value||'').trim();
var m=method||'GET';
var o={method:m,headers:{'Authorization':'Bearer '+t}};
if(m!=='GET'&&m!=='HEAD'){o.headers['Content-Type']='application/json';o.body=JSON.stringify(body||{});}
return fetch(path,o);
}
async function load(){
var t=(tok.value||'').trim();
if(!t){stat.textContent='Enter admin token.';tbody.innerHTML='';cards.innerHTML='';return;}
stat.textContent='Loading...';
try{
var r=await apicall(ADMIN_API+'/summary');
var j=await r.json().catch(function(){return {};});
if(!r.ok){stat.textContent='HTTP '+r.status+' '+(typeof j.detail==='string'?j.detail:JSON.stringify(j.detail||j));tbody.innerHTML='';cards.innerHTML='';return;}
stat.textContent='Updated '+new Date().toLocaleTimeString();
var ss=j.session_status||{};
var stLine=Object.keys(ss).map(function(k){return k+'='+ss[k];}).join(' ');
cards.innerHTML=
card('Live workers',j.active_subprocesses,'Running MM processes')+
card('Sessions',j.sessions.length,stLine||'none')+
card('Users',j.users_count,'Registered accounts')+
card('Boot',fmtts(j.boot_time),'API start time');
var rl=j.relay||{},rfeeds=rl.feeds||[],rhtml='';
if(rfeeds.length){
function fmtRelayMid(mid){
if(mid==null||mid==='')return '--';
var n=Number(mid);
if(!isFinite(n))return '--';
if(n>=1000)return n.toLocaleString(undefined,{maximumFractionDigits:2});
if(n>=10)return n.toFixed(4).replace(/\.?0+$/,'');
return n.toFixed(5).replace(/\.?0+$/,'');
}
function srcView(refSrc, src, refData){
if(refSrc===src)return refData;
return {};
}
function srcDot(v){
if(v.connected===true)return 'dot-ok';
if((v.msgs||0)>0)return 'dot-idle';
return 'dot-down';
}
function srcAge(v){return v.last_s_ago!=null?v.last_s_ago+'s':'--';}
function srcMsgs(v){return Number(v.msgs||0);}
for(var ri=0;ri<rfeeds.length;ri++){
var rf=rfeeds[ri],bn=rf.binance||{},hs=rf.hotstuff||{};
var refSrc=String(rf.ref_source||'');
var bv=srcView(refSrc,'binance',bn);
var hv=srcView(refSrc,'hyperliquid',bn);
var hsDot=hs.connected?'dot-ok':(hs.msgs>0?'dot-ok':'dot-down');
var bnDot=srcDot(bv),hlDot=srcDot(hv);
var bnAge=srcAge(bv),hlAge=srcAge(hv),hsAge=hs.last_s_ago!=null?hs.last_s_ago+'s':'--';
var bnPrice=fmtRelayMid(bv.mid),hlPrice=fmtRelayMid(hv.mid),hsPrice=fmtRelayMid(hs.mid);
rhtml+='<div class="relay-card"><h3>'+esc(rf.symbol)+'</h3>'+
'<div class="relay-row"><span class="relay-label"><span class="dot '+bnDot+'"></span>Binance</span><span class="relay-val">'+bnPrice+'</span></div>'+
'<div class="relay-row"><span class="relay-label"><span class="dot '+hlDot+'"></span>Hyperliquid</span><span class="relay-val">'+hlPrice+'</span></div>'+
'<div class="relay-row"><span class="relay-label"><span class="dot '+hsDot+'"></span>HotStuff</span><span class="relay-val">'+hsPrice+'</span></div>'+
'<div class="relay-row"><span class="relay-label">Binance</span><span class="relay-val" style="color:var(--muted)">'+srcMsgs(bv)+' msgs, '+bnAge+'</span></div>'+
'<div class="relay-row"><span class="relay-label">Hyperliquid</span><span class="relay-val" style="color:var(--muted)">'+srcMsgs(hv)+' msgs, '+hlAge+'</span></div>'+
'<div class="relay-row"><span class="relay-label">HotStuff</span><span class="relay-val" style="color:var(--muted)">'+(hs.msgs||0)+' msgs, '+hsAge+'</span></div>'+
'<div class="relay-row"><span class="relay-label">Subs</span><span class="relay-val">'+rf.subscribers+'</span></div>'+
'<div class="relay-row" style="margin-top:6px"><button class="btn btn-warn" style="font-size:.65rem;padding:4px 8px" data-relay-restart="'+esc(rf.symbol)+'">Restart</button></div></div>';
}
}else{rhtml='<div class="relay-card"><h3>Relay</h3><div class="relay-row"><span class="relay-label">No active feeds</span></div></div>';}
$('relay').innerHTML=rhtml;
$('relay').querySelectorAll('[data-relay-restart]').forEach(function(btn){
btn.onclick=function(){
var sym=this.getAttribute('data-relay-restart');
this.disabled=true;var self=this;
act(ADMIN_API+'/relay/restart?symbol='+encodeURIComponent(sym),'POST').finally(function(){self.disabled=false;});
};
});
var rows='';
var cfgBySid={};
for(var i=0;i<j.sessions.length;i++){
var s=j.sessions[i];
var sid=String(s.session_id||'');
cfgBySid[sid]=s.start_config||{};
var strat=String(s.strategy||'maker').toLowerCase();
rows+='<tr>'+
'<td><code>'+esc(sid)+'</code></td>'+
'<td><code>'+esc(s.user_id)+'</code></td>'+
'<td class="st-'+esc(s.status)+'">'+esc(s.status)+'</td>'+
'<td>'+esc(s.pid)+'</td>'+
'<td>'+esc((s.symbols||[]).join(', '))+'</td>'+
'<td class="ts">'+fmtts(s.started_at)+(s.stopped_at?'<br/>'+fmtts(s.stopped_at):'')+'</td>'+
'<td><pre class="addr">'+esc(s.agent_address)+'</pre></td>'+
'<td><pre class="addr">'+esc(s.wallet_address||'')+'</pre></td>'+
'<td>'+liveEqCell(s.live_equity)+'</td>'+
'<td><button class="btn btn-sec" style="font-size:.65rem;padding:4px 8px" data-act="cfg" data-sid="'+sid+'">'+esc(strat)+'</button></td>'+
'<td>'+metricCell(s.metrics||{},s.symbols||[],s.last_metrics_ts,s.status)+'</td>'+
'<td class="err">'+(s.error?esc(s.error):'')+'</td>'+
'<td class="actions">'+
'<button class="btn btn-warn" data-act="restart" data-sid="'+sid+'">Restart</button>'+
'<button class="btn btn-dan" data-act="stop" data-sid="'+sid+'">Stop</button>'+
'<button class="btn btn-sec" data-act="delete" data-sid="'+sid+'">Archive</button>'+
'<button class="btn btn-dan" data-act="hard-delete" data-sid="'+sid+'">Hard delete</button>'+
'<button class="btn btn-acc" data-act="logs" data-sid="'+sid+'">'+(s.log_lines>0?'Logs ('+s.log_lines+')':'Logs')+'</button>'+
'</td></tr>'+
'<tr class="cfg-row" id="cfgrow-'+sid+'" style="display:none"><td colspan="13"><pre class="cfg-box open" id="cfgbox-'+sid+'"></pre></td></tr>'+
'<tr class="log-row" id="logrow-'+sid+'" style="display:none"><td colspan="13"><div class="log-box open" id="logbox-'+sid+'">Loading...</div></td></tr>';}
tbody.innerHTML=rows;
tbody._cfgBySid=cfgBySid;
}catch(e){stat.textContent=String(e);tbody.innerHTML='';cards.innerHTML='';}
}
function showResult(ok,msg){
lastact.textContent=msg;
lastact.className=ok?'lastact-ok':'lastact-fail';
}
async function act(path,method){
try{
var r=await apicall(path,method);
var j=await r.json().catch(function(){return {};});
if(r.ok){
showResult(true,'OK '+method+' '+path+(j.pid!=null?' pid='+j.pid:'')+(j.archived?' archived':'')+(j.deleted?' deleted':'')+(j.purged!=null?' purged='+j.purged:'')+(j.metrics_kept?' metrics_kept':'')+(j.metrics_deleted?' metrics_deleted':'')+(j.status?' '+j.status:''));
}else{
showResult(false,'FAIL '+r.status+' '+JSON.stringify(j));
}
await load();
}catch(e){showResult(false,'Error: '+e);}
}
tbody.addEventListener('click',function(ev){
var tog=ev.target&&ev.target.closest?ev.target.closest('[data-toggle="met"]'):null;
if(tog){
var detail=tog.nextElementSibling;
if(detail)detail.classList.toggle('open');
return;
}
var b=ev.target&&ev.target.closest?ev.target.closest('button[data-act]'):null;
if(!b)return;
var a=b.getAttribute('data-act'),sid=b.getAttribute('data-sid');
if(!sid)return;
if(a==='cfg'){
var cr=document.getElementById('cfgrow-'+sid);
var cb=document.getElementById('cfgbox-'+sid);
if(cr&&cb){
if(cr.style.display==='none'){
cr.style.display='';
var cfg=(tbody._cfgBySid&&tbody._cfgBySid[sid])||{};
cb.textContent=cfgText(cfg);
}else{cr.style.display='none';}
}
return;
}
if(a==='logs'){
var lr=document.getElementById('logrow-'+sid);
if(lr){
if(lr.style.display==='none'){
lr.style.display='';
var lb=document.getElementById('logbox-'+sid);
lb.textContent='Loading...';
apicall(ADMIN_API+'/sessions/'+encodeURIComponent(sid)+'/logs').then(function(r){return r.json();}).then(function(j){
lb.textContent=j.lines&&j.lines.length?j.lines.join('\\n'):'No log output captured.';
lb.scrollTop=lb.scrollHeight;
}).catch(function(e){lb.textContent='Error: '+e;});
}else{lr.style.display='none';}
}
return;
}
if(a==='delete'&&!confirm('Archive session '+sid+'? This keeps metrics and allows later restart.'))return;
if(a==='hard-delete'&&!confirm('HARD delete session '+sid+'? This is irreversible and removes metrics too.'))return;
b.disabled=true;
var path,m;
if(a==='restart'){path=ADMIN_API+'/sessions/'+encodeURIComponent(sid)+'/restart';m='POST';}
else if(a==='stop'){path=ADMIN_API+'/sessions/'+encodeURIComponent(sid)+'/stop';m='POST';}
else if(a==='delete'){path=ADMIN_API+'/sessions/'+encodeURIComponent(sid);m='DELETE';}
else if(a==='hard-delete'){path=ADMIN_API+'/sessions/'+encodeURIComponent(sid)+'?hard=true';m='DELETE';}
else return;
act(path,m).finally(function(){b.disabled=false;});
});
$('purge').onclick=function(){
if(!confirm('Archive ALL stopped and error sessions? Metrics are kept and sessions remain restartable.'))return;
this.disabled=true;var self=this;
act(ADMIN_API+'/purge-stopped','POST').finally(function(){self.disabled=false;});
};
$('purge-hard').onclick=function(){
if(!confirm('HARD PURGE all stopped/error/archived sessions? This permanently deletes metrics and cannot be undone.'))return;
this.disabled=true;var self=this;
act(ADMIN_API+'/purge-stopped?hard=true','POST').finally(function(){self.disabled=false;});
};
$('relay-restart').onclick=function(){
if(!confirm('Restart all relay feeds (ref + HotStuff WS)?'))return;
this.disabled=true;var self=this;
act(ADMIN_API+'/relay/restart','POST').finally(function(){self.disabled=false;});
};
$('go').onclick=load;
var iv=null;
$('auto').onchange=function(){if(iv){clearInterval(iv);iv=null;}if(this.checked){iv=setInterval(load,10000);}};
load();
})();
</script>
</body>
</html>"""


# -- Admin routes ----------------------------------------------------------

@app.get(_ADMIN_UI_PATH, response_class=HTMLResponse)
async def admin_dashboard_page(_: None = Depends(require_local_admin_page)):
    return HTMLResponse(_ADMIN_DASHBOARD_HTML)


@app.get(f"{_ADMIN_API_PREFIX}/summary")
async def admin_summary(_: None = Depends(require_local_admin)):
    rows = db.list_all_sessions()
    status_counts = db.count_sessions_by_status()
    relay_status = sm.relay.status() if sm.relay else {"active_feeds": 0, "feeds": []}
    sessions = [_admin_session_summary(r) for r in rows]
    user_wallet = _user_wallet_map(rows)
    live_eq_by_user = await _fetch_live_equity_by_user(user_wallet)
    for s in sessions:
        uid = str(s.get("user_id") or "")
        s["live_equity"] = live_eq_by_user.get(uid)
        s["wallet_address"] = user_wallet.get(uid, "")
    return {
        "boot_time": _boot_time,
        "users_count": db.count_users(),
        "active_subprocesses": sm.active_count(),
        "session_status": status_counts,
        "relay": relay_status,
        "sessions": sessions,
    }


@app.post(f"{_ADMIN_API_PREFIX}/relay/restart")
async def admin_relay_restart(
    symbol: Optional[str] = Query(None),
    _: None = Depends(require_local_admin),
):
    if sm.relay is None:
        raise HTTPException(status_code=503, detail="Relay not initialized")
    if symbol:
        ok = await sm.relay.restart_feed(symbol)
        if not ok:
            raise HTTPException(status_code=404, detail=f"No active feed for {symbol}")
        return {"restarted": [symbol]}
    count = await sm.relay.restart_all()
    return {"restarted": sm.relay.active_symbols, "count": count}


@app.post(f"{_ADMIN_API_PREFIX}/sessions/{{session_id}}/restart")
async def admin_restart_session(session_id: str, _: None = Depends(require_local_admin)):
    try:
        pid = await sm.restart_session_from_db(session_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        db.update_session_status(session_id, "error", error=str(exc))
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {"session_id": session_id, "pid": pid, "status": "running"}


@app.post(f"{_ADMIN_API_PREFIX}/sessions/{{session_id}}/stop")
async def admin_stop_session(session_id: str, _: None = Depends(require_local_admin)):
    row = db.get_session(session_id)
    if not row:
        raise HTTPException(status_code=404, detail="Session not found")
    await sm.stop_session(session_id)
    return {"session_id": session_id, "status": "stopped"}


@app.delete(f"{_ADMIN_API_PREFIX}/sessions/{{session_id}}")
async def admin_delete_session(
    session_id: str,
    hard: bool = Query(False),
    _: None = Depends(require_local_admin),
):
    row = db.get_session(session_id)
    if not row:
        raise HTTPException(status_code=404, detail="Session not found")
    await sm.stop_session(session_id)
    sm.forget_session_memory(session_id)
    if hard:
        if not db.delete_session_cascade(session_id):
            raise HTTPException(status_code=404, detail="Session not found")
        return {
            "session_id": session_id,
            "deleted": True,
            "hard": True,
            "metrics_deleted": True,
        }
    now = datetime.now(timezone.utc).isoformat()
    if not db.archive_session(session_id, now):
        raise HTTPException(status_code=404, detail="Session not found")
    return {
        "session_id": session_id,
        "archived": True,
        "hard": False,
        "restartable": True,
        "metrics_kept": True,
        "status": "archived",
    }


@app.get(f"{_ADMIN_API_PREFIX}/sessions/{{session_id}}/logs")
async def admin_session_logs(session_id: str, _: None = Depends(require_local_admin)):
    row = db.get_session(session_id)
    if not row:
        raise HTTPException(status_code=404, detail="Session not found")
    return {"session_id": session_id, "lines": sm.get_logs(session_id)}


@app.post(f"{_ADMIN_API_PREFIX}/purge-stopped")
async def admin_purge_stopped(
    hard: bool = Query(False),
    _: None = Depends(require_local_admin),
):
    if hard:
        n = db.delete_stopped_sessions()
        return {"purged": n, "hard": True, "metrics_deleted": True}
    n = db.archive_stopped_sessions(datetime.now(timezone.utc).isoformat())
    return {"archived": n, "hard": False, "metrics_kept": True}
