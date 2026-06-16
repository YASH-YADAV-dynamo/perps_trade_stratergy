import asyncio
import json
import logging
import os
import signal
import sys
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from eth_account import Account
from hotstuff import InfoClient
from hotstuff.methods.info.account import AgentsParams
from web3 import Web3

from . import db
from . import crypto
from .models import build_config, build_taker_config
from .relay import Relay

log = logging.getLogger("bot-api.sessions")

_MAKER_STRATEGY_DIR = Path(__file__).resolve().parent / "maker"
_MAKER_STRATEGY_MAIN = _MAKER_STRATEGY_DIR / "main.py"
_TAKER_STRATEGY_DIR = Path(__file__).resolve().parent / "taker"
_TAKER_STRATEGY_MAIN = _TAKER_STRATEGY_DIR / "main.py"
_TAKER_CFG_ENV = {
    "min_spread_usd": "TAKER_MIN_SPREAD_USD",
    "min_spread_bps": "TAKER_MIN_SPREAD_BPS",
    "take_profit_bps": "TAKER_TAKE_PROFIT_BPS",
    "close_bps": "TAKER_CLOSE_BPS",
    "close_timeout_ms": "TAKER_CLOSE_TIMEOUT_MS",
    "order_size_usd": "TAKER_ORDER_SIZE_USD",
    "target_exposure_x": "TAKER_TARGET_EXPOSURE_X",
    "leverage": "TAKER_LEVERAGE",
    "cooldown_s": "TAKER_COOLDOWN_S",
    "max_loss_usd": "TAKER_MAX_LOSS_USD",
    "order_expiry_ms": "TAKER_ORDER_EXPIRY_MS",
    "market_bias": "TAKER_MARKET_BIAS",
}

# session_id -> (process, reader_task)
_processes: Dict[str, Tuple[asyncio.subprocess.Process, asyncio.Task]] = {}

# session_id -> {symbol: latest_metrics_dict}
_snapshots: Dict[str, Dict[str, Dict[str, Any]]] = {}

# session_id -> (relay_port, symbols_list)
_session_relay: Dict[str, Tuple[int, List[str]]] = {}

_next_port: int = 17100

# sessions being intentionally stopped -- reader should not touch DB
_stopping: set = set()

# per-session ring buffer of raw stdout lines (metrics JSON + log text)
_LOG_BUFFER_SIZE = 150
_log_buffers: Dict[str, deque] = {}

_TIMEOUT_LINE_MARKERS = (
    "Receive error: Connection timed out",
    "Keep-alive error: Request timeout",
)
_TIMEOUT_RESTART_WINDOW_S = float(os.getenv("BOT_TIMEOUT_RESTART_WINDOW_S", "420"))
_TIMEOUT_RESTART_THRESHOLD = int(os.getenv("BOT_TIMEOUT_RESTART_THRESHOLD", "20"))
_TIMEOUT_RESTART_COOLDOWN_S = float(os.getenv("BOT_TIMEOUT_RESTART_COOLDOWN_S", "900"))
_TIMEOUT_RESTART_METRIC_STALE_S = float(os.getenv("BOT_TIMEOUT_RESTART_METRIC_STALE_S", "30"))
_timeout_events: Dict[str, deque] = {}
_timeout_restart_until: Dict[str, float] = {}
_timeout_restart_tasks: Dict[str, asyncio.Task] = {}
_last_metric_ts: Dict[str, float] = {}
_timeout_skip_log_until: Dict[str, float] = {}

relay: Optional[Relay] = None

_metrics_buffer: list = []
_FLUSH_INTERVAL = 10.0
_flush_task: Optional[asyncio.Task] = None
_hs_info_client: Optional[InfoClient] = None
_validated_wallet_agent_links: set[Tuple[str, str]] = set()
_STRATEGY_ENV_PASSTHROUGH = (
    "PATH",
    "HOME",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "TZ",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "NO_PROXY",
    "REQUESTS_CA_BUNDLE",
    "CURL_CA_BUNDLE",
    "PYTHONPATH",
)


def get_snapshot(session_id: str, symbol: Optional[str] = None) -> Optional[Dict[str, Any]]:
    snaps = _snapshots.get(session_id)
    if snaps is None:
        return None
    if symbol:
        return snaps.get(symbol)
    return snaps


def active_count() -> int:
    return len(_processes)


def _strategy_base_env() -> Dict[str, str]:
    env: Dict[str, str] = {}
    for key in _STRATEGY_ENV_PASSTHROUGH:
        val = os.environ.get(key)
        if val:
            env[key] = val
    # Keep strategy logging deterministic and avoid inheriting unrelated repo .env values.
    env["LOG_LEVEL"] = os.environ.get("DXD_STRATEGY_LOG_LEVEL", os.environ.get("LOG_LEVEL", "INFO"))
    env["PYTHONUNBUFFERED"] = "1"
    env["DXD_STRATEGY_MODE"] = "api"
    env["DXD_STRATEGY_DISABLE_DOTENV"] = "1"
    for key in ("DXD_BROKER_ADDRESS", "DXD_BROKER_FEE"):
        val = os.environ.get(key)
        if val:
            env[key] = val
    return env


def forget_session_memory(session_id: str) -> None:
    _snapshots.pop(session_id, None)
    _session_relay.pop(session_id, None)
    _log_buffers.pop(session_id, None)
    _clear_timeout_tracking(session_id)


def get_logs(session_id: str) -> List[str]:
    buf = _log_buffers.get(session_id)
    return list(buf) if buf else []


async def _cleanup_relay(session_id: str) -> None:
    relay_info = _session_relay.pop(session_id, None)
    if relay is not None and relay_info is not None:
        port, syms = relay_info
        for sym in syms:
            await relay.unregister(sym, port)


def _clear_timeout_tracking(session_id: str) -> None:
    _timeout_events.pop(session_id, None)
    _timeout_restart_until.pop(session_id, None)
    _last_metric_ts.pop(session_id, None)
    _timeout_skip_log_until.pop(session_id, None)
    task = _timeout_restart_tasks.pop(session_id, None)
    try:
        current_task = asyncio.current_task()
    except RuntimeError:
        current_task = None
    if task and not task.done() and task is not current_task:
        task.cancel()


def _track_timeout_line(session_id: str, line: str) -> bool:
    if _TIMEOUT_RESTART_THRESHOLD <= 0:
        return False
    if not any(marker in line for marker in _TIMEOUT_LINE_MARKERS):
        return False

    now = time.monotonic()
    window = _timeout_events.setdefault(session_id, deque())
    window.append(now)
    cutoff = now - _TIMEOUT_RESTART_WINDOW_S
    while window and window[0] < cutoff:
        window.popleft()

    cooldown_until = _timeout_restart_until.get(session_id, 0.0)
    if now < cooldown_until:
        return False
    if len(window) < _TIMEOUT_RESTART_THRESHOLD:
        return False
    metric_ts = _last_metric_ts.get(session_id, now)
    metric_age = now - metric_ts
    if metric_age < _TIMEOUT_RESTART_METRIC_STALE_S:
        log_after = _timeout_skip_log_until.get(session_id, 0.0)
        if now >= log_after:
            _timeout_skip_log_until[session_id] = now + 60.0
            log.warning(
                "Session %s: timeout streak but metrics fresh (age=%.1fs), skip auto-restart",
                session_id,
                metric_age,
            )
        return False
    return True


async def _auto_restart_after_timeouts(session_id: str) -> None:
    try:
        if session_id in _stopping:
            return
        row = db.get_session(session_id)
        if row is None or row.get("status") != "running":
            return
        log.warning(
            "Session %s: auto-restarting after repeated transport timeouts",
            session_id,
        )
        await restart_session_from_db(session_id)
    except Exception as exc:
        log.error(
            "Session %s: auto-restart failed after timeout streak: %s",
            session_id,
            exc,
        )


def _schedule_timeout_restart(session_id: str) -> None:
    existing = _timeout_restart_tasks.get(session_id)
    if existing is not None and not existing.done():
        return

    _timeout_restart_until[session_id] = time.monotonic() + _TIMEOUT_RESTART_COOLDOWN_S
    task = asyncio.create_task(_auto_restart_after_timeouts(session_id))
    _timeout_restart_tasks[session_id] = task

    def _drop_task(_done: asyncio.Task) -> None:
        current = _timeout_restart_tasks.get(session_id)
        if current is _done:
            _timeout_restart_tasks.pop(session_id, None)

    task.add_done_callback(_drop_task)


def _allocate_port() -> int:
    global _next_port
    port = _next_port
    _next_port += 1
    return port


def _get_hs_info_client() -> InfoClient:
    global _hs_info_client
    if _hs_info_client is None:
        _hs_info_client = InfoClient(is_testnet=False)
    return _hs_info_client


def _to_checksum(addr: str) -> str:
    try:
        return Web3.to_checksum_address(addr)
    except Exception as exc:
        raise RuntimeError(f"Invalid address: {addr}") from exc


async def validate_session_credentials(
    private_key: str,
    agent_address: str,
    account_address: Optional[str],
) -> None:
    try:
        signer = Account.from_key(private_key).address
    except Exception as exc:
        raise RuntimeError(f"Invalid agent_private_key: {exc}") from exc

    agent_cs = _to_checksum(agent_address)
    if signer.lower() != agent_cs.lower():
        raise RuntimeError(
            f"agent_private_key does not match agent_address (derived={signer}, provided={agent_cs})"
        )

    account_cs = _to_checksum(account_address or agent_cs)
    if account_cs.lower() == agent_cs.lower():
        return

    cache_key = (account_cs.lower(), agent_cs.lower())
    if cache_key in _validated_wallet_agent_links:
        return

    try:
        info = _get_hs_info_client()
        # hotstuff-python-sdk method name differs across versions.
        # Current versions expose `all_agents`; older ones may have `agents`.
        all_agents_fn = getattr(info, "all_agents", None) or getattr(info, "agents", None)
        if all_agents_fn is None:
            raise RuntimeError("HotStuff InfoClient missing all_agents/agents method")
        agents = await asyncio.to_thread(all_agents_fn, AgentsParams(user=account_cs))
    except Exception as exc:
        raise RuntimeError(f"Failed wallet/agent linkage check: {exc}") from exc

    agent_rows = agents if isinstance(agents, list) else getattr(agents, "agents", [])
    target_agent = agent_cs.lower()
    linked = False
    for item in agent_rows:
        if isinstance(item, dict):
            addr = item.get("agent_address") or item.get("address") or ""
        else:
            # HotStuff Agent rows use `agent_address` (not `address`) in current SDK.
            addr = (
                getattr(item, "agent_address", None)
                or getattr(item, "address", None)
                or ""
            )
        if str(addr).lower() == target_agent:
            linked = True
            break
    if not linked:
        raise RuntimeError(
            f"agent_address {agent_cs} is not linked under wallet {account_cs} (allAgents)"
        )

    _validated_wallet_agent_links.add(cache_key)


def _session_account_address(row: Dict[str, Any]) -> str:
    user_id = row.get("user_id")
    if user_id:
        user = db.get_user_by_id(str(user_id))
        if user and user.get("wallet_address"):
            return str(user["wallet_address"])
    return str(row.get("agent_address", ""))


def _parse_overrides(row: Dict[str, Any]) -> Dict[str, Any]:
    raw = row.get("config_overrides")
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return {}


def _stored_strategy(overrides: Dict[str, Any]) -> str:
    strategy = str(overrides.get("strategy") or "maker").strip().lower()
    return strategy if strategy in ("maker", "taker") else "maker"


def _maker_symbol_configs(overrides: Dict[str, Any], symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    global_cfg = overrides.get("global", {})
    per_sym = overrides.get("per_symbol", {})
    merged_cfg: Dict[str, Dict[str, Any]] = {}
    for sym in symbols:
        merged = {**global_cfg, **per_sym.get(sym, {})}
        merged_cfg[sym] = build_config(sym, merged)
    return merged_cfg


def _taker_strategy_options(overrides: Dict[str, Any]) -> Dict[str, Any]:
    taker = overrides.get("taker") if isinstance(overrides.get("taker"), dict) else {}
    symbol = str(taker.get("symbol") or "")
    if not symbol:
        raise RuntimeError("Missing taker symbol in session config")
    cfg = build_taker_config(symbol, taker.get("config") if isinstance(taker.get("config"), dict) else {})
    return {
        "config": cfg,
    }


def _row_symbols(row: Dict[str, Any]) -> List[str]:
    raw = row.get("symbols")
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except (TypeError, ValueError) as exc:
            raise RuntimeError("Invalid symbols payload in session row") from exc
        if isinstance(parsed, list):
            return parsed
    raise RuntimeError("Missing symbols in session row")


def _taker_symbol(overrides: Dict[str, Any], symbols: List[str]) -> str:
    taker = overrides.get("taker") if isinstance(overrides.get("taker"), dict) else {}
    sym = str(taker.get("symbol") or (symbols[0] if symbols else ""))
    if not sym:
        raise RuntimeError("Missing taker symbol in session config")
    return sym


async def _start_session_from_row(row: Dict[str, Any]) -> int:
    sid = str(row.get("id") or "")
    if not sid:
        raise RuntimeError("Missing session id")

    stored = _parse_overrides(row)
    strategy = _stored_strategy(stored)
    symbols = _row_symbols(row)
    pk = crypto.decrypt(row["encrypted_private_key"])
    agent_address = row["agent_address"]
    account_address = _session_account_address(row)

    if strategy == "maker":
        per_symbol_cfgs = _maker_symbol_configs(stored, symbols)
        return await start_session(
            session_id=sid,
            private_key=pk,
            agent_address=agent_address,
            account_address=account_address,
            symbols=symbols,
            symbol_configs=per_symbol_cfgs,
            strategy="maker",
        )

    sym = _taker_symbol(stored, symbols)
    return await start_session(
        session_id=sid,
        private_key=pk,
        agent_address=agent_address,
        account_address=account_address,
        symbols=[sym],
        symbol_configs=None,
        strategy="taker",
        strategy_options=_taker_strategy_options(stored),
    )


async def start_session(
    session_id: str,
    private_key: str,
    agent_address: str,
    account_address: Optional[str],
    symbols: List[str],
    symbol_configs: Optional[Dict[str, Dict[str, Any]]] = None,
    strategy: str = "maker",
    strategy_options: Optional[Dict[str, Any]] = None,
) -> int:
    if session_id in _processes:
        raise RuntimeError(f"Session {session_id} already running")

    strategy = (strategy or "maker").strip().lower()
    if strategy not in ("maker", "taker"):
        raise RuntimeError(f"Unsupported strategy: {strategy}")

    await validate_session_credentials(private_key, agent_address, account_address)

    env = _strategy_base_env()
    env["HOTSTUFF_PRIVATE_KEY"] = private_key
    env["HOTSTUFF_AGENT_ADDRESS"] = agent_address
    env["HOTSTUFF_ACCOUNT_ADDRESS"] = account_address or agent_address

    cmd: List[str]
    strategy_dir: Path
    port = None
    if strategy == "maker":
        # Default live trading; set DXD_MM_ENABLE_TRADING=false on API process for dry-run E2E
        env["MM_ENABLE_TRADING"] = os.environ.get(
            "DXD_MAKER_ENABLE_TRADING",
            os.environ.get("DXD_MM_ENABLE_TRADING", "true"),
        )
        cmd = [
            sys.executable,
            str(_MAKER_STRATEGY_MAIN),
            "--session-id",
            session_id,
            "--symbols",
            ",".join(symbols),
        ]
        if symbol_configs:
            cmd.extend(["--symbol-configs", json.dumps(symbol_configs)])
        if relay is not None:
            port = _allocate_port()
            cmd.extend(["--relay-port", str(port)])
            _session_relay[session_id] = (port, symbols)
            for sym in symbols:
                await relay.register(sym, port)
        strategy_dir = _MAKER_STRATEGY_DIR
    else:
        if len(symbols) != 1:
            raise RuntimeError("taker strategy requires exactly one symbol")
        opts = strategy_options or {}
        env["TAKER_SYMBOL"] = symbols[0]
        env["DN_SYMBOL"] = symbols[0]  # Backward compatibility for older taker runs.
        env["TAKER_ENABLE_TRADING"] = os.environ.get(
            "DXD_TAKER_ENABLE_TRADING",
            os.environ.get("DXD_DN_ENABLE_TRADING", os.environ.get("DXD_MM_ENABLE_TRADING", "true")),
        )
        env["DN_ENABLE_TRADING"] = env["TAKER_ENABLE_TRADING"]
        cfg = opts.get("config")
        if isinstance(cfg, dict):
            cfg = build_taker_config(symbols[0], cfg)
        else:
            cfg = build_taker_config(symbols[0], {})
        for key, env_key in _TAKER_CFG_ENV.items():
            if key in cfg and cfg[key] is not None:
                env[env_key] = str(cfg[key])
        # Backward-compatible DN_* env mirrors.
        env["DN_MIN_SPREAD_USD"] = env.get("TAKER_MIN_SPREAD_USD", env.get("DN_MIN_SPREAD_USD", "1.0"))
        env["DN_LEVERAGE"] = env.get("TAKER_LEVERAGE", env.get("DN_LEVERAGE", "20"))
        env["DN_COOLDOWN_S"] = env.get("TAKER_COOLDOWN_S", env.get("DN_COOLDOWN_S", "0.05"))
        env["DN_MAX_LOSS_USD"] = env.get("TAKER_MAX_LOSS_USD", env.get("DN_MAX_LOSS_USD", "5.0"))
        env["DN_ORDER_EXPIRY_MS"] = env.get("TAKER_ORDER_EXPIRY_MS", env.get("DN_ORDER_EXPIRY_MS", "15000"))
        cmd = [
            sys.executable,
            str(_TAKER_STRATEGY_MAIN),
            "--session-id",
            session_id,
            "--symbol",
            symbols[0],
        ]
        if relay is not None:
            port = _allocate_port()
            cmd.extend(["--relay-port", str(port)])
            _session_relay[session_id] = (port, symbols)
            for sym in symbols:
                await relay.register(sym, port)
        strategy_dir = _TAKER_STRATEGY_DIR

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        env=env,
        cwd=str(strategy_dir),
    )

    _last_metric_ts[session_id] = time.monotonic()
    reader = asyncio.create_task(_read_stdout(session_id, proc))
    _processes[session_id] = (proc, reader)

    db.update_session_status(session_id, "running", pid=proc.pid, error=None, stopped_at=None)
    log.info(
        "Session %s started (pid=%d, strategy=%s, symbols=%s, relay_port=%s)",
        session_id,
        proc.pid,
        strategy,
        symbols,
        port,
    )
    return proc.pid


async def stop_session(session_id: str):
    entry = _processes.get(session_id)
    if entry is None:
        _clear_timeout_tracking(session_id)
        db.update_session_status(
            session_id, "stopped",
            stopped_at=datetime.now(timezone.utc).isoformat(),
        )
        return

    _stopping.add(session_id)
    proc, reader = entry
    if proc.returncode is None:
        try:
            proc.send_signal(signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            await asyncio.wait_for(proc.wait(), timeout=15.0)
        except asyncio.TimeoutError:
            log.warning("Session %s did not exit gracefully, killing", session_id)
            proc.kill()
            await proc.wait()

    reader.cancel()
    _processes.pop(session_id, None)
    _snapshots.pop(session_id, None)
    await _cleanup_relay(session_id)
    _clear_timeout_tracking(session_id)
    _stopping.discard(session_id)

    db.update_session_status(
        session_id, "stopped",
        stopped_at=datetime.now(timezone.utc).isoformat(),
        pid=None, error=None,
    )
    log.info("Session %s stopped", session_id)


async def restart_session(session_id: str, new_symbol_configs: Dict[str, Dict]) -> int:
    """Stop a session and restart it with updated per-symbol configs."""
    row = db.get_session(session_id)
    if row is None:
        raise RuntimeError(f"Session {session_id} not found")
    overrides = _parse_overrides(row)
    if _stored_strategy(overrides) != "maker":
        raise RuntimeError("Config patch restart is only supported for maker sessions")

    await stop_session(session_id)

    config_json = json.dumps({
        "strategy": "maker",
        "global": {},
        "per_symbol": new_symbol_configs,
    })
    db.update_session_config(session_id, config_json)

    pk = crypto.decrypt(row["encrypted_private_key"])
    symbols = _row_symbols(row)
    pid = await start_session(
        session_id=session_id,
        private_key=pk,
        agent_address=row["agent_address"],
        account_address=_session_account_address(row),
        symbols=symbols,
        symbol_configs=new_symbol_configs,
        strategy="maker",
    )
    return pid


async def restart_session_from_db(session_id: str) -> int:
    """Stop subprocess if any, then start again using encrypted key and config from DB."""
    row = db.get_session(session_id)
    if row is None:
        raise RuntimeError(f"Session {session_id} not found")

    await stop_session(session_id)

    row = db.get_session(session_id)
    if row is None:
        raise RuntimeError(f"Session {session_id} missing after stop")

    return await _start_session_from_row(row)


async def stop_all():
    ids = list(_processes.keys())
    await asyncio.gather(*(stop_session(sid) for sid in ids), return_exceptions=True)


async def resume_running_sessions():
    rows = db.get_all_running_sessions()
    for row in rows:
        sid = row["id"]
        try:
            await _start_session_from_row(row)
            log.info("Resumed session %s", sid)
        except Exception as exc:
            log.error("Failed to resume session %s: %s", sid, exc)
            db.update_session_status(sid, "error", error=str(exc))


async def _read_stdout(session_id: str, proc: asyncio.subprocess.Process):
    buf = _log_buffers.setdefault(session_id, deque(maxlen=_LOG_BUFFER_SIZE))
    try:
        while True:
            raw = await proc.stdout.readline()
            if not raw:
                break
            line = raw.decode("utf-8", errors="replace").rstrip("\n")
            buf.append(line)
            try:
                payload = json.loads(line)
                sym = payload.get("symbol", "")
                if sym:
                    if session_id not in _snapshots:
                        _snapshots[session_id] = {}
                    _snapshots[session_id][sym] = payload
                    _last_metric_ts[session_id] = time.monotonic()
                _metrics_buffer.append(_payload_to_row(session_id, payload))
            except (json.JSONDecodeError, KeyError):
                if line:
                    log.info("[%s] %s", session_id, line)
                    if _track_timeout_line(session_id, line):
                        _schedule_timeout_restart(session_id)
    except asyncio.CancelledError:
        return
    finally:
        rc = proc.returncode
        if rc is None:
            try:
                rc = await proc.wait()
            except Exception:
                rc = proc.returncode
        current = _processes.get(session_id)
        is_current_proc = current is not None and current[0] is proc
        if is_current_proc:
            _processes.pop(session_id, None)
            _snapshots.pop(session_id, None)
            await _cleanup_relay(session_id)
            _clear_timeout_tracking(session_id)
        if session_id in _stopping or not is_current_proc:
            return
        if rc is not None and rc != 0:
            db.update_session_status(
                session_id, "error",
                stopped_at=datetime.now(timezone.utc).isoformat(),
                error=f"exit_code={rc}",
            )
            log.warning("Session %s exited with code %d", session_id, rc)
        elif rc == 0:
            db.update_session_status(
                session_id, "stopped",
                stopped_at=datetime.now(timezone.utc).isoformat(),
            )
            log.info("Session %s exited cleanly", session_id)
        else:
            db.update_session_status(
                session_id,
                "error",
                stopped_at=datetime.now(timezone.utc).isoformat(),
                error="unknown_exit",
            )
            log.warning("Session %s exited with unknown status", session_id)


def _payload_to_row(session_id: str, p: dict) -> tuple:
    return (
        session_id,
        p.get("symbol", ""),
        p.get("ts", ""),
        p.get("pnl", 0.0),
        p.get("inventory", 0.0),
        p.get("inv_tier", 0),
        p.get("total_fills", 0),
        p.get("total_volume_usd", 0.0),
        p.get("round_trips", 0),
        p.get("spread_bps", 0.0),
        p.get("vol_bps", 0.0),
        p.get("alpha", 0.0),
        p.get("toxic", 0.0),
        p.get("adverse_rate", 0.0),
        p.get("avg_markout_1s", 0.0),
        p.get("avg_markout_5s", 0.0),
        p.get("guard_interventions", 0),
        int(p.get("guard_halted", False)),
        p.get("guard_spread_mult", 1.0),
        p.get("account_equity", 0.0),
        p.get("fair_mid", 0.0),
        p.get("hs_mid", 0.0),
        p.get("bn_mid", 0.0),
    )


async def _flush_metrics_loop():
    global _metrics_buffer
    while True:
        await asyncio.sleep(_FLUSH_INTERVAL)
        if _metrics_buffer:
            batch = _metrics_buffer
            _metrics_buffer = []
            try:
                db.insert_metrics(batch)
            except Exception as exc:
                log.error("Metrics flush failed: %s", exc)


def start_flush_task():
    global _flush_task
    if _flush_task is None or _flush_task.done():
        _flush_task = asyncio.create_task(_flush_metrics_loop())


def stop_flush_task():
    global _flush_task
    if _flush_task and not _flush_task.done():
        _flush_task.cancel()
        _flush_task = None
    if _metrics_buffer:
        try:
            db.insert_metrics(_metrics_buffer)
            _metrics_buffer.clear()
        except Exception:
            pass
