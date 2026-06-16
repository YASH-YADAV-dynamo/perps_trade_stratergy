import json
import sqlite3
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

_DB_PATH = Path(os.getenv("BOT_DB_PATH", Path(__file__).resolve().parent / "bot.db"))

_SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    wallet_address TEXT UNIQUE NOT NULL,
    nonce TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES users(id),
    agent_address TEXT NOT NULL,
    encrypted_private_key BLOB NOT NULL,
    symbols TEXT NOT NULL,
    config_overrides TEXT,
    status TEXT NOT NULL DEFAULT 'starting',
    pid INTEGER,
    started_at TEXT NOT NULL,
    stopped_at TEXT,
    error TEXT
);
CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id, status);

CREATE TABLE IF NOT EXISTS metrics (
    session_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    ts TEXT NOT NULL,
    pnl REAL, inventory REAL, inv_tier INTEGER,
    total_fills INTEGER, total_volume REAL, round_trips INTEGER,
    spread_bps REAL, vol_bps REAL, alpha REAL, toxic REAL,
    adverse_rate REAL, avg_markout_1s REAL, avg_markout_5s REAL,
    guard_interventions INTEGER, guard_halted INTEGER,
    guard_spread_mult REAL, account_equity REAL,
    fair_mid REAL, hs_mid REAL, bn_mid REAL,
    PRIMARY KEY (session_id, symbol, ts)
) WITHOUT ROWID;
"""

_conn: Optional[sqlite3.Connection] = None


def get_conn() -> sqlite3.Connection:
    global _conn
    if _conn is None:
        _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        _conn = sqlite3.connect(str(_DB_PATH), check_same_thread=False)
        _conn.execute("PRAGMA journal_mode=WAL")
        _conn.execute("PRAGMA busy_timeout=5000")
        _conn.execute("PRAGMA synchronous=NORMAL")
        _conn.row_factory = sqlite3.Row
        _conn.executescript(_SCHEMA)
        _conn.commit()
    return _conn


def close():
    global _conn
    if _conn is not None:
        _conn.close()
        _conn = None


# -- Users -----------------------------------------------------------------

def upsert_user(user_id: str, wallet_address: str, nonce: str, created_at: str):
    c = get_conn()
    c.execute(
        "INSERT INTO users (id, wallet_address, nonce, created_at) "
        "VALUES (?, ?, ?, ?) "
        "ON CONFLICT(wallet_address) DO UPDATE SET nonce = excluded.nonce",
        (user_id, wallet_address, nonce, created_at),
    )
    c.commit()


def get_user_by_wallet(wallet_address: str) -> Optional[Dict[str, Any]]:
    row = get_conn().execute(
        "SELECT * FROM users WHERE wallet_address = ?", (wallet_address,)
    ).fetchone()
    return dict(row) if row else None


def get_user_by_id(user_id: str) -> Optional[Dict[str, Any]]:
    row = get_conn().execute(
        "SELECT * FROM users WHERE id = ?", (user_id,)
    ).fetchone()
    return dict(row) if row else None


def rotate_nonce(wallet_address: str, new_nonce: str):
    c = get_conn()
    c.execute(
        "UPDATE users SET nonce = ? WHERE wallet_address = ?",
        (new_nonce, wallet_address),
    )
    c.commit()


# -- Sessions --------------------------------------------------------------

def create_session(
    session_id: str, user_id: str, agent_address: str,
    encrypted_pk: bytes, symbols: List[str], config_overrides: Optional[str],
    started_at: str,
):
    c = get_conn()
    c.execute(
        "INSERT INTO sessions "
        "(id, user_id, agent_address, encrypted_private_key, symbols, "
        "config_overrides, status, started_at) "
        "VALUES (?, ?, ?, ?, ?, ?, 'starting', ?)",
        (session_id, user_id, agent_address, encrypted_pk,
         json.dumps(symbols), config_overrides, started_at),
    )
    c.commit()


def update_session_status(session_id: str, status: str, **kwargs):
    sets = ["status = ?"]
    vals: list = [status]
    for k, v in kwargs.items():
        sets.append(f"{k} = ?")
        vals.append(v)
    vals.append(session_id)
    c = get_conn()
    c.execute(f"UPDATE sessions SET {', '.join(sets)} WHERE id = ?", vals)
    c.commit()


def get_session(session_id: str, user_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
    if user_id:
        row = get_conn().execute(
            "SELECT * FROM sessions WHERE id = ? AND user_id = ?",
            (session_id, user_id),
        ).fetchone()
    else:
        row = get_conn().execute(
            "SELECT * FROM sessions WHERE id = ?",
            (session_id,),
        ).fetchone()
    return dict(row) if row else None


def list_sessions(user_id: str) -> List[Dict[str, Any]]:
    rows = get_conn().execute(
        "SELECT * FROM sessions WHERE user_id = ? AND status != 'archived' ORDER BY started_at DESC",
        (user_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def has_symbol_conflict(user_id: str, symbols: List[str]) -> Optional[Dict[str, Any]]:
    rows = get_conn().execute(
        "SELECT * FROM sessions WHERE user_id = ? AND status IN ('starting', 'running')",
        (user_id,),
    ).fetchall()
    for r in rows:
        existing_syms = set(json.loads(r["symbols"]))
        overlap = existing_syms & set(symbols)
        if overlap:
            return {"session_id": r["id"], "overlap": sorted(overlap)}
    return None


def update_session_config(session_id: str, config_json: str):
    c = get_conn()
    c.execute(
        "UPDATE sessions SET config_overrides = ? WHERE id = ?",
        (config_json, session_id),
    )
    c.commit()


def get_all_running_sessions() -> List[Dict[str, Any]]:
    rows = get_conn().execute(
        "SELECT * FROM sessions WHERE status = 'running'"
    ).fetchall()
    return [dict(r) for r in rows]


# -- Admin queries ---------------------------------------------------------

def count_users() -> int:
    return int(get_conn().execute("SELECT COUNT(*) FROM users").fetchone()[0])


def list_all_sessions(limit: int = 500) -> List[Dict[str, Any]]:
    rows = get_conn().execute(
        "SELECT * FROM sessions ORDER BY started_at DESC LIMIT ?",
        (limit,),
    ).fetchall()
    return [dict(r) for r in rows]


def count_sessions_by_status() -> Dict[str, int]:
    rows = get_conn().execute(
        "SELECT status, COUNT(*) FROM sessions GROUP BY status"
    ).fetchall()
    return {r[0]: r[1] for r in rows}


def delete_session_cascade(session_id: str) -> bool:
    c = get_conn()
    c.execute("DELETE FROM metrics WHERE session_id = ?", (session_id,))
    cur = c.execute("DELETE FROM sessions WHERE id = ?", (session_id,))
    c.commit()
    return cur.rowcount > 0


def archive_session(session_id: str, stopped_at: str) -> bool:
    c = get_conn()
    cur = c.execute(
        "UPDATE sessions "
        "SET status = 'archived', pid = NULL, stopped_at = COALESCE(stopped_at, ?) "
        "WHERE id = ?",
        (stopped_at, session_id),
    )
    c.commit()
    return cur.rowcount > 0


def archive_stopped_sessions(stopped_at: str) -> int:
    c = get_conn()
    cur = c.execute(
        "UPDATE sessions "
        "SET status = 'archived', pid = NULL, stopped_at = COALESCE(stopped_at, ?) "
        "WHERE status IN ('stopped', 'error')",
        (stopped_at,),
    )
    c.commit()
    return cur.rowcount


def delete_stopped_sessions() -> int:
    c = get_conn()
    sids = [r[0] for r in c.execute(
        "SELECT id FROM sessions WHERE status IN ('stopped', 'error', 'archived')"
    ).fetchall()]
    if not sids:
        return 0
    ph = ",".join("?" * len(sids))
    c.execute(f"DELETE FROM metrics WHERE session_id IN ({ph})", sids)
    cur = c.execute(f"DELETE FROM sessions WHERE id IN ({ph})", sids)
    c.commit()
    return cur.rowcount


# -- Metrics ---------------------------------------------------------------

def insert_metrics(rows: List[Tuple]):
    if not rows:
        return
    c = get_conn()
    c.executemany(
        "INSERT OR REPLACE INTO metrics "
        "(session_id, symbol, ts, pnl, inventory, inv_tier, total_fills, total_volume, "
        "round_trips, spread_bps, vol_bps, alpha, toxic, adverse_rate, "
        "avg_markout_1s, avg_markout_5s, guard_interventions, guard_halted, "
        "guard_spread_mult, account_equity, fair_mid, hs_mid, bn_mid) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    c.commit()


def get_metrics_history(
    session_id: str, symbol: Optional[str] = None,
    since: Optional[str] = None, until: Optional[str] = None,
    limit: int = 500,
) -> List[Dict[str, Any]]:
    sql = "SELECT * FROM metrics WHERE session_id = ?"
    params: list = [session_id]
    if symbol:
        sql += " AND symbol = ?"
        params.append(symbol)
    if since:
        sql += " AND ts >= ?"
        params.append(since)
    if until:
        sql += " AND ts <= ?"
        params.append(until)
    sql += " ORDER BY ts DESC LIMIT ?"
    params.append(limit)
    rows = get_conn().execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def get_latest_metrics(session_id: str) -> Dict[str, Dict[str, Any]]:
    """Return the latest metric row per symbol for a session."""
    rows = get_conn().execute(
        "SELECT m.* FROM metrics m "
        "INNER JOIN (SELECT symbol, MAX(ts) as max_ts FROM metrics "
        "WHERE session_id = ? GROUP BY symbol) latest "
        "ON m.session_id = ? AND m.symbol = latest.symbol AND m.ts = latest.max_ts",
        (session_id, session_id),
    ).fetchall()
    return {r["symbol"]: dict(r) for r in rows}


def get_metrics_cumulative(session_id: str) -> Dict[str, Dict[str, Any]]:
    """Return per-symbol cumulative maxima for monotonic metric counters."""
    rows = get_conn().execute(
        "SELECT symbol, "
        "MAX(total_fills) AS total_fills, "
        "MAX(total_volume) AS total_volume, "
        "MAX(round_trips) AS round_trips, "
        "MAX(guard_interventions) AS guard_interventions "
        "FROM metrics WHERE session_id = ? GROUP BY symbol",
        (session_id,),
    ).fetchall()
    return {r["symbol"]: dict(r) for r in rows}
