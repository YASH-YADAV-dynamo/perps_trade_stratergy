# Bot API -- Deployment & Operations

## What runs

- `uvicorn dxd_api.server:app --host 0.0.0.0 --port 8199 --workers 1`
- SQLite DB at `dxd_api/dxd.db` (override with `DXD_DB_PATH`)
- Market data relay in-process -- do not run multiple uvicorn workers

---

## Environment Variables

### Runtime env loading model (authoritative)

DXD API does **not** call `python-dotenv`. It reads only the process environment provided by the runtime:

- `systemd` production: `EnvironmentFile=/etc/dxd-api.env`
- Docker: `environment:` or `--env-file`
- Local shell: exported vars in your terminal session

Strategy subprocesses (`dxd_api/maker`, `dxd_api/taker`) are launched with an isolated env by `dxd_api/session_manager.py`:

- only a small passthrough set is inherited (`PATH`, `HOME`, locale, TLS/proxy vars, `PYTHONPATH`)
- API forces `DXD_STRATEGY_MODE=api` and `DXD_STRATEGY_DISABLE_DOTENV=1`
- session-specific signer vars are injected (`HOTSTUFF_PRIVATE_KEY`, `HOTSTUFF_AGENT_ADDRESS`, `HOTSTUFF_ACCOUNT_ADDRESS`)
- strategy toggles/config are injected (`MM_ENABLE_TRADING`, `TAKER_*`, legacy `DN_*` mirrors)

Standalone strategy runs (direct `python dxd_api/maker/main.py` / `python dxd_api/taker/main.py`) do load repo `.env` unless `DXD_STRATEGY_DISABLE_DOTENV=1`.

See `dxd_api/secrets.md` for secret inventory, rotation, and safety rules.

### API process vars

| Variable | Required | Default | Description |
|---|---|---|---|
| `DXD_MASTER_ENCRYPTION_KEY` | yes | -- | Fernet key for agent private keys in DB. Generate: `python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"` |
| `DXD_JWT_SECRET` | no | falls back to `DXD_MASTER_ENCRYPTION_KEY` | JWT signing secret for wallet auth tokens. |
| `DXD_ADMIN_TOKEN` | no | -- | Bearer token for `/admin-8888` and `/v1/admin-8888/*`. |
| `DXD_DB_PATH` | no | `dxd_api/dxd.db` | Absolute path to SQLite file. |
| `DXD_CORS_ORIGINS` | no | `*` | Comma-separated CORS allowlist. |
| `DXD_MAKER_ENABLE_TRADING` | no | `true` | Preferred maker execution toggle (`false` = dry-run quotes only). |
| `DXD_TAKER_ENABLE_TRADING` | no | `true` | Preferred taker execution toggle (`false` = dry-run). |
| `DXD_MM_ENABLE_TRADING` | no | `true` | Legacy fallback if `DXD_MAKER_ENABLE_TRADING` is not set. |
| `DXD_DN_ENABLE_TRADING` | no | `true` | Legacy fallback if `DXD_TAKER_ENABLE_TRADING` is not set. |
| `DXD_STRATEGY_LOG_LEVEL` | no | `INFO` | Log level injected into maker/taker subprocesses. |
| `DXD_BROKER_ADDRESS` | no | empty | Optional broker address forwarded to maker worker. |
| `DXD_BROKER_FEE` | no | `0.00001` | Optional broker fee forwarded to maker worker. |

Store secrets in `/etc/dxd-api.env` (chmod 600) on servers. Never commit real secrets.

---

## Ubuntu Server Setup

### One-time

```bash
sudo apt-get update && sudo apt-get install -y python3.12-venv python3-pip rsync
```

### App directory

Default: `/home/ubuntu/dxd-backend`. Sync from laptop:

```bash
rsync -az --delete \
  --exclude='.git' --exclude='__pycache__' --exclude='*.pyc' \
  --exclude='.venv' --exclude='*.db' --exclude='*.db-wal' --exclude='*.db-shm' \
  --exclude='.env' \
  ./ ubuntu@YOUR_IP:~/dxd-backend/
```

### Virtualenv

```bash
cd ~/dxd-backend
python3 -m venv .venv
. .venv/bin/activate
pip install -U pip && pip install -r requirements.txt
```

### Secrets

```bash
sudo bash -c 'KEY=$(/home/ubuntu/dxd-backend/.venv/bin/python -c \
  "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())") && \
  umask 077 && printf "DXD_MASTER_ENCRYPTION_KEY=%s\n" "$KEY" > /etc/dxd-api.env'
sudo chmod 600 /etc/dxd-api.env
```

Add admin token and optional flags:

```bash
sudo bash -c 'echo "DXD_ADMIN_TOKEN=$(openssl rand -hex 24)" >> /etc/dxd-api.env'
```

### systemd

Create `/etc/systemd/system/dxd-api.service`:

```ini
[Unit]
Description=DXD Aggressive MM API
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=ubuntu
Group=ubuntu
WorkingDirectory=/home/ubuntu/dxd-backend
EnvironmentFile=/etc/dxd-api.env
ExecStart=/home/ubuntu/dxd-backend/.venv/bin/python -m uvicorn dxd_api.server:app --host 0.0.0.0 --port 8199 --workers 1
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable dxd-api
sudo systemctl restart dxd-api
sudo journalctl -u dxd-api -f
```

---

## Code Updates

```bash
# laptop (repo root)
rsync -az --delete \
  --exclude='.git' --exclude='__pycache__' --exclude='*.pyc' \
  --exclude='.venv' --exclude='*.db' --exclude='*.db-wal' --exclude='*.db-shm' \
  --exclude='.env' \
  ./ ubuntu@YOUR_IP:~/dxd-backend/

# server
sudo systemctl restart dxd-api
```

---

## SSH Tunnel

Forward admin access to your laptop (VM-local only):

```bash
ssh -N -L 8888:127.0.0.1:8199 ubuntu@YOUR_VM_IP
```

Then open `http://127.0.0.1:8888/admin-8888` locally.

---

## Admin Dashboard

UI at `GET /admin-8888`. Enter `DXD_ADMIN_TOKEN` once (stored in browser sessionStorage).
Admin endpoints are VM-local only; remote direct calls return `403`.

### Admin API

All routes require `Authorization: Bearer <DXD_ADMIN_TOKEN>`.

| Method | Path | Description |
|---|---|---|
| GET | `/admin-8888` | HTML ops console |
| GET | `/v1/admin-8888/summary` | Users, subprocess count, session list with metrics, status breakdown |
| POST | `/v1/admin-8888/sessions/{id}/restart` | Restart from DB config (decrypts stored key) |
| POST | `/v1/admin-8888/sessions/{id}/stop` | Stop subprocess, mark stopped |
| DELETE | `/v1/admin-8888/sessions/{id}` | Soft-delete by default: stop + archive session, keep metrics, remains restartable (`?hard=true` for irreversible delete + metrics wipe) |
| POST | `/v1/admin-8888/purge-stopped` | Soft archive all stopped/error by default (`?hard=true` for irreversible purge including archived rows + metrics) |

`active_subprocesses` = live MM worker processes. `sessions` = all DB rows (any status). Workers can be 0 while session count is higher.

---

## SQLite

File-based, no separate daemon. WAL mode creates `dxd.db-wal` and `dxd.db-shm` alongside the main file.

Tables: `users`, `sessions`, `metrics`. Created automatically on first startup.

---

## Encryption Key Rotation

A new key invalidates all `encrypted_private_key` rows. For a fresh system, replace the key in `/etc/dxd-api.env` and restart. If you have active users, either re-encrypt or clear the DB and have users re-register.

---

## Docker (local dev)

From repo root:

```bash
export DXD_ADMIN_TOKEN="$(openssl rand -hex 24)"
export DXD_MASTER_ENCRYPTION_KEY="$(python3 -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())')"
export DXD_MAKER_ENABLE_TRADING=true
export DXD_TAKER_ENABLE_TRADING=true
docker compose up --build
```

SQLite persists in the `dxd-data` Docker volume at `/data/dxd.db`.

---

## Network (GCP)

Allow TCP 8199 on the VM firewall (or proxy via nginx on 80/443).

---

## TLS / nginx (optional)

Terminate TLS on nginx, proxy to `127.0.0.1:8199`. Restrict `:8199` to localhost if only exposing nginx.

---

## E2E Dry Run

```bash
DXD_MAKER_ENABLE_TRADING=false  # in /etc/dxd-api.env
sudo systemctl restart dxd-api
python3 dxd_api/e2e_dry_run_live.py
```
