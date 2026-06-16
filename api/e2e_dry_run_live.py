"""
Live E2E dry-run: real HotStuff + Binance via relay UDP, API create / PATCH / restart / stop.

Prerequisites:
  - API running with:
      BOT_MASTER_ENCRYPTION_KEY=<fernet key>
      BOT_MM_ENABLE_TRADING=false
    Example:
      export BOT_MASTER_ENCRYPTION_KEY=...
      export BOT_MM_ENABLE_TRADING=false
      python3 -m uvicorn api.server:app --host 127.0.0.1 --port 8199

  - Repo root .env with HOTSTUFF_PRIVATE_KEY and HOTSTUFF_AGENT_ADDRESS (not printed by this script).

Run (from repo root):
  python3 api/e2e_dry_run_live.py

Then grep server logs for relay + strategy proof lines.
"""

import asyncio
import os
import sys
from pathlib import Path

import httpx
from eth_account import Account
from eth_account.messages import encode_defunct

REPO = Path(__file__).resolve().parent.parent
BASE = os.environ.get("BOT_E2E_BASE", "http://127.0.0.1:8199/v1")


def _load_creds():
    try:
        from dotenv import load_dotenv
    except ImportError:
        print("Install python-dotenv: pip install python-dotenv", file=sys.stderr)
        sys.exit(1)
    load_dotenv(REPO / ".env")
    pk = os.getenv("HOTSTUFF_PRIVATE_KEY", "").strip()
    addr = os.getenv("HOTSTUFF_AGENT_ADDRESS", "").strip()
    if not pk or not addr:
        print("Missing HOTSTUFF_PRIVATE_KEY or HOTSTUFF_AGENT_ADDRESS in .env", file=sys.stderr)
        sys.exit(1)
    return pk, addr


async def _wallet_login(c: httpx.AsyncClient, wallet_key: str) -> dict:
    acct = Account.from_key(wallet_key)
    address = acct.address.lower()

    r = await c.post(f"{BASE}/auth/nonce", json={"address": address})
    r.raise_for_status()
    nonce_data = r.json()

    msg = encode_defunct(text=nonce_data["message"])
    sig = acct.sign_message(msg).signature.hex()

    r = await c.post(f"{BASE}/auth/login", json={"address": address, "signature": sig})
    r.raise_for_status()
    return r.json()


async def main():
    pk, addr = _load_creds()
    async with httpx.AsyncClient(timeout=120.0) as c:
        r = await c.get(f"{BASE}/health")
        r.raise_for_status()
        print("health:", r.json())

        login = await _wallet_login(c, pk)
        auth = {"Authorization": f"Bearer {login['token']}"}
        print("wallet login OK, user_id:", login["user_id"])

        body = {
            "agent_address": addr,
            "agent_private_key": pk,
            "symbols": ["HYPE-PERP", "ETH-PERP"],
            "config": {"min_spread_bps": 1.5, "levels": 3},
            "symbol_config": {"ETH-PERP": {"min_spread_bps": 2.0}},
        }
        r = await c.post(f"{BASE}/sessions", headers=auth, json=body)
        r.raise_for_status()
        sess = r.json()
        sid = sess["session_id"]
        print("session:", sid, "symbols:", sess["symbols"])
        print("effective HYPE min_spread_bps:", sess["config"]["HYPE-PERP"]["min_spread_bps"])
        print("effective ETH min_spread_bps:", sess["config"]["ETH-PERP"]["min_spread_bps"])

        print("\n-- waiting 22s for warmup (Binance relay + HotStuff WS + fair price) --")
        await asyncio.sleep(22)

        r = await c.get(f"{BASE}/sessions/{sid}", headers=auth)
        print("session status:", r.json().get("status"))

        r = await c.get(f"{BASE}/sessions/{sid}/metrics", headers=auth)
        print("metrics status:", r.status_code)
        if r.status_code == 200:
            m = r.json().get("metrics", {})
            for sym, row in m.items():
                print(f"  {sym}: fair_mid={row.get('fair_mid')} bn_mid={row.get('bn_mid')} vol_bps={row.get('vol_bps')}")
        else:
            print("  (metrics may still be warming -- check logs)")

        print("\n-- PATCH config (global min_spread_bps 2.2) -> subprocess restart --")
        r = await c.patch(f"{BASE}/sessions/{sid}/config", headers=auth, json={"min_spread_bps": 2.2})
        print("PATCH status:", r.status_code, r.text[:200] if r.status_code != 200 else "ok")
        if r.status_code == 200:
            cf = r.json()["configs"]
            print("after PATCH HYPE min_spread_bps:", cf["HYPE-PERP"]["min_spread_bps"])
            print("after PATCH ETH min_spread_bps:", cf["ETH-PERP"]["min_spread_bps"])

        print("\n-- waiting 18s after restart --")
        await asyncio.sleep(18)

        r = await c.get(f"{BASE}/sessions/{sid}/metrics", headers=auth)
        print("metrics after restart:", r.status_code)

        print("\n-- PATCH per-symbol ETH only --")
        r = await c.patch(
            f"{BASE}/sessions/{sid}/config",
            headers=auth,
            json={"symbol": "ETH-PERP", "min_spread_bps": 2.8},
        )
        print("PATCH symbol status:", r.status_code)
        if r.status_code == 200:
            cf = r.json()["configs"]
            print("HYPE min_spread_bps:", cf["HYPE-PERP"]["min_spread_bps"])
            print("ETH min_spread_bps:", cf["ETH-PERP"]["min_spread_bps"])

        await asyncio.sleep(12)

        r = await c.post(f"{BASE}/sessions/{sid}/stop", headers=auth)
        print("\nstop:", r.status_code, r.json())

    print("\n=== Grep your server log for these strings ===")
    print("  dxd-api.relay: Relay: registered port")
    print("  dxd-api.relay: Relay: Binance WS connected for")
    print("  dxd-api.relay: Relay: HotStuff orderbook sub for")
    print("  aggressive_mm: Relay receiver bound to 127.0.0.1:")
    print("  aggressive_mm: Mode           : DRY-RUN")
    print("  aggressive_mm: Sub: L2 orderbook for  (skipped when relay -- check hs_book via relay)")
    print("  aggressive_mm: DRY [HYPE-PERP/...] | bid=")
    print("  (stdout JSON lines with symbol field for metrics)")


if __name__ == "__main__":
    asyncio.run(main())
