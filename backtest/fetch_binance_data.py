"""
Fetch Binance USD-M Futures klines for latency-arb backtest reference prices.

Usage:
    python fetch_binance_data.py --symbol HYPEUSDT --start 2026-03-01 --end 2026-06-01 --resolution 5m
"""

import argparse
import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
import pandas as pd

BINANCE_KLINES = "https://fapi.binance.com/fapi/v1/klines"
INTERVAL_MAP = {
    "1m": "1m", "5m": "5m", "15m": "15m", "30m": "30m",
    "1h": "1h", "2h": "2h", "4h": "4h", "1d": "1d",
}
MAX_LIMIT = 1500


async def fetch_chunk(session: aiohttp.ClientSession, symbol: str, interval: str,
                      start_ms: int, end_ms: int) -> list:
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": MAX_LIMIT,
    }
    async with session.get(BINANCE_KLINES, params=params) as resp:
        if resp.status != 200:
            text = await resp.text()
            print(f"Binance error {resp.status}: {text[:200]}")
            return []
        return await resp.json()


async def fetch_all(symbol: str, interval: str, start_ts: int, end_ts: int) -> pd.DataFrame:
    start_ms = start_ts * 1000
    end_ms = end_ts * 1000
    rows = []
    async with aiohttp.ClientSession() as session:
        cursor = start_ms
        while cursor < end_ms:
            chunk = await fetch_chunk(session, symbol, interval, cursor, end_ms)
            if not chunk:
                break
            for k in chunk:
                ts_s = int(k[0]) // 1000
                rows.append({
                    "timestamp": ts_s,
                    "datetime": datetime.fromtimestamp(ts_s, tz=timezone.utc),
                    "open": float(k[1]),
                    "high": float(k[2]),
                    "low": float(k[3]),
                    "close": float(k[4]),
                    "volume": float(k[5]),
                })
            last_ms = int(chunk[-1][0])
            cursor = last_ms + 1
            if len(chunk) < MAX_LIMIT:
                break
            await asyncio.sleep(0.12)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows).drop_duplicates("timestamp").sort_values("timestamp")
    return df.reset_index(drop=True)


def main():
    p = argparse.ArgumentParser(description="Fetch Binance futures klines")
    p.add_argument("--symbol", required=True, help="e.g. HYPEUSDT")
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    p.add_argument("--resolution", default="5m", choices=list(INTERVAL_MAP.keys()))
    p.add_argument("--output-dir", default="./data")
    args = p.parse_args()

    start_dt = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    df = asyncio.run(fetch_all(args.symbol, INTERVAL_MAP[args.resolution],
                               int(start_dt.timestamp()), int(end_dt.timestamp())))
    if df.empty:
        print("No data fetched")
        sys.exit(1)

    out = Path(args.output_dir) / f"BN_{args.symbol}_{args.resolution}.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, index=False)
    print(f"Saved {len(df)} candles to {out}")
    print(f"Range: {df['datetime'].min()} -> {df['datetime'].max()}")


if __name__ == "__main__":
    main()
