"""
Historical Data Fetcher for Hotstuff Exchange
=============================================

Fetches candle data for backtesting via POST https://api.hotstuff.trade/info
method=chart. Symbol IDs are resolved dynamically from method=instruments.

Usage:
    python fetch_data.py --list-symbols
    python fetch_data.py --symbol BTC-PERP --start 2026-03-01 --end 2026-06-01 --resolution 5m
"""

import argparse
import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import aiohttp
import pandas as pd

RESOLUTION_MAP = {
    "1m": "1",
    "5m": "5",
    "15m": "15",
    "30m": "30",
    "1h": "60",
    "2h": "120",
    "4h": "240",
    "6h": "360",
    "1d": "D",
    "1w": "W",
}

HOTSTUFF_API = "https://api.hotstuff.trade/info"
MAX_CANDLES_PER_REQUEST = 1500


async def fetch_symbols(session: aiohttp.ClientSession) -> Dict[str, int]:
    """Fetch perp symbol name -> instrument id."""
    payload = {"method": "instruments", "params": {"type": "perps"}}
    try:
        async with session.post(HOTSTUFF_API, json=payload) as resp:
            if resp.status != 200:
                print(f"Error fetching symbols: HTTP {resp.status}")
                return {}
            data = await resp.json()
            perps = data.get("perps", [])
            return {p["name"]: int(p["id"]) for p in perps if p.get("name") and p.get("id") is not None}
    except Exception as e:
        print(f"Exception fetching symbols: {e}")
        return {}


def _parse_chart_rows(rows: list, symbol: str) -> List[dict]:
    """Parse list-of-objects chart response from Hotstuff."""
    candles = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        ts_ms = int(row.get("time", 0))
        ts_s = ts_ms // 1000 if ts_ms > 1e11 else ts_ms
        candles.append({
            "timestamp": ts_s,
            "datetime": datetime.fromtimestamp(ts_s, tz=timezone.utc),
            "open": float(row.get("open", 0)),
            "high": float(row.get("high", 0)),
            "low": float(row.get("low", 0)),
            "close": float(row.get("close", 0)),
            "volume": float(row.get("volume", 0)),
            "symbol": symbol,
        })
    return candles


def _parse_chart_arrays(data: dict, symbol: str) -> List[dict]:
    """Parse {t,o,h,l,c,v} array chart response."""
    result = data.get("result", data) if isinstance(data, dict) else {}
    timestamps = result.get("t", [])
    if not timestamps:
        return []
    opens = result.get("o", [])
    highs = result.get("h", [])
    lows = result.get("l", [])
    closes = result.get("c", [])
    volumes = result.get("v", [])
    candles = []
    for i, ts in enumerate(timestamps):
        ts_i = int(ts)
        ts_s = ts_i // 1000 if ts_i > 1e11 else ts_i
        candles.append({
            "timestamp": ts_s,
            "datetime": datetime.fromtimestamp(ts_s, tz=timezone.utc),
            "open": float(opens[i]) if i < len(opens) else 0.0,
            "high": float(highs[i]) if i < len(highs) else 0.0,
            "low": float(lows[i]) if i < len(lows) else 0.0,
            "close": float(closes[i]) if i < len(closes) else 0.0,
            "volume": float(volumes[i]) if i < len(volumes) else 0.0,
            "symbol": symbol,
        })
    return candles


async def fetch_candles(
    session: aiohttp.ClientSession,
    symbol: str,
    symbol_id: int,
    resolution: str,
    start_ts: int,
    end_ts: int,
    chart_type: str = "mark",
) -> Optional[List[dict]]:
    """Fetch candles from Hotstuff chart API."""
    payload = {
        "method": "chart",
        "params": {
            "symbol": str(symbol_id),
            "resolution": RESOLUTION_MAP.get(resolution, "5"),
            "from": start_ts,
            "to": end_ts,
            "chart_type": chart_type,
        },
    }
    try:
        async with session.post(HOTSTUFF_API, json=payload) as resp:
            if resp.status != 200:
                print(f"Error fetching {symbol}: HTTP {resp.status}")
                return None
            data = await resp.json()
            if isinstance(data, dict) and data.get("error"):
                print(f"API error for {symbol}: {data['error']}")
                return None
            if isinstance(data, list):
                candles = _parse_chart_rows(data, symbol)
            else:
                candles = _parse_chart_arrays(data, symbol)
            return candles if candles else []
    except Exception as e:
        print(f"Exception fetching {symbol}: {e}")
        return None


def _resolution_seconds(resolution: str) -> int:
    mapping = {
        "1m": 60, "5m": 300, "15m": 900, "30m": 1800,
        "1h": 3600, "2h": 7200, "4h": 14400, "6h": 21600,
        "1d": 86400, "1w": 604800,
    }
    return mapping.get(resolution, 300)


async def fetch_with_retry(
    session: aiohttp.ClientSession,
    symbol: str,
    symbol_id: int,
    resolution: str,
    start_ts: int,
    end_ts: int,
    max_retries: int = 3,
) -> Optional[List[dict]]:
    """Fetch with chunking (API caps ~1500 candles per request)."""
    bar_s = _resolution_seconds(resolution)
    chunk_span = max(bar_s * (MAX_CANDLES_PER_REQUEST - 10), 7 * 24 * 3600)

    all_candles: List[dict] = []
    current_start = start_ts
    chunk_num = 0

    while current_start < end_ts:
        chunk_num += 1
        current_end = min(current_start + chunk_span, end_ts)
        print(
            f"Fetching chunk {chunk_num}: "
            f"{datetime.fromtimestamp(current_start, tz=timezone.utc)} -> "
            f"{datetime.fromtimestamp(current_end, tz=timezone.utc)}"
        )

        chunk_candles = None
        for attempt in range(max_retries):
            chunk_candles = await fetch_candles(
                session, symbol, symbol_id, resolution, current_start, current_end
            )
            if chunk_candles is not None:
                all_candles.extend(chunk_candles)
                print(f"  -> Got {len(chunk_candles)} candles")
                break
            print(f"  -> Retry {attempt + 1}/{max_retries}...")
            await asyncio.sleep(1)

        if chunk_candles is None:
            print(f"  -> Failed chunk {chunk_num}")
            return None

        if len(chunk_candles) >= MAX_CANDLES_PER_REQUEST - 1:
            last_ts = chunk_candles[-1]["timestamp"]
            current_start = last_ts + bar_s
        else:
            current_start = current_end

        await asyncio.sleep(0.15)

    if not all_candles:
        return None

    # Deduplicate by timestamp
    seen = set()
    deduped = []
    for c in sorted(all_candles, key=lambda x: x["timestamp"]):
        if c["timestamp"] in seen:
            continue
        seen.add(c["timestamp"])
        deduped.append(c)
    return deduped


def save_to_parquet(candles: List[dict], output_path: Path) -> pd.DataFrame:
    df = pd.DataFrame(candles).sort_values("timestamp").reset_index(drop=True)
    df["returns"] = df["close"].pct_change()
    df["range"] = df["high"] - df["low"]
    df["body"] = (df["close"] - df["open"]).abs()
    df.to_parquet(output_path, index=False)
    print(f"Saved {len(df)} candles to {output_path}")
    return df


async def run_fetch(args):
    async with aiohttp.ClientSession() as session:
        print("Fetching available symbols from Hotstuff API...")
        symbol_map = await fetch_symbols(session)
        if not symbol_map:
            print("Failed to fetch symbol list.")
            sys.exit(1)

        print(f"Found {len(symbol_map)} symbols")
        symbol_id = symbol_map.get(args.symbol)
        if symbol_id is None:
            print(f"Symbol '{args.symbol}' not found.")
            print("Available:", ", ".join(sorted(symbol_map.keys())[:30]))
            sys.exit(1)

        print(f"Resolved {args.symbol} -> ID {symbol_id}")

        start_dt = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        end_dt = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        start_ts = int(start_dt.timestamp())
        end_ts = int(end_dt.timestamp())

        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        candles = await fetch_with_retry(
            session, args.symbol, symbol_id, args.resolution, start_ts, end_ts
        )
        if not candles:
            print("Failed to fetch data")
            sys.exit(1)

        output_file = output_dir / f"{args.symbol.replace('-', '_')}_{args.resolution}.parquet"
        df = save_to_parquet(candles, output_file)

        print(f"\n{'='*60}")
        print(f"SUMMARY FOR {args.symbol}")
        print(f"{'='*60}")
        print(f"  Total candles:     {len(df)}")
        print(f"  Date range:        {df['datetime'].min()} to {df['datetime'].max()}")
        print(f"  Avg close price:   {df['close'].mean():.4f}")
        print(f"  Volatility (std):  {df['close'].std():.4f}")
        print(f"  Return std:        {df['returns'].std() * 100:.4f}%")
        print(f"  Total volume:      {df['volume'].sum():.4f}")
        print(f"{'='*60}")
        print(f"File saved: {output_file}")


def main():
    parser = argparse.ArgumentParser(description="Fetch historical data from Hotstuff")
    parser.add_argument("--symbol", help="Symbol to fetch (e.g., BTC-PERP)")
    parser.add_argument("--start", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", help="End date (YYYY-MM-DD)")
    parser.add_argument("--resolution", default="5m", choices=list(RESOLUTION_MAP.keys()))
    parser.add_argument("--output-dir", default="./data")
    parser.add_argument("--list-symbols", action="store_true")
    args = parser.parse_args()

    if args.list_symbols:
        async def list_only():
            async with aiohttp.ClientSession() as session:
                symbol_map = await fetch_symbols(session)
                for name, idx in sorted(symbol_map.items()):
                    print(f"  {name} (ID: {idx})")
        asyncio.run(list_only())
        return

    if not args.symbol or not args.start or not args.end:
        parser.error("--symbol, --start, and --end are required unless --list-symbols is set")

    asyncio.run(run_fetch(args))


if __name__ == "__main__":
    main()
