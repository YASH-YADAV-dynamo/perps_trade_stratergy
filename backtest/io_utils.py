"""Shared loaders and Hotstuff↔Binance symbol map."""

import subprocess
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).parent
DATA_DIR = ROOT / "data"

HS_TO_BINANCE = {
    "HYPE-PERP": "HYPEUSDT",
    "SOL-PERP": "SOLUSDT",
    "XRP-PERP": "XRPUSDT",
    "ZEC-PERP": "ZECUSDT",
    "BTC-PERP": "BTCUSDT",
    "ETH-PERP": "ETHUSDT",
}


def fetch_hotstuff(symbol: str, start: str, end: str, resolution: str = "5m") -> None:
    out = DATA_DIR / f"{symbol.replace('-', '_')}_{resolution}.parquet"
    if out.exists():
        return
    subprocess.run([
        sys.executable, str(ROOT / "fetch_data.py"),
        "--symbol", symbol, "--start", start, "--end", end,
        "--resolution", resolution, "--output-dir", str(DATA_DIR),
    ], check=True)


def fetch_binance(symbol: str, start: str, end: str, resolution: str = "5m") -> None:
    out = DATA_DIR / f"BN_{symbol}_{resolution}.parquet"
    if out.exists():
        return
    subprocess.run([
        sys.executable, str(ROOT / "fetch_binance_data.py"),
        "--symbol", symbol, "--start", start, "--end", end,
        "--resolution", resolution, "--output-dir", str(DATA_DIR),
    ], check=True)


def load_hotstuff(symbols: list, resolution: str = "5m") -> dict:
    data = {}
    for symbol in symbols:
        path = DATA_DIR / f"{symbol.replace('-', '_')}_{resolution}.parquet"
        if not path.exists():
            raise FileNotFoundError(path)
        df = pd.read_parquet(path)
        df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
        if df["timestamp"].dtype != "datetime64[ns, UTC]":
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
        data[symbol] = df.sort_values("timestamp").reset_index(drop=True)
    return align(data)


def load_merged_hs_bn(hs: str, bn: str, resolution: str = "5m") -> pd.DataFrame:
    hs_df = pd.read_parquet(DATA_DIR / f"{hs.replace('-', '_')}_{resolution}.parquet")
    bn_df = pd.read_parquet(DATA_DIR / f"BN_{bn}_{resolution}.parquet")
    for df in (hs_df, bn_df):
        df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
        if df["timestamp"].dtype != "datetime64[ns, UTC]":
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    bn_cols = bn_df[["timestamp", "close", "open"]].rename(
        columns={"close": "bn_close", "open": "bn_open"}
    )
    return hs_df.merge(bn_cols, on="timestamp", how="inner").dropna(subset=["bn_close"]).sort_values("timestamp").reset_index(drop=True)


def align(data: dict) -> dict:
    min_ts = max(df["timestamp"].min() for df in data.values())
    max_ts = min(df["timestamp"].max() for df in data.values())
    out = {}
    for sym, df in data.items():
        out[sym] = df[(df["timestamp"] >= min_ts) & (df["timestamp"] <= max_ts)].reset_index(drop=True)
    return out
