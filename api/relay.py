"""
Market Data Relay -- shared Binance or Hyperliquid WS + HotStuff orderbook per symbol.

Runs inside the API process as asyncio tasks. Per symbol, maintains either a
Binance USD-M futures stream or a Hyperliquid HIP-3 l2Book (when Binance has
no contract), plus a HotStuff orderbook subscription, then fans out every
message to all registered subscriber ports via UDP unicast on 127.0.0.1.

Subprocess side: bind a UDP socket on the assigned port, recv datagrams,
dispatch by the "t" (type) field to existing handlers.
"""

import asyncio
import json
import logging
import socket
import time
from typing import Any, Dict, List, Optional, Set

import aiohttp

from hotstuff import (
    WebSocketTransport,
    SubscriptionClient,
    WebSocketTransportOptions,
)
from hotstuff.methods.subscription.channels import OrderbookSubscriptionParams

log = logging.getLogger("bot-api.relay")

INST_BINANCE_MAP: Dict[str, str] = {
    "HYPE-PERP": "hypeusdt",
    "BTC-PERP": "btcusdt",
    "ETH-PERP": "ethusdt",
    "SOL-PERP": "solusdt",
    "XRP-PERP": "xrpusdt",
    "ZEC-PERP": "zecusdt",
    "BNB-PERP": "bnbusdt",
    "GOLD-PERP": "xauusdt",
    "SILVER-PERP": "xagusdt",
    "WTIOIL-PERP": "clusdt",
    "BRENTOIL-PERP": "bzusdt",
    "NATGAS-PERP": "natgasusdt",
}

# HIP-3 names on Hyperliquid (xyz dex). Binance USD-M has no FX; HL l2Book maps to HotStuff -PERP symbols.
INST_HYPERLIQUID_MAP: Dict[str, str] = {
    "EURUSD-PERP": "xyz:EUR",
    "USDJPY-PERP": "xyz:JPY",
    "USA500-PERP": "xyz:SP500",
    "USA100-PERP": "xyz:XYZ100",
}

_HL_WS_URL = "wss://api.hyperliquid.xyz/ws"

_SEND_ADDR = "127.0.0.1"
_SNAPSHOT_REPLAY_WINDOW_S = 8.0


class _SymbolFeed:
    """Manages WS connections and subscriber fan-out for a single symbol."""

    __slots__ = (
        "symbol", "bn_symbol", "hl_coin", "_subscribers", "_bn_session",
        "_bn_task", "_hl_session", "_hl_task", "_hl_sub_text",
        "_hs_ws", "_hs_sub_client", "_hs_sub",
        "_sock", "_running",
        "bn_msg_count", "hs_msg_count", "bn_last_ts", "hs_last_ts",
        "bn_connected", "hs_connected",
        "bn_bid", "bn_ask", "hs_best_bid", "hs_best_ask",
        "_last_hs_snapshot", "_snapshot_retry_until",
    )

    def __init__(self, symbol: str):
        self.symbol = symbol
        self.bn_symbol = INST_BINANCE_MAP.get(symbol, "")
        self.hl_coin = INST_HYPERLIQUID_MAP.get(symbol, "")
        self._subscribers: Set[int] = set()
        self._bn_session: Optional[aiohttp.ClientSession] = None
        self._bn_task: Optional[asyncio.Task] = None
        self._hl_session: Optional[aiohttp.ClientSession] = None
        self._hl_task: Optional[asyncio.Task] = None
        self._hl_sub_text: str = ""
        if self.hl_coin:
            self._hl_sub_text = json.dumps(
                {"method": "subscribe", "subscription": {"type": "l2Book", "coin": self.hl_coin}}
            )
        self._hs_ws: Optional[WebSocketTransport] = None
        self._hs_sub_client: Optional[SubscriptionClient] = None
        self._hs_sub = None
        self._sock: Optional[socket.socket] = None
        self._running = False
        self.bn_msg_count: int = 0
        self.hs_msg_count: int = 0
        self.bn_last_ts: float = 0.0
        self.hs_last_ts: float = 0.0
        self.bn_connected: bool = False
        self.hs_connected: bool = False
        self.bn_bid: float = 0.0
        self.bn_ask: float = 0.0
        self.hs_best_bid: float = 0.0
        self.hs_best_ask: float = 0.0
        self._last_hs_snapshot: Optional[bytes] = None
        self._snapshot_retry_until: Dict[int, float] = {}

    def add_subscriber(self, port: int):
        self._subscribers.add(port)
        self._snapshot_retry_until[port] = time.monotonic() + _SNAPSHOT_REPLAY_WINDOW_S
        if self._last_hs_snapshot and self._sock:
            try:
                self._sock.sendto(self._last_hs_snapshot, (_SEND_ADDR, port))
            except OSError:
                pass

    def remove_subscriber(self, port: int):
        self._subscribers.discard(port)
        self._snapshot_retry_until.pop(port, None)

    @property
    def subscriber_count(self) -> int:
        return len(self._subscribers)

    async def start(self):
        if self._running:
            return
        self._running = True

        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.setblocking(False)

        if self.bn_symbol:
            self._bn_session = aiohttp.ClientSession()
            self._bn_task = asyncio.create_task(self._run_binance_ws())
        elif self.hl_coin:
            self._hl_session = aiohttp.ClientSession()
            self._hl_task = asyncio.create_task(self._run_hyperliquid_ws())
        else:
            self.bn_connected = False
            log.warning(
                "Relay: no Binance or Hyperliquid ref for %s; running HotStuff-only feed",
                self.symbol,
            )

        try:
            ws_server = {"mainnet": "wss://api.hotstuff.trade/ws/"}
            self._hs_ws = WebSocketTransport(WebSocketTransportOptions(
                is_testnet=False, timeout=15.0,
                keep_alive={"interval": 20.0, "timeout": 10.0},
                auto_connect=True, server=ws_server,
            ))
            self._hs_sub_client = SubscriptionClient(transport=self._hs_ws)
            self._hs_sub = await asyncio.wait_for(
                asyncio.to_thread(
                    self._hs_sub_client.orderbook,
                    OrderbookSubscriptionParams(instrument_id=self.symbol),
                    self._on_hs_orderbook,
                ),
                timeout=10.0,
            )
            self.hs_connected = True
            log.info("Relay: HotStuff orderbook sub for %s", self.symbol)
        except asyncio.TimeoutError:
            self.hs_connected = False
            log.warning("Relay: HotStuff orderbook sub timed out for %s (will retry via watchdog)", self.symbol)
        except Exception as exc:
            self.hs_connected = False
            log.error("Relay: HotStuff orderbook sub failed for %s: %s", self.symbol, exc)

        log.info("Relay: feed started for %s (%d subscribers)", self.symbol, len(self._subscribers))

    async def stop(self):
        self._running = False
        self.bn_connected = False
        self.hs_connected = False
        if self._bn_task:
            self._bn_task.cancel()
            try:
                await self._bn_task
            except asyncio.CancelledError:
                pass
        if self._bn_session:
            await self._bn_session.close()
        if self._hl_task:
            self._hl_task.cancel()
            try:
                await self._hl_task
            except asyncio.CancelledError:
                pass
        if self._hl_session:
            await self._hl_session.close()
        try:
            await asyncio.wait_for(asyncio.to_thread(self._disconnect_hs), timeout=5.0)
        except (asyncio.TimeoutError, Exception):
            pass
        if self._sock:
            self._sock.close()
        log.info("Relay: feed stopped for %s", self.symbol)

    def _disconnect_hs(self):
        if self._hs_sub:
            try:
                if isinstance(self._hs_sub, dict) and "unsubscribe" in self._hs_sub:
                    self._hs_sub["unsubscribe"]()
            except Exception:
                pass
        if self._hs_ws:
            try:
                self._hs_ws.disconnect()
            except Exception:
                pass

    def _broadcast(self, payload: bytes):
        if not self._subscribers or not self._sock:
            return
        for port in self._subscribers:
            try:
                self._sock.sendto(payload, (_SEND_ADDR, port))
            except OSError:
                pass

    def _replay_last_snapshot(self):
        if not self._last_hs_snapshot or not self._snapshot_retry_until or not self._sock:
            return
        now = time.monotonic()
        stale_ports: List[int] = []
        payload = self._last_hs_snapshot
        for port, until in list(self._snapshot_retry_until.items()):
            if port not in self._subscribers or now > until:
                stale_ports.append(port)
                continue
            try:
                self._sock.sendto(payload, (_SEND_ADDR, port))
            except OSError:
                pass
        for port in stale_ports:
            self._snapshot_retry_until.pop(port, None)

    def _on_hs_orderbook(self, msg):
        data = msg.data if hasattr(msg, "data") else msg
        if not isinstance(data, dict):
            return
        self.hs_msg_count += 1
        self.hs_last_ts = time.monotonic()
        books = data.get("books", data)
        if isinstance(books, dict):
            bids = books.get("bids", [])
            asks = books.get("asks", [])
            if bids:
                try:
                    self.hs_best_bid = float(bids[0]["price"]) if isinstance(bids[0], dict) else float(bids[0])
                except (KeyError, IndexError, ValueError, TypeError):
                    pass
            if asks:
                try:
                    self.hs_best_ask = float(asks[0]["price"]) if isinstance(asks[0], dict) else float(asks[0])
                except (KeyError, IndexError, ValueError, TypeError):
                    pass
        if self.hs_best_bid > 0 and self.hs_best_ask > 0 and self.bn_bid > 0 and self.bn_ask > 0:
            bn_mid = (self.bn_bid + self.bn_ask) * 0.5
            if bn_mid > 0:
                hs_mid = (self.hs_best_bid + self.hs_best_ask) * 0.5
                basis_bps = abs(hs_mid - bn_mid) / bn_mid * 10000.0
                if basis_bps > 200.0:
                    log.warning(
                        "Relay TOXIC HS: %s hs_mid=%.2f bn_mid=%.2f basis=%.0fbps",
                        self.symbol, hs_mid, bn_mid, basis_bps,
                    )
        packet = {
            "t": "hs_book",
            "s": self.symbol,
            "data": data,
        }
        payload = json.dumps(packet).encode()
        if data.get("update_type") == "snapshot":
            self._last_hs_snapshot = payload
        self._replay_last_snapshot()
        self._broadcast(payload)

    async def _run_binance_ws(self):
        if not self.bn_symbol:
            return
        streams = (
            f"{self.bn_symbol}@bookTicker/"
            f"{self.bn_symbol}@aggTrade/"
            f"{self.bn_symbol}@kline_1m"
        )
        url = f"wss://fstream.binance.com/stream?streams={streams}"

        while self._running:
            try:
                async with self._bn_session.ws_connect(url, heartbeat=20) as ws:
                    self.bn_connected = True
                    log.info("Relay: Binance WS connected for %s", self.symbol)
                    async for msg in ws:
                        if not self._running:
                            break
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            if msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                                break
                            continue
                        try:
                            payload = json.loads(msg.data)
                            stream = payload.get("stream", "")
                            data = payload.get("data", {})
                            if "bookTicker" in stream:
                                b = data.get("b")
                                a = data.get("a")
                                if b:
                                    self.bn_bid = float(b)
                                if a:
                                    self.bn_ask = float(a)
                                pkt = {"t": "bn_book", "s": self.symbol,
                                       "b": b, "a": a,
                                       "B": data.get("B"), "A": data.get("A")}
                            elif "aggTrade" in stream:
                                pkt = {"t": "bn_trade", "s": self.symbol,
                                       "p": data.get("p"), "q": data.get("q"),
                                       "m": data.get("m")}
                            elif "kline" in stream:
                                k = data.get("k", {})
                                pkt = {"t": "bn_kline", "s": self.symbol,
                                       "k": k}
                            else:
                                continue
                            self.bn_msg_count += 1
                            self.bn_last_ts = time.monotonic()
                            self._broadcast(json.dumps(pkt).encode())
                        except (json.JSONDecodeError, KeyError, ValueError):
                            pass
                    self.bn_connected = False
            except asyncio.CancelledError:
                return
            except Exception as exc:
                log.warning("Relay: Binance WS error for %s: %s -- reconnect 3s", self.symbol, exc)
                await asyncio.sleep(3.0)

    async def _run_hyperliquid_ws(self):
        if not self.hl_coin or not self._hl_sub_text:
            return
        while self._running:
            try:
                async with self._hl_session.ws_connect(_HL_WS_URL, heartbeat=30) as ws:
                    await ws.send_str(self._hl_sub_text)
                    self.bn_connected = True
                    log.info(
                        "Relay: Hyperliquid WS connected for %s (coin=%s)",
                        self.symbol,
                        self.hl_coin,
                    )
                    async for msg in ws:
                        if not self._running:
                            break
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            if msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                                break
                            continue
                        try:
                            payload = json.loads(msg.data)
                        except (json.JSONDecodeError, TypeError):
                            continue
                        if payload.get("channel") != "l2Book":
                            continue
                        data = payload.get("data")
                        if not isinstance(data, dict):
                            continue
                        if data.get("coin") != self.hl_coin:
                            continue
                        levels = data.get("levels")
                        if not isinstance(levels, list) or len(levels) < 2:
                            continue
                        bids = levels[0]
                        asks = levels[1]
                        if not bids or not asks:
                            continue
                        try:
                            b0 = bids[0]
                            a0 = asks[0]
                            bp = float(b0["px"] if isinstance(b0, dict) else b0[0])
                            ap = float(a0["px"] if isinstance(a0, dict) else a0[0])
                            bsz = float(b0.get("sz", 0) if isinstance(b0, dict) else 0)
                            asz = float(a0.get("sz", 0) if isinstance(a0, dict) else 0)
                        except (KeyError, IndexError, TypeError, ValueError):
                            continue
                        if bp <= 0.0 or ap <= 0.0:
                            continue
                        self.bn_bid = bp
                        self.bn_ask = ap
                        self.bn_msg_count += 1
                        self.bn_last_ts = time.monotonic()
                        pkt = {
                            "t": "bn_book",
                            "s": self.symbol,
                            "b": bp,
                            "a": ap,
                            "B": bsz,
                            "A": asz,
                        }
                        self._broadcast(json.dumps(pkt).encode())
                    self.bn_connected = False
            except asyncio.CancelledError:
                return
            except Exception as exc:
                self.bn_connected = False
                log.warning(
                    "Relay: Hyperliquid WS error for %s: %s -- reconnect 3s",
                    self.symbol,
                    exc,
                )
                await asyncio.sleep(3.0)


_WATCHDOG_INTERVAL = 15.0
_STALE_THRESHOLD = 60.0


class Relay:
    """Top-level relay manager. One instance in the API process."""

    def __init__(self):
        self._feeds: Dict[str, _SymbolFeed] = {}
        self._watchdog_task: Optional[asyncio.Task] = None

    async def register(self, symbol: str, port: int):
        feed = self._feeds.get(symbol)
        if feed is None:
            feed = _SymbolFeed(symbol)
            self._feeds[symbol] = feed
            feed.add_subscriber(port)
            await feed.start()
        else:
            feed.add_subscriber(port)
        log.info("Relay: registered port %d for %s (%d subs)",
                 port, symbol, feed.subscriber_count)
        self._ensure_watchdog()

    async def unregister(self, symbol: str, port: int):
        feed = self._feeds.get(symbol)
        if feed is None:
            return
        feed.remove_subscriber(port)
        log.info("Relay: unregistered port %d for %s (%d subs remaining)",
                 port, symbol, feed.subscriber_count)
        if feed.subscriber_count == 0:
            await feed.stop()
            del self._feeds[symbol]
            log.info("Relay: last subscriber gone, stopped feed for %s", symbol)

    async def restart_feed(self, symbol: str) -> bool:
        feed = self._feeds.get(symbol)
        if feed is None:
            return False
        subs = set(feed._subscribers)
        await feed.stop()
        new_feed = _SymbolFeed(symbol)
        new_feed._subscribers = subs
        self._feeds[symbol] = new_feed
        await new_feed.start()
        log.info("Relay: restarted feed for %s (%d subscribers)", symbol, len(subs))
        return True

    async def restart_all(self) -> int:
        symbols = list(self._feeds.keys())
        for sym in symbols:
            await self.restart_feed(sym)
        return len(symbols)

    async def stop_all(self):
        self._stop_watchdog()
        for feed in self._feeds.values():
            await feed.stop()
        self._feeds.clear()
        log.info("Relay: all feeds stopped")

    @property
    def active_symbols(self) -> list:
        return list(self._feeds.keys())

    def total_subscribers(self) -> int:
        return sum(f.subscriber_count for f in self._feeds.values())

    def status(self) -> Dict[str, Any]:
        now = time.monotonic()
        feeds: List[Dict[str, Any]] = []
        for sym, f in self._feeds.items():
            bn_age = round(now - f.bn_last_ts, 1) if f.bn_last_ts > 0 else None
            hs_age = round(now - f.hs_last_ts, 1) if f.hs_last_ts > 0 else None
            bn_mid = round((f.bn_bid + f.bn_ask) / 2, 4) if f.bn_bid > 0 and f.bn_ask > 0 else None
            hs_mid = round((f.hs_best_bid + f.hs_best_ask) / 2, 4) if f.hs_best_bid > 0 and f.hs_best_ask > 0 else None
            feeds.append({
                "symbol": sym,
                "subscribers": f.subscriber_count,
                "ref_source": "binance" if f.bn_symbol else ("hyperliquid" if f.hl_coin else None),
                "binance": {"connected": f.bn_connected, "msgs": f.bn_msg_count, "last_s_ago": bn_age,
                             "bid": f.bn_bid, "ask": f.bn_ask, "mid": bn_mid},
                "hotstuff": {"connected": f.hs_connected, "msgs": f.hs_msg_count, "last_s_ago": hs_age,
                              "bid": f.hs_best_bid, "ask": f.hs_best_ask, "mid": hs_mid},
            })
        return {"active_feeds": len(self._feeds), "feeds": feeds}

    # -- watchdog ------------------------------------------------------

    def _ensure_watchdog(self):
        if self._watchdog_task is None or self._watchdog_task.done():
            self._watchdog_task = asyncio.create_task(self._watchdog_loop())

    def _stop_watchdog(self):
        if self._watchdog_task and not self._watchdog_task.done():
            self._watchdog_task.cancel()
            self._watchdog_task = None

    async def _watchdog_loop(self):
        try:
            while True:
                await asyncio.sleep(_WATCHDOG_INTERVAL)
                now = time.monotonic()
                for sym, feed in list(self._feeds.items()):
                    if feed.subscriber_count == 0:
                        continue
                    bn_stale = feed.bn_last_ts > 0 and (now - feed.bn_last_ts) > _STALE_THRESHOLD
                    hs_stale = (feed.hs_last_ts > 0 and (now - feed.hs_last_ts) > _STALE_THRESHOLD) or \
                               (not feed.hs_connected and feed.hs_last_ts == 0.0)
                    if bn_stale or hs_stale:
                        parts = []
                        if bn_stale:
                            parts.append(f"ref ({now - feed.bn_last_ts:.0f}s)")
                        if hs_stale:
                            parts.append(f"HotStuff ({now - feed.hs_last_ts:.0f}s)")
                        log.warning("Relay watchdog: %s stale [%s] -- restarting",
                                    sym, ", ".join(parts))
                        await self.restart_feed(sym)
        except asyncio.CancelledError:
            return
