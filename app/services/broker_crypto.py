"""
OniQuant v6.0 — Bybit V5 Crypto Broker Connector
===================================================
Async bridge to Bybit Unified Trading API (V5) for the Alts desk.
Features batch order submission (up to 20/call) and real-time L2
orderbook streaming via WebSocket for Synthetic Matcher consumption.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from typing import Any

import orjson
import redis.asyncio as aioredis
import structlog
from pybit.unified_trading import HTTP as BybitHTTP
from pybit.unified_trading import WebSocket as BybitWebSocket

from app.core.config import get_settings

log: structlog.stdlib.BoundLogger = structlog.get_logger("oniquant.broker.crypto")

PENDING_HASH_PREFIX: str = "oniquant:pending_match"
L2_CACHE_PREFIX: str = "oniquant:l2_cache"
L2_CACHE_TTL: int = 2  # seconds
BATCH_MAX: int = 20     # Bybit V5 max orders per batch


class BybitConnector:
    """
    Bybit Unified V5 connector for crypto perpetuals and spot.

    Capabilities:
        - Batch order placement (20 orders / API call)
        - L2 orderbook WebSocket streaming → Redis cache
        - Simulated order registration for Synthetic Matcher
    """

    def __init__(self) -> None:
        self._settings = get_settings()
        self._http: BybitHTTP | None = None
        self._ws: BybitWebSocket | None = None
        self._redis: aioredis.Redis | None = None
        self._l2_cache: dict[str, list[dict[str, Any]]] = {}
        self._running: bool = False
        self._tasks: list[asyncio.Task] = []

    async def connect(self, redis_pool: aioredis.Redis) -> None:
        """Initialize Bybit REST, WebSocket, and bind Redis pool."""
        self._redis = redis_pool

        self._http = BybitHTTP(
            api_key=self._settings.bybit_api_key,
            api_secret=self._settings.bybit_api_secret,
            testnet=self._settings.bybit_testnet,
        )

        self._ws = BybitWebSocket(
            testnet=self._settings.bybit_testnet,
            channel_type="linear",
        )

        await log.ainfo("bybit_connected", testnet=self._settings.bybit_testnet)

    async def disconnect(self) -> None:
        self._running = False
        for t in self._tasks:
            t.cancel()
        if self._ws:
            self._ws.exit()
        await log.ainfo("bybit_disconnected")

    # ------------------------------------------------------------------
    # Batch Order Submission
    # ------------------------------------------------------------------

    async def place_batch_orders(
        self,
        category: str,
        orders: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """
        Submit orders in batches of 20 (Bybit V5 max).

        For M orders: ceil(M/20) API calls instead of M.
        At 10 req/sec limit → handles 200 orders/sec.
        """
        results = []
        for i in range(0, len(orders), BATCH_MAX):
            batch = orders[i : i + BATCH_MAX]
            entries = []
            for o in batch:
                entry = {
                    "symbol": o["symbol"],
                    "side": o["side"],
                    "orderType": o.get("orderType", "Limit"),
                    "qty": str(o["qty"]),
                    "timeInForce": o.get("timeInForce", "GTC"),
                }
                if "price" in o and o.get("orderType") != "Market":
                    entry["price"] = str(o["price"])
                entries.append(entry)

            resp = await asyncio.to_thread(
                self._http.place_batch_order, category=category, request=entries
            )
            results.append(resp)
            await log.ainfo("crypto_batch_placed", batch_size=len(batch))
        return results

    # ------------------------------------------------------------------
    # Simulated Order Registration
    # ------------------------------------------------------------------

    async def register_simulated_order(
        self,
        symbol: str,
        direction: int,
        quantity: float,
        order_type: str = "LIMIT",
        limit_price: float | None = None,
        target_price: float | None = None,
        ttl_seconds: int = 300,
        desk_id: str = "crypto_alts",
        metadata: dict[str, Any] | None = None,
    ) -> str:
        """Register simulated order into Redis for Synthetic Matcher."""
        order_id = str(uuid.uuid4())
        now = time.time()

        payload = {
            "order_id": order_id,
            "symbol": symbol,
            "direction": direction,
            "quantity": quantity,
            "order_type": order_type,
            "limit_price": limit_price,
            "target_price": target_price,
            "desk_id": desk_id,
            "broker": "BYBIT_V5",
            "created_at": now,
            "expires_at": now + ttl_seconds,
            "status": "PENDING",
            "metadata": metadata or {},
        }

        hash_key = f"{PENDING_HASH_PREFIX}:{order_id}"
        await self._redis.hset(hash_key, mapping={"data": orjson.dumps(payload)})
        await self._redis.expire(hash_key, ttl_seconds + 60)
        await self._redis.sadd(f"{PENDING_HASH_PREFIX}:index", order_id)

        await log.ainfo("crypto_order_registered", order_id=order_id, symbol=symbol)
        return order_id

    # ------------------------------------------------------------------
    # L2 WebSocket Streaming
    # ------------------------------------------------------------------

    def _on_orderbook(self, msg: dict[str, Any]) -> None:
        """WS callback: convert Bybit L2 to unified format, cache in-memory."""
        data = msg.get("data", {})
        symbol = data.get("s", "UNKNOWN")

        levels = []
        for i, (p, s) in enumerate(data.get("b", [])):
            levels.append({"price": float(p), "size": float(s), "side": "BID", "position": i})
        for i, (p, s) in enumerate(data.get("a", [])):
            levels.append({"price": float(p), "size": float(s), "side": "ASK", "position": i})

        self._l2_cache[symbol] = levels

    async def _flush_l2_to_redis(self) -> None:
        """Periodic 10Hz flush of in-memory L2 cache to Redis."""
        while self._running:
            try:
                if self._l2_cache and self._redis:
                    pipe = self._redis.pipeline(transaction=False)
                    for sym, levels in self._l2_cache.items():
                        pipe.set(f"{L2_CACHE_PREFIX}:{sym}", orjson.dumps(levels), ex=L2_CACHE_TTL)
                    await pipe.execute()
            except Exception as e:
                await log.awarning("l2_flush_error", error=str(e))
            await asyncio.sleep(0.1)

    def subscribe_orderbook(self, symbols: list[str], depth: int = 50) -> None:
        """Subscribe to L2 orderbook streams for given symbols."""
        self._ws.orderbook_stream(depth=depth, symbol=symbols, callback=self._on_orderbook)

    async def run(self, symbols: list[str] | None = None) -> None:
        """Start L2 streaming and Redis flushing."""
        symbols = symbols or ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
        self._running = True
        self.subscribe_orderbook(symbols)
        self._tasks.append(asyncio.create_task(self._flush_l2_to_redis()))
        await log.ainfo("bybit_streaming_started", symbols=symbols)

        while self._running:
            await asyncio.sleep(30.0)

    def stop(self) -> None:
        self._running = False
