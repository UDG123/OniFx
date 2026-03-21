"""
OniQuant v6.0 — Phase 4.5: Bybit Unified V5 Crypto Connector
==============================================================
Async bridge to Bybit's Unified Trading API (V5) for the Alts desk.
Implements batch order submission, real-time L2 orderbook streaming
via WebSocket, and feeds L2 snapshots into the Synthetic Matcher.

Deploy: Railway.app worker process.
    railway run python broker_crypto.py

Architecture:
    broker_crypto.py → pybit.unified_trading (REST + WebSocket)
                     → Redis L2 cache    (oniquant:l2_cache:{symbol})
                     → Redis pending     (oniquant:pending_match:{order_id})
                     → matcher.py        (Synthetic Matching Engine)
"""

from __future__ import annotations

import asyncio
import os
import signal
import time
import uuid
from typing import Any

import orjson
import redis.asyncio as aioredis
import structlog
from pybit.unified_trading import HTTP as BybitHTTP
from pybit.unified_trading import WebSocket as BybitWebSocket

# ---------------------------------------------------------------------------
# Logger
# ---------------------------------------------------------------------------
log: structlog.stdlib.BoundLogger = structlog.get_logger("oniquant.broker.crypto")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
BYBIT_API_KEY: str = os.getenv("BYBIT_API_KEY", "")
BYBIT_API_SECRET: str = os.getenv("BYBIT_API_SECRET", "")
BYBIT_TESTNET: bool = os.getenv("BYBIT_TESTNET", "true").lower() == "true"

PENDING_HASH_PREFIX: str = "oniquant:pending_match"
L2_CACHE_PREFIX: str = "oniquant:l2_cache"
L2_CACHE_TTL: int = int(os.getenv("L2_CACHE_TTL", "2"))  # 2-second TTL

# Bybit V5 rate limits: 10 orders/sec per UID for linear, 10 batch/sec.
# We use batch_order for max throughput — up to 20 orders per batch call.
BATCH_ORDER_MAX: int = 20  # Bybit V5 max orders per batch request


# ---------------------------------------------------------------------------
# 1. Bybit REST Client — Batch Order Submission
# ---------------------------------------------------------------------------

class BybitConnector:
    """
    Bybit Unified V5 connector for the Crypto/Alts desk.

    Capabilities:
        - Batch order placement (up to 20 orders per API call).
        - Real-time L2 orderbook streaming via WebSocket.
        - L2 snapshot caching in Redis for the Synthetic Matcher.
        - Simulated order registration for synthetic fill evaluation.

    The Unified Trading API (V5) consolidates spot, linear, inverse,
    and options under a single account and endpoint structure.
    """

    def __init__(
        self,
        api_key: str = BYBIT_API_KEY,
        api_secret: str = BYBIT_API_SECRET,
        testnet: bool = BYBIT_TESTNET,
        redis_url: str = REDIS_URL,
    ) -> None:
        self._api_key = api_key
        self._api_secret = api_secret
        self._testnet = testnet
        self._redis_url = redis_url

        # pybit HTTP client (REST API)
        self._http: BybitHTTP | None = None
        # pybit WebSocket client (orderbook stream)
        self._ws: BybitWebSocket | None = None
        # Redis pool
        self._redis: aioredis.Redis | None = None
        # Track active WS subscriptions
        self._ws_subscriptions: set[str] = set()
        # Latest L2 snapshots (in-memory for sub-ms access)
        self._l2_cache: dict[str, list[dict[str, Any]]] = {}
        # Background tasks
        self._tasks: list[asyncio.Task] = []
        self._running: bool = False

    # ------------------------------------------------------------------
    # Connection Lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """
        Initialize Bybit REST client, WebSocket, and Redis pool.

        The HTTP client is synchronous (pybit design) but we wrap calls
        in asyncio.to_thread() to avoid blocking the event loop.
        """
        # Redis pool
        self._redis = aioredis.from_url(
            self._redis_url,
            decode_responses=False,
            max_connections=10,
            socket_connect_timeout=5.0,
            socket_keepalive=True,
        )
        await self._redis.ping()

        # Bybit REST client
        self._http = BybitHTTP(
            api_key=self._api_key,
            api_secret=self._api_secret,
            testnet=self._testnet,
        )

        # Bybit WebSocket (public — orderbook doesn't require auth)
        self._ws = BybitWebSocket(
            testnet=self._testnet,
            channel_type="linear",  # USDT perpetuals
        )

        await log.ainfo(
            "bybit_connected",
            testnet=self._testnet,
            has_api_key=bool(self._api_key),
        )

    async def disconnect(self) -> None:
        """Gracefully shut down WebSocket, cancel tasks, drain Redis."""
        self._running = False
        for task in self._tasks:
            task.cancel()
        if self._ws is not None:
            self._ws.exit()
        if self._redis is not None:
            await self._redis.aclose()
        await log.ainfo("bybit_disconnected")

    # ------------------------------------------------------------------
    # 2. Batch Order Submission
    # ------------------------------------------------------------------

    async def place_batch_orders(
        self,
        category: str,
        orders: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """
        Submit orders in batches of up to 20 (Bybit V5 maximum).

        This is the most rate-limit-efficient approach: instead of
        N individual place_order calls (each consuming 1 rate unit),
        we pack up to 20 orders into a single batch_place_order call.

        For M orders, this uses ceil(M/20) API calls instead of M.
        At Bybit's 10 req/sec limit, this handles 200 orders/sec.

        Args:
            category: "linear" (USDT perps), "spot", "inverse", "option".
            orders: List of order parameter dicts, each containing:
                - symbol: e.g., "BTCUSDT"
                - side: "Buy" or "Sell"
                - orderType: "Limit" or "Market"
                - qty: Order quantity as string
                - price: Limit price as string (for Limit orders)
                - timeInForce: "GTC", "IOC", "FOK", "PostOnly"

        Returns:
            List of Bybit API response dicts per batch.
        """
        results: list[dict[str, Any]] = []

        # Chunk orders into batches of BATCH_ORDER_MAX
        for i in range(0, len(orders), BATCH_ORDER_MAX):
            batch = orders[i : i + BATCH_ORDER_MAX]

            # Format for Bybit V5 batch_place_order
            request_entries = []
            for order in batch:
                entry = {
                    "symbol": order["symbol"],
                    "side": order["side"],
                    "orderType": order.get("orderType", "Limit"),
                    "qty": str(order["qty"]),
                    "timeInForce": order.get("timeInForce", "GTC"),
                }
                if "price" in order and order.get("orderType") != "Market":
                    entry["price"] = str(order["price"])
                # Optional: reduce-only, close-on-trigger
                if order.get("reduceOnly"):
                    entry["reduceOnly"] = True
                request_entries.append(entry)

            # pybit is synchronous — offload to thread pool
            response = await asyncio.to_thread(
                self._http.place_batch_order,
                category=category,
                request=request_entries,
            )

            results.append(response)

            await log.ainfo(
                "batch_order_placed",
                category=category,
                batch_size=len(batch),
                batch_index=i // BATCH_ORDER_MAX,
                ret_code=response.get("retCode"),
            )

        return results

    # ------------------------------------------------------------------
    # 3. Simulated Order Registration (→ Synthetic Matcher)
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
        """
        Register a simulated order into Redis for synthetic matching.

        Identical interface to IBKRConnector.register_simulated_order()
        for unified matcher consumption. The Synthetic Matcher is
        broker-agnostic — it evaluates fills purely on L2 data.

        Returns:
            order_id (str): UUID for the pending order.
        """
        order_id = str(uuid.uuid4())
        now = time.time()

        order_payload: dict[str, Any] = {
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

        await self._redis.hset(
            hash_key,
            mapping={"data": orjson.dumps(order_payload)},
        )
        await self._redis.expire(hash_key, ttl_seconds + 60)
        await self._redis.sadd(f"{PENDING_HASH_PREFIX}:index", order_id)

        await log.ainfo(
            "crypto_simulated_order",
            order_id=order_id,
            symbol=symbol,
            direction=direction,
            quantity=quantity,
        )

        return order_id

    # ------------------------------------------------------------------
    # 4. L2 WebSocket — Depth of Market Streaming
    # ------------------------------------------------------------------

    def _on_orderbook_update(self, message: dict[str, Any]) -> None:
        """
        WebSocket callback for orderbook delta/snapshot messages.

        Bybit V5 orderbook WS format:
            {
                "topic": "orderbook.50.BTCUSDT",
                "type": "snapshot" | "delta",
                "data": {
                    "s": "BTCUSDT",
                    "b": [["price", "size"], ...],  # bids
                    "a": [["price", "size"], ...],  # asks
                    "u": 12345678,                   # update ID
                    "seq": 98765432                   # sequence
                },
                "ts": 1672304000123
            }

        We convert to our unified L2 format and cache both in-memory
        and in Redis for the Synthetic Matcher.
        """
        data = message.get("data", {})
        symbol = data.get("s", "UNKNOWN")

        # Convert Bybit [price, size] pairs to our L2Level format
        l2_levels: list[dict[str, Any]] = []

        for i, (price_str, size_str) in enumerate(data.get("b", [])):
            l2_levels.append({
                "price": float(price_str),
                "size": float(size_str),
                "side": "BID",
                "position": i,
            })

        for i, (price_str, size_str) in enumerate(data.get("a", [])):
            l2_levels.append({
                "price": float(price_str),
                "size": float(size_str),
                "side": "ASK",
                "position": i,
            })

        # In-memory cache (sub-ms access for hot path)
        self._l2_cache[symbol] = l2_levels

    async def _flush_l2_to_redis(self) -> None:
        """
        Periodic flush of in-memory L2 cache to Redis.

        Runs on a separate asyncio task at ~10Hz (100ms interval).
        This decouples the synchronous WS callback from async Redis I/O.
        The Redis cache is consumed by matcher.py's SyntheticMatcherWorker.
        """
        while self._running:
            try:
                if self._l2_cache and self._redis:
                    pipe = self._redis.pipeline(transaction=False)
                    for symbol, levels in self._l2_cache.items():
                        cache_key = f"{L2_CACHE_PREFIX}:{symbol}"
                        pipe.set(cache_key, orjson.dumps(levels), ex=L2_CACHE_TTL)
                    await pipe.execute()
            except Exception as e:
                await log.awarning("l2_redis_flush_error", error=str(e))
            await asyncio.sleep(0.1)  # 10Hz flush rate

    def subscribe_orderbook(
        self,
        symbols: list[str],
        depth: int = 50,
    ) -> None:
        """
        Subscribe to L2 orderbook streams for given symbols.

        Depth options: 1, 50, 200, 500.
        - depth=50 is optimal: granular enough for queue position
          evaluation, low enough bandwidth for Railway egress limits.

        Args:
            symbols: List of Bybit symbols (e.g., ["BTCUSDT", "ETHUSDT"]).
            depth: Orderbook depth level (1, 50, 200, 500).
        """
        if self._ws is None:
            raise RuntimeError("WebSocket not initialized — call connect() first")

        topics = [f"orderbook.{depth}.{sym}" for sym in symbols]

        self._ws.orderbook_stream(
            depth=depth,
            symbol=symbols,
            callback=self._on_orderbook_update,
        )

        self._ws_subscriptions.update(topics)
        # Synchronous log — WS callback context
        print(f"[broker_crypto] subscribed: {topics}")

    # ------------------------------------------------------------------
    # 5. Account & Position Queries
    # ------------------------------------------------------------------

    async def get_wallet_balance(self, account_type: str = "UNIFIED") -> dict[str, Any]:
        """Fetch unified account wallet balance."""
        return await asyncio.to_thread(
            self._http.get_wallet_balance,
            accountType=account_type,
        )

    async def get_positions(self, category: str = "linear", symbol: str = "") -> dict[str, Any]:
        """Fetch open positions for a category."""
        kwargs: dict[str, str] = {"category": category}
        if symbol:
            kwargs["symbol"] = symbol
        return await asyncio.to_thread(
            self._http.get_positions,
            **kwargs,
        )

    async def get_tickers(self, category: str = "linear", symbol: str = "") -> dict[str, Any]:
        """Fetch latest ticker data."""
        kwargs: dict[str, str] = {"category": category}
        if symbol:
            kwargs["symbol"] = symbol
        return await asyncio.to_thread(
            self._http.get_tickers,
            **kwargs,
        )

    # ------------------------------------------------------------------
    # 6. Main Run Loop
    # ------------------------------------------------------------------

    async def run(
        self,
        symbols: list[str] | None = None,
        orderbook_depth: int = 50,
    ) -> None:
        """
        Start the Bybit connector with L2 streaming and Redis flushing.

        This is the main entry point for the Railway worker process.
        It subscribes to orderbook streams and starts the background
        L2-to-Redis flush task.

        Args:
            symbols: Symbols to stream L2 for. Defaults to majors.
            orderbook_depth: L2 depth (1, 50, 200, 500).
        """
        if symbols is None:
            symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

        self._running = True

        # Subscribe to orderbook streams
        self.subscribe_orderbook(symbols, depth=orderbook_depth)

        # Start L2 → Redis flush task
        flush_task = asyncio.create_task(self._flush_l2_to_redis())
        self._tasks.append(flush_task)

        await log.ainfo(
            "bybit_worker_started",
            symbols=symbols,
            depth=orderbook_depth,
        )

        # Keep alive until stopped
        try:
            while self._running:
                # Periodic health check
                pending_count = await self._redis.scard(f"{PENDING_HASH_PREFIX}:index")
                await log.ainfo(
                    "bybit_heartbeat",
                    l2_symbols=len(self._l2_cache),
                    ws_subscriptions=len(self._ws_subscriptions),
                    pending_orders=pending_count,
                )
                await asyncio.sleep(30.0)  # heartbeat every 30s
        except asyncio.CancelledError:
            pass

    def stop(self) -> None:
        self._running = False


# ---------------------------------------------------------------------------
# Standalone Entrypoint
# ---------------------------------------------------------------------------

async def main() -> None:
    connector = BybitConnector()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, connector.stop)

    try:
        await connector.connect()
        await connector.run(
            symbols=["BTCUSDT", "ETHUSDT", "SOLUSDT", "AVAXUSDT"],
            orderbook_depth=50,
        )
    finally:
        await connector.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
