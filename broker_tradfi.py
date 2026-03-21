"""
OniQuant v6.0 — Phase 4.5: IBeam/IBKR TradFi Connector
=========================================================
Async bridge to IB Gateway (headless IBeam container) via ib_async.
Implements a token-bucket rate limiter (leaky bucket variant) to
enforce IBKR's 45 msg/sec API throttle.

Deploy: Railway.app sidecar alongside IBeam Docker container.
    IBeam container exposes IB Gateway on port 4001 (paper) / 4002 (live).

Architecture:
    broker_tradfi.py → ib_async → IB Gateway (port 4001)
                     → Redis Hash  oniquant:pending_match:{order_id}
                     → matcher.py  (Synthetic Matching Engine)
"""

from __future__ import annotations

import asyncio
import os
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

import orjson
import redis.asyncio as aioredis
import structlog
from ib_async import IB, Contract, LimitOrder, MarketOrder, Order, Trade

# ---------------------------------------------------------------------------
# Logger
# ---------------------------------------------------------------------------
log: structlog.stdlib.BoundLogger = structlog.get_logger("oniquant.broker.tradfi")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
IB_HOST: str = os.getenv("IB_GATEWAY_HOST", "127.0.0.1")
IB_PORT: int = int(os.getenv("IB_GATEWAY_PORT", "4001"))  # 4001=paper, 4002=live
IB_CLIENT_ID: int = int(os.getenv("IB_CLIENT_ID", "1"))
REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
PENDING_HASH_PREFIX: str = "oniquant:pending_match"


# ---------------------------------------------------------------------------
# 1. Leaky Bucket Rate Limiter
# ---------------------------------------------------------------------------
# IBKR enforces a hard 50 msg/sec limit. We cap at 45/sec (90% headroom)
# to absorb burst variance and avoid pacing violations that trigger
# 1-second lockouts.
#
# Implementation: Token bucket with continuous refill.
#   - Bucket capacity = max_tokens (45)
#   - Refill rate = max_tokens per second (45 tokens/sec)
#   - Each API call consumes 1 token
#   - If empty, caller awaits until a token is available
#
# This is mathematically equivalent to a leaky bucket where:
#   leak_rate = 1/45 sec per message ≈ 22.2ms minimum inter-message gap
# ---------------------------------------------------------------------------

@dataclass
class LeakyBucketLimiter:
    """
    Token-bucket rate limiter with async sleep-until-available semantics.

    The bucket refills continuously based on elapsed wall-clock time,
    not discrete intervals. This gives smoother throughput than a
    fixed-window counter.

    Attributes:
        max_rate: Maximum messages per second (default: 45 for IBKR).
        max_tokens: Bucket capacity — allows micro-bursts up to this size.
        _tokens: Current token count (float for fractional refill).
        _last_refill: Timestamp of last refill calculation.
        _lock: Async lock to serialize token consumption.
    """
    max_rate: float = 45.0
    max_tokens: float = 45.0
    _tokens: float = field(init=False, default=45.0)
    _last_refill: float = field(init=False, default_factory=time.monotonic)
    _lock: asyncio.Lock = field(init=False, default_factory=asyncio.Lock)

    async def acquire(self) -> float:
        """
        Acquire one token. Sleeps if the bucket is empty.

        Returns the wait time in seconds (0.0 if no wait was needed).

        The refill calculation uses monotonic clock to avoid issues
        with wall-clock adjustments (NTP, DST, etc.).
        """
        async with self._lock:
            now = time.monotonic()

            # --- Refill tokens based on elapsed time ---
            elapsed = now - self._last_refill
            self._tokens = min(
                self.max_tokens,
                self._tokens + elapsed * self.max_rate,
            )
            self._last_refill = now

            # --- If bucket is empty, calculate sleep duration ---
            if self._tokens < 1.0:
                # Time until 1 token is available:
                #   deficit = 1.0 - self._tokens
                #   wait = deficit / max_rate
                deficit = 1.0 - self._tokens
                wait_time = deficit / self.max_rate
                await asyncio.sleep(wait_time)

                # After sleeping, refill again
                now = time.monotonic()
                elapsed = now - self._last_refill
                self._tokens = min(
                    self.max_tokens,
                    self._tokens + elapsed * self.max_rate,
                )
                self._last_refill = now
            else:
                wait_time = 0.0

            # --- Consume one token ---
            self._tokens -= 1.0
            return wait_time


# ---------------------------------------------------------------------------
# 2. IBKR Gateway Connector
# ---------------------------------------------------------------------------

class IBKRConnector:
    """
    Async connector to IB Gateway via ib_async.

    Responsibilities:
        - Maintain a persistent connection to IB Gateway (auto-reconnect).
        - Rate-limit all outbound API messages via LeakyBucketLimiter.
        - Register simulated orders into Redis pending-match hashes.
        - Subscribe to real-time L2 market data for the Synthetic Matcher.

    Connection lifecycle:
        connect() → [register_simulated_order() | request_l2_snapshot()] → disconnect()
    """

    def __init__(
        self,
        host: str = IB_HOST,
        port: int = IB_PORT,
        client_id: int = IB_CLIENT_ID,
        redis_url: str = REDIS_URL,
    ) -> None:
        self._host = host
        self._port = port
        self._client_id = client_id
        self._redis_url = redis_url

        # ib_async client instance
        self._ib = IB()
        # Rate limiter: 45 msg/sec with burst capacity
        self._limiter = LeakyBucketLimiter(max_rate=45.0, max_tokens=45.0)
        # Redis pool (initialized on connect)
        self._redis: aioredis.Redis | None = None

        # Execution tracking
        self._active_subs: dict[int, Contract] = {}  # conId → Contract

    # ------------------------------------------------------------------
    # Connection Management
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """
        Establish connections to IB Gateway and Redis.

        IB Gateway connection uses ib_async's managed connection with
        automatic heartbeat and reconnection. The readonly flag is False
        because we need order placement capability.
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

        # IB Gateway — ib_async handles the event loop integration
        await self._ib.connectAsync(
            host=self._host,
            port=self._port,
            clientId=self._client_id,
            readonly=False,
            account="",  # auto-detect from Gateway
        )

        await log.ainfo(
            "ibkr_connected",
            host=self._host,
            port=self._port,
            client_id=self._client_id,
            accounts=self._ib.managedAccounts(),
        )

    async def disconnect(self) -> None:
        """Gracefully disconnect from IB Gateway and drain Redis pool."""
        self._ib.disconnect()
        if self._redis is not None:
            await self._redis.aclose()
        await log.ainfo("ibkr_disconnected")

    # ------------------------------------------------------------------
    # Contract Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def stock_contract(symbol: str, exchange: str = "SMART", currency: str = "USD") -> Contract:
        """Build a US equity contract for IB API."""
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = exchange
        contract.currency = currency
        return contract

    @staticmethod
    def futures_contract(symbol: str, exchange: str = "CME", expiry: str = "") -> Contract:
        """Build a futures contract (ES, NQ, etc.)."""
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "FUT"
        contract.exchange = exchange
        contract.currency = "USD"
        contract.lastTradeDateOrContractMonth = expiry
        return contract

    # ------------------------------------------------------------------
    # Rate-Limited API Calls
    # ------------------------------------------------------------------

    async def _rate_limited_call(self, coro_func, *args, **kwargs) -> Any:
        """
        Wrap any ib_async API call with rate limiting.

        Acquires a token from the leaky bucket before executing.
        This ensures we never exceed 45 msg/sec to the Gateway,
        even under concurrent signal bursts.
        """
        wait = await self._limiter.acquire()
        if wait > 0:
            await log.adebug("rate_limited", wait_ms=round(wait * 1000, 1))
        return await coro_func(*args, **kwargs)

    async def qualify_contract(self, contract: Contract) -> Contract:
        """Resolve contract details (conId, exchange) via IB API."""
        await self._limiter.acquire()
        qualified = await self._ib.qualifyContractsAsync(contract)
        if qualified:
            return qualified[0]
        raise ValueError(f"Failed to qualify contract: {contract.symbol}")

    # ------------------------------------------------------------------
    # L2 Market Data
    # ------------------------------------------------------------------

    async def request_l2_snapshot(
        self,
        contract: Contract,
        num_rows: int = 10,
    ) -> list[dict[str, Any]]:
        """
        Request L2 (Market Depth) snapshot from IB Gateway.

        Returns a list of price-level dicts:
            [{"price": 452.30, "size": 150, "side": "BID"}, ...]

        The Synthetic Matcher consumes this to evaluate queue position
        and adverse selection before simulating fills.
        """
        await self._limiter.acquire()
        ticker = self._ib.reqMktDepth(contract, numRows=num_rows)

        # Wait briefly for depth data to populate
        await asyncio.sleep(0.5)

        snapshot: list[dict[str, Any]] = []

        for dom_level in ticker.domBids:
            snapshot.append({
                "price": float(dom_level.price),
                "size": int(dom_level.size),
                "side": "BID",
                "position": dom_level.position,
            })

        for dom_level in ticker.domAsks:
            snapshot.append({
                "price": float(dom_level.price),
                "size": int(dom_level.size),
                "side": "ASK",
                "position": dom_level.position,
            })

        # Cancel subscription after snapshot
        self._ib.cancelMktDepth(contract)

        return snapshot

    # ------------------------------------------------------------------
    # 3. Simulated Order Registration → Redis Pending Match Hash
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
        desk_id: str = "tradfi_equities",
        metadata: dict[str, Any] | None = None,
    ) -> str:
        """
        Register a simulated order into Redis for the Synthetic Matcher.

        This does NOT place a real order on IB Gateway. Instead, it pushes
        the order intent into a Redis Hash keyed by order_id. The Synthetic
        Matching Engine (matcher.py) will evaluate fill probability against
        live L2 data.

        Redis Key: oniquant:pending_match:{order_id}
        Hash Fields:
            - order_id:         UUID4 unique identifier
            - symbol:           Instrument symbol
            - direction:        1 (LONG) / -1 (SHORT)
            - quantity:         Order size
            - order_type:       "LIMIT" | "MARKET"
            - limit_price:      Limit price (for LIMIT orders)
            - target_price:     Model's predicted target
            - desk_id:          Routing desk identifier
            - created_at:       UNIX timestamp of registration
            - expires_at:       UNIX timestamp of expiration
            - status:           "PENDING" (initial state)
            - metadata:         JSONB indicator state

        Args:
            symbol: Instrument symbol (e.g., "SPY").
            direction: 1 for LONG, -1 for SHORT.
            quantity: Number of shares/contracts.
            order_type: "LIMIT" or "MARKET".
            limit_price: Price for limit orders.
            target_price: Model's predicted target price.
            ttl_seconds: Time-to-live before auto-expiry (default 5 min).
            desk_id: Desk routing identifier.
            metadata: Variable indicator state (kNN, LuxAlgo, etc.).

        Returns:
            order_id (str): The generated UUID for this pending order.
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
            "created_at": now,
            "expires_at": now + ttl_seconds,
            "status": "PENDING",
            "metadata": metadata or {},
        }

        hash_key = f"{PENDING_HASH_PREFIX}:{order_id}"

        # Store as a single-field hash with orjson-serialized payload.
        # Single-field avoids partial-read races on the matcher side.
        await self._redis.hset(
            hash_key,
            mapping={"data": orjson.dumps(order_payload)},
        )

        # Set Redis TTL as a safety net — even if the matcher doesn't
        # purge it, Redis will auto-expire the key.
        await self._redis.expire(hash_key, ttl_seconds + 60)  # +60s grace

        # Add order_id to the pending index set for efficient scanning
        await self._redis.sadd(f"{PENDING_HASH_PREFIX}:index", order_id)

        await log.ainfo(
            "simulated_order_registered",
            order_id=order_id,
            symbol=symbol,
            direction=direction,
            quantity=quantity,
            order_type=order_type,
            limit_price=limit_price,
            ttl=ttl_seconds,
        )

        return order_id

    # ------------------------------------------------------------------
    # Real Order Placement (for future live trading — currently unused)
    # ------------------------------------------------------------------

    async def place_real_order(
        self,
        contract: Contract,
        order: Order,
    ) -> Trade:
        """
        Place a real order via IB Gateway (rate-limited).

        WARNING: This submits to the live/paper account on IB Gateway.
        Currently gated behind the Synthetic Matcher — only called after
        a simulated fill is confirmed.
        """
        await self._limiter.acquire()
        trade = self._ib.placeOrder(contract, order)
        await log.ainfo(
            "real_order_placed",
            order_id=trade.order.orderId,
            symbol=contract.symbol,
            action=order.action,
            quantity=order.totalQuantity,
        )
        return trade
