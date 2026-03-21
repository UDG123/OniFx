"""
OniQuant v6.0 — IBKR TradFi Broker Connector
===============================================
Async bridge to IB Gateway (headless IBeam container) via ib_async.
Implements token-bucket rate limiting at 45 msg/sec to stay under
IBKR's 50 msg/sec hard cap.

IBeam container: exposes IB Gateway on port 4001 (paper) / 4002 (live).
"""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

import orjson
import redis.asyncio as aioredis
import structlog
from ib_async import IB, Contract, LimitOrder, MarketOrder, Order, Trade

from app.core.config import get_settings

log: structlog.stdlib.BoundLogger = structlog.get_logger("oniquant.broker.tradfi")

PENDING_HASH_PREFIX: str = "oniquant:pending_match"


# ---------------------------------------------------------------------------
# Leaky Bucket Rate Limiter
# ---------------------------------------------------------------------------

@dataclass
class TokenBucketLimiter:
    """
    Token-bucket rate limiter with async sleep-on-empty semantics.

    Continuous refill via monotonic clock — smoother than fixed-window.
    45 tokens/sec with burst capacity of 45.
    """
    max_rate: float = 45.0
    max_tokens: float = 45.0
    _tokens: float = field(init=False, default=45.0)
    _last_refill: float = field(init=False, default_factory=time.monotonic)
    _lock: asyncio.Lock = field(init=False, default_factory=asyncio.Lock)

    async def acquire(self) -> float:
        """Acquire one token. Returns wait time (0.0 if immediate)."""
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_refill
            self._tokens = min(self.max_tokens, self._tokens + elapsed * self.max_rate)
            self._last_refill = now

            if self._tokens < 1.0:
                wait = (1.0 - self._tokens) / self.max_rate
                await asyncio.sleep(wait)
                now = time.monotonic()
                self._tokens = min(
                    self.max_tokens,
                    self._tokens + (now - self._last_refill) * self.max_rate,
                )
                self._last_refill = now
            else:
                wait = 0.0

            self._tokens -= 1.0
            return wait


# ---------------------------------------------------------------------------
# IBKR Connector
# ---------------------------------------------------------------------------

class IBKRConnector:
    """
    IB Gateway connector with rate limiting, egress IP validation,
    and synthetic order registration.

    Static IP Requirements:
        IBKR requires all API connections to originate from IPs whitelisted
        in Account Management → Settings → API → Trusted IPs. Connections
        from unknown IPs are silently rejected with no error message.

        This connector validates egress IP on connect() and logs a warning
        if the detected IP doesn't match the configured static_egress_ip.

    Lifecycle: connect() → register_simulated_order() / request_l2() → disconnect()
    """

    def __init__(self) -> None:
        self._settings = get_settings()
        self._ib = IB()
        self._limiter = TokenBucketLimiter(
            max_rate=self._settings.ib_rate_limit,
            max_tokens=self._settings.ib_rate_limit,
        )
        self._redis: aioredis.Redis | None = None

    async def _validate_egress_ip(self) -> str | None:
        """
        Detect our public egress IP and compare against the configured
        static_egress_ip. Logs a CRITICAL warning on mismatch because
        IBKR will silently reject connections from non-whitelisted IPs.

        Returns the detected egress IP, or None if detection fails.
        """
        expected = self._settings.static_egress_ip
        if not expected:
            await log.awarning(
                "ibkr_no_static_ip_configured",
                hint="Set STATIC_EGRESS_IP to enable IP validation. "
                     "IBKR requires whitelisted IPs in Account Management.",
            )
            return None

        try:
            import httpx
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get("https://api.ipify.org?format=json")
                detected = resp.json().get("ip", "unknown")
        except Exception as e:
            await log.awarning("egress_ip_detection_failed", error=str(e))
            return None

        if detected != expected:
            await log.acritical(
                "ibkr_egress_ip_mismatch",
                expected=expected,
                detected=detected,
                action="IBKR will REJECT this connection. "
                       "Update Account Management → API → Trusted IPs, "
                       "or fix STATIC_EGRESS_IP / proxy configuration.",
            )
        else:
            await log.ainfo("ibkr_egress_ip_validated", ip=detected)

        return detected

    async def connect(self, redis_pool: aioredis.Redis) -> None:
        """
        Connect to IB Gateway and bind Redis pool.

        Validates egress IP before connecting. If a SOCKS5 proxy is
        configured, the IBeam container routes through it for fixed egress.
        """
        self._redis = redis_pool

        # Validate egress IP against IBKR whitelist expectation
        await self._validate_egress_ip()

        await self._ib.connectAsync(
            host=self._settings.ib_gateway_host,
            port=self._settings.ib_gateway_port,
            clientId=self._settings.ib_client_id,
            readonly=False,
        )
        await log.ainfo(
            "ibkr_connected",
            host=self._settings.ib_gateway_host,
            port=self._settings.ib_gateway_port,
            accounts=self._ib.managedAccounts(),
            egress_proxy=self._settings.socks5_proxy or "direct",
        )

    async def disconnect(self) -> None:
        self._ib.disconnect()
        await log.ainfo("ibkr_disconnected")

    @staticmethod
    def stock(symbol: str, exchange: str = "SMART", currency: str = "USD") -> Contract:
        c = Contract()
        c.symbol, c.secType, c.exchange, c.currency = symbol, "STK", exchange, currency
        return c

    @staticmethod
    def futures(symbol: str, exchange: str = "CME", expiry: str = "") -> Contract:
        c = Contract()
        c.symbol, c.secType, c.exchange, c.currency = symbol, "FUT", exchange, "USD"
        c.lastTradeDateOrContractMonth = expiry
        return c

    async def qualify(self, contract: Contract) -> Contract:
        await self._limiter.acquire()
        qualified = await self._ib.qualifyContractsAsync(contract)
        return qualified[0] if qualified else contract

    async def request_l2(self, contract: Contract, rows: int = 10) -> list[dict[str, Any]]:
        """Request L2 Market Depth snapshot from IB Gateway."""
        await self._limiter.acquire()
        ticker = self._ib.reqMktDepth(contract, numRows=rows)
        await asyncio.sleep(0.5)  # allow data to populate

        snapshot = []
        for lvl in ticker.domBids:
            snapshot.append({"price": float(lvl.price), "size": int(lvl.size), "side": "BID", "position": lvl.position})
        for lvl in ticker.domAsks:
            snapshot.append({"price": float(lvl.price), "size": int(lvl.size), "side": "ASK", "position": lvl.position})

        self._ib.cancelMktDepth(contract)
        return snapshot

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
        """Push order intent to Redis pending-match hash for the Synthetic Matcher."""
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
            "broker": "IBKR_TWS",
            "created_at": now,
            "expires_at": now + ttl_seconds,
            "status": "PENDING",
            "metadata": metadata or {},
        }

        hash_key = f"{PENDING_HASH_PREFIX}:{order_id}"
        await self._redis.hset(hash_key, mapping={"data": orjson.dumps(payload)})
        await self._redis.expire(hash_key, ttl_seconds + 60)
        await self._redis.sadd(f"{PENDING_HASH_PREFIX}:index", order_id)

        await log.ainfo("tradfi_order_registered", order_id=order_id, symbol=symbol)
        return order_id

    async def place_order(self, contract: Contract, order: Order) -> Trade:
        """Place a real order via IB Gateway (rate-limited)."""
        await self._limiter.acquire()
        return self._ib.placeOrder(contract, order)
