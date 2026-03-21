"""
OniQuant v6.0 — Redis Connection Pool (Shared Singleton)
==========================================================
Centralized Redis pool used by all app modules and workers.
Uses hiredis C parser for wire-protocol acceleration.
"""

from __future__ import annotations

import redis.asyncio as aioredis
import structlog

from app.core.config import get_settings

log: structlog.stdlib.BoundLogger = structlog.get_logger("oniquant.redis")

_pool: aioredis.Redis | None = None


async def init_redis() -> aioredis.Redis:
    """Initialize the shared Redis connection pool."""
    global _pool
    settings = get_settings()

    _pool = aioredis.from_url(
        settings.redis_url,
        decode_responses=False,
        max_connections=settings.redis_max_connections,
        socket_connect_timeout=5.0,
        socket_keepalive=True,
        retry_on_timeout=True,
    )

    await _pool.ping()
    await log.ainfo("redis_connected", url=settings.redis_url)
    return _pool


async def close_redis() -> None:
    """Drain and close the Redis pool."""
    global _pool
    if _pool is not None:
        await _pool.aclose()
        _pool = None
        await log.ainfo("redis_pool_closed")


def get_redis() -> aioredis.Redis:
    """Return the active Redis pool. Fails fast if not initialized."""
    if _pool is None:
        raise RuntimeError("Redis pool not initialized — call init_redis() first")
    return _pool
