"""
OniQuant v6.0 — Global Kill Switch (Redis-backed)
====================================================
Centralized circuit breaker that halts ALL order flow when activated.

Redis Key:
    oniquant:global_kill_switch = "1"  (TTL: 24 hours)

Checked by:
    - orchestration_worker._process_signal()  (signal authorization)
    - memory_worker._scan_and_promote()       (price-cross promotion)
    - broker_tradfi.place_order()             (live order submission)

Activated by:
    - logger.alert_kill_switch()  (drawdown > 3%)
    - Manual: redis-cli SET oniquant:global_kill_switch 1 EX 86400
"""

from __future__ import annotations

import redis.asyncio as aioredis
import structlog

log = structlog.get_logger("oniquant.kill_switch")

KILL_SWITCH_KEY: str = "oniquant:global_kill_switch"
KILL_SWITCH_TTL: int = 86400  # 24 hours


async def is_kill_switch_active(redis: aioredis.Redis) -> bool:
    """
    Check whether the global kill switch is currently active.

    Returns True if the Redis key exists and is set to "1",
    meaning all order flow must be halted immediately.
    """
    value = await redis.get(KILL_SWITCH_KEY)
    return value is not None and value in (b"1", b"True", b"true")


async def activate_global_kill_switch(
    redis: aioredis.Redis,
    reason: str = "unknown",
    ttl: int = KILL_SWITCH_TTL,
) -> None:
    """
    Activate the global kill switch by setting the Redis key.

    The key auto-expires after `ttl` seconds (default 24h) as a
    safety net — manual deactivation via deactivate_global_kill_switch()
    is the expected recovery path after investigation.
    """
    await redis.set(KILL_SWITCH_KEY, b"1", ex=ttl)
    await log.acritical(
        "global_kill_switch_activated",
        reason=reason,
        ttl_seconds=ttl,
        key=KILL_SWITCH_KEY,
    )


async def deactivate_global_kill_switch(redis: aioredis.Redis) -> None:
    """
    Deactivate the global kill switch by deleting the Redis key.

    Should only be called after manual investigation confirms
    it is safe to resume trading.
    """
    await redis.delete(KILL_SWITCH_KEY)
    await log.awarning("global_kill_switch_deactivated", key=KILL_SWITCH_KEY)
