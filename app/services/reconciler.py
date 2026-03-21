"""
OniQuant v6.0 — Signal Reconciliation Engine
===============================================
Background task that detects lost signals (present in Redis but
missing from TimescaleDB after TTL expiry). Triggers forensic alerts.

Runs every 60 seconds.
"""

from __future__ import annotations

import asyncio
import os
import time
from typing import Any

import asyncpg
import orjson
import redis.asyncio as aioredis
import structlog

from app.core.logger import send_alert

log = structlog.get_logger("oniquant.reconciler")

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://oniquant:oniquant@timescaledb:5432/oniquant")
PENDING_ZSET = "oniquant:pending_trades"
LOST_STREAM = "oniquant:lost_signals"
RECONCILE_INTERVAL = 60.0  # seconds


class ReconciliationEngine:
    """
    Detects signals that exist in Redis pending state but have no
    corresponding fill/miss record in TimescaleDB after their TTL.
    """

    def __init__(self) -> None:
        self._pool: aioredis.Redis | None = None
        self._db: asyncpg.Pool | None = None
        self._running = False
        self._lost_count = 0

    async def connect(self) -> None:
        self._pool = aioredis.from_url(REDIS_URL, decode_responses=False)
        self._db = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=5)
        await log.ainfo("reconciler_started")

    async def disconnect(self) -> None:
        if self._pool:
            await self._pool.aclose()
        if self._db:
            await self._db.close()

    async def _check_signal_in_ledger(self, signal_id: str) -> bool:
        """Check if a signal_id has a fill/miss record in TimescaleDB."""
        async with self._db.acquire() as conn:
            row = await conn.fetchval(
                """
                SELECT EXISTS(
                    SELECT 1 FROM signal_ledger
                    WHERE indicator_metadata @> $1::jsonb
                )
                """,
                f'{{"signal_id": "{signal_id}"}}',
            )
            return bool(row)

    async def reconcile(self) -> None:
        """
        Scan expired ZSET entries and verify they landed in the ledger.
        """
        now = time.time()

        # Get signals that expired in the last reconcile window
        expired = await self._pool.zrangebyscore(
            PENDING_ZSET,
            min=now - RECONCILE_INTERVAL * 2,
            max=now - 10,  # grace period
            withscores=True,
        )

        for member_bytes, score in expired:
            try:
                payload = orjson.loads(member_bytes)
                signal_id = payload.get("signal_id", "")

                if signal_id and not await self._check_signal_in_ledger(signal_id):
                    # Signal is lost — no ledger record after TTL
                    self._lost_count += 1

                    await self._pool.xadd(
                        LOST_STREAM,
                        {
                            "payload": member_bytes,
                            "detected_at": str(now).encode(),
                            "reason": b"no_ledger_record_after_ttl",
                        },
                        maxlen=5000,
                        approximate=True,
                    )

                    await send_alert(
                        title="Lost Signal Detected",
                        message=f"Signal {signal_id} has no ledger record after TTL.",
                        severity="warning",
                        fields={"signal_id": signal_id, "total_lost": str(self._lost_count)},
                    )

            except Exception as e:
                await log.awarning("reconcile_error", error=str(e))

    async def run(self) -> None:
        self._running = True
        while self._running:
            try:
                await self.reconcile()
            except Exception as e:
                await log.aerror("reconciler_error", error=str(e))
            await asyncio.sleep(RECONCILE_INTERVAL)

    def stop(self) -> None:
        self._running = False
