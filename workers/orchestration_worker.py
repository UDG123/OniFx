"""
OniQuant v6.0 — Orchestration Worker
=======================================
Consumes signals from oniquant:raw_signals via Consumer Group.
Enriches with Alpha Stack math, passes to BayesianArbiter,
routes AUTHORIZED signals to pending_trades, logs REJECTED to TimescaleDB.

Resilience:
    - XREADGROUP with explicit XACK (at-least-once delivery)
    - Failed signals stay in PEL for reclaim by another worker
    - Dead Letter Queue after 3 failed attempts

Deploy: railway run python -m workers.orchestration_worker
"""

from __future__ import annotations

import asyncio
import os
import signal
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import orjson
import redis.asyncio as aioredis
import structlog

from app.core.config import get_settings
from app.core.database import init_pool, insert_signal
from app.services.orchestrator import BayesianArbiter

log = structlog.get_logger("oniquant.orchestration_worker")

REDIS_URL: str = os.getenv("REDIS_URL", "redis://redis:6379/0")
INPUT_STREAM: str = "oniquant:raw_signals"
CONSUMER_GROUP: str = "orchestrator_cg"
CONSUMER_NAME: str = os.getenv("CONSUMER_NAME", f"orch_worker_{os.getpid()}")
PENDING_ZSET: str = "oniquant:pending_trades"
DLQ_STREAM: str = "oniquant:dead_letter_queue"
MAX_RETRIES: int = 3


class OrchestrationWorker:
    """
    Stream consumer that orchestrates the full signal pipeline.

    Signal Flow:
        1. XREADGROUP from oniquant:raw_signals
        2. Enrich with Alpha Stack metadata
        3. BayesianArbiter → AUTHORIZE / REJECT
        4. AUTHORIZE → ZADD to oniquant:pending_trades (Memory Engine)
        5. REJECT → INSERT to signal_ledger (TimescaleDB audit log)
        6. XACK on successful processing
        7. Failed 3× → move to Dead Letter Queue
    """

    def __init__(self, redis_url: str = REDIS_URL) -> None:
        self._redis_url = redis_url
        self._pool: aioredis.Redis | None = None
        self._arbiter: BayesianArbiter | None = None
        self._running: bool = False
        self._processed: int = 0
        self._retry_counts: dict[str, int] = {}

    async def connect(self) -> None:
        self._pool = aioredis.from_url(
            self._redis_url,
            decode_responses=False,
            max_connections=15,
            socket_connect_timeout=5.0,
            socket_keepalive=True,
        )
        await self._pool.ping()

        # Initialize database pool
        try:
            await init_pool()
        except Exception as e:
            await log.awarning("db_init_deferred", error=str(e))

        self._arbiter = BayesianArbiter(self._pool)

        # Create consumer group (idempotent)
        try:
            await self._pool.xgroup_create(INPUT_STREAM, CONSUMER_GROUP, id="0", mkstream=True)
        except aioredis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

        await log.ainfo("orchestration_worker_started", consumer=CONSUMER_NAME)

    async def disconnect(self) -> None:
        if self._pool:
            await self._pool.aclose()
        await log.ainfo(
            "orchestration_worker_stopped",
            processed=self._processed,
            stats=self._arbiter.stats if self._arbiter else {},
        )

    async def _process_signal(self, msg_id: bytes, payload: dict[str, Any]) -> bool:
        """
        Process a single signal through the pipeline.

        Returns True if successfully processed, False on error.
        """
        signal_id = payload.get("signal_id", str(uuid.uuid4()))

        # Step 1: Enrich — pull live WFO params from Redis
        desk_id = payload.get("desk_id", "luxalgo")
        live_params = await self._pool.get(f"oniquant:config:{desk_id}")
        if live_params:
            payload["live_wfo_params"] = orjson.loads(live_params)

        # Step 2: Bayesian evaluation
        decision = await self._arbiter.evaluate(payload)

        if decision["decision"] == "AUTHORIZE":
            # Step 3a: Push to pending_trades ZSET for Memory Engine
            now = time.time()
            ttl = payload.get("ttl_seconds", 300)
            expiry = now + ttl

            trade_payload = {
                **payload,
                "signal_id": signal_id,
                "posterior_probability": decision["posterior_probability"],
                "routing_target": decision["routing_target"],
                "authorized_at": now,
            }

            await self._pool.zadd(
                PENDING_ZSET,
                {orjson.dumps(trade_payload): expiry},
            )
        else:
            # Step 3b: Log rejected signal to TimescaleDB
            try:
                await insert_signal(
                    ts=datetime.now(timezone.utc),
                    desk_id=payload.get("desk_id", "unknown"),
                    asset_symbol=payload.get("asset_symbol", "UNKNOWN"),
                    signal_direction=payload.get("signal_direction", 0),
                    target_price=payload.get("target_price", 0),
                    confidence=payload.get("confidence", 0),
                    indicator_metadata={
                        "decision": "REJECT",
                        "posterior": decision["posterior_probability"],
                        "reasoning": decision["reasoning_math"],
                    },
                )
            except Exception as e:
                await log.awarning("reject_log_error", error=str(e))

        self._processed += 1
        return True

    async def _handle_dead_letter(self, msg_id: bytes, payload: dict[str, Any]) -> None:
        """Move a signal to the Dead Letter Queue after MAX_RETRIES failures."""
        await self._pool.xadd(
            DLQ_STREAM,
            {
                "payload": orjson.dumps(payload),
                "original_msg_id": msg_id,
                "reason": b"max_retries_exceeded",
                "failed_at": str(time.time()).encode(),
            },
            maxlen=10000,
            approximate=True,
        )
        # ACK to remove from PEL
        await self._pool.xack(INPUT_STREAM, CONSUMER_GROUP, msg_id)
        await log.awarning("signal_to_dlq", msg_id=msg_id)

    async def _reclaim_pending(self) -> None:
        """
        Reclaim stale messages from the PEL (Pending Entries List).

        Messages idle for >30s are likely from crashed workers.
        We XCLAIM them and re-process.
        """
        try:
            pending = await self._pool.xpending_range(
                INPUT_STREAM, CONSUMER_GROUP, min="-", max="+", count=50
            )
            for entry in pending:
                msg_id = entry["message_id"]
                idle_ms = entry["time_since_delivered"]
                delivery_count = entry["times_delivered"]

                if idle_ms > 30000:  # 30 seconds idle
                    if delivery_count >= MAX_RETRIES:
                        # DLQ — too many retries
                        claimed = await self._pool.xclaim(
                            INPUT_STREAM, CONSUMER_GROUP, CONSUMER_NAME,
                            min_idle_time=30000, message_ids=[msg_id]
                        )
                        for cid, cdata in claimed:
                            raw = cdata.get(b"payload", b"{}")
                            await self._handle_dead_letter(cid, orjson.loads(raw))
                    else:
                        # Reclaim for reprocessing
                        await self._pool.xclaim(
                            INPUT_STREAM, CONSUMER_GROUP, CONSUMER_NAME,
                            min_idle_time=30000, message_ids=[msg_id]
                        )
        except Exception as e:
            await log.adebug("reclaim_error", error=str(e))

    async def run(self) -> None:
        """Main consumer loop with PEL reclaim and DLQ support."""
        self._running = True

        cycle = 0
        while self._running:
            try:
                # Periodic PEL reclaim (every 100 cycles ≈ 100s)
                if cycle % 100 == 0:
                    await self._reclaim_pending()

                # Read new messages
                messages = await self._pool.xreadgroup(
                    groupname=CONSUMER_GROUP,
                    consumername=CONSUMER_NAME,
                    streams={INPUT_STREAM: ">"},
                    count=10,
                    block=1000,
                )

                if not messages:
                    cycle += 1
                    continue

                for stream_name, entries in messages:
                    for msg_id, msg_data in entries:
                        try:
                            raw = msg_data.get(b"payload", b"{}")
                            payload = orjson.loads(raw)
                            success = await self._process_signal(msg_id, payload)
                            if success:
                                await self._pool.xack(INPUT_STREAM, CONSUMER_GROUP, msg_id)
                        except Exception as e:
                            await log.aerror(
                                "signal_processing_error",
                                msg_id=msg_id, error=str(e),
                            )
                            # Don't ACK — stays in PEL for reclaim

                cycle += 1

                # Heartbeat
                if cycle % 500 == 0:
                    await log.ainfo(
                        "orchestration_heartbeat",
                        processed=self._processed,
                        stats=self._arbiter.stats if self._arbiter else {},
                    )

            except aioredis.ConnectionError as e:
                await log.aerror("redis_lost", error=str(e))
                await asyncio.sleep(2.0)
            except Exception as e:
                await log.aerror("worker_error", error=str(e))
                await asyncio.sleep(0.5)

    def stop(self) -> None:
        self._running = False


async def main() -> None:
    worker = OrchestrationWorker()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, worker.stop)
    try:
        await worker.connect()
        await worker.run()
    finally:
        await worker.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
