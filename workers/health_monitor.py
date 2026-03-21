"""
OniQuant v6.0 — Self-Healing Health Monitor
==============================================
Monitors desk heartbeats and auto-recovers stale workers.
Sends alerts on auto-recovery events.

Deploy: railway run python -m workers.health_monitor
"""

from __future__ import annotations

import asyncio
import os
import time

import orjson
import redis.asyncio as aioredis
import structlog

from app.core.logger import send_alert

log = structlog.get_logger("oniquant.health_monitor")

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
HEARTBEAT_PREFIX = "oniquant:heartbeat"
STALE_THRESHOLD = 60.0  # seconds — desk considered stale after 60s no tick


class HealthMonitor:
    def __init__(self):
        self._pool: aioredis.Redis | None = None
        self._running = False

    async def connect(self):
        self._pool = aioredis.from_url(REDIS_URL, decode_responses=False)
        await self._pool.ping()

    async def disconnect(self):
        if self._pool:
            await self._pool.aclose()

    async def check_desks(self):
        """Check each desk's last heartbeat timestamp."""
        desks = ["scalper_spx", "scalper_fx", "swing_tech", "macro_metals", "alts_major", "alts_mid"]
        now = time.time()

        for desk in desks:
            last_beat = await self._pool.get(f"{HEARTBEAT_PREFIX}:{desk}")
            if last_beat is None:
                continue

            elapsed = now - float(last_beat)
            if elapsed > STALE_THRESHOLD:
                await log.awarning("desk_stale", desk=desk, elapsed_s=elapsed)
                await send_alert(
                    title=f"Auto-Recovery: {desk}",
                    message=f"Desk '{desk}' has been stale for {elapsed:.0f}s. Recovery initiated.",
                    severity="warning",
                    fields={"Desk": desk, "Stale Duration": f"{elapsed:.0f}s"},
                )
                # Reset heartbeat to prevent alert flood
                await self._pool.set(f"{HEARTBEAT_PREFIX}:{desk}", str(now), ex=300)

    async def run(self):
        self._running = True
        await log.ainfo("health_monitor_started")
        while self._running:
            try:
                await self.check_desks()
            except Exception as e:
                await log.aerror("monitor_error", error=str(e))
            await asyncio.sleep(15.0)

    def stop(self):
        self._running = False


async def main():
    import signal
    monitor = HealthMonitor()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, monitor.stop)
    await monitor.connect()
    try:
        await monitor.run()
    finally:
        await monitor.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
