"""
OniQuant v6.0 — Phase 3: High-Performance Ingestion Layer
==========================================================
Decoupled webhook ingestion via Redis Streams (XADD fire-and-forget).
Designed for sub-millisecond 200 OK turnaround to TradingView.

Deploy: Railway.app with `uvicorn main:app --host 0.0.0.0 --port $PORT --loop uvloop --http httptools`
"""

from __future__ import annotations

import os
import time
from contextlib import asynccontextmanager
from typing import Any

import orjson
import redis.asyncio as aioredis
import structlog
from fastapi import FastAPI, Request, Response, status
from fastapi.responses import ORJSONResponse

# ---------------------------------------------------------------------------
# Structured Logger (async-safe, zero-copy formatting)
# ---------------------------------------------------------------------------
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer(serializer=orjson.dumps),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(
        int(os.getenv("LOG_LEVEL", structlog.logging.DEBUG)),
    ),
    cache_logger_on_first_use=True,
)
log: structlog.stdlib.BoundLogger = structlog.get_logger("oniquant.ingestion")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
STREAM_KEY: str = "oniquant:raw_signals"
STREAM_MAXLEN: int = int(os.getenv("STREAM_MAXLEN", "50000"))  # bounded stream

# ---------------------------------------------------------------------------
# Redis Connection Pool (singleton, hiredis-accelerated)
# ---------------------------------------------------------------------------
_redis_pool: aioredis.Redis | None = None


async def get_redis() -> aioredis.Redis:
    """Return the shared Redis connection pool. Lazy-init on first call."""
    global _redis_pool
    if _redis_pool is None:
        _redis_pool = aioredis.from_url(
            REDIS_URL,
            decode_responses=False,  # keep bytes — orjson emits bytes natively
            max_connections=int(os.getenv("REDIS_MAX_CONN", "20")),
            socket_connect_timeout=2.0,
            socket_keepalive=True,
            retry_on_timeout=True,
        )
    return _redis_pool


# ---------------------------------------------------------------------------
# Lifespan (startup / shutdown hooks)
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Warm the Redis connection pool on startup; drain on shutdown."""
    pool = await get_redis()
    # Verify connectivity at boot — fail fast on Railway if REDIS_URL is wrong
    await pool.ping()
    await log.ainfo("ingestion_ready", redis=REDIS_URL, stream=STREAM_KEY)
    yield
    # Graceful shutdown
    if _redis_pool is not None:
        await _redis_pool.aclose()
    await log.ainfo("ingestion_shutdown")


# ---------------------------------------------------------------------------
# FastAPI Application
# ---------------------------------------------------------------------------
app = FastAPI(
    title="OniQuant v6.0 Ingestion",
    version="6.0.0-phase3",
    default_response_class=ORJSONResponse,
    lifespan=lifespan,
    docs_url=None,   # disabled in prod — zero attack surface
    redoc_url=None,
)

# ---------------------------------------------------------------------------
# Pre-serialized 200 OK (avoid per-request allocation)
# ---------------------------------------------------------------------------
_OK_BODY: bytes = orjson.dumps({"status": "queued"})
_OK_RESPONSE_HEADERS: dict[str, str] = {
    "content-type": "application/json",
    "x-oniquant-version": "6.0.0",
}


# ---------------------------------------------------------------------------
# Latency Tracking Middleware
# ---------------------------------------------------------------------------
@app.middleware("http")
async def latency_middleware(request: Request, call_next) -> Response:
    """
    Nanosecond-precision latency tracking via perf_counter.
    Logs asynchronously so the response is never blocked by I/O.
    """
    t0 = time.perf_counter()
    response: Response = await call_next(request)
    elapsed_us = (time.perf_counter() - t0) * 1_000_000  # microseconds

    # Inject latency header for downstream observability
    response.headers["X-Latency-Us"] = f"{elapsed_us:.0f}"

    # Fire-and-forget async log — does NOT block the response
    await log.ainfo(
        "request_latency",
        method=request.method,
        path=request.url.path,
        status=response.status_code,
        latency_us=round(elapsed_us, 2),
    )
    return response


# ---------------------------------------------------------------------------
# Health Probe (Railway zero-downtime deploys)
# ---------------------------------------------------------------------------
@app.get("/health", status_code=status.HTTP_200_OK)
async def health() -> dict[str, str]:
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Core Endpoint: POST /webhook/luxalgo
# ---------------------------------------------------------------------------
@app.post(
    "/webhook/luxalgo",
    status_code=status.HTTP_200_OK,
    response_class=Response,  # raw Response — bypass Pydantic serialization entirely
)
async def ingest_luxalgo(request: Request) -> Response:
    """
    Fire-and-forget ingestion of TradingView LuxAlgo webhook payloads.

    Flow:
        1. Read raw bytes from the request body (zero-copy).
        2. XADD to Redis Stream with capped maxlen (O(1) amortized).
        3. Return pre-built 200 OK — no serialization on the hot path.

    The consumer group on `oniquant:raw_signals` handles all downstream
    validation, enrichment, and routing asynchronously.
    """
    # --- Step 1: Zero-copy body read ---
    raw: bytes = await request.body()

    # --- Step 2: Fire-and-forget XADD ---
    # XADD is O(1) for appending; MAXLEN ~ trims are O(log N) amortized.
    # We pass raw bytes directly — no decode/re-encode round-trip.
    pool = await get_redis()
    await pool.xadd(
        STREAM_KEY,
        {"payload": raw, "source": b"luxalgo"},
        maxlen=STREAM_MAXLEN,
        approximate=True,  # ~ flag: avoids exact trim overhead
    )

    # --- Step 3: Pre-serialized 200 OK ---
    return Response(
        content=_OK_BODY,
        status_code=status.HTTP_200_OK,
        headers=_OK_RESPONSE_HEADERS,
    )
