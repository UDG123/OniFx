"""
OniQuant v6.0 — FastAPI Application (High-Performance Ingestion Layer)
========================================================================
ASGI application configured for uvloop + httptools + orjson.
Decouples TradingView webhooks from signal processing via Redis Streams.

Security:
    - Redis-backed sliding window rate limiter on all webhook endpoints
    - Per-IP throttling: 60 requests/minute default (configurable via env)
    - Returns 429 Too Many Requests with Retry-After header on breach

Deploy:
    uvicorn app.main:app --host 0.0.0.0 --port $PORT --loop uvloop --http httptools
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

from app.core.config import get_settings
from app.core.database import close_pool, init_pool
from app.core.redis import close_redis, get_redis, init_redis
from app.middleware import LatencyMiddleware

# ---------------------------------------------------------------------------
# Rate Limiter Configuration
# ---------------------------------------------------------------------------
RATE_LIMIT_REQUESTS: int = int(os.getenv("WEBHOOK_RATE_LIMIT", "60"))
RATE_LIMIT_WINDOW: int = int(os.getenv("WEBHOOK_RATE_WINDOW", "60"))  # seconds

# ---------------------------------------------------------------------------
# Logger
# ---------------------------------------------------------------------------
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer(serializer=orjson.dumps),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(
        get_settings().log_level,
    ),
    cache_logger_on_first_use=True,
)
log: structlog.stdlib.BoundLogger = structlog.get_logger("oniquant.app")

# ---------------------------------------------------------------------------
# Pre-serialized responses (avoid per-request allocation)
# ---------------------------------------------------------------------------
_OK_BODY: bytes = orjson.dumps({"status": "queued"})
_OK_HEADERS: dict[str, str] = {
    "content-type": "application/json",
    "x-oniquant-version": "6.0.0",
}


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup: warm Redis + DB pools. Shutdown: drain both."""
    settings = get_settings()

    # Initialize connection pools
    await init_redis()
    await init_pool()
    await log.ainfo("app_started", version=settings.app_version)

    yield

    # Graceful shutdown
    await close_pool()
    await close_redis()
    await log.ainfo("app_shutdown")


# ---------------------------------------------------------------------------
# Application
# ---------------------------------------------------------------------------
app = FastAPI(
    title="OniQuant v6.0",
    version="6.0.0",
    default_response_class=ORJSONResponse,
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
)

app.add_middleware(LatencyMiddleware)


# ---------------------------------------------------------------------------
# Rate Limiter (Redis Sliding Window)
# ---------------------------------------------------------------------------
# Atomic Lua script: increment a per-IP counter with TTL.
# Returns the current count AFTER incrementing.
# If the key is new, sets expiry to RATE_LIMIT_WINDOW seconds.
# This is a fixed-window approximation that is simple, fast, and
# good enough for webhook DoS prevention (not billing-grade).
_LUA_RATE_CHECK = """
local current = redis.call('INCR', KEYS[1])
if current == 1 then
    redis.call('EXPIRE', KEYS[1], ARGV[1])
end
return current
"""
_rate_check_script = None


async def _check_rate_limit(pool: aioredis.Redis, client_ip: str) -> int:
    """
    Check and increment the rate limit counter for a client IP.

    Returns the current request count in the window.
    """
    global _rate_check_script
    if _rate_check_script is None:
        _rate_check_script = pool.register_script(_LUA_RATE_CHECK)

    key = f"oniquant:ratelimit:{client_ip}"
    count = await _rate_check_script(keys=[key], args=[RATE_LIMIT_WINDOW])
    return int(count)


def _get_client_ip(request: Request) -> str:
    """Extract client IP, respecting X-Forwarded-For behind a reverse proxy."""
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


# Pre-built 429 response
_RATE_LIMITED_BODY: bytes = orjson.dumps({"error": "rate_limit_exceeded"})


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------
@app.get("/health", status_code=status.HTTP_200_OK)
async def health() -> dict[str, str]:
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# POST /webhook/luxalgo — Fire-and-Forget Ingestion (Rate-Limited)
# ---------------------------------------------------------------------------
@app.post(
    "/webhook/luxalgo",
    status_code=status.HTTP_200_OK,
    response_class=Response,
)
async def ingest_luxalgo(request: Request) -> Response:
    """
    Ingest TradingView LuxAlgo webhook payload.

    1. Rate-limit check (per-IP sliding window).
    2. Read raw bytes (zero-copy).
    3. Pull live params from Redis config (if WFO has published them).
    4. XADD to oniquant:raw_signals stream.
    5. Return pre-built 200 OK.
    """
    settings = get_settings()
    pool = get_redis()

    # ── Rate Limit Gate ───────────────────────────────────────
    client_ip = _get_client_ip(request)
    request_count = await _check_rate_limit(pool, client_ip)

    if request_count > RATE_LIMIT_REQUESTS:
        await log.awarning(
            "rate_limit_exceeded",
            client_ip=client_ip,
            count=request_count,
            limit=RATE_LIMIT_REQUESTS,
        )
        return Response(
            content=_RATE_LIMITED_BODY,
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            headers={
                "content-type": "application/json",
                "retry-after": str(RATE_LIMIT_WINDOW),
            },
        )

    # Zero-copy body read
    raw: bytes = await request.body()

    # Optionally enrich with live WFO parameters for this desk
    # The WFO engine publishes optimal params to oniquant:config:{desk_id}
    desk_params = await pool.get("oniquant:config:luxalgo")

    # Build stream entry — include live config ref if available
    fields: dict[str, bytes] = {"payload": raw, "source": b"luxalgo"}
    if desk_params is not None:
        fields["live_params"] = desk_params

    # Fire-and-forget XADD
    await pool.xadd(
        settings.raw_signals_stream,
        fields,
        maxlen=settings.stream_maxlen,
        approximate=True,
    )

    return Response(
        content=_OK_BODY,
        status_code=status.HTTP_200_OK,
        headers=_OK_HEADERS,
    )
