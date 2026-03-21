"""
OniQuant v6.0 — FastAPI Application (High-Performance Ingestion Layer)
========================================================================
ASGI application configured for uvloop + httptools + orjson.
Decouples TradingView webhooks from signal processing via Redis Streams.

Deploy:
    uvicorn app.main:app --host 0.0.0.0 --port $PORT --loop uvloop --http httptools
"""

from __future__ import annotations

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
# Health
# ---------------------------------------------------------------------------
@app.get("/health", status_code=status.HTTP_200_OK)
async def health() -> dict[str, str]:
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# POST /webhook/luxalgo — Fire-and-Forget Ingestion
# ---------------------------------------------------------------------------
@app.post(
    "/webhook/luxalgo",
    status_code=status.HTTP_200_OK,
    response_class=Response,
)
async def ingest_luxalgo(request: Request) -> Response:
    """
    Ingest TradingView LuxAlgo webhook payload.

    1. Read raw bytes (zero-copy).
    2. Pull live params from Redis config (if WFO has published them).
    3. XADD to oniquant:raw_signals stream.
    4. Return pre-built 200 OK.
    """
    settings = get_settings()
    pool = get_redis()

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
