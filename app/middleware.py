"""
OniQuant v6.0 — Non-Blocking Latency Tracking Middleware
==========================================================
Uses time.perf_counter() for nanosecond-precision request timing.
Logs asynchronously via structlog to avoid blocking the response path.
"""

from __future__ import annotations

import time

import structlog
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint

log: structlog.stdlib.BoundLogger = structlog.get_logger("oniquant.middleware")


class LatencyMiddleware(BaseHTTPMiddleware):
    """
    ASGI middleware that measures per-request latency.

    Injects X-Latency-Us header into every response for downstream
    observability. Logs request path, status, and latency asynchronously.
    """

    async def dispatch(
        self,
        request: Request,
        call_next: RequestResponseEndpoint,
    ) -> Response:
        t0 = time.perf_counter()
        response: Response = await call_next(request)
        elapsed_us = (time.perf_counter() - t0) * 1_000_000

        response.headers["X-Latency-Us"] = f"{elapsed_us:.0f}"

        # Async structured log — does NOT block the response
        await log.ainfo(
            "request_latency",
            method=request.method,
            path=request.url.path,
            status=response.status_code,
            latency_us=round(elapsed_us, 2),
        )

        return response
