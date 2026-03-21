"""
OniQuant v6.0 — Centralized Async Logger with Alert Webhooks
===============================================================
Structured JSON logging via structlog + critical alert dispatch
to Discord/Slack webhooks for kill switch, gateway loss, and
rejection streak events.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

import orjson
import structlog

# ---------------------------------------------------------------------------
# Logger Configuration
# ---------------------------------------------------------------------------

def configure_logging(log_level: int = 10) -> None:
    """Configure structlog for the entire application."""
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            structlog.processors.JSONRenderer(serializer=orjson.dumps),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        cache_logger_on_first_use=True,
    )


# ---------------------------------------------------------------------------
# Alert Webhook Dispatcher
# ---------------------------------------------------------------------------

DISCORD_WEBHOOK_URL: str = os.getenv("DISCORD_WEBHOOK_URL", "")
SLACK_WEBHOOK_URL: str = os.getenv("SLACK_WEBHOOK_URL", "")

log = structlog.get_logger("oniquant.alerts")


async def send_alert(
    title: str,
    message: str,
    severity: str = "critical",
    fields: dict[str, Any] | None = None,
) -> None:
    """
    Dispatch a critical alert to Discord and/or Slack webhooks.

    Triggered by:
        - Global Kill Switch activation (drawdown > 3%)
        - IB Gateway connection loss > 5 minutes
        - 10+ consecutive Bayesian rejections (potential data feed issue)

    Non-blocking: failures are logged but never crash the caller.
    """
    payload_fields = fields or {}

    # Discord webhook
    if DISCORD_WEBHOOK_URL:
        try:
            import httpx
            color = {"critical": 0xFF0000, "warning": 0xFFAA00, "info": 0x00AAFF}.get(severity, 0xFFFFFF)
            discord_payload = {
                "embeds": [{
                    "title": f"OniQuant Alert: {title}",
                    "description": message,
                    "color": color,
                    "fields": [{"name": k, "value": str(v), "inline": True} for k, v in payload_fields.items()],
                }]
            }
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(DISCORD_WEBHOOK_URL, json=discord_payload)
        except Exception as e:
            await log.awarning("discord_alert_failed", error=str(e))

    # Slack webhook
    if SLACK_WEBHOOK_URL:
        try:
            import httpx
            emoji = {"critical": ":rotating_light:", "warning": ":warning:", "info": ":information_source:"}.get(severity, "")
            slack_payload = {
                "text": f"{emoji} *OniQuant Alert: {title}*\n{message}",
            }
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(SLACK_WEBHOOK_URL, json=slack_payload)
        except Exception as e:
            await log.awarning("slack_alert_failed", error=str(e))

    await log.ainfo("alert_dispatched", title=title, severity=severity)


# ---------------------------------------------------------------------------
# Pre-built Alert Functions
# ---------------------------------------------------------------------------

async def alert_kill_switch(reason: str, drawdown: float) -> None:
    await send_alert(
        title="GLOBAL KILL SWITCH ACTIVATED",
        message=f"All authorizations halted. Reason: {reason}",
        severity="critical",
        fields={"Drawdown": f"{drawdown:.2%}", "Action": "All desks paused"},
    )


async def alert_gateway_loss(gateway: str, downtime_minutes: float) -> None:
    await send_alert(
        title=f"{gateway} Gateway Connection Lost",
        message=f"Gateway has been unreachable for {downtime_minutes:.1f} minutes.",
        severity="critical",
        fields={"Gateway": gateway, "Downtime": f"{downtime_minutes:.1f}m"},
    )


async def alert_rejection_streak(count: int, last_symbol: str) -> None:
    await send_alert(
        title="High Rejection Streak Detected",
        message=f"{count} consecutive rejections — possible data feed issue.",
        severity="warning",
        fields={"Consecutive Rejections": str(count), "Last Symbol": last_symbol},
    )
