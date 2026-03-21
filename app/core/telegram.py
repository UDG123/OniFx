"""
OniFx — Per-Sleeve Telegram Signal Dispatcher
================================================
Each of the 3 autonomous floors (STABLE, ACTIVE, AGGRESSIVE)
dispatches signals to its own Telegram channel with sleeve-specific
formatting, risk percentage, and signal type branding.

Environment Variables (one per floor):
    TELEGRAM_BOT_TOKEN          — Shared bot token
    TELEGRAM_CHANNEL_STABLE     — Chat ID for STABLE floor signals
    TELEGRAM_CHANNEL_ACTIVE     — Chat ID for ACTIVE floor signals
    TELEGRAM_CHANNEL_AGGRESSIVE — Chat ID for AGGRESSIVE floor signals
"""

from __future__ import annotations

import os
from typing import Any

import structlog

log = structlog.get_logger("onifx.telegram")

TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")

# ---------------------------------------------------------------------------
# Per-Floor Message Templates
# ---------------------------------------------------------------------------
# Each floor has its own visual identity so subscribers can instantly
# recognize which sleeve a signal belongs to at a glance.

_TEMPLATES: dict[str, str] = {
    "STABLE": (
        "\U0001F3E2 <b>[OniFx STABLE] SIGNAL: {direction} {asset}</b>\n"
        "\n"
        "\U0001F3AF Type: MEAN REVERSION | Confidence: {posterior_pct}%\n"
        "\U0001F4B0 RISK: {sleeve_risk_pct}% of your STABLE SLEEVE (Sub-Account)\n"
        "\n"
        "\U0001F4CA Posterior: {posterior:.4f} | Threshold: {threshold:.0%}\n"
        "\U0001F4CD Target Price: {target_price}\n"
        "\U0001F9EE Raw Kelly: {raw_kelly} | RSSF: {rssf}\n"
        "\U0001F4BC Sleeve Allocation: {sleeve_alloc_pct}% of NLV\n"
        "\n"
        "\U0001F6E1\uFE0F <i>This signal risks {sleeve_risk_pct}% of your Stable sub-account, "
        "which is {total_nlv_impact_pct}% of your total portfolio.</i>"
    ),
    "ACTIVE": (
        "\U0001F4C8 <b>[OniFx ACTIVE] SIGNAL: {direction} {asset}</b>\n"
        "\n"
        "\U0001F3AF Type: TREND FOLLOWING | Confidence: {posterior_pct}%\n"
        "\U0001F4B0 RISK: {sleeve_risk_pct}% of your ACTIVE SLEEVE (Sub-Account)\n"
        "\n"
        "\U0001F4CA Posterior: {posterior:.4f} | Threshold: {threshold:.0%}\n"
        "\U0001F4CD Target Price: {target_price}\n"
        "\U0001F9EE Raw Kelly: {raw_kelly} | RSSF: {rssf}\n"
        "\U0001F4BC Sleeve Allocation: {sleeve_alloc_pct}% of NLV\n"
        "\n"
        "\U0001F6E1\uFE0F <i>This signal risks {sleeve_risk_pct}% of your Active sub-account, "
        "which is {total_nlv_impact_pct}% of your total portfolio.</i>"
    ),
    "AGGRESSIVE": (
        "\U0001F680 <b>[OniFx AGGRESSIVE] SIGNAL: {direction} {asset}</b>\n"
        "\n"
        "\U0001F525 Type: VOLATILITY BREAKOUT | Confidence: {posterior_pct}%\n"
        "\U0001F4B0 RISK: {sleeve_risk_pct}% of your AGGRESSIVE SLEEVE (Sub-Account)\n"
        "\n"
        "\U0001F4CA Posterior: {posterior:.4f} | Threshold: {threshold:.0%}\n"
        "\U0001F4CD Target Price: {target_price}\n"
        "\U0001F9EE Raw Kelly: {raw_kelly} | RSSF: {rssf}\n"
        "\U0001F4BC Sleeve Allocation: {sleeve_alloc_pct}% of NLV\n"
        "\n"
        "\U0001F6E1\uFE0F <i>This signal risks {sleeve_risk_pct}% of your Aggressive sub-account, "
        "which is {total_nlv_impact_pct}% of your total portfolio.</i>"
    ),
}

# Channel ID mapping (floor label → env var name)
_CHANNEL_ENV_MAP: dict[str, str] = {
    "STABLE": "TELEGRAM_CHANNEL_STABLE",
    "ACTIVE": "TELEGRAM_CHANNEL_ACTIVE",
    "AGGRESSIVE": "TELEGRAM_CHANNEL_AGGRESSIVE",
}


# ---------------------------------------------------------------------------
# Message Formatting
# ---------------------------------------------------------------------------

def format_signal_message(
    floor_label: str,
    regime_cfg: dict[str, Any],
    signal: dict[str, Any],
    posterior: float,
) -> str:
    """
    Format a signal message for Telegram dispatch.

    Args:
        floor_label: "STABLE", "ACTIVE", or "AGGRESSIVE"
        regime_cfg: The regime config dict from risk_profiles.yaml
        signal: The raw signal payload
        posterior: The Bayesian posterior probability for this floor

    Returns:
        HTML-formatted Telegram message string
    """
    template = _TEMPLATES.get(floor_label, _TEMPLATES["STABLE"])

    direction_raw = signal.get("signal_direction", 1)
    direction = "BUY" if direction_raw == 1 else "SELL"
    asset = signal.get("asset_symbol", "UNKNOWN")
    target_price = signal.get("target_price", 0)
    threshold = regime_cfg.get("threshold", 0.70)

    raw_kelly = regime_cfg.get("raw_kelly", 0.0)
    rssf = regime_cfg.get("rssf_multiplier", 0.10)
    final_risk = regime_cfg.get("final_sleeve_risk", 0.0)
    sleeve_alloc = regime_cfg.get("sleeve_allocation_pct", 0.0)

    sleeve_risk_pct = round(final_risk * 100, 2)
    sleeve_alloc_pct = round(sleeve_alloc * 100, 0)
    total_nlv_impact = round(final_risk * sleeve_alloc * 100, 3)
    posterior_pct = round(posterior * 100, 1)

    return template.format(
        direction=direction,
        asset=asset,
        posterior=posterior,
        posterior_pct=posterior_pct,
        threshold=threshold,
        target_price=target_price,
        raw_kelly=raw_kelly,
        rssf=rssf,
        sleeve_risk_pct=sleeve_risk_pct,
        sleeve_alloc_pct=int(sleeve_alloc_pct),
        total_nlv_impact_pct=total_nlv_impact,
    )


# ---------------------------------------------------------------------------
# Telegram Dispatch
# ---------------------------------------------------------------------------

async def dispatch_floor_signal(
    floor_label: str,
    regime_cfg: dict[str, Any],
    signal: dict[str, Any],
    posterior: float,
) -> bool:
    """
    Dispatch a formatted signal to the floor-specific Telegram channel.

    Each floor sends to its own channel (set via env var).
    Non-blocking: failures are logged but never crash the caller.

    Returns True if dispatch succeeded, False otherwise.
    """
    if not TELEGRAM_BOT_TOKEN:
        await log.adebug("telegram_skipped_no_token", floor=floor_label)
        return False

    channel_env = _CHANNEL_ENV_MAP.get(floor_label, "")
    chat_id = os.getenv(channel_env, "")
    if not chat_id:
        await log.adebug(
            "telegram_skipped_no_channel",
            floor=floor_label,
            env_var=channel_env,
        )
        return False

    message = format_signal_message(floor_label, regime_cfg, signal, posterior)

    try:
        import httpx

        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }

        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()

        await log.ainfo(
            "telegram_signal_dispatched",
            floor=floor_label,
            asset=signal.get("asset_symbol"),
            posterior=round(posterior, 4),
        )
        return True

    except Exception as e:
        await log.awarning(
            "telegram_dispatch_failed",
            floor=floor_label,
            error=str(e),
        )
        return False


async def dispatch_all_authorized_floors(
    tri_state: dict[str, dict[str, Any]],
    regimes_cfg: dict[str, dict[str, Any]],
    signal: dict[str, Any],
) -> dict[str, bool]:
    """
    Dispatch Telegram signals for ALL authorized floors.

    Called after tri-state evaluation. Each floor that returned
    AUTHORIZE gets its own Telegram message sent to its own channel.

    Args:
        tri_state: The tri-state results dict from BayesianArbiter.evaluate()
        regimes_cfg: The regimes config from risk_profiles.yaml
        signal: The raw signal payload

    Returns:
        Dict mapping floor label → dispatch success boolean
    """
    results: dict[str, bool] = {}

    for regime_name, regime_result in tri_state.items():
        if regime_result["decision"] != "AUTHORIZE":
            continue

        floor_label = regime_result.get("label", regime_name.upper())
        cfg = regimes_cfg.get(regime_name, {})
        posterior = regime_result.get("posterior", 0.0)

        ok = await dispatch_floor_signal(floor_label, cfg, signal, posterior)
        results[floor_label] = ok

    return results
