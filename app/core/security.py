"""
OniQuant v6.0 — Security & Secrets Management
=================================================
Loads secrets from Railway environment variables.
Ensures sensitive values are NEVER logged, even in debug mode.

Authentication:
    - SIGNAL_SECRET_TOKEN: shared secret for TradingView webhook auth.
      Must be sent as the X-OniQuant-Auth header on every webhook request.
      Generate with: python -c "import secrets; print(secrets.token_urlsafe(32))"
"""

from __future__ import annotations

import hmac
import os
import re
from typing import Any

import structlog

log = structlog.get_logger("oniquant.security")

# ---------------------------------------------------------------------------
# Secret Redaction
# ---------------------------------------------------------------------------

# Patterns that indicate a secret value — never log these.
# Covers: passwords, API keys, tokens, credentials, private keys,
# the signal auth token, and rotated desk_id suffixes (hex tails).
_SECRET_PATTERNS = re.compile(
    r"(password|secret|api_key|api_secret|token|credential|private_key"
    r"|signal_secret|oniquant.auth|desk_id|x.oniquant)",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Signal Authentication (X-OniQuant-Auth header)
# ---------------------------------------------------------------------------

# Loaded once at import time. If unset, all webhook requests are rejected.
_SIGNAL_SECRET_TOKEN: str = os.getenv("SIGNAL_SECRET_TOKEN", "")


def verify_signal_auth(header_value: str | None) -> bool:
    """
    Constant-time comparison of the X-OniQuant-Auth header against
    the configured SIGNAL_SECRET_TOKEN.

    Returns True only if:
        1. SIGNAL_SECRET_TOKEN is configured (non-empty).
        2. header_value matches SIGNAL_SECRET_TOKEN exactly.

    Uses hmac.compare_digest to prevent timing side-channels.
    """
    if not _SIGNAL_SECRET_TOKEN:
        # Fail closed: if no token configured, reject everything.
        return False
    if not header_value:
        return False
    return hmac.compare_digest(header_value, _SIGNAL_SECRET_TOKEN)


def load_secret(env_var: str, required: bool = True) -> str:
    """
    Load a secret from environment variables.

    Never logs the actual value — only logs that a key was loaded/missing.
    """
    value = os.getenv(env_var, "")
    if not value and required:
        raise EnvironmentError(f"Required secret '{env_var}' is not set")
    return value


def redact_secrets(data: dict[str, Any]) -> dict[str, Any]:
    """
    Redact any values whose keys match secret patterns.

    Returns a copy with secret values replaced by '***REDACTED***'.
    Safe for logging and error reporting.
    """
    redacted = {}
    for key, value in data.items():
        if _SECRET_PATTERNS.search(key):
            redacted[key] = "***REDACTED***"
        elif isinstance(value, dict):
            redacted[key] = redact_secrets(value)
        else:
            redacted[key] = value
    return redacted


def redact_value(value: str) -> str:
    """Redact a raw string value, showing only the last 4 chars."""
    if len(value) <= 4:
        return "***REDACTED***"
    return f"***...{value[-4:]}"


def validate_required_secrets() -> dict[str, bool]:
    """
    Validate that all required secrets are present.

    Returns dict of {secret_name: is_present}.
    """
    required = [
        "REDIS_URL",
        "DATABASE_URL",
        "SIGNAL_SECRET_TOKEN",
    ]
    optional = [
        "IBKR_USER",
        "IBKR_PASSWORD",
        "IBKR_2FA_SECRET",
        "BYBIT_API_KEY",
        "BYBIT_API_SECRET",
        "DISCORD_WEBHOOK_URL",
        "SLACK_WEBHOOK_URL",
    ]

    status = {}
    for key in required:
        status[key] = bool(os.getenv(key))
    for key in optional:
        status[key] = bool(os.getenv(key))
    return status
