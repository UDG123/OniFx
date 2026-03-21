"""
OniQuant v6.0 — Security & Secrets Management
=================================================
Loads secrets from Railway environment variables.
Ensures sensitive values are NEVER logged, even in debug mode.
"""

from __future__ import annotations

import os
import re
from typing import Any

import structlog

log = structlog.get_logger("oniquant.security")

# Patterns that indicate a secret value — never log these
_SECRET_PATTERNS = re.compile(
    r"(password|secret|api_key|api_secret|token|credential|private_key)",
    re.IGNORECASE,
)


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


def validate_required_secrets() -> dict[str, bool]:
    """
    Validate that all required secrets are present.

    Returns dict of {secret_name: is_present}.
    """
    required = [
        "REDIS_URL",
        "DATABASE_URL",
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
