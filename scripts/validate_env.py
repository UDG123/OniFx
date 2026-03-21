"""
OniQuant v6.0 — Environment Variable Validator
=================================================
Runs during Railway build phase to prevent partial deploys.
Fails the build if required secrets are missing.

Usage: python scripts/validate_env.py
"""

from __future__ import annotations

import os
import sys


REQUIRED = [
    "REDIS_URL",
    "DATABASE_URL",
]

RECOMMENDED = [
    "IBKR_USER",
    "IBKR_PASSWORD",
    "BYBIT_API_KEY",
    "BYBIT_API_SECRET",
    "DISCORD_WEBHOOK_URL",
]


def main() -> int:
    print("OniQuant v6.0 — Environment Validation")
    print("=" * 45)

    missing_required = []
    missing_recommended = []

    for key in REQUIRED:
        val = os.getenv(key)
        if not val:
            missing_required.append(key)
            print(f"  [FAIL] {key} — REQUIRED but not set")
        else:
            print(f"  [ OK ] {key}")

    for key in RECOMMENDED:
        val = os.getenv(key)
        if not val:
            missing_recommended.append(key)
            print(f"  [WARN] {key} — recommended but not set")
        else:
            # Never print secret values
            print(f"  [ OK ] {key} — set (value redacted)")

    print("=" * 45)

    if missing_required:
        print(f"BUILD FAILED: {len(missing_required)} required variable(s) missing.")
        print(f"Missing: {', '.join(missing_required)}")
        return 1

    if missing_recommended:
        print(f"WARNING: {len(missing_recommended)} recommended variable(s) not set.")

    print("Environment validation passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
