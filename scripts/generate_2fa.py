"""
OniQuant v6.0 — IB Gateway 2FA TOTP Generator
================================================
Generates a 6-digit TOTP code from the TWS_2FA_SECRET environment
variable using oathtool. Used by the IBeam container during the
daily login challenge for unattended authentication.

Usage:
    python generate_2fa.py              # prints 6-digit code to stdout
    code=$(python generate_2fa.py)      # capture in shell script
"""

from __future__ import annotations

import os
import subprocess
import sys


def generate_totp(secret: str | None = None) -> str:
    """
    Generate a 6-digit TOTP code using oathtool.

    Args:
        secret: Base32-encoded TOTP secret. If None, reads from
                TWS_2FA_SECRET or IBKR_2FA_SECRET env vars.

    Returns:
        6-digit TOTP code string.

    Raises:
        SystemExit: If no secret is configured or oathtool fails.
    """
    if secret is None:
        secret = os.getenv("TWS_2FA_SECRET") or os.getenv("IBKR_2FA_SECRET")

    if not secret:
        print("ERROR: TWS_2FA_SECRET or IBKR_2FA_SECRET not set", file=sys.stderr)
        sys.exit(1)

    try:
        # -w 1: accept codes from adjacent 30-second windows (N-1, N, N+1)
        # to tolerate up to ±30s of container clock drift (HIGH-02 fix).
        result = subprocess.run(
            ["oathtool", "--totp", "--base32", "-w", "1", secret],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode != 0:
            print(f"ERROR: oathtool failed: {result.stderr.strip()}", file=sys.stderr)
            sys.exit(1)
        # With -w 1, oathtool outputs multiple codes (one per line).
        # Return the LAST code — it corresponds to the current or next window,
        # which is most likely to still be valid when IBKR processes it.
        codes = result.stdout.strip().splitlines()
        return codes[-1] if codes else result.stdout.strip()
    except FileNotFoundError:
        print("ERROR: oathtool not installed", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    code = generate_totp()
    print(code)
