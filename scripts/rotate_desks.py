"""
OniFx — Desk ID Rotation Script
=================================
Generates new high-entropy desk_id strings and propagates them to:
    1. config/symbols.yaml  (desk key names)
    2. app/schemas.py       (_KNOWN_DESKS frozenset)
    3. DOCS/TRADINGVIEW_PAYLOAD.txt (user-facing reference)

Usage:
    python scripts/rotate_desks.py [--dry-run] [--suffix-length 6]

The script appends a cryptographic random hex suffix to each desk's
base name (e.g., "scalper_spx" -> "scalper_spx_a7f3b1").

IMPORTANT: After rotation you MUST update TradingView alert
configurations to use the new desk_id values.
"""

from __future__ import annotations

import argparse
import re
import secrets
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# All known desk base names (canonical list lives in app/schemas.py)
_BASE_DESKS = [
    "scalper_spx", "scalper_fx", "scalper_ndx",
    "swing_tech", "macro_metals", "macro_gold",
    "alts_major", "alts_mid", "alts_btc", "alts_eth",
    "luxalgo",
]

# Pattern to strip a previous rotation suffix (hex tail after last _)
_SUFFIX_RE = re.compile(r"^(.+?)_[0-9a-f]{4,12}$")


def _base_name(desk_id: str) -> str:
    """Strip any existing rotation suffix to get the base name."""
    m = _SUFFIX_RE.match(desk_id)
    if m and m.group(1) in _BASE_DESKS:
        return m.group(1)
    return desk_id


def generate_rotated_ids(suffix_length: int = 6) -> dict[str, str]:
    """Return {old_base_name: new_rotated_name} for every desk."""
    mapping: dict[str, str] = {}
    for base in _BASE_DESKS:
        suffix = secrets.token_hex(suffix_length // 2 + suffix_length % 2)[:suffix_length]
        mapping[base] = f"{base}_{suffix}"
    return mapping


def _rotate_symbols_yaml(mapping: dict[str, str], dry_run: bool) -> None:
    """Replace desk keys in config/symbols.yaml."""
    path = PROJECT_ROOT / "config" / "symbols.yaml"
    content = path.read_text()

    for old, new in mapping.items():
        # Match the desk key at the start of a YAML mapping entry
        # e.g., "  scalper_spx:" -> "  scalper_spx_a7f3b1:"
        content = re.sub(
            rf"^(\s*){re.escape(old)}(\s*:)",
            rf"\1{new}\2",
            content,
            flags=re.MULTILINE,
        )

    if dry_run:
        print(f"[DRY RUN] Would update {path}")
    else:
        path.write_text(content)
        print(f"[OK] Updated {path}")


def _rotate_schemas_py(mapping: dict[str, str], dry_run: bool) -> None:
    """Replace _KNOWN_DESKS entries in app/schemas.py."""
    path = PROJECT_ROOT / "app" / "schemas.py"
    content = path.read_text()

    for old, new in mapping.items():
        content = content.replace(f'"{old}"', f'"{new}"')

    if dry_run:
        print(f"[DRY RUN] Would update {path}")
    else:
        path.write_text(content)
        print(f"[OK] Updated {path}")


def _regenerate_payload_docs(mapping: dict[str, str], dry_run: bool) -> None:
    """Regenerate DOCS/TRADINGVIEW_PAYLOAD.txt with current desk IDs."""
    path = PROJECT_ROOT / "DOCS" / "TRADINGVIEW_PAYLOAD.txt"
    rotated_ids = sorted(mapping.values())
    desk_list = ", ".join(rotated_ids)
    first_desk = rotated_ids[0] if rotated_ids else "luxalgo"
    # Find the luxalgo-derived rotated ID for the example
    luxalgo_id = next((v for k, v in mapping.items() if k == "luxalgo"), first_desk)

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    content = f"""\
OniFx — TradingView Webhook Payload Reference
=================================================
Last Updated: {now}
Schema Source: app/schemas.py (TradingViewPayload)

This file documents the exact JSON format expected by the
OniFx webhook endpoint:

    POST https://<your-domain>/webhook/luxalgo

    Headers:
        Content-Type: application/json
        X-OniQuant-Auth: <your SIGNAL_SECRET_TOKEN>

Keep this file in sync with app/schemas.py. The Night Auditor
(scripts/night_auditor.py) validates sync weekly.

==========================================================
AUTHENTICATION
==========================================================

Every request MUST include the X-OniQuant-Auth header with
the value of the SIGNAL_SECRET_TOKEN environment variable.
Requests without a valid token receive 403 Forbidden.

==========================================================
REQUIRED FIELDS
==========================================================

signal_id       (string, max 64 chars)
    Unique identifier for this signal. If empty, the system
    generates a UUID. Used for deduplication and audit trail.
    Example: "luxalgo_SPY_20260321_143022"

desk_id         (string, max 64 chars)
    Which desk/strategy generated this signal. Must be one of:
    {desk_list}
    Example: "{first_desk}"

asset_symbol    (string, max 32 chars)
    Instrument ticker. Alphanumeric + underscores/hyphens only.
    Example: "SPY", "BTCUSDT", "GC_F", "EURUSD"

signal_direction (integer, -1 or 1)
    Trade direction. 1 = BUY/LONG, -1 = SELL/SHORT.
    0 is not permitted.

target_price    (float, > 0, < 1,000,000)
    The target/limit price for the trade.
    Example: 451.20

confidence      (float, 0.0 to 1.0)
    Signal confidence from the indicator. Mapped to the
    Bayesian likelihood pipeline.
    Example: 0.85

==========================================================
OPTIONAL FIELDS (with defaults)
==========================================================

iof_strength    (float, 0.0 to 1.0, default: 0.5)
    Institutional Order Flow strength score.
    >0.80 -> strong boost, <0.30 -> penalty.

hurst_exponent  (float, 0.0 to 1.0, default: 0.5)
    Hurst exponent from Alpha Stack.
    <0.45 = mean-reverting, >0.55 = trending.

model_source    (string, default: "luxalgo")
    Which Alpha Stack model generated the signal.
    Allowed: "luxalgo", "spline", "knn", "unknown"

asset_class     (string, default: "equity")
    Asset class for routing and macro alignment.
    Allowed: "equity", "forex", "metals", "gold", "futures",
    "index", "crypto", "perpetual", "spot_crypto"

indicator_metadata (object, default: {{}})
    Free-form metadata dict. Max serialized size: 4096 bytes.
    Used for audit trail only — not processed by the engine.

ttl_seconds     (integer, 10 to 3600, default: 300)
    Time-to-live for this signal in the pending trades ZSET.
    After TTL expires, the signal is discarded if not filled.

==========================================================
TRADINGVIEW ALERT MESSAGE FORMAT
==========================================================

Paste this into your TradingView alert "Message" field:

{{
  "signal_id": "{{{{ticker}}}}_{{{{timenow}}}}",
  "desk_id": "{luxalgo_id}",
  "asset_symbol": "{{{{ticker}}}}",
  "signal_direction": {{{{strategy.order.action == "buy" ? 1 : -1}}}},
  "target_price": {{{{close}}}},
  "confidence": 0.75,
  "iof_strength": 0.5,
  "hurst_exponent": 0.5,
  "model_source": "luxalgo",
  "asset_class": "equity",
  "indicator_metadata": {{}},
  "ttl_seconds": 300
}}

==========================================================
VALIDATION RULES
==========================================================

1. Maximum payload size: 8192 bytes (8 KiB)
2. All string fields: alphanumeric + _ - . / : space only
3. desk_id must be in the known desk whitelist
4. asset_class and model_source must be in allowed sets
5. indicator_metadata serialized size <= 4096 bytes
6. Payloads failing validation return 422 with error detail

==========================================================
SECURITY
==========================================================

- X-OniQuant-Auth header required (403 on missing/invalid)
- Rate limited: 60 requests/minute per IP (429 on breach)
- Unknown fields are silently stripped (Pydantic strict mode)
- Oversized payloads rejected with 413 before parsing
- Invalid JSON rejected with 400 before validation
- desk_id values are rotated periodically for security
"""

    if dry_run:
        print(f"[DRY RUN] Would regenerate {path}")
    else:
        path.write_text(content)
        print(f"[OK] Regenerated {path}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Rotate OniFx desk IDs with high-entropy suffixes"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print changes without writing files",
    )
    parser.add_argument(
        "--suffix-length", type=int, default=6,
        help="Hex suffix length (default: 6 chars = 24 bits entropy)",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("OniFx Desk ID Rotation")
    print(f"Timestamp: {datetime.now(timezone.utc).isoformat()}")
    print(f"Suffix length: {args.suffix_length} hex chars")
    print(f"Dry run: {args.dry_run}")
    print("=" * 60)

    mapping = generate_rotated_ids(suffix_length=args.suffix_length)

    print("\nNew Desk ID Mapping:")
    for old, new in sorted(mapping.items()):
        print(f"  {old:20s} -> {new}")

    print()
    _rotate_symbols_yaml(mapping, dry_run=args.dry_run)
    _rotate_schemas_py(mapping, dry_run=args.dry_run)
    _regenerate_payload_docs(mapping, dry_run=args.dry_run)

    if not args.dry_run:
        print("\n[ACTION REQUIRED]")
        print("  1. Update TradingView alert messages with new desk_id values.")
        print("  2. Restart the FastAPI application to load new schemas.")
        print("  3. Verify with: curl -X POST .../webhook/luxalgo")

    print("\nRotation complete.")


if __name__ == "__main__":
    main()
