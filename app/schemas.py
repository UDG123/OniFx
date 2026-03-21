"""
OniFx — Webhook Payload Schemas (Pydantic v2)
================================================
Strict validation for all incoming TradingView webhook payloads.
Rejects malformed, oversized, or injection-bearing payloads before
they reach the Redis stream.

Keep this file in sync with DOCS/TRADINGVIEW_PAYLOAD.txt.
"""

from __future__ import annotations

import re
from typing import Any

from pydantic import BaseModel, Field, field_validator


# Maximum payload size (bytes) — reject anything larger
MAX_PAYLOAD_SIZE: int = 8192  # 8 KiB

# Allowed characters in string fields (injection prevention)
# Permits: alphanumeric, underscores, hyphens, dots, spaces, slashes
_SAFE_STRING_PATTERN = re.compile(r"^[a-zA-Z0-9_\-./: ]{1,128}$")

# Known valid asset symbols (prevent arbitrary string injection)
_KNOWN_DESKS = frozenset({
    "scalper_spx", "scalper_fx", "scalper_ndx",
    "swing_tech", "macro_metals", "macro_gold",
    "alts_major", "alts_mid", "alts_btc", "alts_eth",
    "luxalgo",
})


class TradingViewPayload(BaseModel):
    """
    Pydantic schema for TradingView LuxAlgo webhook payloads.

    This schema is the single source of truth. The corresponding
    TradingView alert message format is documented in
    DOCS/TRADINGVIEW_PAYLOAD.txt.
    """

    # Required fields
    signal_id: str = Field(default="", max_length=64)
    desk_id: str = Field(max_length=64)
    asset_symbol: str = Field(max_length=32)
    signal_direction: int = Field(ge=-1, le=1)
    target_price: float = Field(gt=0, lt=1_000_000)
    confidence: float = Field(ge=0.0, le=1.0)

    # Alpha Stack inputs
    iof_strength: float = Field(default=0.5, ge=0.0, le=1.0)
    hurst_exponent: float = Field(default=0.5, ge=0.0, le=1.0)
    model_source: str = Field(default="luxalgo", max_length=32)
    asset_class: str = Field(default="equity", max_length=32)

    # Optional metadata
    indicator_metadata: dict[str, Any] = Field(default_factory=dict)
    ttl_seconds: int = Field(default=300, ge=10, le=3600)

    @field_validator("desk_id")
    @classmethod
    def validate_desk_id(cls, v: str) -> str:
        if v not in _KNOWN_DESKS:
            raise ValueError(f"Unknown desk_id: {v}")
        return v

    @field_validator("asset_symbol")
    @classmethod
    def validate_asset_symbol(cls, v: str) -> str:
        if not _SAFE_STRING_PATTERN.match(v):
            raise ValueError(f"Invalid asset_symbol format: {v}")
        return v

    @field_validator("model_source")
    @classmethod
    def validate_model_source(cls, v: str) -> str:
        allowed = {"luxalgo", "spline", "knn", "unknown"}
        if v.lower() not in allowed:
            raise ValueError(f"Invalid model_source: {v}")
        return v.lower()

    @field_validator("asset_class")
    @classmethod
    def validate_asset_class(cls, v: str) -> str:
        allowed = {"equity", "forex", "metals", "gold", "futures",
                   "index", "crypto", "perpetual", "spot_crypto"}
        if v.lower() not in allowed:
            raise ValueError(f"Invalid asset_class: {v}")
        return v.lower()

    @field_validator("indicator_metadata")
    @classmethod
    def validate_metadata_size(cls, v: dict) -> dict:
        """Prevent oversized metadata injection."""
        import orjson
        serialized = orjson.dumps(v)
        if len(serialized) > 4096:
            raise ValueError(
                f"indicator_metadata too large: {len(serialized)} bytes (max 4096)"
            )
        return v
