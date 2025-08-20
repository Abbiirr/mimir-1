# rts_kafka_schemas.py
"""
Pydantic models for RTS (real-time streaming) Kafka messages.

- Kafka key: str  (use a stable session_id or per-click UUID)
- Kafka value: RTSClickPayloadV1 (below)

You can export a JSON Schema for documentation/registry via :func:`click_json_schema`.
"""

from __future__ import annotations
from typing import Any, Dict, Optional, Union, Literal
from datetime import datetime
from pydantic import BaseModel, Field


class RTSClickPayloadV1(BaseModel):
    """Minimal payload for a UI click event."""
    action: Literal["click"] = Field(default="click", description="Event action type")
    button: Union[str, Literal["A", "B", "C", "D"]] = Field(..., description="Which button was clicked")
    ts: datetime = Field(..., description="Event timestamp (UTC ISO)")
    session_id: Optional[str] = Field(None, description="Browser/session identifier")
    user_agent: Optional[str] = Field(None, description="User-Agent string (trimmed)")
    page: Optional[str] = Field(None, description="Page path or URL")
    extra: Optional[Dict[str, Any]] = Field(None, description="Any extra fields (non-indexed)")


class RTSClickEvent(BaseModel):
    """Wrapper that matches a simple (key, payload) style used by producer utilities."""
    key: str = Field(..., description="Kafka partition key; prefer session_id or a stable id")
    payload: RTSClickPayloadV1 = Field(..., description="Event payload")


# ---- Optional helpers --------------------------------------------------------

def click_json_schema() -> dict:
    """Export JSON Schema for the Kafka value if you ever want to register it.

    Works with Pydantic v2 (.model_json_schema) and v1 (.schema).
    """
    m = RTSClickPayloadV1
    if hasattr(m, "model_json_schema"):  # Pydantic v2
        return m.model_json_schema()
    # Pydantic v1 fallback
    return m.schema()
