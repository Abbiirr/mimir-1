# real_time_streaming/rts_kafka_counts_schemas.py
from __future__ import annotations
from typing import Dict, Optional
from datetime import datetime
from pydantic import BaseModel, Field


class RTSClickCountsPayloadV1(BaseModel):
    """
    Aggregated counts for a tumbling window.
    """
    window_start: datetime = Field(..., description="Window start (UTC ISO)")
    window_end: datetime = Field(..., description="Window end (UTC ISO)")
    counts: Dict[str, int] = Field(default_factory=dict, description="Button -> count")
    total: int = Field(0, description="Total events observed in this window")
    notes: Optional[str] = Field(None, description="Optional metadata")


class RTSClickCountsEvent(BaseModel):
    """
    Simple (key, payload) wrapper for publishing to Kafka.
    """
    key: str = Field(..., description="Partition key (e.g., 'counts' or window id)")
    payload: RTSClickCountsPayloadV1
