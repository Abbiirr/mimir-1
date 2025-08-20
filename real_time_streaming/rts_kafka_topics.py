# rts_kafka_topics.py
"""
Centralized Kafka topic names for the *real-time streaming* (RTS) module.

Conventions:
- Lowercase, dot-separated names with an explicit version suffix (e.g., .v1).
- Keep this file as the single source of truth for RTS topic names.

Usage:
    from rts_kafka_topics import RTS_CLICK_V1, RTS_COUNT_V1, TOPICS

    topic = RTS_CLICK_V1.name
"""

from __future__ import annotations
from dataclasses import dataclass


@dataclass(frozen=True)
class Topic:
    """Lightweight container for topic names (and optional schema-registry subjects)."""
    name: str

    @property
    def key_subject(self) -> str:
        return f"{self.name}-key"

    @property
    def value_subject(self) -> str:
        return f"{self.name}-value"


# ---- RTS topics ----
RTS_CLICK_V1 = Topic(name="rts.click.v1")         # raw click events (single event per button click)
RTS_COUNT_V1 = Topic(name="rts.click.counts.v1")  # optional aggregated counts stream (future use)

# ---- Convenience mapping for imports ----
TOPICS = {
    "rts_click_v1": RTS_CLICK_V1,
    "rts_count_v1": RTS_COUNT_V1,
}
