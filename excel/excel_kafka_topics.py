# excel_kafka_topics.py
"""
Centralized Kafka topic names for the Excel ingestion pipeline.

Conventions:
- Lowercase, dot-separated names with an explicit version suffix (e.g., .v1).
- If you use a Schema Registry later, the default TopicNameStrategy derives subjects
  from the topic: <topic>-key and <topic>-value.

Usage:
    from excel_kafka_settings import EXCEL_ROW_V1, TOPICS, subjects_for

    topic = EXCEL_ROW_V1.name              # "excel.row.v1"
    key_subj, val_subj = subjects_for(EXCEL_ROW_V1)  # "excel.row.v1-key", "excel.row.v1-value"
"""

from __future__ import annotations
from dataclasses import dataclass

@dataclass(frozen=True)
class Topic:
    name: str

    @property
    def key_subject(self) -> str:
        return f"{self.name}-key"

    @property
    def value_subject(self) -> str:
        return f"{self.name}-value"


# ---- Single source of truth for topic names ----
EXCEL_ROW_V1 = Topic(name="excel.row.v1")


# ---- Convenience mapping for imports ----
TOPICS = {
    "excel_row_v1": EXCEL_ROW_V1,
}


def subjects_for(topic: Topic) -> tuple[str, str]:
    """Return (key_subject, value_subject) for a topic."""
    return topic.key_subject, topic.value_subject
