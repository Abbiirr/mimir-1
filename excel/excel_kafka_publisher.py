# excel_kafka_publisher.py
"""
Decoupled Kafka publisher for Excel row change events.

Usage (from excel_watcher.py):
    from excel_kafka_publisher import publish_excel_changes, close_publisher

    publish_excel_changes(path, new_rows, upd_rows)  # call after you detect changes
    ...
    close_publisher()  # on shutdown

Assumptions:
- You already have producer utilities in producer.py:
    - build_producer_from_settings(module_name: str | None) -> KafkaProducer
    - send_batch(producer, topic, events: list[tuple[str, dict]], flush_after: bool=True) -> int
    - flush(producer, timeout: float | None=None) -> None
- Default topic comes from settings.py -> settings.kafka.topic.excel_rows
- Keys are the sheet-scoped row keys you already computed in the watcher (e.g., "Sheet1:INV-42")
"""

from __future__ import annotations
from pathlib import Path
from typing import Iterable, Tuple, List, Dict, Optional
import logging
import datetime as dt
from decimal import Decimal
from core.settings import get_settings
from core.producer import build_producer_from_settings, send_batch, flush

log = logging.getLogger(__name__)

# Module-level singletons
_SETTINGS = get_settings()
_TOPIC_DEFAULT = _SETTINGS.kafka.topic.excel_rows  # e.g., "excel.rows.v1"
_PRODUCER = None  # type: ignore


# ---------- JSON helpers ----------

def _jsonify_value(v):
    """Convert values to JSON-safe primitives (ISO datetime, float for Decimal, utf-8 for bytes)."""
    if isinstance(v, (dt.datetime, dt.date, dt.time)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, bytes):
        return v.decode("utf-8", "replace")
    return v

def _jsonify_row(row: dict) -> dict:
    return {k: _jsonify_value(v) for k, v in row.items()}


# ---------- Producer lifecycle ----------

def _ensure_producer(module_name: Optional[str] = "excel"):
    """Build the shared Kafka producer once using your settings."""
    global _PRODUCER
    if _PRODUCER is None:
        _PRODUCER = build_producer_from_settings(module_name=module_name)
        log.info("Kafka producer created (module=%s) -> bootstrap=%s",
                 module_name, _SETTINGS.kafka.connection.bootstrap)

def close_publisher(timeout: float | None = None):
    """Flush and (optionally) close producer on shutdown."""
    global _PRODUCER
    try:
        if _PRODUCER:
            flush(_PRODUCER, timeout=timeout)
            log.info("Kafka producer flushed")
    finally:
        # If you want to fully close the underlying producer, uncomment:
        # if _PRODUCER:
        #     _PRODUCER.close()
        pass


# ---------- Event building & publish ----------

def _build_events(
    path: Path,
    new_rows: List[Tuple[str, str, dict]],  # (sheet, sheet_key, row)
    upd_rows: List[Tuple[str, str, dict]],
) -> list[tuple[str, dict]]:
    """Return a list[(key, payload)] ready for send_batch()."""
    mtime_iso = dt.datetime.fromtimestamp(path.stat().st_mtime, tz=dt.timezone.utc).isoformat()
    observed_iso = dt.datetime.now(tz=dt.timezone.utc).isoformat()

    events: list[tuple[str, dict]] = []

    def _mk(sheet: str, sheet_key: str, row: dict, action: str) -> tuple[str, dict]:
        # Kafka key: keep the sheet-scoped key (stable partitioning & ordering)
        key = sheet_key
        # Also include the non-namespaced row_key for convenience
        row_key = sheet_key.split(":", 1)[1] if ":" in sheet_key else sheet_key
        payload = {
            "action": action,
            "file": path.name,
            "path": str(path),
            "sheet": sheet,
            "row_key": row_key,
            "observed_at": observed_iso,
            "file_mtime": mtime_iso,
            "row": _jsonify_row(row),
        }
        return key, payload

    for sheet, sheet_key, row in new_rows:
        events.append(_mk(sheet, sheet_key, row, "row_added"))

    for sheet, sheet_key, row in upd_rows:
        events.append(_mk(sheet, sheet_key, row, "row_updated"))

    return events


def publish_excel_changes(
    path: Path,
    new_rows: List[Tuple[str, str, dict]],
    upd_rows: List[Tuple[str, str, dict]],
    *,
    topic: Optional[str] = None,
    module_name: Optional[str] = "excel",
    flush_after: bool = False,
) -> int:
    """
    Publish row changes to Kafka.

    Args:
        path: Path to the Excel file (used for metadata)
        new_rows: list of (sheet, sheet_key, row_dict)
        upd_rows: list of (sheet, sheet_key, row_dict)
        topic: override topic name (defaults to settings.kafka.topic.excel_rows)
        module_name: used by build_producer_from_settings (for per-module overrides)
        flush_after: call flush() after sending (default False for throughput)

    Returns:
        Number of events successfully queued for send.
    """
    if not new_rows and not upd_rows:
        return 0

    _ensure_producer(module_name=module_name)
    resolved_topic = topic or _TOPIC_DEFAULT

    events = _build_events(path, new_rows, upd_rows)
    try:
        sent = send_batch(_PRODUCER, resolved_topic, events, flush_after=flush_after)
        log.info("[%s] Published %d events to %s", path.name, sent, resolved_topic)
        return sent
    except Exception as e:
        log.error("Kafka publish error for %s -> %s: %s", path.name, resolved_topic, e)
        return 0
