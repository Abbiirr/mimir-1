# real_time_streaming/rts_kafka_counts_publisher.py
from __future__ import annotations
import logging
from typing import Dict, Optional, TYPE_CHECKING
from datetime import datetime, timezone

if TYPE_CHECKING:
    from kafka import KafkaProducer  # type only

from real_time_streaming.rts_kafka_topics import RTS_COUNT_V1
from real_time_streaming.rts_kafka_counts_schemas import RTSClickCountsPayloadV1, RTSClickCountsEvent
from core.producer import build_producer_from_settings, send_event, flush  # ← core stays untouched

log = logging.getLogger(__name__)

_PRODUCER: Optional["KafkaProducer"] = None
_DEFAULT_TOPIC = RTS_COUNT_V1.name
_DEFAULT_MODULE = "rts"


def _ensure_producer(*, module_name: Optional[str] = None):
    global _PRODUCER
    if _PRODUCER is None:
        _PRODUCER = build_producer_from_settings(module_name=module_name or _DEFAULT_MODULE)
        log.info("RTS counts-producer ready (module=%s)", module_name or _DEFAULT_MODULE)
    return _PRODUCER


def close_counts_publisher(timeout: Optional[float] = None) -> None:
    global _PRODUCER
    if _PRODUCER is None:
        return
    try:
        flush(_PRODUCER, timeout=timeout)
    finally:
        try:
            _PRODUCER.close()
        except Exception:
            pass
        _PRODUCER = None
        log.info("RTS counts-producer closed")


def publish_counts(
    *,
    counts: Dict[str, int],
    window_start: datetime,
    window_end: datetime,
    topic: Optional[str] = None,
    key: str = "counts",
    module_name: Optional[str] = None,
    flush_after: bool = False,
) -> bool:
    """
    Publish an aggregate snapshot for a single window.
    """
    try:
        prod = _ensure_producer(module_name=module_name)
        payload = RTSClickCountsPayloadV1(
            window_start=window_start.astimezone(timezone.utc),
            window_end=window_end.astimezone(timezone.utc),
            counts=dict(counts),
            total=sum(counts.values()),
        )
        evt = RTSClickCountsEvent(key=key, payload=payload)
        value = {"payload": evt.payload.model_dump(mode="json")}
        send_event(prod, topic or _DEFAULT_TOPIC, key=evt.key, value=value)
        if flush_after:
            flush(prod)
        return True
    except Exception as e:
        log.error("RTS publish_counts error: %s", e)
        return False
