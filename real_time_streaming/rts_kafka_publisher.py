# rts_kafka_publisher.py
"""
Decoupled Kafka publisher for *real-time streaming* (RTS) click events.

Usage (from your FastAPI app):
    from rts_kafka_publisher import publish_click, publish_clicks, close_publisher

    # single
    ok = publish_click(button="A", session_id="sid-123", user_agent=request.headers.get("user-agent"))

    # batch
    publish_clicks([
        {"button": "A", "session_id": "sid-123"},
        {"button": "B", "session_id": "sid-123"},
    ])

    # on shutdown:
    close_publisher()

Assumptions:
- You already have producer utilities in producer.py:
    - build_producer_from_settings(module_name: str | None) -> KafkaProducer
    - send_event(producer, topic, key, value, on_success=None, on_error=None) -> None
    - send_batch(producer, topic, events: list[tuple[str, dict]], flush_after: bool=True) -> int
    - flush(producer, timeout: float | None=None) -> None

Nothing in core producer/consumer/bus is modified by this module.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Iterable, List, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from kafka import KafkaProducer  # type-only; avoids hard import dependency at runtime

from real_time_streaming.rts_kafka_topics import RTS_CLICK_V1
from real_time_streaming.rts_kafka_schemas import RTSClickPayloadV1, RTSClickEvent
from core.producer import build_producer_from_settings, send_event, send_batch, flush

log = logging.getLogger(__name__)

_PRODUCER: Optional["KafkaProducer"] = None
_DEFAULT_TOPIC = RTS_CLICK_V1.name
_DEFAULT_MODULE = "rts"  # module name to scope settings if you use per-module settings


# ---- lifecycle ---------------------------------------------------------------

def _ensure_producer(*, module_name: Optional[str] = None):
    global _PRODUCER
    if _PRODUCER is None:
        _PRODUCER = build_producer_from_settings(module_name=module_name or _DEFAULT_MODULE)
        log.info("RTS Kafka producer ready (module=%s)", module_name or _DEFAULT_MODULE)
    return _PRODUCER


def close_publisher(timeout: Optional[float] = None) -> None:
    """Flush and close the shared producer."""
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
        log.info("RTS Kafka producer closed")


# ---- builders ----------------------------------------------------------------

def _build_event(
    *,
    button: str,
    session_id: Optional[str],
    user_agent: Optional[str],
    page: Optional[str],
    extra: Optional[Dict[str, Any]],
) -> RTSClickEvent:
    sid = session_id or f"anon-{uuid.uuid4()}"
    payload = RTSClickPayloadV1(
        action="click",
        button=str(button),
        ts=datetime.now(timezone.utc),
        session_id=sid,
        user_agent=(user_agent or "")[:300],
        page=page,
        extra=extra or None,
    )
    return RTSClickEvent(key=sid, payload=payload)


# ---- API ---------------------------------------------------------------------

def publish_click(
    button: str,
    *,
    session_id: Optional[str] = None,
    user_agent: Optional[str] = None,
    page: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
    topic: Optional[str] = None,
    module_name: Optional[str] = None,
    flush_after: bool = False,
    on_success=None,
    on_error=None,
) -> bool:
    """Publish a single click event to Kafka.

    Returns True if the send call was issued (doesn't await delivery).
    """
    try:
        producer = _ensure_producer(module_name=module_name)
        evt = _build_event(
            button=button,
            session_id=session_id,
            user_agent=user_agent,
            page=page,
            extra=extra,
        )
        # your producer's value_serializer does json.dumps; use JSON-friendly dict
        value_dict = {"payload": evt.payload.model_dump(mode="json")}
        send_event(
            producer,
            topic or _DEFAULT_TOPIC,
            key=evt.key,
            value=value_dict,
            on_success=on_success,
            on_error=on_error,
        )
        if flush_after:
            flush(producer)
        return True
    except Exception as e:
        log.error("RTS publish_click error: %s", e)
        return False


def publish_clicks(
    events: Iterable[Dict[str, Any]],
    *,
    topic: Optional[str] = None,
    module_name: Optional[str] = None,
    flush_after: bool = True,
) -> int:
    """Publish a batch of click events.

    'events' is an iterable of dicts with keys compatible with publish_click(...).
    Returns the number of records queued.
    """
    producer = _ensure_producer(module_name=module_name)
    resolved_topic = topic or _DEFAULT_TOPIC

    batch: List[Tuple[str, Dict[str, Any]]] = []
    for e in events:
        evt = _build_event(
            button=e.get("button"),
            session_id=e.get("session_id"),
            user_agent=e.get("user_agent"),
            page=e.get("page"),
            extra=e.get("extra"),
        )
        value_dict = {"payload": evt.payload.model_dump(mode="json")}
        batch.append((evt.key, value_dict))

    try:
        # send_batch returns the number of queued records
        return send_batch(producer, resolved_topic, batch, flush_after=flush_after)
    except Exception as ex:
        log.error("RTS publish_clicks error: %s", ex)
        return 0
