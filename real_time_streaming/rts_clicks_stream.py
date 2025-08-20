# real_time_streaming/rts_clicks_stream.py
"""
A Python "Kafka Streams–style" processor:
- Consumes from rts.click.v1
- Maintains per-button counts in a tumbling window (default 5s)
- Publishes each window to rts.click.counts.v1
- Exposes latest snapshot for WebSocket broadcasting

Notes:
- KafkaProducer is thread-safe; KafkaConsumer is NOT. Use a single consumer thread.
"""
from __future__ import annotations
import json, os, threading, time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional

from kafka import KafkaConsumer  # pip install kafka-python

from real_time_streaming.rts_kafka_topics import RTS_CLICK_V1
from real_time_streaming.rts_kafka_counts_publisher import publish_counts
from core.settings import get_settings, KafkaSettings
# incoming schema from your publisher:
# {"payload": {"action":"click","button":"A","ts": "...", "session_id": ...}}

# ---- config helpers -----------------------------------------------------------
from core.settings import get_settings
SETTINGS = get_settings()


@dataclass
class StreamConfig:
    group_id: str = "rts-clicks-stream"
    window_seconds: int = 5
    poll_timeout_ms: int = 1000
    auto_offset_reset: str = "latest"  # or "earliest"
    enable_auto_commit: bool = True

# ---- aggregator ---------------------------------------------------------------
class ClickStreamAggregator:
    def __init__(self, cfg: Optional[StreamConfig] = None):
        self.cfg = cfg or StreamConfig()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._counts = Counter()
        self._last_snapshot: Dict[str, int] = {}
        self._window_start = datetime.now(timezone.utc)
        self._lock = threading.Lock()
        

    # public API for WS
    def snapshot(self) -> Dict[str, int]:
        with self._lock:
            return dict(self._last_snapshot)

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run_loop, name="rts-clicks-stream", daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    # core loop
    def _run_loop(self):
        conn = SETTINGS.kafka.connection.get_connection_params()  # includes bootstrap_servers
        group_id = SETTINGS.kafka.get_consumer_group("rts-clicks-stream")
        consumer = KafkaConsumer(
            RTS_CLICK_V1.name,
            bootstrap_servers= SETTINGS.kafka.connection.bootstrap,
            group_id=self.cfg.group_id,
            enable_auto_commit=self.cfg.enable_auto_commit,
            auto_offset_reset=self.cfg.auto_offset_reset,
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            key_deserializer=lambda b: b.decode("utf-8") if b else None,
        )
        next_flush = time.time() + self.cfg.window_seconds
        try:
            while not self._stop.is_set():
                # poll
                records = consumer.poll(timeout_ms=self.cfg.poll_timeout_ms)
                progressed = False

                for tp, msgs in records.items():
                    for msg in msgs:
                        data = msg.value or {}
                        payload = data.get("payload") or {}
                        btn = payload.get("button")
                        if btn:
                            with self._lock:
                                self._counts[btn] += 1
                                # keep latest snapshot fresh for WS
                                self._last_snapshot = dict(self._counts)
                            progressed = True

                # publish window if time
                now = time.time()
                if now >= next_flush:
                    self._flush_window()
                    next_flush = now + self.cfg.window_seconds

                # small sleep if nothing happened
                if not progressed:
                    time.sleep(0.05)
        finally:
            try:
                consumer.close()
            except Exception:
                pass

    def _flush_window(self):
        with self._lock:
            counts = dict(self._counts)
            window_start = self._window_start
            window_end = datetime.now(timezone.utc)
            # reset for next window
            self._counts = Counter()
            self._window_start = window_end
            self._last_snapshot = {}  # optional: clear live snapshot after publish

        if counts:
            publish_counts(
                counts=counts,
                window_start=window_start,
                window_end=window_end,
                flush_after=False,
            )


# Singleton aggregator instance you can import in main.py
aggregator = ClickStreamAggregator()
