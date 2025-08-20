# producer.py – Modular Kafka producer with settings integration
import json
import logging
from typing import Any, Mapping, Optional, Dict
from kafka import KafkaProducer
from kafka.errors import KafkaError

from core.settings import get_settings

logger = logging.getLogger(__name__)


def build_producer(
        *,
        bootstrap_servers: str,
        acks: str = "all",
        compression_type: Optional[str] = None,
        batch_size: int = 16384,
        linger_ms: int = 5,
        request_timeout_ms: int = 180_000,
        delivery_timeout_ms: int,
        retries: int = 5,
        extra: Optional[Mapping[str, Any]] = None,
) -> KafkaProducer:
    """
    Build a configured KafkaProducer with proper error handling.

    Args:
        bootstrap_servers: Kafka broker addresses
        acks: Number of acknowledgments (0, 1, or 'all')
        compression_type: Compression algorithm ('snappy', 'lz4', 'zstd', 'gzip', None)
        batch_size: Maximum bytes to batch before sending
        linger_ms: Time to wait for batching
        request_timeout_ms: Timeout for requests
        retries: Number of retries on failure
        extra: Additional kafka-python parameters

    Returns:
        Configured KafkaProducer instance
    """
    cfg: Dict[str, Any] = {
        "bootstrap_servers": bootstrap_servers,
        "acks": acks,
        "compression_type": compression_type,
        "batch_size": batch_size,
        "linger_ms": linger_ms,
        "request_timeout_ms": request_timeout_ms,
        "delivery_timeout_ms": delivery_timeout_ms,
        "retries": retries,
        "key_serializer": lambda k: k if isinstance(k, (bytes, bytearray)) else str(k).encode("utf-8"),
        "value_serializer": lambda v: json.dumps(v, default=str).encode("utf-8"),
    }

    if extra:
        cfg.update(extra)

    logger.info(f"Building producer with config: bootstrap={bootstrap_servers}, acks={acks}")
    return KafkaProducer(**cfg)


def build_producer_from_settings(module_name: Optional[str] = None) -> KafkaProducer:
    """
    Build a producer using settings for a specific module.

    Args:
        module_name: Optional module name for module-specific settings

    Returns:
        Configured KafkaProducer instance
    """
    s = get_settings()

    # Get connection parameters
    conn_params = s.kafka.connection.get_connection_params()

    # Calculate batch size with boost factor
    batch_base = s.kafka.producer.batch_size
    boost = s.kafka.producer.batch_size_boost_factor or 1.0
    batch_size = int(batch_base * boost)

    # Build extra parameters (security, etc.)
    extra = {k: v for k, v in conn_params.items() if k != "bootstrap_servers"}

    return build_producer(
        bootstrap_servers=conn_params["bootstrap_servers"],
        acks=s.kafka.producer.acks,
        compression_type=s.kafka.producer.compression_type,
        batch_size=batch_size,
        linger_ms=s.kafka.producer.linger_ms,
        request_timeout_ms=s.kafka.producer.request_timeout_ms,
        delivery_timeout_ms=s.kafka.producer.delivery_timeout_ms,
        retries=s.kafka.producer.retry_count,
        extra=extra
    )


def send_event(
        producer: KafkaProducer,
        topic: str,
        *,
        key: str,
        value: dict,
        on_success: Optional[callable] = None,
        on_error: Optional[callable] = None
) -> None:
    """
    Send an event with optional callbacks.

    Args:
        producer: KafkaProducer instance
        topic: Target topic
        key: Message key for partitioning
        value: Message value (will be JSON serialized)
        on_success: Optional success callback
        on_error: Optional error callback
    """
    try:
        future = producer.send(topic, key=key, value=value)

        if on_success or on_error:
            # Add callbacks if provided
            if on_success:
                future.add_callback(on_success)
            if on_error:
                future.add_errback(on_error)

        logger.debug(f"Sent event to {topic} with key={key}")

    except KafkaError as e:
        logger.error(f"Failed to send event to {topic}: {e}")
        if on_error:
            on_error(e)
        raise


def send_batch(
        producer: KafkaProducer,
        topic: str,
        events: list[tuple[str, dict]],
        flush_after: bool = True
) -> int:
    """
    Send a batch of events efficiently.

    Args:
        producer: KafkaProducer instance
        topic: Target topic
        events: List of (key, value) tuples
        flush_after: Whether to flush after sending batch

    Returns:
        Number of events sent
    """
    count = 0
    errors = []

    for key, value in events:
        try:
            producer.send(topic, key=key, value=value)
            count += 1
        except KafkaError as e:
            errors.append((key, e))
            logger.error(f"Failed to send event with key={key}: {e}")

    if flush_after:
        flush(producer)

    if errors:
        logger.warning(f"Batch send completed with {len(errors)} errors out of {len(events)} events")
    else:
        logger.info(f"Successfully sent batch of {count} events to {topic}")

    return count


def flush(producer: KafkaProducer, timeout: Optional[float] = None) -> None:
    """
    Flush all buffered messages.

    Args:
        producer: KafkaProducer instance
        timeout: Optional timeout in seconds
    """
    try:
        producer.flush(timeout=timeout)
        logger.debug("Producer buffer flushed successfully")
    except KafkaError as e:
        logger.error(f"Error during flush: {e}")
        raise


# Example usage with the new settings
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Method 1: Build from settings
    producer = build_producer_from_settings()

    # Method 2: Build with specific module settings
    s = get_settings()
    producer_with_module = build_producer_from_settings(module_name="excel")

    # Send a test event
    test_event = {
        "timestamp": "2024-01-01T00:00:00Z",
        "message": "Test from modular producer",
        "module": "test"
    }

    # Send to a topic from settings
    topic = s.kafka.topic.excel_rows
    send_event(producer, topic, key="test-key", value=test_event)

    # Send batch
    batch_events = [
        (f"key-{i}", {"id": i, "data": f"event-{i}"})
        for i in range(10)
    ]
    sent = send_batch(producer, topic, batch_events)
    print(f"Sent {sent} events in batch")

    # Cleanup
    flush(producer)
    producer.close()
    print("Producer test completed")
