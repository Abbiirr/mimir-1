# consumer.py – Modular Kafka consumer with settings integration
import logging
from typing import Callable, Iterable, Optional, Dict, Any
from kafka import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord
from kafka.errors import KafkaError

from settings import get_settings

logger = logging.getLogger(__name__)

# Type alias for batch handlers
BatchHandler = Callable[[Iterable[ConsumerRecord]], None]


def build_consumer(
        *,
        bootstrap_servers: str,
        group_id: str,
        auto_offset_reset: str = "earliest",
        enable_auto_commit: bool = False,
        session_timeout_ms: int = 120_000,
        heartbeat_interval_ms: int = 3_000,
        max_poll_interval_ms: int = 300_000,
        max_partition_fetch_bytes: int = 1_048_576,
        extra: Optional[dict] = None,
) -> KafkaConsumer:
    """
    Build a configured KafkaConsumer with manual commit control.

    Args:
        bootstrap_servers: Kafka broker addresses
        group_id: Consumer group ID
        auto_offset_reset: Where to start reading ('earliest' or 'latest')
        enable_auto_commit: Whether to auto-commit offsets (False for manual control)
        session_timeout_ms: Session timeout
        heartbeat_interval_ms: Heartbeat interval
        max_poll_interval_ms: Maximum time between polls
        max_partition_fetch_bytes: Max bytes per partition
        extra: Additional kafka-python parameters

    Returns:
        Configured KafkaConsumer instance
    """
    cfg = {
        "bootstrap_servers": bootstrap_servers,
        "group_id": group_id,
        "auto_offset_reset": auto_offset_reset,
        "enable_auto_commit": enable_auto_commit,
        "session_timeout_ms": session_timeout_ms,
        "heartbeat_interval_ms": heartbeat_interval_ms,
        "max_poll_interval_ms": max_poll_interval_ms,
        "max_partition_fetch_bytes": max_partition_fetch_bytes,
    }

    if extra:
        cfg.update(extra)

    logger.info(f"Building consumer: group={group_id}, servers={bootstrap_servers}")
    return KafkaConsumer(**cfg)


def build_consumer_from_settings(
        module_name: str,
        group_override: Optional[str] = None
) -> KafkaConsumer:
    """
    Build a consumer using settings for a specific module.

    Args:
        module_name: Module name to get consumer group for
        group_override: Optional override for consumer group ID

    Returns:
        Configured KafkaConsumer instance
    """
    s = get_settings()

    # Get connection parameters
    conn_params = s.kafka.connection.get_connection_params()

    # Get consumer group
    group_id = group_override or s.kafka.get_consumer_group(module_name)

    # Build extra parameters (security, etc.)
    extra = {k: v for k, v in conn_params.items() if k != "bootstrap_servers"}

    return build_consumer(
        bootstrap_servers=conn_params["bootstrap_servers"],
        group_id=group_id,
        auto_offset_reset=s.kafka.consumer.auto_offset_reset,
        enable_auto_commit=s.kafka.consumer.enable_auto_commit,
        session_timeout_ms=s.kafka.consumer.session_timeout_ms,
        heartbeat_interval_ms=s.kafka.consumer.heartbeat_interval_ms,
        max_poll_interval_ms=s.kafka.consumer.max_poll_interval_ms,
        max_partition_fetch_bytes=s.kafka.consumer.max_partition_fetch_bytes_default,
        extra=extra
    )


def poll_batch_and_process(
        consumer: KafkaConsumer,
        topic: str,
        handler: BatchHandler,
        *,
        max_records: int = 500,
        timeout_ms: int = 1_000,
        commit_on_success: bool = True,
        error_handler: Optional[Callable[[Exception, ConsumerRecord], None]] = None
) -> int:
    """
    Poll one batch, process it, and optionally commit on success.

    Args:
        consumer: KafkaConsumer instance
        topic: Topic to consume from
        handler: Function to process batch of records
        max_records: Maximum records per poll
        timeout_ms: Poll timeout in milliseconds
        commit_on_success: Whether to commit after successful processing
        error_handler: Optional error handler for individual records

    Returns:
        Number of records processed
    """
    if topic not in consumer.subscription():
        consumer.subscribe([topic])
        logger.info(f"Subscribed to topic: {topic}")

    try:
        batches = consumer.poll(timeout_ms=timeout_ms, max_records=max_records)

        # Flatten batches to a list of ConsumerRecord
        records = [r for _, rs in batches.items() for r in rs]

        if not records:
            return 0

        logger.debug(f"Polled {len(records)} records from {topic}")

        # Process the batch
        try:
            handler(records)

            # Commit on success if configured
            if commit_on_success:
                consumer.commit()
                logger.debug(f"Committed offsets for {len(records)} records")

        except Exception as e:
            logger.error(f"Error processing batch: {e}")

            # Try error handler if provided
            if error_handler:
                for record in records:
                    try:
                        error_handler(e, record)
                    except Exception as eh_error:
                        logger.error(f"Error in error handler: {eh_error}")

            raise  # Re-raise to prevent offset commit

        return len(records)

    except KafkaError as e:
        logger.error(f"Kafka error during poll: {e}")
        raise


def consume_forever(
        consumer: KafkaConsumer,
        topics: list[str],
        handler: BatchHandler,
        *,
        max_records: int = 500,
        timeout_ms: int = 1_000,
        on_shutdown: Optional[Callable[[], None]] = None
) -> None:
    """
    Consume from topics forever until interrupted.

    Args:
        consumer: KafkaConsumer instance
        topics: List of topics to consume from
        handler: Function to process batches
        max_records: Maximum records per poll
        timeout_ms: Poll timeout
        on_shutdown: Optional cleanup function
    """
    consumer.subscribe(topics)
    logger.info(f"Started consuming from topics: {topics}")

    try:
        while True:
            batches = consumer.poll(timeout_ms=timeout_ms, max_records=max_records)

            # Process all topic-partitions
            for topic_partition, records in batches.items():
                if records:
                    logger.debug(f"Processing {len(records)} records from {topic_partition}")

                    try:
                        handler(records)
                        consumer.commit()
                    except Exception as e:
                        logger.error(f"Error processing batch from {topic_partition}: {e}")
                        # Skip commit on error (at-least-once semantics)

    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error in consumer loop: {e}")
        raise
    finally:
        if on_shutdown:
            on_shutdown()
        consumer.close()
        logger.info("Consumer closed")


class ModularConsumer:
    """
    High-level consumer wrapper for modular architecture.
    """

    def __init__(self, module_name: str):
        """
        Initialize consumer for a specific module.

        Args:
            module_name: Name of the module
        """
        self.module_name = module_name
        self.settings = get_settings()
        self.consumer = None
        self.logger = logging.getLogger(f"{__name__}.{module_name}")

    def start(
            self,
            topics: list[str],
            handler: BatchHandler,
            group_override: Optional[str] = None
    ) -> None:
        """
        Start consuming from topics.

        Args:
            topics: List of topics to consume
            handler: Batch handler function
            group_override: Optional consumer group override
        """
        self.consumer = build_consumer_from_settings(
            self.module_name,
            group_override=group_override
        )

        max_records = self.settings.kafka.consumer.max_poll_records
        timeout_ms = self.settings.kafka.consumer.poll_timeout_ms

        self.logger.info(f"Starting {self.module_name} consumer for topics: {topics}")

        consume_forever(
            self.consumer,
            topics,
            handler,
            max_records=max_records,
            timeout_ms=timeout_ms,
            on_shutdown=self._shutdown
        )

    def _shutdown(self):
        """Cleanup on shutdown"""
        self.logger.info(f"Shutting down {self.module_name} consumer")
        if self.consumer:
            self.consumer.close()


# Example usage
if __name__ == "__main__":
    import json

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    s = get_settings()


    # Example 1: Direct consumer with manual control
    def process_batch(records: Iterable[ConsumerRecord]) -> None:
        """Example batch processor"""
        for record in records:
            key = record.key.decode("utf-8") if record.key else None
            value = json.loads(record.value.decode("utf-8")) if record.value else None

            print(f"Processing: key={key}, value={value}")


    # Example 2: Using ModularConsumer
    consumer_module = ModularConsumer("example")

    # This would run forever
    # consumer_module.start(
    #     topics=[s.kafka.topic.excel_rows],
    #     handler=process_batch
    # )

    # Example 3: Quick test with single poll
    test_consumer = build_consumer_from_settings("test")

    processed = poll_batch_and_process(
        test_consumer,
        s.kafka.topic.excel_rows,
        handler=process_batch,
        max_records=10,
        timeout_ms=5000
    )

    print(f"Processed {processed} records in test")
    test_consumer.close()