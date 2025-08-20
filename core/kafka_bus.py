# kafka_bus.py
# Modular Kafka bus with plug-and-play module support

from __future__ import annotations
from typing import Any, Callable, Dict, Iterable, Optional, Type
from enum import Enum
from datetime import datetime, timezone
from dataclasses import dataclass
import logging

from pydantic import BaseModel, Field
from kafka.consumer.fetcher import ConsumerRecord
from kafka import KafkaConsumer

from settings import get_settings, ModuleSettings, get_module_settings
from producer import build_producer, send_event, flush
from consumer import build_consumer, poll_batch_and_process

logger = logging.getLogger(__name__)


# ---------------------------
# Event Schemas (Pydantic)
# ---------------------------

class BaseEvent(BaseModel):
    """Common envelope for all events"""
    key: str = Field(..., description="Partition key & business identity")
    produced_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    schema_version: int = Field(1, description="Schema version for this event model")


class GenericPayloadEvent(BaseEvent):
    """Minimal, flexible payload container"""
    payload: Dict[str, Any] = Field(default_factory=dict, description="Arbitrary data")


class TypedPayloadEvent(BaseEvent):
    """Typed payload with event_type for routing"""
    event_type: str = Field(..., description="Event type for routing/processing")
    payload: Dict[str, Any] = Field(default_factory=dict)


# ---------------------------
# Module Registration
# ---------------------------

@dataclass
class ModuleConfig:
    """Configuration for a pluggable module"""
    name: str
    topics: list[str]  # Topics this module publishes/consumes
    consumer_group: str
    event_schema: Type[BaseModel]
    handler: Optional[Callable[[BaseModel], None]] = None
    settings: Optional[ModuleSettings] = None


class ModuleRegistry:
    """Registry for pluggable modules"""

    def __init__(self):
        self._modules: Dict[str, ModuleConfig] = {}
        self._topic_to_module: Dict[str, str] = {}
        self._topic_to_schema: Dict[str, Type[BaseModel]] = {}

    def register(self, config: ModuleConfig) -> None:
        """Register a new module"""
        if config.name in self._modules:
            raise ValueError(f"Module {config.name} already registered")

        self._modules[config.name] = config

        # Map topics to module and schema
        for topic in config.topics:
            self._topic_to_module[topic] = config.name
            self._topic_to_schema[topic] = config.event_schema

        logger.info(f"Registered module: {config.name} with topics: {config.topics}")

    def get_module(self, name: str) -> Optional[ModuleConfig]:
        """Get module by name"""
        return self._modules.get(name)

    def get_schema_for_topic(self, topic: str) -> Type[BaseModel]:
        """Get schema for a topic"""
        return self._topic_to_schema.get(topic, GenericPayloadEvent)

    def get_module_for_topic(self, topic: str) -> Optional[str]:
        """Get module name that handles a topic"""
        return self._topic_to_module.get(topic)

    @property
    def modules(self) -> Dict[str, ModuleConfig]:
        """Get all registered modules"""
        return self._modules.copy()


# Global registry
_registry = ModuleRegistry()


def register_module(
        name: str,
        topics: list[str],
        event_schema: Type[BaseModel] = GenericPayloadEvent,
        handler: Optional[Callable[[BaseModel], None]] = None,
        settings: Optional[ModuleSettings] = None
) -> None:
    """
    Register a new module with the Kafka bus.

    Example:
        register_module(
            name="user_service",
            topics=["user.events.v1", "user.commands.v1"],
            event_schema=UserEvent,
            handler=process_user_event
        )
    """
    s = get_settings()

    # Get or create settings
    if settings is None:
        settings = get_module_settings(name)

    # Get consumer group
    consumer_group = s.kafka.get_consumer_group(name)

    config = ModuleConfig(
        name=name,
        topics=topics,
        consumer_group=consumer_group,
        event_schema=event_schema,
        handler=handler,
        settings=settings
    )

    _registry.register(config)


def get_module_registry() -> ModuleRegistry:
    """Get the global module registry"""
    return _registry


# ---------------------------
# Event Bus
# ---------------------------

class KafkaEventBus:
    """
    Modular Kafka event bus that works with registered modules.
    Each module can publish/consume without managing connections.
    """

    def __init__(self, module_name: Optional[str] = None):
        """
        Initialize bus for a specific module or general use.

        Args:
            module_name: Name of the module using this bus instance
        """
        self.s = get_settings()
        self.module_name = module_name
        self._producer = None
        self._consumer = None

    @property
    def producer(self):
        """Lazy-load producer"""
        if self._producer is None:
            conn_params = self.s.kafka.connection.get_connection_params()

            # Calculate batch size
            batch_base = self.s.kafka.producer.batch_size
            boost = self.s.kafka.producer.batch_size_boost_factor or 1
            batch_size = int(batch_base * boost)

            self._producer = build_producer(
                bootstrap_servers=conn_params["bootstrap_servers"],
                acks=self.s.kafka.producer.acks,
                compression_type=self.s.kafka.producer.compression_type,
                batch_size=batch_size,
                linger_ms=self.s.kafka.producer.linger_ms,
                request_timeout_ms=self.s.kafka.producer.request_timeout_ms,
                delivery_timeout_ms=self.s.kafka.producer.delivery_timeout_ms,
                retries=self.s.kafka.producer.retry_count,
                extra={k: v for k, v in conn_params.items() if k != "bootstrap_servers"}
            )
        return self._producer

    def get_consumer(self, consumer_group: Optional[str] = None) -> KafkaConsumer:
        """
        Get a consumer for a specific consumer group.

        Args:
            consumer_group: Override consumer group (uses module's group by default)
        """
        if consumer_group is None and self.module_name:
            consumer_group = self.s.kafka.get_consumer_group(self.module_name)
        elif consumer_group is None:
            raise ValueError("Must provide consumer_group or initialize with module_name")

        conn_params = self.s.kafka.connection.get_connection_params()

        return build_consumer(
            bootstrap_servers=conn_params["bootstrap_servers"],
            group_id=consumer_group,
            auto_offset_reset=self.s.kafka.consumer.auto_offset_reset,
            enable_auto_commit=self.s.kafka.consumer.enable_auto_commit,
            session_timeout_ms=self.s.kafka.consumer.session_timeout_ms,
            heartbeat_interval_ms=self.s.kafka.consumer.heartbeat_interval_ms,
            max_poll_interval_ms=self.s.kafka.consumer.max_poll_interval_ms,
            max_partition_fetch_bytes=self.s.kafka.consumer.max_partition_fetch_bytes_default,
            extra={k: v for k, v in conn_params.items() if k != "bootstrap_servers"}
        )

    def publish(
            self,
            topic: str,
            value: Dict[str, Any],
            validate: bool = True
    ) -> None:
        """
        Publish an event to a topic.

        Args:
            topic: Topic name
            value: Event data
            validate: Whether to validate against registered schema
        """
        if validate:
            schema = _registry.get_schema_for_topic(topic)
            evt = schema(**value)
            send_event(self.producer, topic, key=evt.key, value=evt.model_dump())
        else:
            # Assume value has a 'key' field
            key = value.get("key", "unknown")
            send_event(self.producer, topic, key=key, value=value)

    def batch_publish(
            self,
            topic: str,
            events: list[Dict[str, Any]],
            validate: bool = True
    ) -> None:
        """Publish multiple events efficiently"""
        for event in events:
            self.publish(topic, event, validate=validate)
        self.flush()

    def flush(self) -> None:
        """Flush producer buffer"""
        if self._producer:
            flush(self._producer)

    def consume(
            self,
            topic: str,
            handler: Callable[[BaseModel], None],
            consumer_group: Optional[str] = None,
            max_records: Optional[int] = None,
            timeout_ms: Optional[int] = None
    ) -> None:
        """
        Start consuming from a topic. This is a blocking call.

        Args:
            topic: Topic to consume from
            handler: Function to handle each validated event
            consumer_group: Override consumer group
            max_records: Max records per poll
            timeout_ms: Poll timeout
        """
        consumer = self.get_consumer(consumer_group)
        schema = _registry.get_schema_for_topic(topic)

        _max = max_records or self.s.kafka.consumer.max_poll_records
        _timeout = timeout_ms or self.s.kafka.consumer.poll_timeout_ms

        def _wrapper(records: Iterable[ConsumerRecord]) -> None:
            import json
            for r in records:
                try:
                    raw = r.value.decode("utf-8") if isinstance(r.value, bytes) else r.value
                    data = json.loads(raw) if isinstance(raw, str) else raw
                    obj = schema(**data)
                    handler(obj)
                except Exception as e:
                    logger.error(f"Error processing record from {topic}: {e}")
                    raise  # Re-raise to prevent offset commit

        consumer.subscribe([topic])
        logger.info(
            f"Started consuming from {topic} with group {consumer_group or self.s.kafka.get_consumer_group(self.module_name)}")

        while True:
            poll_batch_and_process(
                consumer,
                topic,
                handler=_wrapper,
                max_records=_max,
                timeout_ms=_timeout
            )


# ---------------------------
# Module Base Class
# ---------------------------

class KafkaModule:
    """
    Base class for Kafka modules. Inherit from this to create plug-and-play modules.
    """

    def __init__(self, name: str, settings: Optional[ModuleSettings] = None):
        self.name = name
        self.settings = settings or get_module_settings(name)
        self.bus = KafkaEventBus(module_name=name)
        self.logger = logging.getLogger(f"{__name__}.{name}")

    def register(
            self,
            topics: list[str],
            event_schema: Type[BaseModel] = GenericPayloadEvent
    ) -> None:
        """Register this module with its topics and schema"""
        register_module(
            name=self.name,
            topics=topics,
            event_schema=event_schema,
            handler=self.handle_event if hasattr(self, 'handle_event') else None,
            settings=self.settings
        )

    def publish(self, topic: str, event: Dict[str, Any]) -> None:
        """Publish an event"""
        self.bus.publish(topic, event)

    def batch_publish(self, topic: str, events: list[Dict[str, Any]]) -> None:
        """Publish multiple events"""
        self.bus.batch_publish(topic, events)

    def start_consumer(
            self,
            topic: str,
            handler: Optional[Callable[[BaseModel], None]] = None
    ) -> None:
        """
        Start consuming from a topic. This is a blocking call.
        Uses the module's handle_event method if no handler provided.
        """
        handler = handler or self.handle_event
        self.bus.consume(topic, handler)

    def handle_event(self, event: BaseModel) -> None:
        """Override this method to handle events"""
        raise NotImplementedError("Subclasses must implement handle_event")


# ---------------------------
# Pre-register core modules
# ---------------------------

def initialize_core_modules():
    """Initialize core modules from settings"""
    s = get_settings()

    # Register recommendation module
    register_module(
        name="recommendation",
        topics=[s.kafka.topic.recommendation_generate, s.kafka.topic.recommendation_update],
        event_schema=TypedPayloadEvent
    )

    # Register Excel module
    register_module(
        name="excel",
        topics=[s.kafka.topic.excel_rows],
        event_schema=GenericPayloadEvent
    )


# Initialize on import
initialize_core_modules()

# ---------------------------
# Example Usage
# ---------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Example 1: Using the bus directly
    bus = KafkaEventBus(module_name="example")
    s = get_settings()

    # Publish an event
    bus.publish(
        s.kafka.topic.recommendation_generate,
        {"key": "user-123", "event_type": "generate", "payload": {"user_id": 123}}
    )
    bus.flush()


    # Example 2: Creating a custom module
    class CustomModule(KafkaModule):
        def handle_event(self, event: BaseModel):
            print(f"Processing event: {event}")


    # Register and use the module
    module = CustomModule("custom")
    module.register(
        topics=["custom.events.v1"],
        event_schema=TypedPayloadEvent
    )

    # Module can publish
    module.publish("custom.events.v1", {
        "key": "test-1",
        "event_type": "test",
        "payload": {"data": "test"}
    })

    print("Kafka bus initialized with modular architecture")