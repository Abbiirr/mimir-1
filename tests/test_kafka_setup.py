#!/usr/bin/env python
"""
Test script to verify the modular Kafka setup is working correctly.
Run this to test your configuration without needing Kafka running.
"""

import logging
import sys
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def test_settings():
    """Test that settings load correctly"""
    logger.info("Testing settings...")
    try:
        from settings import get_settings, get_module_settings

        # Load main settings
        settings = get_settings()
        logger.info(f"✓ Main settings loaded")
        logger.info(f"  - Environment: {settings.environment}")
        logger.info(f"  - Kafka Bootstrap: {settings.kafka.connection.bootstrap}")
        logger.info(f"  - Database Host: {settings.database.host}")

        # Test module settings
        module_settings = get_module_settings("test_module")
        logger.info(f"✓ Module settings loaded for 'test_module'")

        return True
    except Exception as e:
        logger.error(f"✗ Settings test failed: {e}")
        return False


def test_event_schemas():
    """Test that event schemas work correctly"""
    logger.info("\nTesting event schemas...")
    try:
        from kafka_bus import BaseEvent, GenericPayloadEvent, TypedPayloadEvent

        # Test BaseEvent
        base_event = BaseEvent(key="test-key")
        logger.info(f"✓ BaseEvent created: key={base_event.key}, version={base_event.schema_version}")

        # Test GenericPayloadEvent
        generic_event = GenericPayloadEvent(
            key="generic-key",
            payload={"data": "test"}
        )
        logger.info(f"✓ GenericPayloadEvent created: {generic_event.model_dump()}")

        # Test TypedPayloadEvent
        typed_event = TypedPayloadEvent(
            key="typed-key",
            event_type="test_event",
            payload={"value": 123}
        )
        logger.info(f"✓ TypedPayloadEvent created: type={typed_event.event_type}")

        return True
    except Exception as e:
        logger.error(f"✗ Event schema test failed: {e}")
        return False


def test_module_registration():
    """Test module registration system"""
    logger.info("\nTesting module registration...")
    try:
        from kafka_bus import register_module, get_module_registry, GenericPayloadEvent

        # Register a test module
        register_module(
            name="test_module",
            topics=["test.events.v1", "test.commands.v1"],
            event_schema=GenericPayloadEvent
        )

        # Check registration
        registry = get_module_registry()
        module = registry.get_module("test_module")

        if module:
            logger.info(f"✓ Module registered: {module.name}")
            logger.info(f"  - Topics: {module.topics}")
            logger.info(f"  - Consumer Group: {module.consumer_group}")
            return True
        else:
            logger.error("✗ Module not found in registry")
            return False

    except Exception as e:
        logger.error(f"✗ Module registration test failed: {e}")
        return False


def test_kafka_module():
    """Test KafkaModule base class"""
    logger.info("\nTesting KafkaModule...")
    try:
        from kafka_bus import KafkaModule, GenericPayloadEvent

        class TestModule(KafkaModule):
            def __init__(self):
                super().__init__(name="test_kafka_module")
                self.events_handled = []

            def handle_event(self, event):
                self.events_handled.append(event)
                logger.info(f"  Handled event: {event.key}")

        # Create module
        module = TestModule()
        module.register(
            topics=["test.topic.v1"],
            event_schema=GenericPayloadEvent
        )

        logger.info(f"✓ TestModule created: {module.name}")

        # Test event creation (without actually publishing)
        test_event = {
            "key": "test-1",
            "payload": {"message": "test"}
        }

        # Validate event structure
        GenericPayloadEvent(**test_event)
        logger.info(f"✓ Event validation passed")

        return True

    except Exception as e:
        logger.error(f"✗ KafkaModule test failed: {e}")
        return False


def test_producer_builder():
    """Test producer builder (without connecting)"""
    logger.info("\nTesting producer builder...")
    try:
        from producer import build_producer
        from settings import get_settings

        s = get_settings()

        # Just test that we can create the configuration
        # Don't actually connect to Kafka
        config = {
            "bootstrap_servers": s.kafka.connection.bootstrap,
            "acks": s.kafka.producer.acks,
            "compression_type": s.kafka.producer.compression_type,
            "batch_size": s.kafka.producer.batch_size,
        }

        logger.info(f"✓ Producer config created:")
        logger.info(f"  - Bootstrap: {config['bootstrap_servers']}")
        logger.info(f"  - Acks: {config['acks']}")
        logger.info(f"  - Compression: {config['compression_type']}")

        return True

    except Exception as e:
        logger.error(f"✗ Producer builder test failed: {e}")
        return False


def test_consumer_builder():
    """Test consumer builder (without connecting)"""
    logger.info("\nTesting consumer builder...")
    try:
        from consumer import build_consumer
        from settings import get_settings

        s = get_settings()

        # Just test that we can create the configuration
        config = {
            "bootstrap_servers": s.kafka.connection.bootstrap,
            "group_id": s.kafka.get_consumer_group("test"),
            "auto_offset_reset": s.kafka.consumer.auto_offset_reset,
            "enable_auto_commit": s.kafka.consumer.enable_auto_commit,
        }

        logger.info(f"✓ Consumer config created:")
        logger.info(f"  - Bootstrap: {config['bootstrap_servers']}")
        logger.info(f"  - Group ID: {config['group_id']}")
        logger.info(f"  - Auto Offset Reset: {config['auto_offset_reset']}")

        return True

    except Exception as e:
        logger.error(f"✗ Consumer builder test failed: {e}")
        return False


def test_example_module():
    """Test the example module imports"""
    logger.info("\nTesting example module...")
    try:
        from example_module import UserServiceModule, UserEvent, UserCreatedEvent

        # Create module instance
        module = UserServiceModule()
        logger.info(f"✓ UserServiceModule created: {module.name}")

        # Test event creation
        event = UserCreatedEvent(
            user_id="test-user",
            email="test@example.com",
            username="testuser"
        )
        logger.info(f"✓ UserCreatedEvent created: {event.user_id}")

        return True

    except Exception as e:
        logger.error(f"✗ Example module test failed: {e}")
        return False


def main():
    """Run all tests"""
    logger.info("=" * 60)
    logger.info("Starting Modular Kafka System Tests")
    logger.info("=" * 60)

    tests = [
        ("Settings", test_settings),
        ("Event Schemas", test_event_schemas),
        ("Module Registration", test_module_registration),
        ("Kafka Module", test_kafka_module),
        ("Producer Builder", test_producer_builder),
        ("Consumer Builder", test_consumer_builder),
        ("Example Module", test_example_module),
    ]

    results = []
    for name, test_func in tests:
        try:
            result = test_func()
            results.append((name, result))
        except Exception as e:
            logger.error(f"Unexpected error in {name}: {e}")
            results.append((name, False))

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("TEST SUMMARY")
    logger.info("=" * 60)

    passed = sum(1 for _, result in results if result)
    total = len(results)

    for name, result in results:
        status = "✓ PASSED" if result else "✗ FAILED"
        logger.info(f"{name:25} {status}")

    logger.info("-" * 60)
    logger.info(f"Total: {passed}/{total} tests passed")

    if passed == total:
        logger.info("\n🎉 All tests passed! Your setup is ready.")
        logger.info("\nNext steps:")
        logger.info("1. Ensure Kafka is running")
        logger.info("2. Run: python example_module.py producer  # To produce events")
        logger.info("3. Run: python example_module.py consumer  # To consume events")
    else:
        logger.error(f"\n⚠️  {total - passed} test(s) failed. Please check the errors above.")
        sys.exit(1)


if __name__ == "__main__":
    main()