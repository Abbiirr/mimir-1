# example_module.py
# Example of a plug-and-play module using the modular Kafka bus

import logging
from typing import Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field

from kafka_bus import KafkaModule, BaseEvent, GenericPayloadEvent, TypedPayloadEvent
from settings import ModuleSettings, get_settings, register_module_settings

logger = logging.getLogger(__name__)


# ---------------------------
# Module-specific settings
# ---------------------------

class UserServiceSettings(ModuleSettings):
    """Settings specific to the user service module"""
    module_name: str = "user_service"

    # Module-specific configurations
    user_cache_ttl: int = 600  # seconds
    max_batch_size: int = 100
    enable_notifications: bool = True
    notification_delay_seconds: int = 5


# Register the settings
register_module_settings("user_service", UserServiceSettings)


# ---------------------------
# Module-specific events
# ---------------------------

class UserEvent(BaseModel):
    """User-specific event schema"""
    key: str = Field(..., description="Partition key")
    user_id: str = Field(..., description="User identifier")
    action: str = Field(..., description="Action performed")
    payload: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    produced_at: datetime = Field(default_factory=lambda: datetime.now())


class UserCreatedEvent(UserEvent):
    """Event when a new user is created"""
    action: str = Field(default="user_created")
    email: str = Field(..., description="User email")
    username: str = Field(..., description="Username")

    def __init__(self, **data):
        # Set key based on user_id if not provided
        if "key" not in data:
            data["key"] = data.get("user_id", "unknown")
        super().__init__(**data)


class UserUpdatedEvent(UserEvent):
    """Event when user is updated"""
    action: str = Field(default="user_updated")
    fields_updated: list[str] = Field(default_factory=list)

    def __init__(self, **data):
        # Set key based on user_id if not provided
        if "key" not in data:
            data["key"] = data.get("user_id", "unknown")
        super().__init__(**data)


# ---------------------------
# Module implementation
# ---------------------------

class UserServiceModule(KafkaModule):
    """
    User service module - handles all user-related events.
    This is completely plug-and-play - just instantiate and start!
    """

    def __init__(self):
        # Initialize with module name
        super().__init__(name="user_service")

        # Get module-specific settings
        self.module_settings = UserServiceSettings()

        # Define topics this module uses
        self.USER_EVENTS_TOPIC = "user.events.v1"
        self.USER_COMMANDS_TOPIC = "user.commands.v1"

        # Register the module with its topics and schema
        self.register(
            topics=[self.USER_EVENTS_TOPIC, self.USER_COMMANDS_TOPIC],
            event_schema=GenericPayloadEvent  # Use GenericPayloadEvent for flexibility
        )

        # Internal state (in production, use a database)
        self._user_cache = {}
        self._processed_count = 0

    def handle_event(self, event: GenericPayloadEvent) -> None:
        """
        Handle incoming user events.
        This method is called for each event consumed from subscribed topics.
        """
        # Extract user_id and action from payload
        user_id = event.payload.get("user_id", "unknown")
        action = event.payload.get("action", "unknown")

        self.logger.info(f"Processing user event: {action} for user {user_id}")

        # Route based on action
        if action == "user_created":
            self._handle_user_created(event)
        elif action == "user_updated":
            self._handle_user_updated(event)
        elif action == "user_deleted":
            self._handle_user_deleted(event)
        else:
            self.logger.warning(f"Unknown action: {action}")

        self._processed_count += 1

        # Log progress
        if self._processed_count % 100 == 0:
            self.logger.info(f"Processed {self._processed_count} events")

    def _handle_user_created(self, event: GenericPayloadEvent) -> None:
        """Handle user creation"""
        user_id = event.payload.get("user_id", "unknown")
        self._user_cache[user_id] = {
            "created_at": datetime.now(),
            "data": event.payload
        }

        # Publish a notification event if enabled
        if self.module_settings.enable_notifications:
            self.publish_notification(user_id, "welcome")

    def _handle_user_updated(self, event: GenericPayloadEvent) -> None:
        """Handle user update"""
        user_id = event.payload.get("user_id", "unknown")
        if user_id in self._user_cache:
            self._user_cache[user_id]["data"].update(event.payload)
            self._user_cache[user_id]["updated_at"] = datetime.now()

    def _handle_user_deleted(self, event: GenericPayloadEvent) -> None:
        """Handle user deletion"""
        user_id = event.payload.get("user_id", "unknown")
        if user_id in self._user_cache:
            del self._user_cache[user_id]

    def publish_notification(self, user_id: str, notification_type: str) -> None:
        """Publish a notification event"""
        # This could publish to a different topic that other modules consume
        notification_event = {
            "key": f"notif-{user_id}",
            "event_type": "notification",
            "user_id": user_id,
            "payload": {
                "type": notification_type,
                "timestamp": datetime.now().isoformat()
            }
        }

        # If we had a notification topic registered, we'd publish there
        self.logger.info(f"Would publish notification: {notification_event}")

    def create_user(self, user_id: str, email: str, username: str, **kwargs) -> None:
        """
        Public method to create a user.
        This publishes an event that will be consumed by this or other modules.
        """
        event = UserCreatedEvent(
            user_id=user_id,
            email=email,
            username=username,
            payload=kwargs,
            metadata={"created_by": "user_service", "version": "1.0"}
        )

        self.publish(self.USER_EVENTS_TOPIC, event.model_dump())
        self.logger.info(f"Published user creation event for {user_id}")

    def update_user(self, user_id: str, updates: Dict[str, Any]) -> None:
        """
        Public method to update a user.
        """
        event = UserUpdatedEvent(
            user_id=user_id,
            fields_updated=list(updates.keys()),
            payload=updates,
            metadata={"updated_by": "user_service", "version": "1.0"}
        )

        self.publish(self.USER_EVENTS_TOPIC, event.model_dump())
        self.logger.info(f"Published user update event for {user_id}")

    def start(self) -> None:
        """
        Start the module consumer.
        This is a blocking call that processes events continuously.
        """
        self.logger.info(f"Starting {self.name} module...")
        self.logger.info(f"Settings: cache_ttl={self.module_settings.user_cache_ttl}s, "
                         f"notifications={self.module_settings.enable_notifications}")

        try:
            # Start consuming from the user events topic
            self.start_consumer(self.USER_EVENTS_TOPIC)
        except KeyboardInterrupt:
            self.logger.info("Shutting down user service module...")
        except Exception as e:
            self.logger.error(f"Error in user service: {e}")
            raise


# ---------------------------
# Analytics Module Example
# ---------------------------

class AnalyticsModule(KafkaModule):
    """
    Another example module - analytics processor.
    Shows how multiple modules can work independently.
    """

    def __init__(self):
        super().__init__(name="analytics")

        # Topics this module consumes from (can consume from other modules' topics!)
        self.topics = [
            "user.events.v1",  # Consume user events
            "recommendation.generate.v1",  # Consume recommendation events
            "analytics.commands.v1"  # Its own command topic
        ]

        self.register(
            topics=self.topics,
            event_schema=GenericPayloadEvent  # Use GenericPayloadEvent for flexibility
        )

        self.metrics = {
            "events_processed": 0,
            "users_seen": set(),
            "event_types": {}
        }

    def handle_event(self, event: GenericPayloadEvent) -> None:
        """Process events for analytics"""
        self.metrics["events_processed"] += 1

        # Track event types if present in payload
        if "action" in event.payload:
            action = event.payload["action"]
            self.metrics["event_types"][action] = \
                self.metrics["event_types"].get(action, 0) + 1

        # Track unique users if present
        if "user_id" in event.payload:
            self.metrics["users_seen"].add(event.payload["user_id"])

        # Log metrics periodically
        if self.metrics["events_processed"] % 50 == 0:
            self.logger.info(f"Analytics metrics: {self.get_summary()}")

    def get_summary(self) -> Dict[str, Any]:
        """Get analytics summary"""
        return {
            "total_events": self.metrics["events_processed"],
            "unique_users": len(self.metrics["users_seen"]),
            "event_breakdown": self.metrics["event_types"]
        }

    def start(self) -> None:
        """Start consuming from all registered topics"""
        self.logger.info("Starting analytics module...")

        # In production, you'd run consumers for each topic in separate threads
        # For this example, we'll consume from the first topic
        if self.topics:
            self.start_consumer(self.topics[0])


# ---------------------------
# Usage Examples
# ---------------------------

def example_producer_only():
    """Example of using a module just for publishing"""
    # Create module instance
    user_module = UserServiceModule()

    # Publish some events
    user_module.create_user(
        user_id="user-001",
        email="alice@example.com",
        username="alice",
        role="admin"
    )

    user_module.update_user(
        user_id="user-001",
        updates={"last_login": datetime.now().isoformat()}
    )

    # Flush to ensure delivery
    user_module.bus.flush()
    print("Published user events")


def example_consumer():
    """Example of running a module as a consumer"""
    # Create and start the module
    user_module = UserServiceModule()

    # This will run forever, processing events
    user_module.start()


def example_multiple_modules():
    """Example of running multiple modules together"""
    import threading

    # Create modules
    user_module = UserServiceModule()
    analytics_module = AnalyticsModule()

    # Run each in a separate thread
    user_thread = threading.Thread(target=user_module.start, daemon=True)
    analytics_thread = threading.Thread(target=analytics_module.start, daemon=True)

    user_thread.start()
    analytics_thread.start()

    # Keep main thread alive
    try:
        user_thread.join()
    except KeyboardInterrupt:
        print("Shutting down all modules...")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    import sys

    if len(sys.argv) > 1:
        mode = sys.argv[1]

        if mode == "producer":
            example_producer_only()
        elif mode == "consumer":
            example_consumer()
        elif mode == "multi":
            example_multiple_modules()
        else:
            print(f"Unknown mode: {mode}")
            print("Usage: python example_module.py [producer|consumer|multi]")
    else:
        example_multiple_modules()
        print("Usage: python example_module.py [producer|consumer|multi]")
        print("\nQuick demo:")
        # example_producer_only()