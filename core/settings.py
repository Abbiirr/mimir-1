# settings.py
from functools import lru_cache
from typing import Optional, Dict, Any
from pydantic import SecretStr, Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class KafkaProducerSettings(BaseSettings):
    """Producer-specific settings"""
    acks: str = "all"
    compression_type: Optional[str] = "snappy"
    batch_size: int
    batch_size_boost_factor: float = 1.0
    linger_ms: int = 5
    request_timeout_ms: int
    retry_count: int = 5
    enable_idempotence: bool = True
    delivery_timeout_ms: int


class KafkaConsumerSettings(BaseSettings):
    """Consumer-specific settings"""
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = False
    session_timeout_ms: int = 120_000
    heartbeat_interval_ms: int = 3_000
    max_poll_interval_ms: int = 300_000
    max_partition_fetch_bytes_default: int = 1_048_576
    max_poll_records: int = 500
    poll_timeout_ms: int = 1_000


class KafkaTopicSettings(BaseSettings):
    """Topic definitions - modules register their topics here"""
    # Core topics
    recommendation_generate: str = "recommendation.generate.v1"
    recommendation_update: str = "recommendation.update.v1"
    excel_rows: str = "excel.rows.v1"

    # Order service topics
    order_events: str = "order.events.v1"
    order_commands: str = "order.commands.v1"

    # User service topics
    user_events: str = "user.events.v1"
    user_commands: str = "user.commands.v1"

    # Add new topics as needed by modules
    # e.g., analytics_events: str = "analytics.events.v1"


class KafkaConnectionSettings(BaseSettings):
    """Core Kafka connection settings"""
    bootstrap: str = Field(default="localhost:9092", description="Bootstrap servers")

    # Security settings (optional)
    security_protocol: Optional[str] = None  # "SASL_PLAINTEXT", "SASL_SSL", etc.
    sasl_mechanism: Optional[str] = None  # "PLAIN", "SCRAM-SHA-256", etc.
    sasl_username: Optional[str] = None
    sasl_password: Optional[SecretStr] = None

    @model_validator(mode='after')
    def build_connection_config(self) -> 'KafkaConnectionSettings':
        """Build connection config for kafka-python"""
        return self

    def get_connection_params(self) -> Dict[str, Any]:
        """Get connection parameters for Kafka client"""
        params = {"bootstrap_servers": self.bootstrap}

        if self.security_protocol:
            params["security_protocol"] = self.security_protocol
        if self.sasl_mechanism:
            params["sasl_mechanism"] = self.sasl_mechanism
        if self.sasl_username:
            params["sasl_plain_username"] = self.sasl_username
        if self.sasl_password:
            params["sasl_plain_password"] = self.sasl_password.get_secret_value()

        return params


class KafkaSettings(BaseSettings):
    """Aggregated Kafka settings"""
    connection: KafkaConnectionSettings = Field(default_factory=KafkaConnectionSettings)
    producer: KafkaProducerSettings = Field(default_factory=KafkaProducerSettings)
    consumer: KafkaConsumerSettings = Field(default_factory=KafkaConsumerSettings)
    topic: KafkaTopicSettings = Field(default_factory=KafkaTopicSettings)

    # Module-specific consumer groups
    consumer_groups: Dict[str, str] = Field(default_factory=lambda: {
        "recommendation": "recommendation-processor-v1",
        "excel": "excel-loader-v1",
        "analytics": "analytics-aggregator-v1",
        "order_service": "order-service-v1",
        "user_service": "user-service-v1",
        # Add more as modules register
    })

    def get_consumer_group(self, module_name: str) -> str:
        """Get or create consumer group for a module"""
        if module_name not in self.consumer_groups:
            self.consumer_groups[module_name] = f"{module_name}-consumer-v1"
        return self.consumer_groups[module_name]


class DatabaseSettings(BaseSettings):
    """Database connection settings"""
    host: str = "localhost"
    port: int = 5432
    database: str = "etl"
    username: str = "postgres"
    password: SecretStr = SecretStr("postgres")
    pool_size: int = 10
    max_overflow: int = 20

    @property
    def url(self) -> str:
        """Build database URL"""
        pwd = self.password.get_secret_value()
        return f"postgresql+psycopg2://{self.username}:{pwd}@{self.host}:{self.port}/{self.database}"


class AppSettings(BaseSettings):
    """Main application settings"""
    # App metadata
    app_name: str = "kafka-etl-system"
    environment: str = "development"  # development, staging, production
    debug: bool = False

    # App-specific settings
    watch_dir: str = "./dropbox"
    log_level: str = "INFO"

    # Infrastructure
    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)

    model_config = SettingsConfigDict(
        env_file="../.env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        case_sensitive=False,
        extra="ignore"  # Ignore extra env vars
    )


class ModuleSettings(BaseSettings):
    """Base class for module-specific settings"""
    module_name: str
    enabled: bool = True

    # Each module can override these
    batch_size: int = 100
    processing_interval_seconds: int = 5
    error_retry_count: int = 3

    def get_topic(self, settings: AppSettings, topic_key: str) -> str:
        """Get topic from central settings"""
        return getattr(settings.kafka.topic, topic_key)

    def get_consumer_group(self, settings: AppSettings) -> str:
        """Get consumer group for this module"""
        return settings.kafka.get_consumer_group(self.module_name)


# Example module settings
class ExcelProcessorSettings(ModuleSettings):
    """Settings for Excel processing module"""
    module_name: str = "excel"
    input_formats: list[str] = Field(default_factory=lambda: [".xlsx", ".xls", ".csv"])
    max_file_size_mb: int = 100
    archive_processed: bool = True
    archive_dir: str = "./processed"


class RecommendationSettings(ModuleSettings):
    """Settings for recommendation module"""
    module_name: str = "recommendation"
    model_version: str = "v1.0"
    cache_ttl_seconds: int = 3600
    max_recommendations: int = 10


class OrderServiceSettings(ModuleSettings):
    """Settings for order service module"""
    module_name: str = "order_service"
    max_order_amount: float = 10000.0
    auto_approve_threshold: float = 100.0
    enable_fraud_check: bool = True


class UserServiceSettings(ModuleSettings):
    """Settings for user service module"""
    module_name: str = "user_service"
    user_cache_ttl: int = 600
    enable_notifications: bool = True


@lru_cache(maxsize=1)
def get_settings() -> AppSettings:
    """Get cached singleton settings"""
    return AppSettings()


@lru_cache(maxsize=1)
def get_excel_settings() -> ExcelProcessorSettings:
    """Get Excel processor settings"""
    return ExcelProcessorSettings()


@lru_cache(maxsize=1)
def get_recommendation_settings() -> RecommendationSettings:
    """Get recommendation settings"""
    return RecommendationSettings()


@lru_cache(maxsize=1)
def get_order_service_settings() -> OrderServiceSettings:
    """Get order service settings"""
    return OrderServiceSettings()


@lru_cache(maxsize=1)
def get_user_service_settings() -> UserServiceSettings:
    """Get user service settings"""
    return UserServiceSettings()


# Settings registry for dynamic module loading
MODULE_SETTINGS_REGISTRY: Dict[str, type[ModuleSettings]] = {
    "excel": ExcelProcessorSettings,
    "recommendation": RecommendationSettings,
    "order_service": OrderServiceSettings,
    "user_service": UserServiceSettings,
}


def register_module_settings(name: str, settings_class: type[ModuleSettings]):
    """Register a new module's settings class"""
    MODULE_SETTINGS_REGISTRY[name] = settings_class


def get_module_settings(module_name: str) -> ModuleSettings:
    """Get settings for a specific module"""
    if module_name not in MODULE_SETTINGS_REGISTRY:
        # Return generic module settings if not registered
        return ModuleSettings(module_name=module_name)

    settings_class = MODULE_SETTINGS_REGISTRY[module_name]
    return settings_class()