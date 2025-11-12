"""
Configuration constants for F-Droid Metrics application.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class CacheConfig:
    """Cache configuration settings."""

    APP_CACHE_SIZE: int = 100
    SEARCH_CACHE_SIZE: int = 1000
    METADATA_CACHE_SIZE: int = 500


@dataclass(frozen=True)
class DataFetcherConfig:
    """Data fetcher configuration settings."""

    REQUEST_TIMEOUT: int = 30
    MAX_DATE_RANGE_DAYS: int = 732  # 2 years
    RATE_LIMIT_INTERVAL: float = 0.1  # seconds between requests
    BATCH_SIZE: int = 8  # Number of concurrent requests per batch


@dataclass(frozen=True)
class ProcessingConfig:
    """Data processing configuration settings."""

    TOP_ITEMS_LIMIT: int = 20  # Default number of top items to return


@dataclass(frozen=True)
class MetadataConfig:
    """F-Droid metadata configuration."""

    FDROID_METADATA_BASE_URL: str = (
        "https://gitlab.com/fdroid/fdroiddata/-/raw/master/metadata"
    )
    RETRY_TOTAL: int = 3
    RETRY_BACKOFF_FACTOR: float = 1.0
    STATUS_FORCELIST: tuple[int, ...] = (429, 500, 502, 503, 504)


# Instantiate global config objects
cache_config = CacheConfig()
fetcher_config = DataFetcherConfig()
processing_config = ProcessingConfig()
metadata_config = MetadataConfig()
