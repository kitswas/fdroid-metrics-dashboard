"""
F-Droid metadata fetcher for getting real package categories and information.
"""

import logging
import time
from pathlib import Path
from urllib.parse import quote

import requests
import yaml
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from etl.config import fetcher_config, metadata_config

logger = logging.getLogger(__name__)


class FDroidMetadataFetcher:
    """Fetches F-Droid package metadata from the fdroiddata repository."""

    def __init__(self, cache_dir: str | None = None) -> None:
        """
        Initialize the metadata fetcher.

        Args:
            cache_dir: Directory to cache metadata files. If None, uses ./cache/metadata
        """
        self.base_url = metadata_config.FDROID_METADATA_BASE_URL
        self.cache_dir = Path(cache_dir or "./cache/metadata")
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # Session with retry strategy
        self.session = requests.Session()
        retry_strategy = Retry(
            total=metadata_config.RETRY_TOTAL,
            status_forcelist=metadata_config.STATUS_FORCELIST,
            backoff_factor=metadata_config.RETRY_BACKOFF_FACTOR,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        # Rate limiting
        self.last_request_time: float = 0
        self.min_request_interval = fetcher_config.RATE_LIMIT_INTERVAL

        # Cache for parsed metadata
        self._metadata_cache: dict[str, dict] = {}

    def _rate_limit(self) -> None:
        """
        Implement rate limiting to be respectful to GitLab servers.

        Ensures minimum time interval between consecutive requests.
        """
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.min_request_interval:
            time.sleep(self.min_request_interval - time_since_last)
        self.last_request_time = time.time()

    def _get_cache_path(self, package_id: str) -> Path:
        """
        Get the cache file path for a package.

        Args:
            package_id: The package ID (e.g., 'com.example.app')

        Returns:
            Path object pointing to the cache file location
        """
        return self.cache_dir / f"{package_id}.yml"

    def _fetch_metadata_from_remote(self, package_id: str) -> dict | None:
        """
        Fetch metadata from the remote fdroiddata repository.

        Args:
            package_id: The package ID (e.g., 'com.example.app')

        Returns:
            Parsed YAML metadata or None if not found
        """
        # URL encode the package ID to handle special characters
        encoded_package_id = quote(package_id, safe="")
        url = f"{self.base_url}/{encoded_package_id}.yml"

        try:
            self._rate_limit()
            response = self.session.get(url, timeout=10)

            if response.status_code == 200:
                # Parse YAML content
                metadata = yaml.safe_load(response.text)

                # Cache the result
                cache_path = self._get_cache_path(package_id)
                with open(cache_path, "w", encoding="utf-8") as f:
                    yaml.dump(metadata, f, default_flow_style=False)

                return metadata
            elif response.status_code == 404:
                # Package not found in fdroiddata - this is normal for some packages
                return None
            else:
                logger.warning(
                    f"Failed to fetch metadata for {package_id}: HTTP {response.status_code}"
                )
                return None

        except requests.exceptions.RequestException as e:
            logger.warning(f"Network error fetching metadata for {package_id}: {e}")
            return None
        except yaml.YAMLError as e:
            logger.error(f"YAML parsing error for {package_id}: {e}")
            return None
        except OSError as e:
            logger.error(f"File system error for {package_id}: {e}")
            return None

    def _load_cached_metadata(self, package_id: str) -> dict | None:
        """
        Load metadata from cache if available.

        Args:
            package_id: The package ID

        Returns:
            Cached metadata or None if not cached
        """
        cache_path = self._get_cache_path(package_id)

        if cache_path.exists():
            try:
                with open(cache_path, encoding="utf-8") as f:
                    return yaml.safe_load(f)
            except (yaml.YAMLError, OSError) as e:
                logger.warning(f"Error reading cached metadata for {package_id}: {e}")
                # Remove corrupted cache file
                try:
                    cache_path.unlink()
                except OSError:
                    pass

        return None

    def get_package_metadata(
        self, package_id: str, use_cache: bool = True
    ) -> dict | None:
        """
        Get metadata for a specific package.

        Args:
            package_id: The package ID (e.g., 'com.example.app')
            use_cache: Whether to use cached data if available

        Returns:
            Package metadata dictionary or None if not found
        """
        # Check memory cache first
        if package_id in self._metadata_cache:
            return self._metadata_cache[package_id]

        # Try cache if enabled
        metadata = None
        if use_cache:
            metadata = self._load_cached_metadata(package_id)

        # Fetch from remote if not cached
        if metadata is None:
            metadata = self._fetch_metadata_from_remote(package_id)

        # Store in memory cache
        if metadata is not None:
            self._metadata_cache[package_id] = metadata

        return metadata

    def get_package_categories(
        self, package_id: str, use_cache: bool = True
    ) -> list[str]:
        """
        Get categories for a specific package.

        Args:
            package_id: The package ID
            use_cache: Whether to use cached data

        Returns:
            List of categories, empty if not found
        """
        metadata = self.get_package_metadata(package_id, use_cache)

        if metadata and "Categories" in metadata:
            categories = metadata["Categories"]
            if isinstance(categories, list):
                return categories
            elif isinstance(categories, str):
                return [categories]

        return []

    def get_bulk_categories(
        self, package_ids: set[str], use_cache: bool = True
    ) -> dict[str, list[str]]:
        """
        Get categories for multiple packages efficiently.

        Args:
            package_ids: Set of package IDs to fetch
            use_cache: Whether to use cached data

        Returns:
            Dictionary mapping package_id to list of categories
        """
        results = {}

        for i, package_id in enumerate(package_ids):
            categories = self.get_package_categories(package_id, use_cache)
            results[package_id] = categories

            # Progress indicator for large batches
            if i > 0 and i % 50 == 0:
                logger.info(f"Fetched metadata for {i}/{len(package_ids)} packages…")

        return results

    def get_primary_category(self, package_id: str, use_cache: bool = True) -> str:
        """
        Get the primary (first) category for a package, with fallback to pattern-based categorization.

        Args:
            package_id: The package ID
            use_cache: Whether to use cached data

        Returns:
            Primary category string
        """
        categories = self.get_package_categories(package_id, use_cache)

        if categories:
            return categories[0]  # Return first category as primary

        # Fallback to pattern-based categorization
        return self._categorize_by_pattern(package_id)

    def _categorize_by_pattern(self, package_name: str) -> str:
        """
        Fallback categorization based on package name patterns.
        This is the same logic as the original function but as a fallback.
        """
        package_lower = package_name.lower()

        if any(word in package_lower for word in ["fdroid", "f-droid"]):
            return "F-Droid Core"
        elif any(word in package_lower for word in ["newpipe", "tube", "youtube"]):
            return "Multimedia"
        elif any(
            word in package_lower
            for word in [
                "telegram",
                "chat",
                "message",
                "signal",
                "element",
                "matrix",
            ]
        ):
            return "Internet"
        elif any(word in package_lower for word in ["launcher", "home", "desktop"]):
            return "System"
        elif any(
            word in package_lower for word in ["gallery", "photo", "image", "camera"]
        ):
            return "Graphics"
        elif any(
            word in package_lower for word in ["music", "audio", "player", "sound"]
        ):
            return "Multimedia"
        elif any(
            word in package_lower
            for word in ["calculator", "calendar", "note", "task", "todo"]
        ):
            return "Office"
        elif any(word in package_lower for word in ["game", "puzzle", "play"]):
            return "Games"
        elif any(
            word in package_lower for word in ["browser", "web", "firefox", "chrome"]
        ):
            return "Internet"
        elif any(word in package_lower for word in ["keyboard", "input"]):
            return "System"
        else:
            return "Unknown"

    def clear_cache(self) -> None:
        """Clear the metadata cache."""
        if self.cache_dir.exists():
            for cache_file in self.cache_dir.glob("*.yml"):
                cache_file.unlink()
        self._metadata_cache.clear()

    def get_cache_stats(self) -> dict[str, int]:
        """Get statistics about the cache."""
        cache_files = (
            list(self.cache_dir.glob("*.yml")) if self.cache_dir.exists() else []
        )
        cache_size_mb = sum(f.stat().st_size for f in cache_files) / (1024 * 1024)
        return {
            "cached_packages": len(cache_files),
            "memory_cache_size": len(self._metadata_cache),
            "cache_dir_size_mb": int(cache_size_mb),
        }
