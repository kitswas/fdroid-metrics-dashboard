"""
Data fetcher for F-Droid metrics that can be used within Streamlit dashboard
"""

import json
import logging
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any

import requests

from etl.config import fetcher_config

# Import existing data fetching functions
from etl.getdata_apps import (
    BASE_URL as APPS_BASE_URL,
)
from etl.getdata_apps import (
    SERVERS,
)
from etl.getdata_apps import (
    SUB_DATA_DIR as APPS_DATA_DIR,
)
from etl.getdata_search import (
    BASE_URL as SEARCH_BASE_URL,
)
from etl.getdata_search import (
    INDEX_URL,
)
from etl.getdata_search import (
    SUB_DATA_DIR as SEARCH_DATA_DIR,
)
from etl.security import safe_open

logger = logging.getLogger(__name__)


class DataFetcher:
    """Unified data fetcher for both search and app metrics."""

    def __init__(self) -> None:
        """Initialize the data fetcher with configured URLs and directories."""
        self.apps_base_url = APPS_BASE_URL
        self.search_base_url = SEARCH_BASE_URL
        self.search_index_url = INDEX_URL
        self.servers = SERVERS
        self.apps_data_dir = APPS_DATA_DIR
        self.search_data_dir = SEARCH_DATA_DIR

        # Constants
        self.DATA_TYPE_ERROR_MSG = "data_type must be 'search' or 'apps'"
        self.JSON_EXT = ".json"

    def get_available_remote_dates(self, data_type: str) -> list[str]:
        """
        Get available dates from remote servers.

        Args:
            data_type: Type of data to query - either 'search' or 'apps'

        Returns:
            Sorted list of date strings in YYYY-MM-DD format available on remote servers

        Raises:
            ValueError: If data_type is not 'search' or 'apps'
        """
        if data_type == "search":
            return self._get_search_remote_dates()
        elif data_type == "apps":
            return self._get_apps_remote_dates()
        else:
            raise ValueError(self.DATA_TYPE_ERROR_MSG)

    def _get_search_remote_dates(self) -> list[str]:
        """
        Get available search data dates from remote server.

        Returns:
            Sorted list of date strings available for search metrics

        Note:
            Returns empty list if fetching fails (error is logged)
        """
        try:
            response = requests.get(
                self.search_index_url, timeout=fetcher_config.REQUEST_TIMEOUT
            )
            response.raise_for_status()
            index = response.json()

            dates: list[str] = []
            for filename in index:
                if filename == "last_submitted_to_cimp.json":
                    continue
                try:
                    date_str = filename.replace(self.JSON_EXT, "")
                    datetime.strptime(date_str, "%Y-%m-%d")  # Validate format
                    dates.append(date_str)
                except ValueError:
                    continue

            return sorted(dates)
        except (requests.RequestException, json.JSONDecodeError) as e:
            logger.error(f"Failed to fetch search data index: {e}")
            return []

    def _get_apps_remote_dates(self) -> list[str]:
        """
        Get available app data dates from remote servers.

        Returns:
            Sorted list of date strings available across all app servers

        Note:
            Returns empty list if fetching fails (error is logged)
        """
        try:
            dates: list[str] = []
            for server in self.servers:
                index_url = f"{self.apps_base_url}/{server}/index.json"
                response = requests.get(
                    index_url, timeout=fetcher_config.REQUEST_TIMEOUT
                )
                response.raise_for_status()
                index = response.json()

                for filename in index:
                    try:
                        date_str = filename.replace(self.JSON_EXT, "")
                        datetime.strptime(date_str, "%Y-%m-%d")  # Validate format
                        dates.append(date_str)
                    except ValueError:
                        continue

            return sorted(dates)
        except (requests.RequestException, json.JSONDecodeError) as e:
            logger.error(f"Failed to fetch app data index: {e}")
            return []

    def _get_apps_per_server_dates(self) -> dict[str, set[str]]:
        """
        Get available app data dates per server.

        Returns:
            Dictionary mapping server names to sets of available date strings.
            If a server's index fails to fetch, it maps to an empty set.
        """
        server_dates: dict[str, set[str]] = {}

        for server in self.servers:
            try:
                index_url = f"{self.apps_base_url}/{server}/index.json"
                response = requests.get(
                    index_url, timeout=fetcher_config.REQUEST_TIMEOUT
                )
                response.raise_for_status()
                index = response.json()

                available: set[str] = set()
                for filename in index:
                    try:
                        date_str = filename.replace(self.JSON_EXT, "")
                        datetime.strptime(date_str, "%Y-%m-%d")
                        available.add(date_str)
                    except ValueError:
                        continue

                server_dates[server] = available
            except (requests.RequestException, json.JSONDecodeError) as e:
                logger.warning(f"Failed to fetch index for {server}: {e}")
                server_dates[server] = set()

        return server_dates

    def get_local_dates(self, data_type: str) -> list[str]:
        """
        Get available dates from local data files.

        Args:
            data_type: Type of data to query - either 'search' or 'apps'

        Returns:
            Sorted list of date strings found in local data files

        Raises:
            ValueError: If data_type is not 'search' or 'apps'
        """
        if data_type == "search":
            data_dir = self.search_data_dir
        elif data_type == "apps":
            # For apps, check all server directories
            dates: set[str] = set()
            for server in self.servers:
                server_dir = self.apps_data_dir / server
                if server_dir.exists():
                    for file in server_dir.glob("*.json"):
                        try:
                            date_str = file.stem
                            datetime.strptime(date_str, "%Y-%m-%d")
                            dates.add(date_str)
                        except ValueError:
                            continue
            return sorted(dates)
        else:
            raise ValueError(self.DATA_TYPE_ERROR_MSG)

        # For search data
        dates_list: list[str] = []
        if data_dir.exists():
            for file in data_dir.glob("*.json"):
                if file.name == "last_submitted_to_cimp.json":
                    continue
                try:
                    date_str = file.stem
                    datetime.strptime(date_str, "%Y-%m-%d")
                    dates_list.append(date_str)
                except ValueError:
                    continue
        return sorted(dates_list)

    def fetch_date_range(
        self,
        data_type: str,
        start_date: str,
        end_date: str,
        progress_callback: Callable[[float], None] | None = None,
        status_callback: Callable[[str], None] | None = None,
    ) -> dict[str, Any]:
        """Fetch data for a date range with progress feedback via callbacks.

        This method fetches the index.json first to determine available dates,
        then filters by the provided date range to download only available files.

        Note: Data files represent cumulative metrics since the previous date (usually weekly).
        """
        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError as e:
            raise ValueError(f"Invalid date format. Use YYYY-MM-DD: {e}")

        if start_dt > end_dt:
            raise ValueError("Start date must be before or equal to end date")

        max_days = fetcher_config.MAX_DATE_RANGE_DAYS
        if (end_dt - start_dt).days > max_days:
            raise ValueError(f"Date range too large. Maximum {max_days} days allowed")

        # Get available dates from index.json and filter by date range
        available_dates = self.get_available_remote_dates(data_type)
        dates_to_fetch = [
            date for date in available_dates if start_date <= date <= end_date
        ]

        if not dates_to_fetch:
            if status_callback:
                status_callback(
                    f"No data available for date range {start_date} to {end_date}"
                )
            return {
                "total_files": 0,
                "successful": 0,
                "failed": 0,
                "errors": ["No data files available in the specified date range"],
            }

        if data_type == "search":
            return self._fetch_search_dates(
                dates_to_fetch, progress_callback, status_callback
            )
        elif data_type == "apps":
            # Get per-server availability for apps
            server_dates = self._get_apps_per_server_dates()
            return self._fetch_apps_dates(
                dates_to_fetch, server_dates, progress_callback, status_callback
            )
        else:
            raise ValueError(self.DATA_TYPE_ERROR_MSG)

    def _fetch_search_dates(
        self,
        dates: list[str],
        progress_callback: Callable[[float], None] | None = None,
        status_callback: Callable[[str], None] | None = None,
    ) -> dict[str, Any]:
        """Fetch search data for specified dates, overwriting existing files."""
        # Create data directory if it doesn't exist
        self.search_data_dir.mkdir(parents=True, exist_ok=True)

        results = {
            "total_files": len(dates),
            "successful": 0,
            "failed": 0,
            "errors": [],
        }

        for i, date in enumerate(dates):
            progress = (i + 1) / len(dates)
            if progress_callback:
                progress_callback(progress)
            if status_callback:
                status_callback(f"Fetching search data for {date}...")

            filepath = self.search_data_dir / f"{date}.json"

            try:
                url = f"{self.search_base_url}/{date}.json"
                response = requests.get(url, timeout=fetcher_config.REQUEST_TIMEOUT)
                response.raise_for_status()

                with safe_open(filepath, "w", encoding="utf-8") as f:
                    json.dump(response.json(), f, indent=2)

                results["successful"] += 1

            except (requests.RequestException, json.JSONDecodeError) as e:
                error_msg = f"Failed to download {date}: {str(e)}"
                results["errors"].append(error_msg)
                results["failed"] += 1
                logger.warning(error_msg)

        if progress_callback:
            progress_callback(1.0)
        if status_callback:
            status_callback("Search data fetch complete!")
        return results

    def _fetch_apps_dates(
        self,
        dates: list[str],
        server_dates: dict[str, set[str]],
        progress_callback: Callable[[float], None] | None = None,
        status_callback: Callable[[str], None] | None = None,
    ) -> dict[str, Any]:
        """Fetch app data for specified dates from all servers, overwriting existing files.

        Args:
            dates: List of date strings to fetch
            server_dates: Dictionary mapping server names to sets of available dates
            progress_callback: Optional callback for progress updates
            status_callback: Optional callback for status messages

        Only attempts to download files that are actually available on each server.
        """
        # Create data directory if it doesn't exist
        self.apps_data_dir.mkdir(parents=True, exist_ok=True)

        total_operations = sum(
            sum(1 for date in dates if date in server_dates.get(server, set()))
            for server in self.servers
        )
        if total_operations == 0:
            return {
                "total_files": 0,
                "successful": 0,
                "failed": 0,
                "errors": ["No files available to download"],
            }
        results = {
            "total_files": total_operations,
            "successful": 0,
            "failed": 0,
            "errors": [],
        }
        op_count = 0
        batch_size = (
            fetcher_config.BATCH_SIZE
        )  # Number of concurrent requests per batch
        session = requests.Session()
        for batch_start in range(0, len(dates), batch_size):
            batch_dates = dates[batch_start : batch_start + batch_size]
            for date in batch_dates:
                for server in self.servers:
                    if date not in server_dates.get(server, set()):
                        continue
                    url = f"{self.apps_base_url}/{server}/{date}.json"
                    server_dir = self.apps_data_dir / server
                    server_dir.mkdir(parents=True, exist_ok=True)
                    filepath = server_dir / f"{date}.json"
                    try:
                        response = session.get(
                            url, timeout=fetcher_config.REQUEST_TIMEOUT
                        )
                        response.raise_for_status()
                        with safe_open(filepath, "w", encoding="utf-8") as f:
                            json.dump(response.json(), f, indent=2)
                        results["successful"] += 1
                    except (
                        requests.RequestException,
                        json.JSONDecodeError,
                    ) as e:
                        error_msg = f"Failed to download {server}/{date}: {str(e)}"
                        results["errors"].append(error_msg)
                        results["failed"] += 1
                        logger.warning(error_msg)
                    op_count += 1
                    if progress_callback:
                        progress_callback(op_count / total_operations)
        if progress_callback:
            progress_callback(1.0)
        if status_callback:
            status_callback("App data fetch complete!")
        return results

    def get_missing_dates(
        self, data_type: str, start_date: str, end_date: str
    ) -> list[str]:
        """
        Get list of dates that are missing locally within a date range.

        Args:
            data_type: Type of data - either 'search' or 'apps'
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format

        Returns:
            List of date strings that are missing from local storage

        Raises:
            ValueError: If date format is invalid
        """
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")

        # Generate list of all dates in range
        all_dates: list[str] = []
        current_date = start_dt
        while current_date <= end_dt:
            all_dates.append(current_date.strftime("%Y-%m-%d"))
            current_date += timedelta(days=1)

        # Get locally available dates
        local_dates = set(self.get_local_dates(data_type))

        # Return missing dates
        return [date for date in all_dates if date not in local_dates]

    def check_data_availability(self, data_type: str) -> dict[str, Any]:
        """
        Check data availability both locally and remotely.

        Args:
            data_type: Type of data - either 'search' or 'apps'

        Returns:
            Dictionary containing:
            - local_count: Number of local data files
            - remote_count: Number of remote data files
            - local_date_range: Tuple of (earliest, latest) local dates
            - remote_date_range: Tuple of (earliest, latest) remote dates
            - missing_dates: List of first 10 dates available remotely but not locally
        """
        local_dates = self.get_local_dates(data_type)
        remote_dates = self.get_available_remote_dates(data_type)

        return {
            "local_count": len(local_dates),
            "remote_count": len(remote_dates),
            "local_date_range": (local_dates[0], local_dates[-1])
            if local_dates
            else (None, None),
            "remote_date_range": (remote_dates[0], remote_dates[-1])
            if remote_dates
            else (None, None),
            "missing_dates": [date for date in remote_dates if date not in local_dates][
                :10
            ],  # Show first 10
        }
