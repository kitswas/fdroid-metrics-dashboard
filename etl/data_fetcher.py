"""
Data fetcher for F-Droid metrics that can be used within Streamlit dashboard
"""

import json
from datetime import datetime, timedelta
from typing import Any, Dict, List

import requests
import streamlit as st

# Import existing data fetching functions
from etl.getdata_apps import (
    BASE_URL as APPS_BASE_URL,
    SERVERS,
    SUB_DATA_DIR as APPS_DATA_DIR,
)
from etl.getdata_search import (
    BASE_URL as SEARCH_BASE_URL,
    INDEX_URL,
    SUB_DATA_DIR as SEARCH_DATA_DIR,
)


class DataFetcher:
    """Unified data fetcher for both search and app metrics with Streamlit integration."""

    def __init__(self):
        """Initialize the data fetcher."""
        self.apps_base_url = APPS_BASE_URL
        self.search_base_url = SEARCH_BASE_URL
        self.search_index_url = INDEX_URL
        self.servers = SERVERS
        self.apps_data_dir = APPS_DATA_DIR
        self.search_data_dir = SEARCH_DATA_DIR

    def get_available_remote_dates(self, data_type: str) -> List[str]:
        """Get available dates from remote servers."""
        if data_type == "search":
            return self._get_search_remote_dates()
        elif data_type == "apps":
            return self._get_apps_remote_dates()
        else:
            raise ValueError("data_type must be 'search' or 'apps'")

    def _get_search_remote_dates(self) -> List[str]:
        """Get available search data dates from remote server."""
        try:
            response = requests.get(self.search_index_url, timeout=10)
            response.raise_for_status()
            index = response.json()

            dates = []
            for filename in index:
                if filename == "last_submitted_to_cimp.json":
                    continue
                try:
                    date_str = filename.replace(".json", "")
                    datetime.strptime(date_str, "%Y-%m-%d")  # Validate format
                    dates.append(date_str)
                except ValueError:
                    continue

            return sorted(dates)
        except Exception as e:
            st.error(f"Failed to fetch search data index: {e}")
            return []

    def _get_apps_remote_dates(self) -> List[str]:
        """Get available app data dates from remote servers (using first server as reference)."""
        try:
            # Use first server to get available dates
            server = self.servers[0]
            index_url = f"{self.apps_base_url}/{server}/index.json"
            response = requests.get(index_url, timeout=10)
            response.raise_for_status()
            index = response.json()

            dates = []
            for filename in index:
                try:
                    date_str = filename.replace(".json", "")
                    datetime.strptime(date_str, "%Y-%m-%d")  # Validate format
                    dates.append(date_str)
                except ValueError:
                    continue

            return sorted(dates)
        except Exception as e:
            st.error(f"Failed to fetch app data index: {e}")
            return []

    def get_local_dates(self, data_type: str) -> List[str]:
        """Get available dates from local data files."""
        if data_type == "search":
            data_dir = self.search_data_dir
        elif data_type == "apps":
            # For apps, check all server directories
            dates = set()
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
            return sorted(list(dates))
        else:
            raise ValueError("data_type must be 'search' or 'apps'")

        # For search data
        dates = []
        if data_dir.exists():
            for file in data_dir.glob("*.json"):
                if file.name == "last_submitted_to_cimp.json":
                    continue
                try:
                    date_str = file.stem
                    datetime.strptime(date_str, "%Y-%m-%d")
                    dates.append(date_str)
                except ValueError:
                    continue
        return sorted(dates)

    def fetch_date_range(
        self, data_type: str, start_date: str, end_date: str
    ) -> Dict[str, Any]:
        """Fetch data for a date range with progress feedback."""
        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError as e:
            raise ValueError(f"Invalid date format. Use YYYY-MM-DD: {e}")

        if start_dt > end_dt:
            raise ValueError("Start date must be before or equal to end date")

        # Reasonable date range check
        limit_days = 366 * 2  # (not more than 2 years)
        if (end_dt - start_dt).days > limit_days:
            raise ValueError(f"Date range too large. Maximum {limit_days} days allowed")

        # Generate list of dates to fetch
        dates_to_fetch = []
        current_date = start_dt
        while current_date <= end_dt:
            dates_to_fetch.append(current_date.strftime("%Y-%m-%d"))
            current_date += timedelta(days=1)

        if data_type == "search":
            return self._fetch_search_dates(dates_to_fetch)
        elif data_type == "apps":
            return self._fetch_apps_dates(dates_to_fetch)
        else:
            raise ValueError("data_type must be 'search' or 'apps'")

    def _fetch_search_dates(self, dates: List[str]) -> Dict[str, Any]:
        """Fetch search data for specified dates."""
        # Create data directory if it doesn't exist
        self.search_data_dir.mkdir(parents=True, exist_ok=True)

        results = {
            "total_files": len(dates),
            "successful": 0,
            "failed": 0,
            "skipped": 0,
            "errors": [],
        }

        # Create progress bar
        progress_bar = st.progress(0)
        status_text = st.empty()

        for i, date in enumerate(dates):
            progress = (i + 1) / len(dates)
            progress_bar.progress(progress)
            status_text.text(f"Fetching search data for {date}...")

            filepath = self.search_data_dir / f"{date}.json"

            # Check if file already exists
            if filepath.exists():
                results["skipped"] += 1
                continue

            try:
                url = f"{self.search_base_url}/{date}.json"
                response = requests.get(url, timeout=30)
                response.raise_for_status()

                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(response.json(), f, indent=2)

                results["successful"] += 1

            except Exception as e:
                error_msg = f"Failed to download {date}: {str(e)}"
                results["errors"].append(error_msg)
                results["failed"] += 1

        progress_bar.progress(1.0)
        status_text.text("Search data fetch complete!")
        return results

    def _fetch_apps_dates(self, dates: List[str]) -> Dict[str, Any]:
        """Fetch app data for specified dates from all servers."""
        # Create data directory if it doesn't exist
        self.apps_data_dir.mkdir(parents=True, exist_ok=True)

        results = {
            "total_files": len(dates) * len(self.servers),
            "successful": 0,
            "failed": 0,
            "skipped": 0,
            "errors": [],
        }

        # Create progress bar
        progress_bar = st.progress(0)
        status_text = st.empty()

        total_operations = len(dates) * len(self.servers)
        current_operation = 0

        for date in dates:
            for server in self.servers:
                current_operation += 1
                progress = current_operation / total_operations
                progress_bar.progress(progress)
                status_text.text(f"Fetching app data for {server} on {date}...")

                server_dir = self.apps_data_dir / server
                server_dir.mkdir(parents=True, exist_ok=True)
                filepath = server_dir / f"{date}.json"

                # Check if file already exists
                if filepath.exists():
                    results["skipped"] += 1
                    continue

                try:
                    url = f"{self.apps_base_url}/{server}/{date}.json"
                    response = requests.get(url, timeout=30)
                    response.raise_for_status()

                    with open(filepath, "w", encoding="utf-8") as f:
                        json.dump(response.json(), f, indent=2)

                    results["successful"] += 1

                except Exception as e:
                    error_msg = f"Failed to download {server}/{date}: {str(e)}"
                    results["errors"].append(error_msg)
                    results["failed"] += 1

        progress_bar.progress(1.0)
        status_text.text("App data fetch complete!")
        return results

    def get_missing_dates(
        self, data_type: str, start_date: str, end_date: str
    ) -> List[str]:
        """Get list of dates that are missing locally within a date range."""
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")

        # Generate list of all dates in range
        all_dates = []
        current_date = start_dt
        while current_date <= end_dt:
            all_dates.append(current_date.strftime("%Y-%m-%d"))
            current_date += timedelta(days=1)

        # Get locally available dates
        local_dates = set(self.get_local_dates(data_type))

        # Return missing dates
        return [date for date in all_dates if date not in local_dates]

    def check_data_availability(self, data_type: str) -> Dict[str, Any]:
        """Check data availability both locally and remotely."""
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
