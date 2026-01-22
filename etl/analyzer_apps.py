"""
Analyze F-Droid app metrics data from HTTP servers
"""

import json
import logging
import pathlib
from datetime import datetime

import pandas as pd

from etl.config import cache_config
from etl.getdata_apps import SERVERS
from etl.getdata_apps import SUB_DATA_DIR as DATA_DIR
from etl.security import safe_open

logger = logging.getLogger(__name__)


class AppMetricsAnalyzer:
    """Analyzer for F-Droid app metrics data from HTTP servers."""

    def __init__(self, data_dir: pathlib.Path | None = None) -> None:
        """
        Initialize analyzer with raw data directory.

        Args:
            data_dir: Path to directory containing raw app metrics data.
                     If None, uses default DATA_DIR.
        """
        if data_dir is None:
            data_dir = DATA_DIR
        self.data_dir = data_dir
        self._cache: dict[str, dict] = {}
        self._cache_size_limit = cache_config.APP_CACHE_SIZE

        # HTTP servers to aggregate data from
        self.servers = SERVERS

        # Constants for path prefixes
        self.API_PACKAGES_PREFIX = "/api/v1/packages/"
        self.REPO_PREFIX = "/repo/"

    def get_available_dates(self) -> list[str]:
        """
        Get list of available data dates across all servers.

        Returns:
            Sorted list of date strings in YYYY-MM-DD format found
            across all server data directories.
        """
        dates: set[str] = set()

        for server in self.servers:
            server_dir = self.data_dir / server
            if not server_dir.exists():
                continue

            for file in server_dir.glob("*.json"):
                try:
                    date_str = file.stem
                    # Validate date format
                    datetime.strptime(date_str, "%Y-%m-%d")
                    dates.add(date_str)
                except ValueError:
                    continue

        return sorted(dates)

    def load_data(self, date: str, server: str) -> dict:
        """
        Load data for a specific date and server.

        Args:
            date: Date string in YYYY-MM-DD format
            server: Server name (e.g., 'http01.fdroid.net')

        Returns:
            Dictionary containing the metrics data for the specified date and server

        Raises:
            FileNotFoundError: If data file doesn't exist for the given date and server
            json.JSONDecodeError: If data file contains invalid JSON
        """
        cache_key = f"{server}_{date}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        file_path = self.data_dir / server / f"{date}.json"
        if not file_path.exists():
            raise FileNotFoundError(f"No data found for {server} on {date}")

        with safe_open(file_path, encoding="utf-8") as f:
            data = json.load(f)

        # Simple cache size management - remove oldest entries if cache is too large
        if len(self._cache) >= self._cache_size_limit:
            # Remove the first (oldest) cache entry
            oldest_key = next(iter(self._cache))
            del self._cache[oldest_key]

        self._cache[cache_key] = data
        return data

    def load_merged_data(self, date: str) -> dict:
        """
        Load and merge data from all servers for a specific date.

        Args:
            date: Date string in YYYY-MM-DD format

        Returns:
            Dictionary containing merged metrics from all servers with keys:
            - hits: Total hits across all servers
            - errors: Aggregated error data
            - hitsPerCountry: Country-wise hit distribution
            - paths: Request path statistics
            - queries: Query statistics
            - servers: List of servers that had data
        """
        merged_data: dict = {
            "hits": 0,
            "errors": {},
            "hitsPerCountry": {},
            "paths": {},
            "queries": {},
            "servers": [],
        }

        for server in self.servers:
            try:
                data = self.load_data(date, server)
                merged_data["servers"].append(server)

                # Merge hits
                merged_data["hits"] += data.get("hits", 0)

                # Merge errors
                for error_code, error_data in data.get("errors", {}).items():
                    if error_code not in merged_data["errors"]:
                        merged_data["errors"][error_code] = {"hits": 0, "paths": {}}
                    merged_data["errors"][error_code]["hits"] += error_data.get(
                        "hits", 0
                    )

                    # Merge error paths
                    for path, path_hits in error_data.get("paths", {}).items():
                        if path not in merged_data["errors"][error_code]["paths"]:
                            merged_data["errors"][error_code]["paths"][path] = 0
                        merged_data["errors"][error_code]["paths"][path] += path_hits

                # Merge countries
                for country, hits in data.get("hitsPerCountry", {}).items():
                    if country not in merged_data["hitsPerCountry"]:
                        merged_data["hitsPerCountry"][country] = 0
                    merged_data["hitsPerCountry"][country] += hits

                # Merge paths
                for path, path_data in data.get("paths", {}).items():
                    if path not in merged_data["paths"]:
                        merged_data["paths"][path] = {"hits": 0, "hitsPerCountry": {}}

                    if isinstance(path_data, dict):
                        merged_data["paths"][path]["hits"] += path_data.get("hits", 0)

                        # Merge path countries
                        for country, hits in path_data.get(
                            "hitsPerCountry", {}
                        ).items():
                            if (
                                country
                                not in merged_data["paths"][path]["hitsPerCountry"]
                            ):
                                merged_data["paths"][path]["hitsPerCountry"][
                                    country
                                ] = 0
                            merged_data["paths"][path]["hitsPerCountry"][country] += (
                                hits
                            )
                    else:
                        merged_data["paths"][path]["hits"] += path_data

                # Merge queries
                for query, query_data in data.get("queries", {}).items():
                    if query not in merged_data["queries"]:
                        merged_data["queries"][query] = {
                            "hits": 0,
                            "hitsPerCountry": {},
                        }

                    if isinstance(query_data, dict):
                        merged_data["queries"][query]["hits"] += query_data.get(
                            "hits", 0
                        )

                        # Merge query countries
                        for country, hits in query_data.get(
                            "hitsPerCountry", {}
                        ).items():
                            if (
                                country
                                not in merged_data["queries"][query]["hitsPerCountry"]
                            ):
                                merged_data["queries"][query]["hitsPerCountry"][
                                    country
                                ] = 0
                            merged_data["queries"][query]["hitsPerCountry"][
                                country
                            ] += hits
                    else:
                        merged_data["queries"][query]["hits"] += query_data

            except FileNotFoundError:
                continue

        return merged_data

    def get_daily_summary(self, date: str) -> dict:
        """Get daily summary statistics for merged data."""
        data = self.load_merged_data(date)

        summary = {
            "date": date,
            "total_hits": data.get("hits", 0),
            "servers_active": len(data.get("servers", [])),
            "total_errors": sum(
                error_data.get("hits", 0)
                for error_data in data.get("errors", {}).values()
            ),
            "top_countries": self._get_top_items(data.get("hitsPerCountry", {}), 10),
            "top_paths": self._get_top_items(data.get("paths", {}), 20),
            "unique_paths": len(data.get("paths", {})),
        }

        return summary

    def get_time_series_data(self, dates: list[str] | None = None) -> pd.DataFrame:
        """Get time series data for multiple dates."""
        if dates is None:
            dates = self.get_available_dates()

        records = []
        for date in dates:
            try:
                summary = self.get_daily_summary(date)
                records.append(
                    {
                        "date": pd.to_datetime(date),
                        "total_hits": summary["total_hits"],
                        "servers_active": summary["servers_active"],
                        "total_errors": summary["total_errors"],
                        "unique_paths": summary["unique_paths"],
                    }
                )
            except FileNotFoundError:
                # Skip missing data files
                continue
            except Exception as e:
                # Log unexpected errors but continue processing
                logger.warning(f"Error processing date {date}: {e}")
                continue

        return pd.DataFrame(records).sort_values("date")

    def get_path_analysis(self, dates: list[str] | None = None) -> pd.DataFrame:
        """
        Analyze request paths across multiple dates.

        Returns a DataFrame with columns:
        - path: The request path
        - total_hits: Total hits across all dates
        - appearances: Number of weeks where the path had hits > 0
        - avg_hits: Average hits per active week
        - dates: List of dates where the path was active
        """
        if dates is None:
            dates = self.get_available_dates()

        # Collect all path-date combinations
        path_records = []
        for date in dates:
            try:
                data = self.load_merged_data(date)
                paths = data.get("paths", {})

                for path, path_data in paths.items():
                    hits = (
                        path_data.get("hits", 0)
                        if isinstance(path_data, dict)
                        else path_data
                    )
                    if hits > 0:  # Only include paths with actual hits
                        path_records.append({"path": path, "date": date, "hits": hits})

            except FileNotFoundError:
                # Skip missing data files
                continue
            except Exception as e:
                # Log unexpected errors but continue processing
                logger.warning(f"Error processing date {date}: {e}")
                continue

        # Convert to DataFrame and use vectorized operations
        if not path_records:
            return pd.DataFrame(
                columns=["path", "total_hits", "appearances", "avg_hits", "dates"]
            )

        df = pd.DataFrame(path_records)

        # Group by path and aggregate
        path_analysis = (
            df.groupby("path")
            .agg(
                total_hits=("hits", "sum"),
                appearances=("date", "count"),  # Count of dates with hits
                dates=("date", lambda x: sorted(x.unique())),
            )
            .reset_index()
        )

        # Calculate average hits per active week
        path_analysis["avg_hits"] = (
            path_analysis["total_hits"] / path_analysis["appearances"]
        )

        return path_analysis.sort_values("total_hits", ascending=False)

    def get_country_analysis(self, dates: list[str] | None = None) -> pd.DataFrame:
        """
        Analyze hits by country across multiple dates.

        Returns a DataFrame with columns:
        - country: The country code
        - total_hits: Total hits across all dates
        - appearances: Number of weeks where the country had hits > 0
        - avg_hits: Average hits per active week
        """
        if dates is None:
            dates = self.get_available_dates()

        # Collect all country-date combinations
        country_records = []
        for date in dates:
            try:
                data = self.load_merged_data(date)
                countries = data.get("hitsPerCountry", {})

                for country, hits in countries.items():
                    if hits > 0:  # Only include countries with actual hits
                        country_records.append(
                            {"country": country, "date": date, "hits": hits}
                        )

            except FileNotFoundError:
                # Skip missing data files
                continue
            except Exception as e:
                # Log unexpected errors but continue processing
                logger.warning(f"Error processing date {date}: {e}")
                continue

        # Convert to DataFrame and use vectorized operations
        if not country_records:
            return pd.DataFrame(
                columns=["country", "total_hits", "appearances", "avg_hits"]
            )

        df = pd.DataFrame(country_records)

        # Group by country and aggregate
        country_analysis = (
            df.groupby("country")
            .agg(
                total_hits=("hits", "sum"),
                appearances=("date", "count"),  # Count of dates with hits
            )
            .reset_index()
        )

        # Calculate average hits per active week
        country_analysis["avg_hits"] = (
            country_analysis["total_hits"] / country_analysis["appearances"]
        )

        return country_analysis.sort_values("total_hits", ascending=False)

    def get_server_comparison(self, date: str) -> pd.DataFrame:
        """Compare metrics across servers for a specific date."""
        server_data = []

        for server in self.servers:
            try:
                data = self.load_data(date, server)
                server_data.append(
                    {
                        "server": server,
                        "hits": data.get("hits", 0),
                        "errors": sum(
                            error_data.get("hits", 0)
                            for error_data in data.get("errors", {}).values()
                        ),
                        "unique_paths": len(data.get("paths", {})),
                        "countries": len(data.get("hitsPerCountry", {})),
                    }
                )
            except FileNotFoundError:
                server_data.append(
                    {
                        "server": server,
                        "hits": 0,
                        "errors": 0,
                        "unique_paths": 0,
                        "countries": 0,
                    }
                )

        return pd.DataFrame(server_data)

    def get_package_analysis(self, dates: list[str] | None = None) -> pd.DataFrame:
        """
        Analyze F-Droid package API requests (/api/v1/packages/).

        Returns a DataFrame with columns:
        - package_name: The F-Droid package name
        - total_hits: Total hits across all dates
        - appearances: Number of weeks where the package had hits > 0
        - avg_hits: Average hits per active week
        - dates: List of dates where the package was active
        """
        if dates is None:
            dates = self.get_available_dates()

        package_data = {}

        for date in dates:
            try:
                data = self.load_merged_data(date)
                paths = data.get("paths", {})

                for path, path_data in paths.items():
                    # Filter for package API paths
                    if path.startswith(self.API_PACKAGES_PREFIX):
                        # Extract package name from path
                        package_name = (
                            path.replace(self.API_PACKAGES_PREFIX, "")
                            .strip()
                            .strip("/")
                        )

                        # Skip empty or invalid package names
                        if not package_name or "/" in package_name:
                            continue

                        if package_name not in package_data:
                            package_data[package_name] = {
                                "package_name": package_name,
                                "total_hits": 0,
                                "appearances": 0,
                                "dates": [],
                                "avg_hits": 0,
                            }

                        hits = (
                            path_data.get("hits", 0)
                            if isinstance(path_data, dict)
                            else path_data
                        )
                        package_data[package_name]["total_hits"] += hits
                        # Only count as an appearance if there were actual hits
                        if hits > 0:
                            package_data[package_name]["appearances"] += 1
                            package_data[package_name]["dates"].append(date)

            except FileNotFoundError:
                # Skip missing data files
                continue
            except Exception as e:
                # Log unexpected errors but continue processing
                logger.warning(f"Error processing date {date}: {e}")
                continue

        # Calculate averages
        for pkg_stats in package_data.values():
            if pkg_stats["appearances"] > 0:
                pkg_stats["avg_hits"] = (
                    pkg_stats["total_hits"] / pkg_stats["appearances"]
                )

        # Convert to DataFrame
        if package_data:
            df = pd.DataFrame(list(package_data.values()))
            return df.sort_values("total_hits", ascending=False)
        else:
            return pd.DataFrame()

    def _get_top_items(self, data: dict, limit: int) -> list[tuple[str, int]]:
        """
        Get top N items from a dictionary by value.

        Args:
            data: Dictionary mapping keys to either integer values or
                  dictionaries with a 'hits' key
            limit: Maximum number of items to return

        Returns:
            List of (key, hits) tuples sorted by hits in descending order
        """
        if not data:
            return []

        # Handle nested dictionaries (like queries with metadata)
        items: list[tuple[str, int]] = []
        for key, value in data.items():
            if isinstance(value, dict):
                hits = value.get("hits", 0)
            else:
                hits = value
            items.append((key, hits))

        return sorted(items, key=lambda x: x[1], reverse=True)[:limit]

    def get_package_downloads(
        self, package_id: str, dates: list[str] | None = None
    ) -> dict:
        """
        Get download statistics for a specific package.

        Returns a dictionary with:
        - total_downloads: Total APK downloads across all versions
        - versions: Dict mapping version codes to download counts
        - api_hits: Total API hits for the package info
        - countries: Dict mapping countries to download counts
        """
        if dates is None:
            dates = self.get_available_dates()

        result = {
            "package_id": package_id,
            "total_downloads": 0,
            "versions": {},
            "api_hits": 0,
            "countries": {},
            "dates_active": [],
        }

        for date in dates:
            try:
                data = self.load_merged_data(date)
                paths = data.get("paths", {})
                date_had_activity = False

                for path, path_data in paths.items():
                    # Check for APK downloads: /repo/{package_id}_{version}.apk
                    if path.startswith(self.REPO_PREFIX) and path.endswith(".apk"):
                        # Extract package name and version from path
                        filename = (
                            path.replace(self.REPO_PREFIX, "")
                            .replace(".apk", "")
                            .strip("/")
                        )

                        # Handle potential query parameters (e.g., &pxdate=2025-08-05)
                        if "&" in filename:
                            filename = filename.split("&")[0]

                        # Split on last underscore to separate package name and version
                        if "_" in filename:
                            parts = filename.rsplit("_", 1)
                            if len(parts) == 2:
                                pkg_name, version = parts
                                if pkg_name == package_id:
                                    hits = (
                                        path_data.get("hits", 0)
                                        if isinstance(path_data, dict)
                                        else path_data
                                    )
                                    result["total_downloads"] += hits
                                    result["versions"][version] = (
                                        result["versions"].get(version, 0) + hits
                                    )
                                    date_had_activity = True

                                    # Merge country data for this version
                                    if isinstance(path_data, dict):
                                        for country, country_hits in path_data.get(
                                            "hitsPerCountry", {}
                                        ).items():
                                            result["countries"][country] = (
                                                result["countries"].get(country, 0)
                                                + country_hits
                                            )

                    # Check for API hits: /api/v1/packages/{package_id}
                    elif path == f"{self.API_PACKAGES_PREFIX}{package_id}":
                        hits = (
                            path_data.get("hits", 0)
                            if isinstance(path_data, dict)
                            else path_data
                        )
                        result["api_hits"] += hits
                        date_had_activity = True

                if date_had_activity:
                    result["dates_active"].append(date)

            except FileNotFoundError:
                continue
            except Exception as e:
                logger.warning(
                    f"Error processing date {date} for package {package_id}: {e}"
                )
                continue

        return result

    def get_all_packages_with_downloads(
        self, dates: list[str] | None = None
    ) -> pd.DataFrame:
        """
        Get all packages that have APK downloads with their statistics.

        Returns a DataFrame with columns:
        - package_id: The package identifier
        - total_downloads: Total APK downloads
        - total_versions: Number of different versions downloaded
        - api_hits: Total API hits
        - dates_active: Number of dates with activity
        """
        if dates is None:
            dates = self.get_available_dates()

        # Collect all package activity records
        package_records = []
        for date in dates:
            try:
                data = self.load_merged_data(date)
                paths = data.get("paths", {})

                for path, path_data in paths.items():
                    hits = (
                        path_data.get("hits", 0)
                        if isinstance(path_data, dict)
                        else path_data
                    )

                    # Check for APK downloads
                    if (
                        path.startswith(self.REPO_PREFIX)
                        and path.endswith(".apk")
                        and hits > 0
                    ):
                        filename = (
                            path.replace(self.REPO_PREFIX, "")
                            .replace(".apk", "")
                            .strip("/")
                        )

                        # Handle potential query parameters
                        if "&" in filename:
                            filename = filename.split("&")[0]

                        # Split on last underscore to separate package name and version
                        if "_" in filename:
                            parts = filename.rsplit("_", 1)
                            if len(parts) == 2:
                                pkg_name, version = parts
                                package_records.append(
                                    {
                                        "package_id": pkg_name,
                                        "date": date,
                                        "version": version,
                                        "downloads": hits,
                                        "api_hits": 0,
                                    }
                                )

                    # Check for API hits
                    elif path.startswith(self.API_PACKAGES_PREFIX) and hits > 0:
                        pkg_name = path.replace(self.API_PACKAGES_PREFIX, "").strip()
                        if pkg_name and "/" not in pkg_name:
                            package_records.append(
                                {
                                    "package_id": pkg_name,
                                    "date": date,
                                    "version": None,  # No version for API hits
                                    "downloads": 0,
                                    "api_hits": hits,
                                }
                            )

            except FileNotFoundError:
                continue
            except Exception as e:
                logger.warning(f"Error processing date {date}: {e}")
                continue

        # Convert to DataFrame and use vectorized operations
        if not package_records:
            return pd.DataFrame(
                columns=[
                    "package_id",
                    "total_downloads",
                    "total_versions",
                    "api_hits",
                    "dates_active",
                ]
            )

        df = pd.DataFrame(package_records)

        # Group by package_id and aggregate
        package_analysis = (
            df.groupby("package_id")
            .agg(
                total_downloads=("downloads", "sum"),
                total_versions=(
                    "version",
                    lambda x: len(x.dropna().unique()),
                ),  # Count unique non-null versions
                api_hits=("api_hits", "sum"),
                dates_active=("date", lambda x: len(x.unique())),  # Count unique dates
            )
            .reset_index()
        )

        return package_analysis.sort_values("total_downloads", ascending=False)
