"""
Analyze F-Droid app metrics data from HTTP servers
"""

import json
import logging
import pathlib
from datetime import datetime

import pandas as pd

from etl.getdata_apps import SERVERS
from etl.getdata_apps import SUB_DATA_DIR as DATA_DIR

logger = logging.getLogger(__name__)


class AppMetricsAnalyzer:
    """Analyzer for F-Droid app metrics data from HTTP servers."""

    def __init__(self, data_dir: pathlib.Path | None = None) -> None:
        """Initialize analyzer with raw data directory."""
        if data_dir is None:
            data_dir = DATA_DIR
        self.data_dir = data_dir
        self._cache = {}
        self._cache_size_limit = 100  # Limit cache to 100 entries

        # HTTP servers to aggregate data from
        self.servers = SERVERS

    def get_available_dates(self) -> list[str]:
        """Get list of available data dates across all servers."""
        dates = set()

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

        return sorted(list(dates))

    def load_data(self, date: str, server: str) -> dict:
        """Load data for a specific date and server."""
        cache_key = f"{server}_{date}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        file_path = self.data_dir / server / f"{date}.json"
        if not file_path.exists():
            raise FileNotFoundError(f"No data found for {server} on {date}")

        with open(file_path, encoding="utf-8") as f:
            data = json.load(f)

        # Simple cache size management - remove oldest entries if cache is too large
        if len(self._cache) >= self._cache_size_limit:
            # Remove the first (oldest) cache entry
            oldest_key = next(iter(self._cache))
            del self._cache[oldest_key]

        self._cache[cache_key] = data
        return data

    def load_merged_data(self, date: str) -> dict:
        """Load and merge data from all servers for a specific date."""
        merged_data = {
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

        all_paths = {}

        for date in dates:
            try:
                data = self.load_merged_data(date)
                paths = data.get("paths", {})

                for path, path_data in paths.items():
                    if path not in all_paths:
                        all_paths[path] = {
                            "path": path,
                            "total_hits": 0,
                            "appearances": 0,
                            "avg_hits": 0,
                            "dates": [],
                        }

                    hits = (
                        path_data.get("hits", 0)
                        if isinstance(path_data, dict)
                        else path_data
                    )
                    all_paths[path]["total_hits"] += hits
                    # Only count as an appearance if there were actual hits
                    if hits > 0:
                        all_paths[path]["appearances"] += 1
                        all_paths[path]["dates"].append(date)

            except FileNotFoundError:
                # Skip missing data files
                continue
            except Exception as e:
                # Log unexpected errors but continue processing
                logger.warning(f"Error processing date {date}: {e}")
                continue

        # Calculate averages
        for path_stats in all_paths.values():
            if path_stats["appearances"] > 0:
                path_stats["avg_hits"] = (
                    path_stats["total_hits"] / path_stats["appearances"]
                )

        # Convert to DataFrame
        df = pd.DataFrame(list(all_paths.values()))
        return df.sort_values("total_hits", ascending=False)

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

        country_data = {}

        for date in dates:
            try:
                data = self.load_merged_data(date)
                countries = data.get("hitsPerCountry", {})

                for country, hits in countries.items():
                    if country not in country_data:
                        country_data[country] = {
                            "country": country,
                            "total_hits": 0,
                            "appearances": 0,
                            "avg_hits": 0,
                        }

                    country_data[country]["total_hits"] += hits
                    # Only count as an appearance if there were actual hits
                    if hits > 0:
                        country_data[country]["appearances"] += 1

            except FileNotFoundError:
                # Skip missing data files
                continue
            except Exception as e:
                # Log unexpected errors but continue processing
                logger.warning(f"Error processing date {date}: {e}")
                continue

        # Calculate averages
        for stats in country_data.values():
            if stats["appearances"] > 0:
                stats["avg_hits"] = stats["total_hits"] / stats["appearances"]

        df = pd.DataFrame(list(country_data.values()))
        return df.sort_values("total_hits", ascending=False)

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
                    if path.startswith("/api/v1/packages/"):
                        # Extract package name from path
                        package_name = path.replace("/api/v1/packages/", "").strip()

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
        """Get top N items from a dictionary by value."""
        if not data:
            return []

        # Handle nested dictionaries (like paths with metadata)
        items = []
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
                    if path.startswith("/repo/") and path.endswith(".apk"):
                        # Extract package name and version from path
                        filename = path.replace("/repo/", "").replace(".apk", "")

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
                    elif path == f"/api/v1/packages/{package_id}":
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

        all_packages = {}

        for date in dates:
            try:
                data = self.load_merged_data(date)
                paths = data.get("paths", {})

                for path, path_data in paths.items():
                    # Check for APK downloads
                    if path.startswith("/repo/") and path.endswith(".apk"):
                        filename = path.replace("/repo/", "").replace(".apk", "")

                        # Handle potential query parameters
                        if "&" in filename:
                            filename = filename.split("&")[0]

                        # Split on last underscore to separate package name and version
                        if "_" in filename:
                            parts = filename.rsplit("_", 1)
                            if len(parts) == 2:
                                pkg_name, version = parts

                                if pkg_name not in all_packages:
                                    all_packages[pkg_name] = {
                                        "package_id": pkg_name,
                                        "total_downloads": 0,
                                        "versions": set(),
                                        "api_hits": 0,
                                        "dates_active": set(),
                                    }

                                hits = (
                                    path_data.get("hits", 0)
                                    if isinstance(path_data, dict)
                                    else path_data
                                )
                                all_packages[pkg_name]["total_downloads"] += hits
                                all_packages[pkg_name]["versions"].add(version)
                                all_packages[pkg_name]["dates_active"].add(date)

                    # Check for API hits
                    elif path.startswith("/api/v1/packages/"):
                        pkg_name = path.replace("/api/v1/packages/", "").strip()
                        if pkg_name and "/" not in pkg_name:
                            if pkg_name not in all_packages:
                                all_packages[pkg_name] = {
                                    "package_id": pkg_name,
                                    "total_downloads": 0,
                                    "versions": set(),
                                    "api_hits": 0,
                                    "dates_active": set(),
                                }

                            hits = (
                                path_data.get("hits", 0)
                                if isinstance(path_data, dict)
                                else path_data
                            )
                            all_packages[pkg_name]["api_hits"] += hits
                            all_packages[pkg_name]["dates_active"].add(date)

            except FileNotFoundError:
                continue
            except Exception as e:
                logger.warning(f"Error processing date {date}: {e}")
                continue

        # Convert to DataFrame
        records = []
        for pkg_data in all_packages.values():
            records.append(
                {
                    "package_id": pkg_data["package_id"],
                    "total_downloads": pkg_data["total_downloads"],
                    "total_versions": len(pkg_data["versions"]),
                    "api_hits": pkg_data["api_hits"],
                    "dates_active": len(pkg_data["dates_active"]),
                }
            )

        df = pd.DataFrame(records)
        return df.sort_values("total_downloads", ascending=False)
