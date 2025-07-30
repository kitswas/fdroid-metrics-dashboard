"""
Analyze F-Droid app metrics data from HTTP servers
"""

import json
import pathlib
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd

from etl.getdata_apps import SUB_DATA_DIR as DATA_DIR


class AppMetricsAnalyzer:
    """Analyzer for F-Droid app metrics data from HTTP servers."""

    def __init__(self, data_dir: Optional[pathlib.Path] = None):
        """Initialize analyzer with raw data directory."""
        if data_dir is None:
            data_dir = DATA_DIR
        self.data_dir = data_dir
        self._cache = {}

        # HTTP servers to aggregate data from
        self.servers = ["http01.fdroid.net", "http02.fdroid.net", "http03.fdroid.net"]

    def get_available_dates(self) -> List[str]:
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

    def load_data(self, date: str, server: str) -> Dict:
        """Load data for a specific date and server."""
        cache_key = f"{server}_{date}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        file_path = self.data_dir / server / f"{date}.json"
        if not file_path.exists():
            raise FileNotFoundError(f"No data found for {server} on {date}")

        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        self._cache[cache_key] = data
        return data

    def load_merged_data(self, date: str) -> Dict:
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

    def get_daily_summary(self, date: str) -> Dict:
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

    def get_time_series_data(self, dates: Optional[List[str]] = None) -> pd.DataFrame:
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
            except Exception:
                continue

        return pd.DataFrame(records).sort_values("date")

    def get_path_analysis(self, dates: Optional[List[str]] = None) -> pd.DataFrame:
        """Analyze request paths across multiple dates."""
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
                    all_paths[path]["appearances"] += 1
                    all_paths[path]["dates"].append(date)

            except Exception:
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

    def get_country_analysis(self, dates: Optional[List[str]] = None) -> pd.DataFrame:
        """Analyze hits by country across multiple dates."""
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
                    country_data[country]["appearances"] += 1

            except Exception:
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

    def get_package_analysis(self, dates: Optional[List[str]] = None) -> pd.DataFrame:
        """Analyze F-Droid package API requests (/api/v1/packages/)."""
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
                        package_name = path.replace("/api/v1/packages/", "")

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
                        package_data[package_name]["appearances"] += 1
                        package_data[package_name]["dates"].append(date)

            except Exception:
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

    def _get_top_items(self, data: Dict, limit: int) -> List[Tuple[str, int]]:
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
