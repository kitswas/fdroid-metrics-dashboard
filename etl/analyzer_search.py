"""
Analyze F-Droid search metrics data
"""

import json
import pathlib
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd

from etl.getdata_search import SUB_DATA_DIR as DATA_DIR


class SearchMetricsAnalyzer:
    """Analyzer for F-Droid search metrics data."""

    def __init__(self, data_dir: Optional[pathlib.Path] = None):
        """Initialize analyzer with raw data directory."""
        if data_dir is None:
            data_dir = DATA_DIR
        self.data_dir = data_dir
        self._cache = {}

    def get_available_dates(self) -> List[str]:
        """Get list of available data dates."""
        dates = []
        for file in self.data_dir.glob("*.json"):
            if file.name != "last_submitted_to_cimp.json":
                try:
                    date_str = file.stem
                    # Validate date format
                    datetime.strptime(date_str, "%Y-%m-%d")
                    dates.append(date_str)
                except ValueError:
                    continue
        return sorted(dates)

    def load_data(self, date: str) -> Dict:
        """Load data for a specific date."""
        if date in self._cache:
            return self._cache[date]

        file_path = self.data_dir / f"{date}.json"
        if not file_path.exists():
            raise FileNotFoundError(f"No data found for date {date}")

        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        self._cache[date] = data
        return data

    def get_daily_summary(self, date: str) -> Dict:
        """Get daily summary statistics."""
        data = self.load_data(date)

        summary = {
            "date": date,
            "total_hits": data.get("hits", 0),
            "unique_queries": len(data.get("queries", {})),
            "total_errors": sum(
                error_data.get("hits", 0)
                for error_data in data.get("errors", {}).values()
            ),
            "top_countries": self._get_top_items(data.get("hitsPerCountry", {}), 10),
            "top_languages": self._get_top_items(data.get("hitsPerLanguage", {}), 10),
            "top_queries": self._get_top_items(data.get("queries", {}), 20),
            "top_paths": self._get_top_items(data.get("paths", {}), 10),
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
                        "unique_queries": summary["unique_queries"],
                        "total_errors": summary["total_errors"],
                    }
                )
            except FileNotFoundError:
                continue

        return pd.DataFrame(records).sort_values("date")

    def get_query_analysis(self, dates: Optional[List[str]] = None) -> pd.DataFrame:
        """Analyze search queries across multiple dates."""
        if dates is None:
            dates = self.get_available_dates()

        all_queries = {}

        for date in dates:
            try:
                data = self.load_data(date)
                queries = data.get("queries", {})

                for query, query_data in queries.items():
                    if query not in all_queries:
                        all_queries[query] = {
                            "query": query,
                            "total_hits": 0,
                            "appearances": 0,
                            "avg_hits": 0,
                            "dates": [],
                        }

                    hits = (
                        query_data.get("hits", 0)
                        if isinstance(query_data, dict)
                        else query_data
                    )
                    all_queries[query]["total_hits"] += hits
                    all_queries[query]["appearances"] += 1
                    all_queries[query]["dates"].append(date)

            except FileNotFoundError:
                continue

        # Calculate averages
        for query_stats in all_queries.values():
            if query_stats["appearances"] > 0:
                query_stats["avg_hits"] = (
                    query_stats["total_hits"] / query_stats["appearances"]
                )

        # Convert to DataFrame
        df = pd.DataFrame(list(all_queries.values()))
        return df.sort_values("total_hits", ascending=False)

    def get_country_analysis(self, dates: Optional[List[str]] = None) -> pd.DataFrame:
        """Analyze hits by country across multiple dates."""
        if dates is None:
            dates = self.get_available_dates()

        country_data = {}

        for date in dates:
            try:
                data = self.load_data(date)
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

            except FileNotFoundError:
                continue

        # Calculate averages
        for stats in country_data.values():
            if stats["appearances"] > 0:
                stats["avg_hits"] = stats["total_hits"] / stats["appearances"]

        df = pd.DataFrame(list(country_data.values()))
        return df.sort_values("total_hits", ascending=False)

    def _get_top_items(self, data: Dict, limit: int) -> List[Tuple[str, int]]:
        """Get top N items from a dictionary by value."""
        if not data:
            return []

        # Handle nested dictionaries (like queries with metadata)
        items = []
        for key, value in data.items():
            if isinstance(value, dict):
                hits = value.get("hits", 0)
            else:
                hits = value
            items.append((key, hits))

        return sorted(items, key=lambda x: x[1], reverse=True)[:limit]
