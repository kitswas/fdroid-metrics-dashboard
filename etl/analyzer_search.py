"""
Analyze F-Droid search metrics data
"""

import json
import logging
import pathlib
from datetime import datetime

import pandas as pd

from etl.config import cache_config
from etl.getdata_search import SUB_DATA_DIR as DATA_DIR

logger = logging.getLogger(__name__)


class SearchMetricsAnalyzer:
    """Analyzer for F-Droid search metrics data."""

    def __init__(self, data_dir: pathlib.Path | None = None) -> None:
        """
        Initialize analyzer with raw data directory.

        Args:
            data_dir: Path to directory containing raw search metrics data.
                     If None, uses default DATA_DIR.
        """
        if data_dir is None:
            data_dir = DATA_DIR
        self.data_dir = data_dir
        self._cache: dict[str, dict] = {}
        self._cache_size_limit = cache_config.SEARCH_CACHE_SIZE

    def get_available_dates(self) -> list[str]:
        """
        Get list of available data dates.

        Returns:
            Sorted list of date strings in YYYY-MM-DD format found
            in the data directory.
        """
        dates: list[str] = []
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

    def load_data(self, date: str) -> dict:
        """
        Load data for a specific date.

        Args:
            date: Date string in YYYY-MM-DD format

        Returns:
            Dictionary containing the search metrics data for the specified date

        Raises:
            FileNotFoundError: If data file doesn't exist for the given date
            json.JSONDecodeError: If data file contains invalid JSON
        """
        if date in self._cache:
            return self._cache[date]

        file_path = self.data_dir / f"{date}.json"
        if not file_path.exists():
            raise FileNotFoundError(f"No data found for date {date}")

        with open(file_path, encoding="utf-8") as f:
            data = json.load(f)

        # Simple cache size management - remove oldest entries if cache is too large
        if len(self._cache) >= self._cache_size_limit:
            # Remove the first (oldest) cache entry
            oldest_key = next(iter(self._cache))
            del self._cache[oldest_key]

        self._cache[date] = data
        return data

    def get_daily_summary(self, date: str) -> dict:
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
                        "unique_queries": summary["unique_queries"],
                        "total_errors": summary["total_errors"],
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

    def get_query_analysis(self, dates: list[str] | None = None) -> pd.DataFrame:
        """
        Analyze search queries across multiple dates.

        Returns a DataFrame with columns:
        - query: The search query text
        - total_hits: Total hits across all dates
        - appearances: Number of weeks where the query had hits > 0
        - avg_hits: Average hits per active week
        - dates: List of dates where the query was active
        """
        if dates is None:
            dates = self.get_available_dates()

        # Collect all query-date combinations
        query_records = []
        for date in dates:
            try:
                data = self.load_data(date)
                queries = data.get("queries", {})

                for query, query_data in queries.items():
                    hits = (
                        query_data.get("hits", 0)
                        if isinstance(query_data, dict)
                        else query_data
                    )
                    if hits > 0:  # Only include queries with actual hits
                        query_records.append(
                            {"query": query, "date": date, "hits": hits}
                        )

            except FileNotFoundError:
                # Skip missing data files
                continue
            except Exception as e:
                # Log unexpected errors but continue processing
                logger.warning(f"Error processing date {date}: {e}")
                continue

        # Convert to DataFrame and use vectorized operations
        if not query_records:
            return pd.DataFrame(
                columns=["query", "total_hits", "appearances", "avg_hits", "dates"]
            )

        df = pd.DataFrame(query_records)

        # Group by query and aggregate
        query_analysis = (
            df.groupby("query")
            .agg(
                total_hits=("hits", "sum"),
                appearances=("date", "count"),  # Count of dates with hits
                dates=("date", lambda x: sorted(list(x.unique()))),
            )
            .reset_index()
        )

        # Calculate average hits per active week
        query_analysis["avg_hits"] = (
            query_analysis["total_hits"] / query_analysis["appearances"]
        )

        return query_analysis.sort_values("total_hits", ascending=False)

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
                data = self.load_data(date)
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
