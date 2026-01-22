"""
Script to extract per-package monthly metrics from F-Droid app and search data.
Output: ./processed/monthly/{package_id}.json
"""

import json
import logging
import os
import pathlib
from datetime import datetime

from etl.analyzer_apps import AppMetricsAnalyzer
from etl.analyzer_search import SearchMetricsAnalyzer
from etl.data_fetcher import DataFetcher
from etl.security import safe_open

# --- CONFIG ---
OUTPUT_DIR = pathlib.Path(__file__).parent / "processed" / "monthly"
MONTHLY_SNAPSHOT_COUNT = 4  # Number of snapshots to use for monthly stats

logger = logging.getLogger(__name__)


def get_last_n_months_dates(dates: list[str], n_months: int) -> list[str]:
    """
    Extract the last N months of dates from a sorted date list.

    This function takes the most recent date from each of the last N months,
    working backwards from the most recent date in the list.

    Args:
        dates: Sorted list of date strings in YYYY-MM-DD format (ascending order)
        n_months: Number of months to include in the result

    Returns:
        Sorted list of unique dates representing the last N months

    Raises:
        ValueError: If dates list is empty or contains invalid date formats
    """
    if not dates:
        raise ValueError("Dates list cannot be empty")

    months: dict[str, list[str]] = {}
    unique_dates: set[str] = set()

    for date_str in reversed(dates):
        if date_str in unique_dates:
            continue
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError as e:
            raise ValueError(f"Invalid date format '{date_str}': {e}") from e

        ym = dt.strftime("%Y-%m")
        if ym not in months:
            months[ym] = []
        months[ym].append(date_str)
        unique_dates.add(date_str)
        if len(months) >= n_months:
            # Once we have n_months, stop collecting
            break

    # Flatten and sort
    result: list[str] = []
    for month_dates in months.values():
        result.extend(month_dates)
    # Ensure result is unique
    return sorted(set(result))


def main() -> None:
    """
    Main function to extract per-package monthly metrics.

    This function:
    1. Fetches available dates from remote servers
    2. Downloads the last N months of data
    3. Processes app and search metrics
    4. Generates per-package JSON files with monthly statistics

    Raises:
        ValueError: If insufficient data is available
    """
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    logger.info("Getting available remote dates for apps and search...")
    fetcher = DataFetcher()
    app_remote_dates = fetcher.get_available_remote_dates("apps")
    search_remote_dates = fetcher.get_available_remote_dates("search")
    logger.info(
        f"Found {len(app_remote_dates)} app dates, {len(search_remote_dates)} search dates."
    )

    # Get last n common remote dates
    n = MONTHLY_SNAPSHOT_COUNT
    common_remote_dates = sorted(set(app_remote_dates) & set(search_remote_dates))
    if len(common_remote_dates) < n:
        logger.error(
            f"Not enough common remote dates found (found {len(common_remote_dates)}). Aborting."
        )
        return
    dates_to_fetch = common_remote_dates[-n:]

    def log_progress(progress: float) -> None:
        logger.info(f"Progress: {progress * 100:.1f}%")

    def log_status(msg: str) -> None:
        logger.info(msg)

    logger.info(f"Fetching app data for dates: {dates_to_fetch}")
    fetcher.fetch_date_range(
        "apps",
        dates_to_fetch[0],
        dates_to_fetch[-1],
        progress_callback=log_progress,
        status_callback=log_status,
    )
    logger.info(f"Fetching search data for dates: {dates_to_fetch}")
    fetcher.fetch_date_range(
        "search",
        dates_to_fetch[0],
        dates_to_fetch[-1],
        progress_callback=log_progress,
        status_callback=log_status,
    )

    # Find common dates locally
    app_analyzer = AppMetricsAnalyzer()
    search_analyzer = SearchMetricsAnalyzer()
    app_dates = app_analyzer.get_available_dates()
    search_dates = search_analyzer.get_available_dates()
    common_dates = sorted(set(app_dates) & set(search_dates))
    min_required = MONTHLY_SNAPSHOT_COUNT
    if len(common_dates) < min_required:
        logger.error(
            f"Not enough common dates found (found {len(common_dates)}). Aborting."
        )
        return
    dates = common_dates[-min_required:]
    logger.info(f"Processing last {min_required} common dates: {dates}")

    # --- App metrics ---
    logger.info("Loading app metrics...")
    app_df = app_analyzer.get_all_packages_with_downloads(dates)
    app_stats = {row["package_id"]: row for _, row in app_df.iterrows()}
    logger.info(f"Loaded app metrics for {len(app_stats)} packages.")

    # --- Search metrics ---
    logger.info("Loading search metrics...")
    search_df = search_analyzer.get_query_analysis(dates)
    query_hits = {row["query"]: row["total_hits"] for _, row in search_df.iterrows()}
    logger.info(f"Loaded search metrics for {len(query_hits)} queries.")

    # --- Merge and output ---
    all_package_ids = set(app_stats.keys())
    logger.info(f"Writing output for {len(all_package_ids)} packages to {OUTPUT_DIR}")
    for package_id in all_package_ids:
        package_id = package_id.strip("/")  # Sanitize package ID
        app = app_stats.get(package_id, {})
        search_count = query_hits.get(package_id, 0)
        out = {
            "package_id": package_id,
            "total_downloads": app.get("total_downloads", 0),
            "api_hits": app.get("api_hits", 0),
            "versions": app.get("total_versions", 0),
            "search_count": search_count,
        }
        out_path = OUTPUT_DIR / f"{package_id}.json"
        out_path = out_path.resolve()
        with safe_open(out_path, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
        logger.debug(f"Wrote data for package {package_id} to {out_path}")


if __name__ == "__main__":
    main()
