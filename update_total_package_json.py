"""
Script to update per-package total (all-time) metrics from F-Droid app and search data.
Output: ./processed/total/{package_id}.json
"""

import json
import logging
import os
import pathlib
from bisect import bisect_right

from etl.analyzer_apps import AppMetricsAnalyzer
from etl.analyzer_search import SearchMetricsAnalyzer
from etl.data_fetcher import DataFetcher
from etl.security import safe_open

# --- CONFIG ---
OUTPUT_DIR = pathlib.Path(__file__).parent / "processed" / "total"
SYNCED_TILL_PATH = OUTPUT_DIR / ".synced-till"
MAX_SNAPSHOTS_IN_ONE_FETCH = 100

logger = logging.getLogger(__name__)


def main() -> None:
    """
    Main function to update per-package total (all-time) metrics.

    This function:
    1. Fetches available dates from remote servers
    2. Reads the last sync date
    3. Downloads data since the last sync date
    4. Processes app and search metrics
    5. Generates/updates per-package JSON files with total (all-time) statistics
    6. Updates the last sync date
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

    logger.info("Reading last sync date...")
    last_synced = "1970-01-01"
    if SYNCED_TILL_PATH.exists():
        with safe_open(SYNCED_TILL_PATH, "r", encoding="utf-8") as f:
            last_synced = f.read().strip()
    logger.info(f"Last synced at {last_synced}")

    common_remote_dates = sorted(set(app_remote_dates) & set(search_remote_dates))
    if not common_remote_dates:
        logger.error("No common remote dates found. Aborting.")
        return
    dates_to_fetch = common_remote_dates[
        bisect_right(common_remote_dates, last_synced) :
    ]
    if not dates_to_fetch:
        logger.info("No dates to fetch. Aborting.")
        return

    def log_progress(progress: float) -> None:
        logger.info(f"Progress: {progress * 100:.1f}%")

    def log_status(msg: str) -> None:
        logger.info(msg)

    for i in range(0, len(dates_to_fetch), MAX_SNAPSHOTS_IN_ONE_FETCH):
        dates = dates_to_fetch[i : i + MAX_SNAPSHOTS_IN_ONE_FETCH]
        logger.info(f"Fetching app data for dates: {dates}")
        fetcher.fetch_date_range(
            "apps",
            dates[0],
            dates[-1],
            progress_callback=log_progress,
            status_callback=log_status,
        )
        logger.info(f"Fetching search data for dates: {dates}")
        fetcher.fetch_date_range(
            "search",
            dates[0],
            dates[-1],
            progress_callback=log_progress,
            status_callback=log_status,
        )

    # Find common dates locally
    app_analyzer = AppMetricsAnalyzer()
    search_analyzer = SearchMetricsAnalyzer()
    app_dates = app_analyzer.get_available_dates()
    search_dates = search_analyzer.get_available_dates()
    common_dates = sorted(set(app_dates) & set(search_dates))
    if not common_dates:
        logger.error("No common dates found. Aborting.")
        return
    dates = common_dates[bisect_right(common_dates, last_synced) :]
    if not dates:
        logger.info("No dates to process. Aborting.")
        return
    logger.info(f"Processing new {len(dates)} common dates: {dates}")

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

    # --- Merge and update output ---
    all_package_ids = set(app_stats.keys())
    logger.info(f"Updating output for {len(all_package_ids)} packages in {OUTPUT_DIR}")
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

        if out_path.exists():
            with safe_open(out_path, "r", encoding="utf-8") as f:
                prev = json.load(f)
                out["total_downloads"] += prev.get("total_downloads", 0)
                out["api_hits"] += prev.get("api_hits", 0)
                out["versions"] += prev.get("versions", 0)
                out["search_count"] += prev.get("search_count", 0)

        with safe_open(out_path, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
        logger.debug(f"Updated data for package {package_id} in {out_path}")

    with safe_open(SYNCED_TILL_PATH, "w", encoding="utf-8") as f:
        f.write(dates[-1])
    logger.info("Updated sync date")


if __name__ == "__main__":
    main()
