"""
Script to extract per-package monthly metrics from F-Droid app and search data.
Output: processed/monthly/{package_id}.json
"""

import json
import logging
import os
from datetime import datetime

from etl.analyzer_apps import AppMetricsAnalyzer
from etl.analyzer_search import SearchMetricsAnalyzer
from etl.data_fetcher import DataFetcher

# --- CONFIG ---
OUTPUT_DIR = "processed/monthly"


def get_last_n_months_dates(dates, n_months=2):
    # dates: list of YYYY-MM-DD strings, sorted ascending
    months = {}
    for date_str in reversed(dates):
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        ym = dt.strftime("%Y-%m")
        if ym not in months:
            months[ym] = []
        months[ym].append(date_str)
        if len(months) >= n_months:
            # Once we have n_months, stop collecting
            break
    # Flatten and sort
    result = []
    for month_dates in months.values():
        result.extend(month_dates)
    return sorted(result)


def main():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    logger = logging.getLogger("extract_monthly_package_json")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    logger.info("Getting available remote dates for apps and search...")
    fetcher = DataFetcher()
    app_remote_dates = fetcher.get_available_remote_dates("apps")
    search_remote_dates = fetcher.get_available_remote_dates("search")
    logger.info(
        f"Found {len(app_remote_dates)} app dates, {len(search_remote_dates)} search dates."
    )

    # Get last 2 months of dates for each
    app_dates_to_fetch = get_last_n_months_dates(app_remote_dates, 2)
    search_dates_to_fetch = get_last_n_months_dates(search_remote_dates, 2)

    def log_progress(progress):
        logger.info(f"Progress: {progress * 100:.1f}%")

    def log_status(msg):
        logger.info(msg)

    logger.info(f"Fetching app data for dates: {app_dates_to_fetch}")
    fetcher.fetch_date_range(
        "apps",
        app_dates_to_fetch[0],
        app_dates_to_fetch[-1],
        progress_callback=log_progress,
        status_callback=log_status,
    )
    logger.info(f"Fetching search data for dates: {search_dates_to_fetch}")
    fetcher.fetch_date_range(
        "search",
        search_dates_to_fetch[0],
        search_dates_to_fetch[-1],
        progress_callback=log_progress,
        status_callback=log_status,
    )

    # Find common dates locally
    app_analyzer = AppMetricsAnalyzer()
    search_analyzer = SearchMetricsAnalyzer()
    app_dates = app_analyzer.get_available_dates()
    search_dates = search_analyzer.get_available_dates()
    common_dates = sorted(set(app_dates) & set(search_dates))
    if len(common_dates) < 4:
        logger.error(
            f"Not enough common dates found (found {len(common_dates)}). Aborting."
        )
        return
    dates = common_dates[-4:]
    logger.info(f"Processing last 4 common dates: {dates}")

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
        app = app_stats.get(package_id, {})
        search_count = query_hits.get(package_id, 0)
        out = {
            "package_id": package_id,
            "total_downloads": app.get("total_downloads", 0),
            "api_hits": app.get("api_hits", 0),
            "versions": app.get("total_versions", 0),
            "search_count": search_count,
        }
        out_path = os.path.join(OUTPUT_DIR, f"{package_id}.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
        logger.debug(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
