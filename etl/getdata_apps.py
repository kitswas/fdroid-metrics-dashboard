"""
Download app metrics data from F-Droid HTTP servers based on date range
"""

import argparse
import json
import logging
import pathlib
from datetime import datetime, timedelta

import requests as re

BASE_URL = "https://fdroid.gitlab.io/metrics"
SERVERS = [
    "http01.fdroid.net",
    "http02.fdroid.net",
    "http03.fdroid.net",
    "originserver.f-droid.org",
]
RAW_DATA_DIR = pathlib.Path(__file__).parent / "raw"
SUB_DATA_DIR = RAW_DATA_DIR / "apps"

logger = logging.getLogger(__name__)


def fetch_index(server: str) -> list[str]:
    """Fetch and return the index of available data files for a server."""
    logger.info(f"Fetching index for {server}...")
    index_url = f"{BASE_URL}/{server}/index.json"
    response = re.get(index_url)
    response.raise_for_status()
    index = response.json()
    logger.info(f"Found {len(index)} available files for {server}")
    return index


def filter_files_for_date_range(
    index: list[str], start_date: datetime, end_date: datetime
) -> list[str]:
    """Filter files for a specific date range.

    Note: Each date file represents cumulative data since the previous date (usually weekly).
    """
    date_files = []
    for filename in index:
        try:
            # Parse date from filename (format: YYYY-MM-DD.json)
            date_str = filename.replace(".json", "")
            file_date = datetime.strptime(date_str, "%Y-%m-%d")

            if start_date <= file_date <= end_date:
                date_files.append(filename)
        except ValueError:
            # Skip files that don't match the expected date format
            continue

    return sorted(date_files)


def download_file(server: str, filename: str) -> bool:
    """Download a single data file for a server, overwriting if it exists."""
    url = f"{BASE_URL}/{server}/{filename}"
    server_dir = SUB_DATA_DIR / server
    server_dir.mkdir(parents=True, exist_ok=True)
    filepath = server_dir / filename

    try:
        logger.info(f"Downloading {server}/{filename}...")
        response = re.get(url)
        response.raise_for_status()

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(response.json(), f, indent=2)

        logger.info(f"✓ {server}/{filename} downloaded successfully")
        return True
    except Exception as e:
        logger.error(f"✗ Failed to download {server}/{filename}: {e}")
        return False


def download_date_range_data(start_date: datetime, end_date: datetime) -> None:
    """Download all app metrics data for a specific date range from all servers.

    Args:
        start_date: Start date of the range (inclusive)
        end_date: End date of the range (inclusive)

    Note: Each date file represents cumulative data since the previous date.
    """
    # Create data directory if it doesn't exist
    SUB_DATA_DIR.mkdir(parents=True, exist_ok=True)

    total_successful = 0
    total_failed = 0

    for server in SERVERS:
        logger.info("=" * 50)
        logger.info(f"Processing {server}")
        logger.info("=" * 50)

        try:
            # Fetch index for this server
            index = fetch_index(server)

            # Filter files for the specified date range
            date_files = filter_files_for_date_range(index, start_date, end_date)

            if not date_files:
                logger.info(
                    f"No files found for {server} in date range {start_date.date()} to {end_date.date()}"
                )
                continue

            # Download files for this server
            logger.info(f"Downloading {len(date_files)} files for {server}...")
            successful = 0
            failed = 0

            for filename in date_files:
                if download_file(server, filename):
                    successful += 1
                else:
                    failed += 1

            logger.info(f"{server} download complete:")
            logger.info(f"✓ {successful} files downloaded successfully")
            if failed > 0:
                logger.warning(f"✗ {failed} files failed to download")

            total_successful += successful
            total_failed += failed

        except Exception as e:
            logger.error(f"Failed to process {server}: {e}")
            continue

    logger.info("=" * 50)
    logger.info("OVERALL SUMMARY")
    logger.info("=" * 50)
    logger.info(
        f"✓ {total_successful} files downloaded successfully across all servers"
    )
    if total_failed > 0:
        logger.warning(f"✗ {total_failed} files failed to download")


def download_month_data(year: int, month: int) -> None:
    """Download all app metrics data for a specific month from all servers (legacy function)."""
    # Convert year/month to date range
    start_date = datetime(year, month, 1)
    if month == 12:
        end_date = datetime(year + 1, 1, 1)
    else:
        end_date = datetime(year, month + 1, 1)

    # Adjust end_date to last day of month
    end_date = end_date.replace(day=1) - timedelta(days=1)
    end_date = end_date.replace(hour=23, minute=59, second=59)

    download_date_range_data(start_date, end_date)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download FDroid app metrics data from HTTP servers for a specific date range",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        epilog="Examples:\n"
        "  %(prog)s --start 2024-09-01 --end 2024-09-30\n"
        "  %(prog)s 2024 9  # Download September 2024 (legacy)\n"
        "  %(prog)s  # Download current month",
    )

    # Default to current month
    now = datetime.now()

    # Legacy arguments for backward compatibility
    parser.add_argument(
        "year",
        type=int,
        nargs="?",
        help="Year to download data for (legacy, use --start/--end instead)",
    )

    parser.add_argument(
        "month",
        type=int,
        nargs="?",
        choices=range(1, 13),
        help="Month to download data for 1-12 (legacy, use --start/--end instead)",
    )

    # New date range arguments
    parser.add_argument(
        "--start",
        "--start-date",
        type=str,
        help="Start date in YYYY-MM-DD format (inclusive)",
    )

    parser.add_argument(
        "--end",
        "--end-date",
        type=str,
        help="End date in YYYY-MM-DD format (inclusive)",
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose (DEBUG) logging",
    )

    args = parser.parse_args()

    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level, format="%(levelname)s: %(message)s")

    # Determine which mode to use
    if args.start or args.end:
        # New date range mode
        if not args.start or not args.end:
            parser.error("Both --start and --end must be provided together")

        try:
            start_date = datetime.strptime(args.start, "%Y-%m-%d")
            end_date = datetime.strptime(args.end, "%Y-%m-%d")
        except ValueError as e:
            parser.error(f"Invalid date format. Use YYYY-MM-DD: {e}")

        if start_date > end_date:
            parser.error("Start date must be before or equal to end date")

        logger.info(
            f"Downloading app metrics data for date range {start_date.date()} to {end_date.date()}"
        )
        download_date_range_data(start_date, end_date)
    else:
        # Legacy month mode
        year = args.year if args.year else now.year
        month = args.month if args.month else now.month

        logger.info(f"Downloading app metrics data for {year}-{month:02d}")
        download_month_data(year, month)
