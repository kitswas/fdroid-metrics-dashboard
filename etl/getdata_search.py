"""
Read the index and download a month's worth of data from the FDroid API
"""

import argparse
import json
import logging
import pathlib
from datetime import datetime

import requests as re

BASE_URL = "https://fdroid.gitlab.io/metrics/search.f-droid.org"
INDEX_URL = f"{BASE_URL}/index.json"
RAW_DATA_DIR = pathlib.Path(__file__).parent / "raw"
SUB_DATA_DIR = RAW_DATA_DIR / "search"

logger = logging.getLogger(__name__)


def fetch_index() -> list[str]:
    """Fetch and return the index of available data files."""
    logger.info("Fetching index...")
    response = re.get(INDEX_URL)
    response.raise_for_status()
    index = response.json()
    logger.info(f"Found {len(index)} available files")
    return index


def filter_files_for_month(index: list[str], year: int, month: int) -> list[str]:
    """Filter files for a specific month and year."""
    month_files = []
    for filename in index:
        if filename == "last_submitted_to_cimp.json":
            continue

        try:
            # Parse date from filename (format: YYYY-MM-DD.json)
            date_str = filename.replace(".json", "")
            file_date = datetime.strptime(date_str, "%Y-%m-%d")

            if file_date.year == year and file_date.month == month:
                month_files.append(filename)
        except ValueError:
            # Skip files that don't match the expected date format
            continue

    logger.info(f"Found {len(month_files)} files for {year}-{month:02d}")
    return sorted(month_files)


def download_file(filename: str) -> bool:
    """Download a single data file."""
    url = f"{BASE_URL}/{filename}"
    filepath = SUB_DATA_DIR / filename

    # Check if file already exists
    if filepath.exists():
        logger.debug(f"{filename} already exists, skipping")
        return True

    try:
        logger.info(f"Downloading {filename}...")
        response = re.get(url)
        response.raise_for_status()

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(response.json(), f, indent=2)

        logger.info(f"✓ {filename} downloaded successfully")
        return True
    except Exception as e:
        logger.error(f"✗ Failed to download {filename}: {e}")
        return False


def download_month_data(year: int, month: int) -> None:
    """Download all data files for a specific month."""
    # Create data directory if it doesn't exist
    SUB_DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Fetch index
    index = fetch_index()

    # Filter files for the specified month
    month_files = filter_files_for_month(index, year, month)

    if not month_files:
        logger.info(f"No files found for {year}-{month:02d}")
        return

    # Download files
    logger.info(f"Downloading {len(month_files)} files for {year}-{month:02d}...")
    successful = 0
    failed = 0

    for filename in month_files:
        if download_file(filename):
            successful += 1
        else:
            failed += 1

    logger.info("Download complete:")
    logger.info(f"✓ {successful} files downloaded successfully")
    if failed > 0:
        logger.warning(f"✗ {failed} files failed to download")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download FDroid search metrics data for a specific month",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # Default to current month
    now = datetime.now()

    parser.add_argument(
        "year", type=int, nargs="?", default=now.year, help="Year to download data for"
    )

    parser.add_argument(
        "month",
        type=int,
        nargs="?",
        default=now.month,
        choices=range(1, 13),
        help="Month to download data for (1-12)",
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

    logger.info(f"Downloading data for {args.year}-{args.month:02d}")
    download_month_data(args.year, args.month)
