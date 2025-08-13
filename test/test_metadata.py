#!/usr/bin/env python3
"""
Test script for F-Droid metadata fetcher
"""

import logging

from etl.fdroid_metadata import FDroidMetadataFetcher


def test_metadata_fetcher():
    """Test the metadata fetcher with some known packages."""
    fetcher = FDroidMetadataFetcher(cache_dir="./cache/metadata")

    # Test packages with known metadata
    test_packages = [
        "io.github.kitswas.virtualgamepadmobile",  # My app :)
        "org.fdroid.fdroid",  # F-Droid client itself
        "org.schabi.newpipe",  # Popular NewPipe app
        "com.nonexistent.package",  # Non-existent package for testing fallback
    ]

    logging.info("Testing F-Droid metadata fetcher...")
    logging.info("=" * 50)

    for package_id in test_packages:
        logging.info(f"Testing package: {package_id}")

        # Get metadata
        metadata = fetcher.get_package_metadata(package_id)
        if metadata:
            logging.info("  ✓ Metadata found")
            logging.info(f"  AutoName: {metadata.get('AutoName', 'N/A')}")
            logging.info(f"  Categories: {metadata.get('Categories', 'N/A')}")
            logging.info(f"  License: {metadata.get('License', 'N/A')}")
        else:
            logging.warning("  ✗ No metadata found")

        # Get categories
        categories = fetcher.get_package_categories(package_id)
        logging.info(f"  Categories: {categories}")

        # Get primary category (with fallback)
        primary_category = fetcher.get_primary_category(package_id)
        logging.info(f"  Primary category: {primary_category}")

    # Show cache statistics
    logging.info("=" * 50)
    logging.info("Cache Statistics:")
    stats = fetcher.get_cache_stats()
    for key, value in stats.items():
        logging.info(f"  {key}: {value}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    test_metadata_fetcher()
