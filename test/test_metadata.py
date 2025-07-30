#!/usr/bin/env python3
"""
Test script for F-Droid metadata fetcher
"""

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

    print("Testing F-Droid metadata fetcher...")
    print("=" * 50)

    for package_id in test_packages:
        print(f"\nTesting package: {package_id}")

        # Get metadata
        metadata = fetcher.get_package_metadata(package_id)
        if metadata:
            print("  ✓ Metadata found")
            print(f"  AutoName: {metadata.get('AutoName', 'N/A')}")
            print(f"  Categories: {metadata.get('Categories', 'N/A')}")
            print(f"  License: {metadata.get('License', 'N/A')}")
        else:
            print("  ✗ No metadata found")

        # Get categories
        categories = fetcher.get_package_categories(package_id)
        print(f"  Categories: {categories}")

        # Get primary category (with fallback)
        primary_category = fetcher.get_primary_category(package_id)
        print(f"  Primary category: {primary_category}")

    # Show cache statistics
    print("\n" + "=" * 50)
    print("Cache Statistics:")
    stats = fetcher.get_cache_stats()
    for key, value in stats.items():
        print(f"  {key}: {value}")


if __name__ == "__main__":
    test_metadata_fetcher()
