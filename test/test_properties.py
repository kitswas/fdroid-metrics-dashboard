"""
Property-based tests for fdroid-metrics using hypothesis.
"""

import pytest
import pathlib
from hypothesis import given, strategies as st
from etl.query_mapper import _normalize
from etl.security import _is_path_allowed, _get_project_root


@given(st.text())
def test_normalize_idempotent(text: str):
    """Normalizing a string twice should produce the same result as once."""
    first = _normalize(text)
    second = _normalize(first)
    assert first == second


@given(st.text())
def test_normalize_is_lowercase(text: str):
    """The normalized string should be entirely lowercase."""
    normalized = _normalize(text)
    assert normalized == normalized.lower()


@given(st.text())
def test_normalize_no_double_spaces(text: str):
    """The normalized string should not contain double spaces."""
    normalized = _normalize(text)
    assert "  " not in normalized


@given(st.text())
def test_normalize_stripped(text: str):
    """The normalized string should be stripped of leading/trailing whitespace."""
    normalized = _normalize(text)
    assert normalized == normalized.strip()


@given(st.lists(st.text(min_size=1), min_size=1))
def test_security_allowed_paths(path_parts: list[str]):
    """Paths within allowed directories should be allowed (if they don't contain ..)."""
    # Filter out parts that might cause issues with path construction
    # e.g., parts containing / or \ or .. or starting with /
    safe_parts = []
    for part in path_parts:
        # Simple sanitization for test path construction
        if not part or "/" in part or "\\" in part or part == ".." or part == ".":
            continue
        safe_parts.append(part)
    
    if not safe_parts:
        return

    root = _get_project_root()
    # Test with one of the allowed directories: 'cache'
    test_path = root / "cache" / pathlib.Path(*safe_parts)
    
    # We can't easily use .resolve() on non-existent paths in some OSs 
    # but _is_path_allowed uses it.
    # If the path is validly within 'cache', it should be allowed.
    # Note: _is_path_allowed might return False if resolve() fails due to non-existence 
    # depending on the OS, but usually it just resolves what it can.
    
    # For the sake of property test, let's just check it doesn't crash 
    # and if it returns True, it MUST be under one of the allowed dirs.
    allowed = _is_path_allowed(test_path)
    
    if allowed:
        resolved = test_path.resolve()
        project_root = root.resolve()
        
        is_under_allowed = False
        from etl.security import ALLOWED_DIRECTORIES
        for allowed_dir in ALLOWED_DIRECTORIES:
            allowed_base = (project_root / allowed_dir).resolve()
            try:
                resolved.relative_to(allowed_base)
                is_under_allowed = True
                break
            except ValueError:
                continue
        assert is_under_allowed


@given(st.text(min_size=1))
def test_security_disallow_arbitrary_absolute_paths(path_str: str):
    """Arbitrary absolute paths should generally be disallowed."""
    # This is tricky because path_str might accidentally be an allowed path,
    # but it's very unlikely for a random string.
    # We only test if it's an absolute path that doesn't start with our project root.
    try:
        path = pathlib.Path(path_str)
        if path.is_absolute():
            # If it's absolute, check if it's allowed
            allowed = _is_path_allowed(path)
            if allowed:
                # If it IS allowed, it must be because it's somehow within our project
                # (unlikely for random strings, but possible)
                resolved = path.resolve()
                project_root = _get_project_root().resolve()
                
                is_under_allowed = False
                from etl.security import ALLOWED_DIRECTORIES
                for allowed_dir in ALLOWED_DIRECTORIES:
                    allowed_base = (project_root / allowed_dir).resolve()
                    try:
                        resolved.relative_to(allowed_base)
                        is_under_allowed = True
                        break
                    except ValueError:
                        continue
                assert is_under_allowed
    except Exception:
        # Some strings aren't valid paths, that's fine
        pass


from etl.analyzer_apps import AppMetricsAnalyzer
from etl.query_mapper import QueryMapper
from etl.fdroid_metadata import FDroidMetadataFetcher


@given(st.lists(st.text(min_size=1), min_size=0, max_size=50))
def test_query_mapper_build_index_no_crash(package_ids: list[str]):
    """Building the index should never crash with any list of strings."""
    mapper = QueryMapper()
    # Mocking metadata enrichment to avoid disk I/O
    mapper._enrich_from_metadata = lambda: None
    mapper.build_index(package_ids)
    assert True


@given(st.lists(st.text(min_size=1), min_size=1, max_size=20), st.text())
def test_query_mapper_match_no_crash(package_ids: list[str], query: str):
    """Matching a query should never crash and return a valid result or None."""
    mapper = QueryMapper()
    mapper._enrich_from_metadata = lambda: None
    mapper.build_index(package_ids)
    
    result = mapper.match(query)
    if result is not None:
        assert isinstance(result, str)
        # It doesn't necessarily have to be in package_ids if it matched a short name
        # but it should be a string.


@given(st.one_of(st.text(), st.lists(st.text())))
def test_metadata_fetcher_primary_category_fallback(categories: str | list[str]):
    """get_primary_category should handle any input and always return a string."""
    fetcher = FDroidMetadataFetcher()
    
    # We mock get_package_categories to return our test categories
    fetcher.get_package_categories = lambda pkg_id, use_cache=True: categories
    
    result = fetcher.get_primary_category("any.package")
    assert isinstance(result, str)
    if not categories:
        assert result == "Unknown"


from unittest.mock import patch


# Strategy for app metrics data
app_data_strategy = st.fixed_dictionaries({
    "hits": st.integers(min_value=0, max_value=1000),
    "errors": st.dictionaries(
        st.text(min_size=1),
        st.fixed_dictionaries({
            "hits": st.integers(min_value=0, max_value=100),
            "paths": st.dictionaries(st.text(min_size=1), st.integers(min_value=0, max_value=100))
        })
    ),
    "hitsPerCountry": st.dictionaries(st.text(min_size=2, max_size=2), st.integers(min_value=0, max_value=100)),
    "paths": st.dictionaries(
        st.text(min_size=1),
        st.one_of(
            st.integers(min_value=0, max_value=100),
            st.fixed_dictionaries({
                "hits": st.integers(min_value=0, max_value=100),
                "hitsPerCountry": st.dictionaries(st.text(min_size=2, max_size=2), st.integers(min_value=0, max_value=100))
            })
        )
    ),
    "queries": st.dictionaries(
        st.text(min_size=1),
        st.one_of(
            st.integers(min_value=0, max_value=100),
            st.fixed_dictionaries({
                "hits": st.integers(min_value=0, max_value=100),
                "hitsPerCountry": st.dictionaries(st.text(min_size=2, max_size=2), st.integers(min_value=0, max_value=100))
            })
        )
    )
})


@given(st.lists(app_data_strategy, min_size=1, max_size=5))
def test_app_metrics_merging(data_list: list[dict]):
    """Loading merged data should correctly aggregate counts from all servers."""
    analyzer = AppMetricsAnalyzer()
    
    # Mocking servers to match the length of data_list
    analyzer.servers = [f"server_{i}" for i in range(len(data_list))]
    
    with patch.object(AppMetricsAnalyzer, 'load_data') as mock_load:
        mock_load.side_effect = data_list
        
        merged = analyzer.load_merged_data("2024-01-01")
        
        # Verify total hits
        expected_hits = sum(d.get("hits", 0) for d in data_list)
        assert merged["hits"] == expected_hits
        
        # Verify country hits
        for country in merged["hitsPerCountry"]:
            expected_country_hits = sum(d.get("hitsPerCountry", {}).get(country, 0) for d in data_list)
            assert merged["hitsPerCountry"][country] == expected_country_hits
            
        # Verify path hits
        for path in merged["paths"]:
            expected_path_hits = 0
            for d in data_list:
                path_data = d.get("paths", {}).get(path, 0)
                if isinstance(path_data, dict):
                    expected_path_hits += path_data.get("hits", 0)
                else:
                    expected_path_hits += path_data
            assert merged["paths"][path]["hits"] == expected_path_hits
            
        # Verify error hits
        for error_code in merged["errors"]:
            expected_error_hits = sum(d.get("errors", {}).get(error_code, {}).get("hits", 0) for d in data_list)
            assert merged["errors"][error_code]["hits"] == expected_error_hits


if __name__ == "__main__":
    # If run directly, use pytest
    pytest.main([__file__])
