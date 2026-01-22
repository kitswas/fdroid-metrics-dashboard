"""
Tests for security module to verify path validation works correctly.
"""

import pathlib
import tempfile

from etl.security import _get_project_root, _is_path_allowed, safe_open


class TestPathValidation:
    """Test path validation functionality."""

    def test_project_root_detection(self):
        """Test that project root is correctly identified."""
        root = _get_project_root()
        assert root.exists()
        assert (root / "etl").exists()
        assert (root / "etl" / "security.py").exists()

    def test_allowed_directory_raw_apps(self):
        """Test that paths in etl/raw/apps are allowed."""
        root = _get_project_root()
        test_path = root / "etl" / "raw" / "apps" / "test.json"
        assert _is_path_allowed(test_path)

    def test_allowed_directory_raw_search(self):
        """Test that paths in etl/raw/search are allowed."""
        root = _get_project_root()
        test_path = root / "etl" / "raw" / "search" / "test.json"
        assert _is_path_allowed(test_path)

    def test_allowed_directory_processed(self):
        """Test that paths in processed/ are allowed."""
        root = _get_project_root()
        test_path = root / "processed" / "monthly" / "test.json"
        assert _is_path_allowed(test_path)

    def test_allowed_directory_cache(self):
        """Test that paths in cache/ are allowed."""
        root = _get_project_root()
        test_path = root / "cache" / "metadata" / "test.yml"
        assert _is_path_allowed(test_path)

    def test_disallowed_directory_root(self):
        """Test that paths in project root are not allowed."""
        root = _get_project_root()
        test_path = root / "test.json"
        assert not _is_path_allowed(test_path)

    def test_disallowed_directory_parent(self):
        """Test that paths outside project are not allowed."""
        root = _get_project_root()
        test_path = root.parent / "test.json"
        assert not _is_path_allowed(test_path)

    def test_disallowed_directory_system_temp(self):
        """Test that system temp directory is not allowed."""
        test_path = pathlib.Path(tempfile.gettempdir()) / "test.json"
        assert not _is_path_allowed(test_path)

    def test_safe_open_allowed_path(self):
        """Test that safe_open works with allowed paths."""
        root = _get_project_root()
        test_dir = root / "etl" / "raw" / "apps"
        test_dir.mkdir(parents=True, exist_ok=True)
        test_file = test_dir / "test_security_temp.json"

        try:
            # Should work without raising
            with safe_open(test_file, "w", encoding="utf-8") as f:
                f.write('{"test": "data"}')

            assert test_file.exists()

            # Read it back
            with safe_open(test_file, "r", encoding="utf-8") as f:
                content = f.read()
                assert "test" in content
        finally:
            # Cleanup
            if test_file.exists():
                test_file.unlink()

    def test_safe_open_disallowed_path_raises_permission_error(self):
        """Test that safe_open raises PermissionError for disallowed paths."""
        root = _get_project_root()
        test_file = root / "disallowed_test.json"

        try:
            with safe_open(test_file, "w", encoding="utf-8") as f:
                f.write('{"test": "data"}')
            raise AssertionError("Expected PermissionError was not raised")
        except PermissionError as e:
            assert "Access denied" in str(e)
            assert "outside allowed directories" in str(e)

    def test_safe_open_with_string_path(self):
        """Test that safe_open works with string paths."""
        root = _get_project_root()
        test_dir = root / "cache"
        test_dir.mkdir(parents=True, exist_ok=True)
        test_file = test_dir / "test_string_path.txt"

        try:
            # Should work with string path
            with safe_open(str(test_file), "w", encoding="utf-8") as f:
                f.write("test content")

            assert test_file.exists()
        finally:
            # Cleanup
            if test_file.exists():
                test_file.unlink()

    def test_relative_path_attack_prevention(self):
        """Test that relative path attacks are prevented."""
        root = _get_project_root()
        test_file = root / ".." / ".." / "disallowed.json"

        try:
            with safe_open(test_file, "w", encoding="utf-8") as f:
                f.write('{"test": "data"}')
            raise AssertionError("Expected PermissionError was not raised")
        except PermissionError as e:
            assert "Access denied" in str(e)
            assert "outside allowed directories" in str(e)

    def test_system_dir_attack_prevention(self):
        """Test that attempts to access system directories are prevented."""
        test_file = pathlib.Path("/etc/passwd")

        try:
            with safe_open(test_file, "r", encoding="utf-8") as f:
                f.read()
            raise AssertionError("Expected PermissionError was not raised")
        except PermissionError as e:
            assert "Access denied" in str(e)
            assert "outside allowed directories" in str(e)


if __name__ == "__main__":
    # Run tests manually if pytest not available
    import sys

    test = TestPathValidation()

    try:
        test.test_project_root_detection()
        print("✓ Project root detection test passed")

        test.test_allowed_directory_raw_apps()
        print("✓ Raw apps directory test passed")

        test.test_allowed_directory_processed()
        print("✓ Processed directory test passed")

        test.test_allowed_directory_cache()
        print("✓ Cache directory test passed")

        test.test_disallowed_directory_root()
        print("✓ Disallowed root directory test passed")

        test.test_disallowed_directory_parent()
        print("✓ Disallowed parent directory test passed")

        test.test_safe_open_allowed_path()
        print("✓ Safe open with allowed path test passed")

        test.test_safe_open_with_string_path()
        print("✓ Safe open with string path test passed")

        test.test_safe_open_disallowed_path_raises_permission_error()
        print("✓ Safe open with disallowed path test passed")

        test.test_relative_path_attack_prevention()
        print("✓ Relative path attack prevention test passed")

        test.test_system_dir_attack_prevention()
        print("✓ System directory attack prevention test passed")

        print("\nAll tests passed!")

    except Exception as e:
        print(f"✗ Test failed: {e}", file=sys.stderr)
        sys.exit(1)
