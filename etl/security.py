"""
Security utilities for safe file operations.
"""

import logging
import pathlib
from typing import IO, Any

logger = logging.getLogger(__name__)

# Define allowed base directories for file operations
# These are relative to the project root
ALLOWED_DIRECTORIES = [
    "etl/raw",
    "processed",
    "cache",
]


def _get_project_root() -> pathlib.Path:
    """Get the project root directory."""
    # The security module is in etl/, so go up one level to get project root
    return pathlib.Path(__file__).parent.parent


def _is_path_allowed(filepath: pathlib.Path) -> bool:
    """
    Check if a file path is within allowed directories.

    Args:
        filepath: Path to validate

    Returns:
        True if the path is allowed, False otherwise
    """
    try:
        # Resolve the filepath to get absolute path and resolve symlinks
        resolved_path = filepath.resolve()
        project_root = _get_project_root().resolve()

        # Check if the resolved path starts with any of the allowed directories
        for allowed_dir in ALLOWED_DIRECTORIES:
            allowed_path = (project_root / allowed_dir).resolve()
            try:
                # Check if resolved_path is relative to allowed_path
                resolved_path.relative_to(allowed_path)
                return True
            except ValueError:
                # Path is not relative to this allowed directory, try next one
                continue

        return False
    except (OSError, RuntimeError) as e:
        logger.warning(f"Error resolving path {filepath}: {e}")
        return False


def safe_open(
    filepath: pathlib.Path | str,
    mode: str = "r",
    encoding: str | None = None,
    **kwargs: Any,  # noqa: ANN401
) -> IO[Any]:
    """
    Safely open a file after validating the path is within allowed directories.

    Args:
        filepath: Path to the file to open
        mode: File open mode (default: "r")
        encoding: File encoding (default: None, uses system default)
        **kwargs: Additional arguments to pass to open()

    Returns:
        File object

    Raises:
        PermissionError: If the path is not within allowed directories

    Example:
        with safe_open("etl/raw/apps/data.json", "r", encoding="utf-8") as f:
            data = json.load(f)
    """
    # Convert to Path object if string
    if isinstance(filepath, str):
        filepath = pathlib.Path(filepath)

    # Validate the path
    if not _is_path_allowed(filepath):
        error_msg = (
            f"Access denied: Path '{filepath}' is outside allowed directories. "
            f"Allowed directories: {', '.join(ALLOWED_DIRECTORIES)}"
        )
        logger.warning(error_msg)
        raise PermissionError(error_msg)

    # Open the file
    if encoding is not None:
        return open(filepath, mode, encoding=encoding, **kwargs)
    else:
        return open(filepath, mode, **kwargs)
