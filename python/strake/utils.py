"""Utility helpers for the Strake Python codebase.

This module provides directory resolution helpers and SQL/LanceDB identifier
and value sanitization functions to prevent SQL injection vulnerabilities.
"""

import sys
import os
import re
from pathlib import Path
from typing import Optional


def sanitize_identifier(name: str) -> str:
    """Validate against a simple identifier pattern before interpolating.

    Allows alphanumeric characters, underscores, and dots (for FQN).

    Args:
        name: The identifier string to check.

    Returns:
        The validated name string.

    Raises:
        ValueError: If the identifier contains invalid characters.
    """
    if name and all(c.isalnum() or c in ("_", ".") for c in name):
        return name
    raise ValueError(f"Invalid identifier: {name}")


def sanitize_lance_value(value: str) -> str:
    """Validate a registry LanceDB value to prevent where-clause injection.

    Allows alphanumeric characters, underscores, dots, and colons (for cardinality '1:N').

    Args:
        value: The value string to check.

    Returns:
        The validated value string.

    Raises:
        ValueError: If the value contains invalid characters.
    """
    if value and all(c.isalnum() or c in ("_", ".", ":") for c in value):
        return value
    raise ValueError(f"Invalid value: {value}")


def get_script_dir() -> Optional[Path]:
    """
    Return the absolute directory of the script being executed, or None if unknown.

    Heuristics:
    1. Check sys.argv[0].
    2. Ensure it's not a common runner binary (pytest, etc).
    3. Ensure the parent directory is writable (best effort check for project context).
    """
    if not sys.argv or not sys.argv[0]:
        return None

    try:
        # Resolve to handle relative paths and symlinks
        script_path = Path(sys.argv[0]).resolve()

        # If we're running via a common test runner or package manager,
        # sys.argv[0] might not be the user's script.
        # We look for common markers.
        basename = script_path.name.lower()
        if any(
            marker in basename
            for marker in ("pytest", "pytest-3", "pip", "poetry", "uv")
        ):
            return None

        if script_path.is_file():
            parent = script_path.parent
            # Check for writability to ensure we're not in a read-only volume like /usr/bin
            if os.access(parent, os.W_OK):
                return parent
    except (OSError, ValueError):
        pass

    return None


def get_strake_dir(subdir: Optional[str] = None) -> Path:
    """
    Unified helper to return the resolved .strake directory.
    Checks STRAKE_DIR env var first, then defaults to script-relative if possible, falling back to ~/.strake.
    """
    env_dir = os.environ.get("STRAKE_DIR")
    if env_dir:
        base = Path(env_dir).resolve()
    else:
        script_dir = get_script_dir()
        if script_dir:
            base = (script_dir / ".strake").resolve()
        else:
            base = Path(os.path.expanduser("~/.strake")).resolve()

    if subdir:
        path = (base / subdir).resolve()
    else:
        path = base

    return path
