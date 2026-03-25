"""Shared test helper for adding the repository root to sys.path."""

import sys
from pathlib import Path


def ensure_repo_root() -> Path:
    """Add the repository root to sys.path and return it."""
    repo_root = Path(__file__).resolve().parents[1]
    root_str = str(repo_root)
    if root_str not in sys.path:
        sys.path.insert(0, root_str)
    return repo_root
