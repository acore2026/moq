"""Shared helpers for example scripts."""

import logging
import sys
from pathlib import Path


def ensure_repo_root() -> Path:
    """Add the repository root to ``sys.path`` when running examples directly."""
    repo_root = Path(__file__).resolve().parents[1]
    root_str = str(repo_root)
    if root_str not in sys.path:
        sys.path.insert(0, root_str)
    return repo_root


def setup_logging(level: int = logging.INFO) -> None:
    """Use a consistent log format across example scripts."""
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
