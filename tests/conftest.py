"""Pytest configuration for test imports."""

import sys
from pathlib import Path


tests_dir = Path(__file__).resolve().parent
tests_dir_str = str(tests_dir)
if tests_dir_str not in sys.path:
    sys.path.insert(0, tests_dir_str)
