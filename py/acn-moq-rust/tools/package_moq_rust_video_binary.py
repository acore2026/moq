#!/usr/bin/env python3
"""
Copy a built moq-cli binary into the moq_rust_video package-data directory.

This is intended for CI before running `python -m build --wheel`.
"""

from __future__ import annotations

import argparse
import os
import shutil
import stat
import sys
from pathlib import Path

MOQ_RUST_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(MOQ_RUST_ROOT))

from moq_rust_video.binaries import bundled_moq_cli_path


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'source',
        help='Path to a built moq-cli or moq-cli.exe binary.',
    )
    parser.add_argument(
        '--system',
        help='Override platform.system() for cross-platform CI packaging.',
    )
    parser.add_argument(
        '--machine',
        help='Override platform.machine() for cross-platform CI packaging.',
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    source = Path(args.source)
    if not source.exists():
        raise SystemExit(f'moq-cli binary does not exist: {source}')

    destination = bundled_moq_cli_path(args.system, args.machine)
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)

    mode = destination.stat().st_mode
    destination.chmod(mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    print(os.fspath(destination))


if __name__ == '__main__':
    main()
