#!/usr/bin/env python3
"""WebUI-local Windows camera publisher.

This wrapper keeps WebUI relay defaults isolated from the standalone examples.
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from examples import camera_publisher_example as publisher_impl

RELAY_HOST = os.environ.get("MOQ_RELAY_HOST", "127.0.0.1")
RELAY_PORT = int(os.environ.get("MOQ_RELAY_PORT", "28446"))


def main():
    publisher_impl.RELAY_HOST = RELAY_HOST
    publisher_impl.RELAY_PORT = RELAY_PORT
    asyncio.run(publisher_impl.main())


if __name__ == "__main__":
    main()
