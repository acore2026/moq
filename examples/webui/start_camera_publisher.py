#!/usr/bin/env python3
"""Start the WebUI-local camera publisher for video/camera-h264-live."""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from examples.webui.camera_publisher import main


if __name__ == "__main__":
    main()
