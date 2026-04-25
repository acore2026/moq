#!/usr/bin/env python3
"""WebUI-local synthetic video publisher.

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

from examples import video_publisher_example as publisher_impl
from moq import FullTrackName

RELAY_HOST = os.environ.get("MOQ_RELAY_HOST", "127.0.0.1")
RELAY_PORT = int(os.environ.get("MOQ_RELAY_PORT", "28446"))
TEST_SOURCE = os.environ.get("MOQ_TEST_SOURCE", "testsrc2")

TEST_SOURCES = {
    "testsrc2": {
        "track": "h264-live",
        "filter": lambda: f"testsrc2=size={publisher_impl.FRAME_WIDTH}x{publisher_impl.FRAME_HEIGHT}:rate={publisher_impl.FRAME_RATE}",
    },
    "testsrc": {
        "track": "testsrc-live",
        "filter": lambda: f"testsrc=size={publisher_impl.FRAME_WIDTH}x{publisher_impl.FRAME_HEIGHT}:rate={publisher_impl.FRAME_RATE}",
    },
    "smptebars": {
        "track": "smptebars-live",
        "filter": lambda: f"smptebars=size={publisher_impl.FRAME_WIDTH}x{publisher_impl.FRAME_HEIGHT}:rate={publisher_impl.FRAME_RATE}",
    },
    "smptehdbars": {
        "track": "smptehdbars-live",
        "filter": lambda: f"smptehdbars=size={publisher_impl.FRAME_WIDTH}x{publisher_impl.FRAME_HEIGHT}:rate={publisher_impl.FRAME_RATE}",
    },
    "mandelbrot": {
        "track": "mandelbrot-live",
        "filter": lambda: f"mandelbrot=size={publisher_impl.FRAME_WIDTH}x{publisher_impl.FRAME_HEIGHT}:rate={publisher_impl.FRAME_RATE}",
    },
}


def configure_test_source(source: str):
    if source not in TEST_SOURCES:
        source = "testsrc2"

    source_config = TEST_SOURCES[source]
    base_filter = source_config["filter"]
    publisher_impl.TRACK_NAME = FullTrackName([b"video"], source_config["track"].encode("utf-8"))

    def build_video_filter(include_timestamp: bool = True) -> str:
        video_filter = base_filter()
        if not include_timestamp:
            return video_filter
        return (
            f"{video_filter},"
            "drawtext=expansion=strftime:text=%Y-%m-%d %H\\:%M\\:%S:"
            "x=20:y=20:fontsize=36:fontcolor=white:box=1:boxcolor=0x00000099"
        )

    publisher_impl.build_video_filter = build_video_filter


def main(source: str | None = None, relay_host: str | None = None, relay_port: int | None = None):
    publisher_impl.RELAY_HOST = relay_host or RELAY_HOST
    publisher_impl.RELAY_PORT = relay_port or RELAY_PORT
    configure_test_source(source or TEST_SOURCE)
    asyncio.run(publisher_impl.main())


if __name__ == "__main__":
    main()
