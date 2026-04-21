#!/usr/bin/env python3
"""
MOQ Live Video Publisher Example.

Generates a timestamped test video in real time with ffmpeg and publishes it
as H.264 fragmented MP4 over MOQ stream objects.

Usage:
    python examples/video_publisher_example.py

Recommended order:
    1. python examples/relay_example.py
    2. python examples/video_subscriber_example.py
    3. python examples/video_publisher_example.py
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Optional

try:
    from examples._bootstrap import ensure_repo_root, setup_logging
except ImportError:  # pragma: no cover - direct script execution fallback
    from _bootstrap import ensure_repo_root, setup_logging

ensure_repo_root()
setup_logging()

from moq import MOQPublisher, FullTrackName, PublishedObject

logger = logging.getLogger(__name__)

RELAY_HOST = "127.0.0.1"
RELAY_PORT = 4443
GROUP_ID = 1
SUBGROUP_ID = 0
TRACK_NAME = FullTrackName([b"video"], b"h264-live")
FRAME_WIDTH = 1280
FRAME_HEIGHT = 720
FRAME_RATE = 30
CHUNK_SIZE = 512 * 1024
VIDEO_BITRATE = "2M"
SUBSCRIBER_GRACE_PERIOD = 1.0
DRAIN_GRACE_PERIOD = 60.0
FFMPEG_STARTUP_GRACE_PERIOD = 0.25


def build_video_filter(include_timestamp: bool = True) -> str:
    """Build the ffmpeg lavfi graph for the live test stream."""
    video_filter = f"testsrc2=size={FRAME_WIDTH}x{FRAME_HEIGHT}:rate={FRAME_RATE}"
    if not include_timestamp:
        return video_filter

    return (
        f"{video_filter},"
        "drawtext=expansion=strftime:text=%Y-%m-%d %H\\:%M\\:%S:"
        "x=20:y=20:fontsize=36:fontcolor=white:box=1:boxcolor=0x00000099"
    )


def build_ffmpeg_command(include_timestamp: bool = True) -> list[str]:
    """Build the ffmpeg command for a live timestamped test stream."""
    video_filter = build_video_filter(include_timestamp=include_timestamp)

    return [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-re",
        "-f",
        "lavfi",
        "-i",
        video_filter,
        "-an",
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-tune",
        "zerolatency",
        "-pix_fmt",
        "yuv420p",
        "-g",
        str(FRAME_RATE),
        "-keyint_min",
        str(FRAME_RATE),
        "-sc_threshold",
        "0",
        "-b:v",
        VIDEO_BITRATE,
        "-maxrate",
        VIDEO_BITRATE,
        "-bufsize",
        "4M",
        "-movflags",
        "+frag_keyframe+empty_moov+default_base_moof",
        "-f",
        "mp4",
        "pipe:1",
    ]


async def launch_ffmpeg_live_source() -> Optional[asyncio.subprocess.Process]:
    """Launch ffmpeg and fall back to a plain test source if drawtext is unavailable."""
    for include_timestamp in (True, False):
        ffmpeg_command = build_ffmpeg_command(include_timestamp=include_timestamp)
        logger.info(
            "Launching ffmpeg live test source%s",
            " with timestamp overlay" if include_timestamp else " without timestamp overlay",
        )
        try:
            ffmpeg_process = await asyncio.create_subprocess_exec(
                *ffmpeg_command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except FileNotFoundError:
            logger.error("ffmpeg executable not found in PATH")
            return None

        await asyncio.sleep(FFMPEG_STARTUP_GRACE_PERIOD)
        if ffmpeg_process.returncode is None:
            if not include_timestamp:
                logger.warning("Continuing without ffmpeg timestamp overlay")
            return ffmpeg_process

        stderr = b""
        if ffmpeg_process.stderr is not None:
            stderr = await ffmpeg_process.stderr.read()
        stderr_text = stderr.decode("utf-8", errors="replace")

        if include_timestamp:
            logger.warning(
                "ffmpeg timestamp overlay startup failed with code %d; retrying without drawtext: %s",
                ffmpeg_process.returncode,
                stderr_text,
            )
            continue

        logger.error("ffmpeg exited during startup with code %d: %s", ffmpeg_process.returncode, stderr_text)
        return None

    return None


async def main():
    """Generate and publish a live test stream over MOQ."""
    logger.info("Starting MOQ live video publisher")

    publisher = MOQPublisher(relay_host=RELAY_HOST, relay_port=RELAY_PORT)
    publisher.set_handlers(
        on_connected=lambda: logger.info("Publisher connected to relay"),
        on_disconnected=lambda: logger.info("Publisher disconnected from relay"),
        on_publication_accepted=lambda track_name: logger.info("Publication accepted: %s", track_name),
        on_publication_rejected=lambda track_name, reason: logger.warning(
            "Publication rejected: %s - %s", track_name, reason
        ),
    )

    if not await publisher.connect():
        logger.error("Failed to connect to relay")
        return

    ffmpeg_process = None
    object_id = 2
    sent_bytes = 0
    closed_subgroup = False

    async def close_live_subgroup():
        nonlocal closed_subgroup
        if closed_subgroup:
            return

        request_id = publisher._publications.get(TRACK_NAME)
        if request_id is None or publisher._session is None:
            return

        publication = publisher._session.get_publication(request_id)
        if publication is None:
            return

        await publisher.close_subgroup_stream(
            publication.track_alias,
            GROUP_ID,
            SUBGROUP_ID,
        )
        closed_subgroup = True
        logger.info(
            "Closed subgroup stream: track_alias=%d group=%d subgroup=%d",
            publication.track_alias,
            GROUP_ID,
            SUBGROUP_ID,
        )

    try:
        if not await publisher.publish(TRACK_NAME):
            logger.error("Failed to publish track: %s", TRACK_NAME)
            return

        logger.info(
            "Track published: %s; waiting %.1fs for subscribers before sending",
            TRACK_NAME,
            SUBSCRIBER_GRACE_PERIOD,
        )
        await asyncio.sleep(SUBSCRIBER_GRACE_PERIOD)

        metadata = {
            "type": "live-test-stream",
            "codec": "H.264",
            "container": "fMP4",
            "width": FRAME_WIDTH,
            "height": FRAME_HEIGHT,
            "fps": FRAME_RATE,
            "mode": "continuous",
            "chunk_size": CHUNK_SIZE,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
        await publisher.send_object(
            TRACK_NAME,
            PublishedObject(
                group_id=GROUP_ID,
                object_id=1,
                subgroup_id=SUBGROUP_ID,
                payload=json.dumps(metadata).encode("utf-8"),
            ),
        )
        logger.info("Sent metadata object: group=%d object=%d", GROUP_ID, 1)

        ffmpeg_process = await launch_ffmpeg_live_source()
        if ffmpeg_process is None:
            return
        logger.info("Live stream is running continuously; press Ctrl+C to stop publisher")

        while True:
            chunk = await ffmpeg_process.stdout.read(CHUNK_SIZE)
            if not chunk:
                break

            await publisher.send_object(
                TRACK_NAME,
                PublishedObject(
                    group_id=GROUP_ID,
                    object_id=object_id,
                    subgroup_id=SUBGROUP_ID,
                    payload=chunk,
                ),
            )
            sent_bytes += len(chunk)
            logger.info(
                "Sent live chunk as group=%d object=%d (%d bytes, total=%d)",
                GROUP_ID,
                object_id,
                len(chunk),
                sent_bytes,
            )
            object_id += 1

        stderr = b""
        if ffmpeg_process.stderr is not None:
            stderr = await ffmpeg_process.stderr.read()

        return_code = await ffmpeg_process.wait()
        if return_code != 0:
            logger.error("ffmpeg exited with code %d: %s", return_code, stderr.decode("utf-8", errors="replace"))
            return

        logger.info(
            "Live stream generation complete: %d objects, %d bytes of media payload",
            object_id - 2,
            sent_bytes,
        )
        await close_live_subgroup()

        logger.info(
            "Waiting %.1fs for QUIC stream data to drain before unpublish",
            DRAIN_GRACE_PERIOD,
        )
        await asyncio.sleep(DRAIN_GRACE_PERIOD)
    except KeyboardInterrupt:
        logger.info("Publisher stopped by user")
    except Exception as e:
        logger.error("Publisher error: %s", e)
    finally:
        if ffmpeg_process is not None and ffmpeg_process.returncode is None:
            ffmpeg_process.kill()
            await ffmpeg_process.wait()
        try:
            await close_live_subgroup()
        except Exception as e:
            logger.error("Error closing subgroup stream: %s", e)
        try:
            await publisher.unpublish(TRACK_NAME, "live video demo complete")
        except Exception as e:
            logger.error("Error during unpublish: %s", e)
        publisher.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
