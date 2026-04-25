#!/usr/bin/env python3
"""
MOQ Camera Publisher Example.

Captures a Windows laptop webcam with ffmpeg DirectShow and publishes it as
H.264 fragmented MP4 over MOQ stream objects.

Usage:
    python examples/camera_publisher_example.py

To list available Windows camera names:
    ffmpeg -list_devices true -f dshow -i dummy

Recommended order:
    1. python examples/relay_example.py
    2. python examples/video_webtransport_subscriber_example.py
    3. python examples/camera_publisher_example.py
"""

import asyncio
import json
import logging
import os
import platform
from datetime import datetime, timezone
from typing import Optional

try:
    from examples._bootstrap import ensure_repo_root, setup_logging
except ImportError:  # pragma: no cover - direct script execution fallback
    from _bootstrap import ensure_repo_root, setup_logging

ensure_repo_root()
setup_logging()

from moq import FullTrackName, MOQPublisher, PublishedObject

from examples.video_publisher_example import (
    CHUNK_SIZE,
    DEFAULT_MIME_TYPE,
    DEFAULT_MSE_CODEC,
    DRAIN_GRACE_PERIOD,
    FFMPEG_STARTUP_GRACE_PERIOD,
    FRAME_HEIGHT,
    FRAME_RATE,
    FRAME_WIDTH,
    GROUP_ID,
    KEYFRAME_INTERVAL_FRAMES,
    RELAY_HOST,
    RELAY_PORT,
    SUBGROUP_ID,
    SUBSCRIBER_GRACE_PERIOD,
    VIDEO_BITRATE,
    FragmentedMp4Muxer,
)

logger = logging.getLogger(__name__)

DEFAULT_CAMERA_NAME = "Integrated Camera"
CAMERA_INPUT_FORMAT_ENV = "MOQ_CAMERA_INPUT_FORMAT"
CAMERA_RTBUF_SIZE = "64M"
CAMERA_PROBE_SIZE = "32"
CAMERA_ANALYZE_DURATION = "0"
TRACK_NAME = FullTrackName([b"video"], b"camera-h264-live")


def camera_input_format_from_environment() -> str | None:
    """Return an optional DirectShow input format, for example mjpeg."""
    return os.environ.get(CAMERA_INPUT_FORMAT_ENV) or None


def build_list_devices_command() -> list[str]:
    """Build the ffmpeg command that lists Windows DirectShow devices."""
    return ["ffmpeg", "-list_devices", "true", "-f", "dshow", "-i", "dummy"]


def build_windows_camera_ffmpeg_command(
    camera_name: str = DEFAULT_CAMERA_NAME,
    input_format: str | None = None,
) -> list[str]:
    """Build the ffmpeg command for capturing a Windows DirectShow webcam."""
    command = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-fflags",
        "nobuffer",
        "-flags",
        "low_delay",
        "-analyzeduration",
        CAMERA_ANALYZE_DURATION,
        "-probesize",
        CAMERA_PROBE_SIZE,
        "-use_wallclock_as_timestamps",
        "1",
        "-f",
        "dshow",
        "-rtbufsize",
        CAMERA_RTBUF_SIZE,
        "-framerate",
        str(FRAME_RATE),
        "-video_size",
        f"{FRAME_WIDTH}x{FRAME_HEIGHT}",
    ]

    if input_format:
        command.extend(["-vcodec", input_format])

    command.extend([
        "-i",
        f"video={camera_name}",
        "-an",
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-tune",
        "zerolatency",
        "-bf",
        "0",
        "-pix_fmt",
        "yuv420p",
        "-g",
        str(KEYFRAME_INTERVAL_FRAMES),
        "-keyint_min",
        str(KEYFRAME_INTERVAL_FRAMES),
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
    ])
    return command


async def launch_ffmpeg_camera_source(
    camera_name: str,
    input_format: str | None = None,
) -> Optional[asyncio.subprocess.Process]:
    """Launch ffmpeg to capture the Windows camera."""
    ffmpeg_command = build_windows_camera_ffmpeg_command(camera_name, input_format=input_format)
    logger.info(
        "Launching Windows camera source: camera=%r%s",
        camera_name,
        f" input_format={input_format}" if input_format else "",
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
        return ffmpeg_process

    stderr = b""
    if ffmpeg_process.stderr is not None:
        stderr = await ffmpeg_process.stderr.read()
    logger.error(
        "ffmpeg camera capture exited during startup with code %d: %s",
        ffmpeg_process.returncode,
        stderr.decode("utf-8", errors="replace"),
    )
    logger.error("List camera names with: %s", " ".join(build_list_devices_command()))
    return None


async def main():
    """Capture and publish a Windows camera stream over MOQ."""
    logger.info("Starting MOQ Windows camera publisher")
    if platform.system() != "Windows":
        logger.warning("This example targets Windows DirectShow cameras; current platform is %s", platform.system())

    camera_name = DEFAULT_CAMERA_NAME
    input_format = camera_input_format_from_environment()
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
    muxer = FragmentedMp4Muxer()

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

        await publisher.close_subgroup_stream(publication.track_alias, GROUP_ID, SUBGROUP_ID)
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
            "type": "windows-camera-stream",
            "browser_track_profile": "moq-browser-fmp4-h264-v1",
            "source": "windows-camera",
            "camera_name": camera_name,
            "codec": "H.264",
            "container": "fMP4",
            "mime_type": DEFAULT_MIME_TYPE,
            "mse_codec": DEFAULT_MSE_CODEC,
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
        logger.info("Sent metadata object: group=%d object=%d camera=%r", GROUP_ID, 1, camera_name)

        ffmpeg_process = await launch_ffmpeg_camera_source(camera_name, input_format=input_format)
        if ffmpeg_process is None:
            return
        logger.info("Camera stream is running continuously; press Ctrl+C to stop publisher")

        async def publish_media_unit(unit_type: str, payload: bytes):
            nonlocal object_id, sent_bytes

            await publisher.send_object(
                TRACK_NAME,
                PublishedObject(
                    group_id=GROUP_ID,
                    object_id=object_id,
                    subgroup_id=SUBGROUP_ID,
                    payload=payload,
                ),
            )
            sent_bytes += len(payload)
            logger.info(
                "Sent %s as group=%d object=%d (%d bytes, total=%d)",
                unit_type,
                GROUP_ID,
                object_id,
                len(payload),
                sent_bytes,
            )
            object_id += 1

        while True:
            chunk = await ffmpeg_process.stdout.read(CHUNK_SIZE)
            if not chunk:
                break

            for unit_type, payload in muxer.feed(chunk):
                await publish_media_unit(unit_type, payload)

        stderr = b""
        if ffmpeg_process.stderr is not None:
            stderr = await ffmpeg_process.stderr.read()

        return_code = await ffmpeg_process.wait()
        if return_code != 0:
            logger.error("ffmpeg exited with code %d: %s", return_code, stderr.decode("utf-8", errors="replace"))
            return

        for unit_type, payload in muxer.flush():
            await publish_media_unit(unit_type, payload)

        logger.info(
            "Camera stream complete: %d objects, %d bytes of media payload",
            object_id - 2,
            sent_bytes,
        )
        await close_live_subgroup()

        logger.info("Waiting %.1fs for QUIC stream data to drain before unpublish", DRAIN_GRACE_PERIOD)
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
            await publisher.unpublish(TRACK_NAME, "windows camera demo complete")
        except Exception as e:
            logger.error("Error during unpublish: %s", e)
        publisher.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
