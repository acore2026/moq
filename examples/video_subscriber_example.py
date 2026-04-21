#!/usr/bin/env python3
"""
MOQ Live Video Subscriber Example.

Subscribes to the live `video/h264-live` track, reconstructs the fragmented
MP4 byte stream, and saves it as `receive.mp4` in the current working
directory.

Usage:
    python examples/video_subscriber_example.py
"""

import asyncio
import hashlib
import json
import logging
import shutil
from pathlib import Path

try:
    from examples._bootstrap import ensure_repo_root, setup_logging
except ImportError:  # pragma: no cover - direct script execution fallback
    from _bootstrap import ensure_repo_root, setup_logging

ensure_repo_root()
setup_logging()

from moq import MOQSubscriber, FullTrackName, ReceivedObject, ObjectStatus

logger = logging.getLogger(__name__)

RELAY_HOST = "127.0.0.1"
RELAY_PORT = 4443
TRACK_NAME = FullTrackName([b"video"], b"h264-live")
OUTPUT_PATH = Path.cwd() / "receive.mp4"
ENABLE_LIVE_PREVIEW = True
FILE_FLUSH_INTERVAL = 1024 * 1024


async def launch_ffplay():
    """Launch ffplay for live preview if it is available."""
    if not ENABLE_LIVE_PREVIEW:
        return None

    ffplay_path = shutil.which("ffplay")
    if ffplay_path is None:
        logger.info("ffplay not found; live preview disabled")
        return None

    try:
        process = await asyncio.create_subprocess_exec(
            ffplay_path,
            "-hide_banner",
            "-loglevel",
            "warning",
            "-fflags",
            "nobuffer",
            "-flags",
            "low_delay",
            "-i",
            "pipe:0",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        logger.info("Started ffplay live preview")
        return process
    except Exception as e:
        logger.warning("Failed to start ffplay live preview: %s", e)
        return None


async def probe_output_file(path: Path):
    """Run ffprobe on the reconstructed output file."""
    process = await asyncio.create_subprocess_exec(
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "format=format_name,duration,size",
        "-show_streams",
        str(path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    if process.returncode == 0:
        logger.info("ffprobe output for %s:\n%s", path, stdout.decode("utf-8", errors="replace").strip())
    else:
        logger.warning("ffprobe failed for %s: %s", path, stderr.decode("utf-8", errors="replace").strip())


async def main():
    """Subscribe to the live video track and reconstruct the MP4 stream."""
    logger.info("Starting MOQ live video subscriber")

    subscriber = MOQSubscriber(relay_host=RELAY_HOST, relay_port=RELAY_PORT)
    transfer_done = asyncio.Event()
    object_queue: asyncio.Queue[ReceivedObject] = asyncio.Queue()

    metadata = {}
    received_chunk_count = 0
    bytes_written = 0
    output_file = None
    hasher = hashlib.sha256()
    bytes_since_flush = 0
    ffplay_process = await launch_ffplay()

    async def close_ffplay():
        nonlocal ffplay_process
        if ffplay_process is None:
            return

        if ffplay_process.stdin is not None and not ffplay_process.stdin.is_closing():
            ffplay_process.stdin.close()

        stderr = b""
        if ffplay_process.stderr is not None:
            stderr = await ffplay_process.stderr.read()

        return_code = await ffplay_process.wait()
        if return_code != 0:
            logger.warning(
                "ffplay exited with code %d: %s",
                return_code,
                stderr.decode("utf-8", errors="replace").strip(),
            )
        else:
            logger.info("ffplay live preview stopped")
        ffplay_process = None

    async def process_received_objects():
        nonlocal metadata, received_chunk_count, bytes_written, output_file, bytes_since_flush

        while True:
            obj = await object_queue.get()

            if obj.group_id != 1:
                logger.warning("Ignoring unexpected group: %d", obj.group_id)
                continue

            if obj.object_status == ObjectStatus.END_OF_SUBGROUP:
                if output_file is not None and not output_file.closed:
                    output_file.flush()
                logger.info("Received end of live subgroup")
                digest = hasher.hexdigest()
                if output_file is not None and not output_file.closed:
                    output_file.close()
                logger.info(
                    "Live stream saved to %s (%d bytes written across %d chunks, sha256=%s)",
                    OUTPUT_PATH,
                    bytes_written,
                    received_chunk_count,
                    digest,
                )
                transfer_done.set()
                continue

            if obj.object_id == 1:
                metadata = json.loads(obj.payload.decode("utf-8"))
                logger.info(
                    "Received metadata: codec=%s container=%s resolution=%sx%s fps=%s mode=%s output=%s",
                    metadata["codec"],
                    metadata["container"],
                    metadata["width"],
                    metadata["height"],
                    metadata["fps"],
                    metadata.get("mode", "unknown"),
                    OUTPUT_PATH,
                )
                OUTPUT_PATH.write_bytes(b"")
                output_file = OUTPUT_PATH.open("ab", buffering=4 * 1024 * 1024)
                bytes_since_flush = 0
                continue

            if output_file is None:
                logger.warning("Media object received before metadata; object=%d", obj.object_id)
                continue

            output_file.write(obj.payload)
            bytes_since_flush += len(obj.payload)
            if bytes_since_flush >= FILE_FLUSH_INTERVAL:
                output_file.flush()
                bytes_since_flush = 0

            if ffplay_process is not None and ffplay_process.stdin is not None:
                try:
                    ffplay_process.stdin.write(obj.payload)
                    await ffplay_process.stdin.drain()
                except Exception as e:
                    logger.warning("Failed writing to ffplay stdin: %s", e)

            hasher.update(obj.payload)
            bytes_written += len(obj.payload)
            received_chunk_count += 1
            logger.info(
                "Received live chunk %d as object=%d (%d bytes, total=%d)",
                received_chunk_count,
                obj.object_id,
                len(obj.payload),
                bytes_written,
            )

    def on_object_received(obj: ReceivedObject):
        object_queue.put_nowait(obj)

    subscriber.set_handlers(
        on_connected=lambda: logger.info("Subscriber connected to relay"),
        on_disconnected=lambda: logger.info("Subscriber disconnected from relay"),
        on_object_received=on_object_received,
        on_subscription_accepted=lambda track_name: logger.info("Subscription accepted: %s", track_name),
        on_subscription_rejected=lambda track_name, reason: logger.warning(
            "Subscription rejected: %s - %s", track_name, reason
        ),
    )

    if not await subscriber.connect():
        logger.error("Failed to connect to relay")
        return

    processor_task = asyncio.create_task(process_received_objects())

    try:
        await subscriber.subscribe(TRACK_NAME, start_group=1, start_object=1)
        logger.info("Subscribed to track: %s", TRACK_NAME)
        logger.info("Live playback is running; press Ctrl+C to stop subscriber")
        await transfer_done.wait()
        await close_ffplay()
        await probe_output_file(OUTPUT_PATH)
    except KeyboardInterrupt:
        logger.info("Subscriber stopped by user")
    except Exception as e:
        logger.error("Subscriber error: %s", e)
    finally:
        processor_task.cancel()
        try:
            await processor_task
        except asyncio.CancelledError:
            pass
        await close_ffplay()
        if output_file is not None and not output_file.closed:
            output_file.flush()
            output_file.close()
        try:
            await subscriber.unsubscribe(TRACK_NAME)
        except Exception as e:
            logger.error("Error during unsubscribe: %s", e)
        subscriber.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
