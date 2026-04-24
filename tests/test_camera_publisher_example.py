from unittest.mock import AsyncMock

import pytest

from _path_helper import ensure_repo_root

ensure_repo_root()

from examples import camera_publisher_example


def test_build_list_devices_command_uses_directshow():
    assert camera_publisher_example.build_list_devices_command() == [
        "ffmpeg",
        "-list_devices",
        "true",
        "-f",
        "dshow",
        "-i",
        "dummy",
    ]


def test_default_camera_name_is_integrated_camera():
    assert camera_publisher_example.DEFAULT_CAMERA_NAME == "Integrated Camera"


def test_build_windows_camera_ffmpeg_command_uses_directshow_camera():
    command = camera_publisher_example.build_windows_camera_ffmpeg_command("USB Camera")

    assert command[:13] == [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-fflags",
        "nobuffer",
        "-flags",
        "low_delay",
        "-analyzeduration",
        camera_publisher_example.CAMERA_ANALYZE_DURATION,
        "-probesize",
        camera_publisher_example.CAMERA_PROBE_SIZE,
        "-use_wallclock_as_timestamps",
    ]
    assert command[13] == "1"
    assert command[command.index("-rtbufsize") + 1] == camera_publisher_example.CAMERA_RTBUF_SIZE
    assert command[command.index("-i") + 1] == "video=USB Camera"
    assert command[command.index("-video_size") + 1] == "1280x720"
    assert command[command.index("-framerate") + 1] == "30"
    assert command[command.index("-bf") + 1] == "0"
    assert command[command.index("-g") + 1] == str(camera_publisher_example.KEYFRAME_INTERVAL_FRAMES)
    assert command[-2:] == ["mp4", "pipe:1"]


def test_build_windows_camera_ffmpeg_command_allows_input_format_override():
    command = camera_publisher_example.build_windows_camera_ffmpeg_command(
        "USB Camera",
        input_format="mjpeg",
    )

    assert command[command.index("-vcodec") + 1] == "mjpeg"
    assert command.index("-vcodec") < command.index("-i")


@pytest.mark.asyncio
async def test_launch_ffmpeg_camera_source_logs_device_hint_on_startup_failure(monkeypatch):
    process = AsyncMock()
    process.returncode = 1
    process.stderr.read = AsyncMock(return_value=b"Could not find video device")

    create_subprocess_exec = AsyncMock(return_value=process)
    monkeypatch.setattr(camera_publisher_example.asyncio, "create_subprocess_exec", create_subprocess_exec)

    launched = await camera_publisher_example.launch_ffmpeg_camera_source("Missing Camera")

    assert launched is None
    assert create_subprocess_exec.await_count == 1
    assert "video=Missing Camera" in create_subprocess_exec.await_args.args
