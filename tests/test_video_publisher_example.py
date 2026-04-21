from unittest.mock import AsyncMock

import pytest

from _path_helper import ensure_repo_root

ensure_repo_root()

from examples import video_publisher_example


def _make_box(box_type: bytes, payload: bytes) -> bytes:
    return (8 + len(payload)).to_bytes(4, "big") + box_type + payload


def test_build_video_filter_uses_strftime_drawtext():
    video_filter = video_publisher_example.build_video_filter()
    assert "drawtext=expansion=strftime" in video_filter
    assert "text=%Y-%m-%d %H\\:%M\\:%S" in video_filter


def test_build_video_filter_can_disable_timestamp_overlay():
    video_filter = video_publisher_example.build_video_filter(include_timestamp=False)
    assert "drawtext" not in video_filter
    assert video_filter.startswith("testsrc2=")


def test_publisher_metadata_uses_matching_browser_codec():
    assert video_publisher_example.DEFAULT_MSE_CODEC == "avc1.64001F"
    assert video_publisher_example.DEFAULT_MIME_TYPE == 'video/mp4; codecs="avc1.64001F"'


def test_fragmented_mp4_muxer_splits_init_and_media_fragments():
    muxer = video_publisher_example.FragmentedMp4Muxer()
    init_boxes = _make_box(b"ftyp", b"init-a") + _make_box(b"moov", b"init-b")
    first_fragment = _make_box(b"moof", b"frag-1-head") + _make_box(b"mdat", b"frag-1-body")
    second_fragment = _make_box(b"moof", b"frag-2-head") + _make_box(b"mdat", b"frag-2-body")

    emitted = muxer.feed(init_boxes + first_fragment + second_fragment)

    assert emitted == [
        ("init", init_boxes),
        ("fragment", first_fragment),
    ]
    assert muxer.flush() == [("fragment", second_fragment)]


def test_fragmented_mp4_muxer_handles_boxes_split_across_input_chunks():
    muxer = video_publisher_example.FragmentedMp4Muxer()
    init_boxes = _make_box(b"ftyp", b"one") + _make_box(b"moov", b"two")
    first_fragment = _make_box(b"moof", b"head") + _make_box(b"mdat", b"body")

    emitted = muxer.feed((init_boxes + first_fragment)[:11])
    assert emitted == []

    emitted = muxer.feed((init_boxes + first_fragment)[11:])
    assert emitted == [("init", init_boxes)]
    assert muxer.flush() == [("fragment", first_fragment)]


@pytest.mark.asyncio
async def test_launch_ffmpeg_live_source_retries_without_drawtext(monkeypatch):
    first_process = AsyncMock()
    first_process.returncode = 4294967274
    first_process.stderr.read = AsyncMock(return_value=b"Invalid argument")

    second_process = AsyncMock()
    second_process.returncode = None

    create_subprocess_exec = AsyncMock(side_effect=[first_process, second_process])
    monkeypatch.setattr(video_publisher_example.asyncio, "create_subprocess_exec", create_subprocess_exec)

    process = await video_publisher_example.launch_ffmpeg_live_source()

    assert process is second_process
    assert create_subprocess_exec.await_count == 2
    assert "drawtext=expansion=strftime" in create_subprocess_exec.await_args_list[0].args[8]
    assert "drawtext" not in create_subprocess_exec.await_args_list[1].args[8]
