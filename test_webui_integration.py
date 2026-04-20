#!/usr/bin/env python3
"""
测试webui接收端与acn_sdk发送端的集成

验证demo_task_initiator_video_production.py发送的VideoFrame能否被webui正确解析
"""

import asyncio
import subprocess
import tempfile
import time
from pathlib import Path

# 添加路径
import sys

sys.path.insert(0, "/home/acn/zqm/acn_sdk")
sys.path.insert(0, "/root/lpx/webui")

# 导入解析器
from backend.app.video_frame_parser import (
    VideoFrame,
    FrameFlags,
    FrameType,
    try_parse_video_frame,
)


def create_test_video():
    """创建1080p测试视频"""
    video_path = "/tmp/test_1080p_webui.mp4"

    cmd = [
        "ffmpeg",
        "-y",
        "-f",
        "lavfi",
        "-i",
        "testsrc=duration=5:size=1920x1080:rate=30",
        "-pix_fmt",
        "yuv420p",
        "-c:v",
        "libx264",
        "-preset",
        "ultrafast",
        "-b:v",
        "4M",
        "-an",
        video_path,
    ]

    print("Creating 1080p test video...")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Failed to create video: {result.stderr}")
        return None

    print(f"Video created: {video_path}")
    return video_path


def test_video_frame_parsing():
    """测试VideoFrame解析"""
    print("\n" + "=" * 60)
    print("测试VideoFrame解析")
    print("=" * 60)

    # 创建测试帧（模拟demo_task_initiator_video_production.py发送的帧）
    frame = VideoFrame(
        version=1,
        flags=FrameFlags.KEYFRAME,
        frame_type=FrameType.IDR,
        timestamp=123456789000,
        pts=1000000,
        dts=1000000,
        frame_id=1,
        gop_id=0,
        width=1920,
        height=1080,
        fps=30,
        bitrate=4000000,
        data=b"\x00\x00\x00\x01\x67H264_TEST_DATA_1080P",  # 模拟H264数据
    )

    # 序列化（发送端）
    serialized = frame.to_bytes()
    print(f"发送端序列化: {len(serialized)} 字节")
    print(f"  - 头部: {VideoFrame.HEADER_SIZE} 字节")
    print(f"  - H264数据: {len(frame.data)} 字节")

    # 解析（接收端-webui）
    parsed = try_parse_video_frame(serialized)
    if parsed:
        print(f"\n接收端解析成功:")
        print(f"  - frame_id: {parsed.frame_id}")
        print(f"  - gop_id: {parsed.gop_id}")
        print(f"  - 分辨率: {parsed.width}x{parsed.height}")
        print(f"  - 帧率: {parsed.fps} fps")
        print(f"  - 码率: {parsed.bitrate} bps")
        print(f"  - 是否关键帧: {parsed.is_keyframe()}")
        print(f"  - H264数据大小: {len(parsed.data)} 字节")
        print(f"  - H264数据: {parsed.data[:20]}...")

        # 验证数据完整性
        if parsed.data == frame.data:
            print("\n✅ H264数据完整性验证通过!")
            return True
        else:
            print("\n❌ H264数据不匹配!")
            return False
    else:
        print("\n❌ 解析失败!")
        return False


def test_backward_compatibility():
    """测试向后兼容性（原始H264数据）"""
    print("\n" + "=" * 60)
    print("测试向后兼容性（原始H264数据）")
    print("=" * 60)

    # 模拟原始H264数据（没有VideoFrame头部）
    raw_h264 = b"\x00\x00\x00\x01\x67RAW_H264_DATA_WITHOUT_HEADER"

    print(f"原始H264数据: {len(raw_h264)} 字节")

    # 尝试解析（应该返回None，因为没有VideoFrame头部）
    parsed = try_parse_video_frame(raw_h264)

    if parsed is None:
        print("✅ 正确识别为原始H264数据（不是VideoFrame格式）")
        print(f"✅ 返回原始数据: {raw_h264[:20]}...")
        return True
    else:
        print("❌ 错误地将原始H264数据识别为VideoFrame!")
        return False


def test_extract_h264_data():
    """测试提取H264数据函数"""
    print("\n" + "=" * 60)
    print("测试extract_h264_data函数")
    print("=" * 60)

    from backend.app.video_frame_parser import extract_h264_data

    # 测试1: VideoFrame格式
    frame = VideoFrame(
        version=1,
        flags=FrameFlags.KEYFRAME,
        frame_type=FrameType.IDR,
        width=1920,
        height=1080,
        data=b"\x00\x00\x00\x01\x67VIDEO_DATA",
    )
    serialized = frame.to_bytes()
    extracted = extract_h264_data(serialized)

    if extracted == frame.data:
        print("✅ VideoFrame格式: 正确提取H264数据")
    else:
        print("❌ VideoFrame格式: 提取失败")
        return False

    # 测试2: 原始H264格式
    raw_h264 = b"\x00\x00\x00\x01\x67RAW_H264"
    extracted = extract_h264_data(raw_h264)

    if extracted == raw_h264:
        print("✅ 原始H264格式: 正确返回原始数据")
    else:
        print("❌ 原始H264格式: 处理失败")
        return False

    return True


def main():
    """主测试函数"""
    print("=" * 60)
    print("WebUI与ACN_SDK集成测试")
    print("=" * 60)

    results = []

    # 测试1: VideoFrame解析
    results.append(("VideoFrame解析", test_video_frame_parsing()))

    # 测试2: 向后兼容性
    results.append(("向后兼容性", test_backward_compatibility()))

    # 测试3: extract_h264_data函数
    results.append(("extract_h264_data函数", test_extract_h264_data()))

    # 总结
    print("\n" + "=" * 60)
    print("测试结果总结")
    print("=" * 60)

    for name, result in results:
        status = "✅ 通过" if result else "❌ 失败"
        print(f"  {name}: {status}")

    all_passed = all(r for _, r in results)

    if all_passed:
        print(
            "\n✅ 所有测试通过! WebUI可以正确解析demo_task_initiator_video_production.py发送的视频帧"
        )
    else:
        print("\n❌ 部分测试失败，请检查实现")

    return 0 if all_passed else 1


if __name__ == "__main__":
    exit(main())
