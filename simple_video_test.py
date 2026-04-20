#!/usr/bin/env python3
"""
简单视频传输测试

1. 启动发送端和接收端
2. 使用简单的MOQ传输
3. 查看日志确认传输
"""

import asyncio
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, "/home/acn/zqm/acn_sdk")
sys.path.insert(0, "/home/acn/zqm/moq-modified")

from moq_video_service import MOQVideoPublisher, MOQVideoSubscriber, MOQVideoRelay


async def test_video_transmission():
    """测试视频传输"""
    print("=" * 60)
    print("简单视频传输测试")
    print("=" * 60)

    # 生成测试视频
    video_path = "/tmp/simple_test.mp4"
    print(f"\n[1/4] 生成测试视频...")
    subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-f",
            "lavfi",
            "-i",
            "testsrc=duration=5:size=640x480:rate=30",
            "-pix_fmt",
            "yuv420p",
            "-c:v",
            "libx264",
            "-preset",
            "ultrafast",
            "-b:v",
            "1M",
            "-an",
            video_path,
        ],
        capture_output=True,
    )
    print(f"✅ 视频生成: {video_path}")

    # 启动Relay
    print(f"\n[2/4] 启动MOQ Relay...")
    relay = MOQVideoRelay(host="127.0.0.1", port=0)
    relay_port = await relay.start()
    print(f"✅ Relay启动: 127.0.0.1:{relay_port}")

    # 启动Subscriber
    print(f"\n[3/4] 启动Subscriber...")
    sub = MOQVideoSubscriber("127.0.0.1", relay_port)
    await sub.connect()
    print(f"✅ Subscriber连接成功")

    # 启动接收任务
    async def receive_task():
        stats = await sub.subscribe("test-video", "/tmp/received.mp4", wait_time=30)
        print(f"\n[接收完成]")
        print(f"  Chunks: {stats.chunks_received}")
        print(f"  Bytes: {stats.bytes_received}")
        hash_match = (
            stats.original_hash == stats.received_hash
            if stats.original_hash and stats.received_hash
            else False
        )
        print(f"  Hash匹配: {hash_match}")
        return stats

    receive_future = asyncio.create_task(receive_task())
    await asyncio.sleep(1)  # 等待订阅建立

    # 启动Publisher
    print(f"\n[4/4] 启动Publisher...")
    pub = MOQVideoPublisher("127.0.0.1", relay_port)
    await pub.connect()
    print(f"✅ Publisher连接成功")

    # 发送视频
    print(f"\n[*] 发送视频...")
    pub_stats = await pub.publish_video(video_path, "test-video")
    print(f"\n[发送完成]")
    print(f"  Chunks: {pub_stats.chunks_sent}")
    print(f"  Throughput: {pub_stats.throughput_mbps:.2f} Mbps")

    # 等待接收完成
    print(f"\n[*] 等待接收完成...")
    try:
        sub_stats = await asyncio.wait_for(receive_future, timeout=30)

        print("\n" + "=" * 60)
        print("测试结果")
        print("=" * 60)
        print(f"发送: {pub_stats.chunks_sent} chunks")
        print(f"接收: {sub_stats.chunks_received} chunks")
        print(f"原始Hash: {pub_stats.original_hash}")
        print(f"接收Hash: {sub_stats.received_hash}")
        print(f"匹配: {sub_stats.hash_match}")

        if sub_stats.hash_match:
            print("\n✅ 测试通过: 视频传输成功！")
            return 0
        else:
            print("\n❌ 测试失败: Hash不匹配！")
            return 1

    except asyncio.TimeoutError:
        print("\n❌ 测试超时！")
        return 1
    finally:
        pub.disconnect()
        sub.disconnect()
        await relay.stop()


if __name__ == "__main__":
    exit_code = asyncio.run(test_video_transmission())
    sys.exit(exit_code)
