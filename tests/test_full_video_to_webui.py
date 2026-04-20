#!/usr/bin/env python3
"""
完整视频传输测试 - 发送真实视频文件到WebUI

步骤:
1. 先通知WebUI订阅
2. 等待订阅完成
3. 连接relay发布track
4. 发送视频chunks
"""

import asyncio
import sys
import time
import requests
from pathlib import Path

sys.path.insert(0, "/home/acn/zqm")

from moq import MOQPublisher, FullTrackName, PublishedObject

RELAY_HOST = "localhost"
RELAY_PORT = 9003
WEBUI_HOST = "localhost"
WEBUI_PORT = 9005
VIDEO_FILE = "/home/acn/zqm/acn_sdk/test_video.mp4"
CHUNK_SIZE = 16384


async def send_video_to_webui():
    task_id = f"video-test-{int(time.time())}"
    agent_id = "video-publisher"
    namespace = f"/{task_id}/{agent_id}"
    track_name = b"video"
    track_id = f"{agent_id}_{task_id}_video"

    # Step 1: 通知WebUI订阅
    print("=" * 60)
    print("Step 1: 通知 WebUI 订阅")
    print("=" * 60)

    subscribe_url = f"http://{WEBUI_HOST}:{WEBUI_PORT}/api/acn/v3/subscribe_track"
    payload = {
        "type": "SUBSCRIBE_TRACK",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "payload": {
            "src_agent_id": "ACF",
            "dst_agent_id": agent_id,
            "task_id": task_id,
            "track_list": [{"namespace": namespace, "track": "video"}],
        },
    }

    print(f"Track ID: {track_id}")
    print(f"Namespace: {namespace}")

    resp = requests.post(subscribe_url, json=payload, timeout=5)
    result = resp.json()
    print(f"响应: {resp.status_code}")
    print(f"订阅结果: {result}")

    # Step 2: 等待订阅生效
    print("\n等待订阅生效 (2秒)...")
    await asyncio.sleep(2.0)

    # Step 3: 连接relay并发布track
    print("\n" + "=" * 60)
    print("Step 2: 连接 Relay 并发布 Track")
    print("=" * 60)

    publisher = MOQPublisher(RELAY_HOST, RELAY_PORT)
    pub_connected = asyncio.Event()
    publisher.set_handlers(on_connected=lambda: pub_connected.set())

    print(f"连接 Relay: {RELAY_HOST}:{RELAY_PORT}")
    if not await publisher.connect(agent_id="video-test-pub"):
        print("❌ Publisher连接失败")
        return

    await asyncio.wait_for(pub_connected.wait(), timeout=5.0)
    print("✅ Publisher已连接")

    full_track_name = FullTrackName(
        namespace=[namespace.encode()], track_name=track_name
    )
    await publisher.publish(full_track_name)
    print(f"✅ 已发布track: {full_track_name}")

    await asyncio.sleep(0.5)

    # Step 4: 发送视频文件
    print("\n" + "=" * 60)
    print("Step 3: 发送视频文件")
    print("=" * 60)

    video_path = Path(VIDEO_FILE)
    file_size = video_path.stat().st_size
    print(f"文件: {VIDEO_FILE}")
    print(f"大小: {file_size} bytes ({file_size / 1024:.1f} KB)")

    chunks_sent = 0
    total_bytes = 0

    with open(VIDEO_FILE, "rb") as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break

            obj = PublishedObject(
                group_id=0,
                object_id=chunks_sent,
                payload=chunk,
                use_datagram=False,
            )
            await publisher.send_object(full_track_name, obj)

            chunks_sent += 1
            total_bytes += len(chunk)

            progress = total_bytes / file_size * 100
            print(f"  Chunk #{chunks_sent}: {len(chunk)} bytes, {progress:.1f}%")

            await asyncio.sleep(0.05)

    print(f"\n✅ 发送完成: {chunks_sent} chunks, {total_bytes} bytes")

    # Step 5: 等待WebUI处理
    print("\n" + "=" * 60)
    print("Step 4: 等待 WebUI 处理")
    print("=" * 60)

    await asyncio.sleep(3.0)

    # Step 6: 检查状态
    print("\n检查WebUI状态...")
    try:
        resp = requests.get(
            f"http://{WEBUI_HOST}:{WEBUI_PORT}/api/moq/status", timeout=5
        )
        result = resp.json()
        print(f"MOQ状态: connected={result.get('connected')}")
        if result.get("subscription_debug"):
            for t in result["subscription_debug"]:
                print(f"  Track: {t.get('track_id')}")
                print(f"    State: {t.get('state')}")
                print(f"    Objects: {t.get('object_count')}")
                print(f"    Frames: {t.get('buffered_frames')}")
    except Exception as e:
        print(f"状态查询失败: {e}")

    # 清理
    print("\n清理资源...")
    publisher.disconnect()
    print("✅ 完成")


if __name__ == "__main__":
    asyncio.run(send_video_to_webui())
