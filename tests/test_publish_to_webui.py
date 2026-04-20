#!/usr/bin/env python3
"""
直接发布视频数据到agent_gw relay (localhost:9003)
WebUI订阅并接收

步骤:
1. 先调用WebUI API订阅测试track
2. 发布数据到relay
3. 检查WebUI是否收到
"""

import asyncio
import sys
import requests
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from moq import MOQPublisher, FullTrackName, PublishedObject

RELAY_HOST = "127.0.0.1"
RELAY_PORT = 9003
WEBUI_HOST = "127.0.0.1"
WEBUI_PORT = 9005


async def main():
    # 测试数据 - 模拟视频帧
    # WebUI期望单个元素namespace格式，带/前缀
    test_namespace_str = ["test", "video"]  # 字符串格式给WebUI API
    test_namespace_bytes = [b"/test/video"]  # 单个bytes元素，带/前缀，匹配WebUI格式
    test_track_name = b"video"
    track_id = "test_video_001"

    # 1. 通过WebUI API订阅track
    print("=" * 60)
    print("步骤1: 通过WebUI API订阅track")
    print("=" * 60)

    subscribe_url = f"http://{WEBUI_HOST}:{WEBUI_PORT}/api/moq/subscribe"
    subscribe_payload = {
        "track_id": track_id,
        "namespace": test_namespace_str,  # 字符串数组
        "track_name": "video",
    }

    print(f"订阅请求: {subscribe_url}")
    print(f"参数: track_id={track_id}, namespace=[test, video], track_name=video")

    try:
        resp = requests.post(subscribe_url, json=subscribe_payload, timeout=5)
        print(f"响应: {resp.status_code} - {resp.json()}")
    except Exception as e:
        print(f"订阅失败: {e}")
        return

    await asyncio.sleep(1.0)  # 等待订阅生效

    # 2. 创建Publisher并发布track
    print("\n" + "=" * 60)
    print("步骤2: 创建Publisher并发布track")
    print("=" * 60)

    publisher = MOQPublisher(RELAY_HOST, RELAY_PORT)
    pub_connected = asyncio.Event()
    publisher.set_handlers(on_connected=lambda: pub_connected.set())

    print(f"连接relay: {RELAY_HOST}:{RELAY_PORT}")
    if not await publisher.connect(agent_id="test-video-publisher"):
        print("Publisher连接失败")
        return

    await asyncio.wait_for(pub_connected.wait(), timeout=5.0)
    print("Publisher已连接")

    full_track_name = FullTrackName(
        namespace=test_namespace_bytes, track_name=test_track_name
    )
    await publisher.publish(full_track_name)
    print(f"已发布track: {full_track_name}")

    await asyncio.sleep(0.5)

    # 3. 发送视频数据
    print("\n" + "=" * 60)
    print("步骤3: 发送视频数据chunks")
    print("=" * 60)

    # 模拟5个视频帧，每个16KB
    chunk_size = 16384
    num_chunks = 5

    for i in range(num_chunks):
        payload = f"VIDEO_CHUNK_{i}_".encode() * (chunk_size // 15)
        payload = payload[:chunk_size]  # 截断到16KB

        obj = PublishedObject(
            group_id=0,  # 小帧模式
            object_id=i,
            payload=payload,
            use_datagram=False,  # 使用stream模式
        )
        await publisher.send_object(full_track_name, obj)
        print(f"已发送 chunk {i}: {len(payload)} bytes, group=0, object={i}")
        await asyncio.sleep(0.1)

    print(f"\n发送完成: {num_chunks} chunks, 总计 {num_chunks * chunk_size} bytes")

    # 4. 等待WebUI接收
    print("\n" + "=" * 60)
    print("步骤4: 等待WebUI接收 (3秒)")
    print("=" * 60)

    await asyncio.sleep(3.0)

    # 5. 检查WebUI接收状态
    print("\n" + "=" * 60)
    print("步骤5: 检查WebUI接收状态")
    print("=" * 60)

    status_url = f"http://{WEBUI_HOST}:{WEBUI_PORT}/api/moq/tracks/{track_id}/debug"
    try:
        resp = requests.get(status_url, timeout=5)
        result = resp.json()
        print(f"Track状态: {result}")

        if result.get("tracks"):
            for t in result["tracks"]:
                print(f"  - track_id: {t.get('track_id')}")
                print(f"    state: {t.get('state')}")
                print(f"    object_count: {t.get('object_count')}")
                print(f"    buffered_frames: {t.get('buffered_frames')}")
    except Exception as e:
        print(f"获取状态失败: {e}")

    # 6. 检查WebUI日志
    print("\n" + "=" * 60)
    print("步骤6: 检查WebUI日志 (最近50行)")
    print("=" * 60)

    import subprocess

    try:
        result = subprocess.run(
            ["tail", "-50", "/tmp/webui.log"], capture_output=True, text=True
        )
        print(result.stdout)
    except Exception as e:
        print(f"读取日志失败: {e}")

    # 清理
    print("\n清理资源...")
    publisher.disconnect()
    print("完成!")


if __name__ == "__main__":
    asyncio.run(main())
