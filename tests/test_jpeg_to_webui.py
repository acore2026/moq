#!/usr/bin/env python3
"""
JPEG图片传输测试 - 验证前端渲染

JPEG格式前端可以直接显示
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
CHUNK_SIZE = 16384


def create_test_jpeg():
    """创建一个简单的测试JPEG图片"""
    import struct
    import zlib

    # 创建一个简单的红色JPEG图片 (64x64)
    # JPEG structure: SOI + APP0 + DQT + SOF0 + DHT + SOS + EOI

    width, height = 64, 64

    # SOI (Start of Image)
    soi = b"\xff\xd8"

    # APP0 (JFIF marker)
    app0 = b"\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00"

    # DQT (Define Quantization Table) - simplified
    dqt = b"\xff\xdb\x00\x43\x00" + bytes([16] * 64)

    # SOF0 (Start of Frame) - baseline DCT
    sof0 = (
        b"\xff\xc0\x00\x0b\x08"
        + struct.pack(">HH", height, width)
        + b"\x01\x01\x11\x00"
    )

    # DHT (Define Huffman Table) - DC table
    dht_dc = b"\xff\xc4\x00\x1f\x00\x00\x01\x05\x01\x01\x01\x01\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b"

    # DHT - AC table
    dht_ac = b"\xff\xc4\x00\xb5\x10\x00\x02\x01\x03\x03\x02\x04\x03\x05\x05\x04\x04\x00\x00\x01\x7d\x01\x02\x03\x00\x04\x11\x05\x12\x21\x31\x41\x06\x13\x51\x61\x07\x22\x71\x14\x32\x81\xa1\x08\x23\x42\xb1\xc1\x15\x52\xd1\xf0\x24\x33\x62\x72\x82\x09\x0a\x16\x17\x18\x19\x1a\x25\x26\x27\x28\x29\x2a\x34\x35\x36\x37\x38\x39\x3a\x43\x44\x45\x46\x47\x48\x49\x4a\x53\x54\x55\x56\x57\x58\x59\x5a\x63\x64\x65\x66\x67\x68\x69\x6a\x73\x74\x75\x76\x77\x78\x79\x7a\x83\x84\x85\x86\x87\x88\x89\x8a\x92\x93\x94\x95\x96\x97\x98\x99\x9a\xa2\xa3\xa4\xa5\xa6\xa7\xa8\xa9\xaa\xb2\xb3\xb4\xb5\xb6\xb7\xb8\xb9\xba\xc2\xc3\xc4\xc5\xc6\xc7\xc8\xc9\xca\xd2\xd3\xd4\xd5\xd6\xd7\xd8\xd9\xda\xe1\xe2\xe3\xe4\xe5\xe6\xe7\xe8\xe9\xea\xf1\xf2\xf3\xf4\xf5\xf6\xf7\xf8\xf9\xfa"

    # SOS (Start of Scan)
    sos = b"\xff\xda\x00\x08\x01\x01\x00\x00\x3f\x00"

    # Scan data - simple red block
    # For a solid color, we use minimal entropy coding
    scan_data = (
        b"\xfb\xd3\x28\xa2\x80\x0a\x28\xa0\x02\x28\xa0\x02\x28\xa0\x02\x28\xa0" * 10
    )

    # EOI (End of Image)
    eoi = b"\xff\xd9"

    jpeg = soi + app0 + dqt + sof0 + dht_dc + dht_ac + sos + scan_data + eoi
    return jpeg


async def send_jpeg_to_webui():
    task_id = f"jpeg-test-{int(time.time())}"
    agent_id = "jpeg-publisher"
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
    print(f"响应: {resp.status_code}")

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
    if not await publisher.connect(agent_id="jpeg-test-pub"):
        print("❌ Publisher连接失败")
        return

    await asyncio.wait_for(pub_connected.wait(), timeout=5.0)
    print("✅ Publisher已连接")

    full_track_name = FullTrackName(
        namespace=[namespace.encode()], track_name=track_name
    )
    await publisher.publish(full_track_name)
    print(f"✅ 已发布track")

    await asyncio.sleep(0.5)

    # Step 4: 创建并发送JPEG图片
    print("\n" + "=" * 60)
    print("Step 3: 发送 JPEG 图片")
    print("=" * 60)

    jpeg_data = create_test_jpeg()
    print(f"JPEG大小: {len(jpeg_data)} bytes")
    print(f"JPEG头部: {jpeg_data[:10].hex()} (应该是 ffd8ffe0...)")

    # 发送JPEG作为单个object
    obj = PublishedObject(
        group_id=0,
        object_id=0,
        payload=jpeg_data,
        use_datagram=False,
    )
    await publisher.send_object(full_track_name, obj)
    print("✅ 已发送JPEG图片")

    # 发送几帧模拟连续视频
    for i in range(1, 5):
        await asyncio.sleep(0.2)
        obj = PublishedObject(
            group_id=0,
            object_id=i,
            payload=jpeg_data,
            use_datagram=False,
        )
        await publisher.send_object(full_track_name, obj)
        print(f"  发送帧 #{i}")

    print(f"\n✅ 发送完成: 5帧")

    # Step 5: 等待WebUI处理
    print("\n" + "=" * 60)
    print("Step 4: 等待 WebUI 处理 (5秒)")
    print("=" * 60)
    print("请打开浏览器访问: http://localhost:9005")
    print("查看右侧 LIVE FEEDS 区域是否有视频显示")

    await asyncio.sleep(5.0)

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
    asyncio.run(send_jpeg_to_webui())
