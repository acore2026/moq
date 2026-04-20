#!/usr/bin/env python3
"""
诊断webui视频接收问题
"""

import sys

sys.path.insert(0, "/root/lpx/webui")

print("=" * 60)
print("WebUI视频接收诊断")
print("=" * 60)

# 1. 检查VideoFrame解析器是否存在
print("\n[1] 检查VideoFrame解析器...")
try:
    from backend.app.video_frame_parser import VideoFrame, try_parse_video_frame

    print("✅ VideoFrame解析器导入成功")
except Exception as e:
    print(f"❌ VideoFrame解析器导入失败: {e}")

# 2. 检查main.py是否包含解析逻辑
print("\n[2] 检查main.py是否包含解析逻辑...")
with open("/root/lpx/webui/backend/app/main.py", "r") as f:
    content = f.read()
    if "try_parse_video_frame" in content:
        print("✅ main.py包含VideoFrame解析逻辑")
    else:
        print("❌ main.py不包含VideoFrame解析逻辑")

    if "Parsed VideoFrame" in content:
        print("✅ main.py包含'Parsed VideoFrame'日志输出")
    else:
        print("❌ main.py不包含'Parsed VideoFrame'日志输出")

# 3. 检查webui进程
print("\n[3] 检查webui进程...")
import subprocess

result = subprocess.run(["ps", "aux"], capture_output=True, text=True)
webui_processes = [
    line for line in result.stdout.split("\n") if "uvicorn" in line and "9005" in line
]
if webui_processes:
    print(f"✅ WebUI进程在运行 (端口9005):")
    for line in webui_processes:
        print(f"   {line[:80]}...")
else:
    print("❌ WebUI进程未在端口9005运行")

# 4. 检查后端日志
print("\n[4] 检查后端日志...")
import os

log_path = "/root/lpx/webui/logs/backend.log"
if os.path.exists(log_path):
    size = os.path.getsize(log_path)
    print(f"✅ 日志文件存在: {log_path} ({size} bytes)")

    # 检查是否有VIDEO_FRAME日志
    result = subprocess.run(
        ["grep", "-c", "VIDEO_FRAME", log_path], capture_output=True, text=True
    )
    if result.returncode == 0:
        count = result.stdout.strip()
        print(f"   包含VIDEO_FRAME日志: {count} 条")
    else:
        print("   没有找到VIDEO_FRAME日志")
else:
    print(f"❌ 日志文件不存在: {log_path}")

# 5. 检查发送端
print("\n[5] 检查发送端进程...")
result = subprocess.run(["ps", "aux"], capture_output=True, text=True)
sender_processes = [
    line
    for line in result.stdout.split("\n")
    if "production_video_streamer" in line or "demo_task_initiator_video" in line
]
if sender_processes:
    print(f"✅ 视频发送端进程在运行:")
    for line in sender_processes:
        print(f"   {line[:80]}...")
else:
    print("⚠️ 没有检测到视频发送端进程 (可能已停止)")

# 6. 检查MOQ连接
print("\n[6] 检查MOQ Relay连接...")
# 检查是否有Disconnected日志
result = subprocess.run(
    ["grep", "-c", "Disconnected from MOQ Relay", log_path],
    capture_output=True,
    text=True,
)
if result.returncode == 0:
    count = result.stdout.strip()
    print(f"   检测到断开连接: {count} 次")
else:
    print("   未检测到断开连接日志")

# 7. 测试VideoFrame解析
print("\n[7] 测试VideoFrame解析...")
test_data = (
    bytes(
        [
            0x01,  # version
            0x01,  # flags (KEYFRAME)
            0x00,  # frame_type (IDR)
            0x00,  # reserved
        ]
    )
    + bytes(8)
    + bytes(8)
    + bytes(8)
    + bytes(4) * 7
    + b"\x00\x00\x00\x01\x67TEST_H264_DATA"
)

parsed = try_parse_video_frame(test_data)
if parsed:
    print(
        f"✅ VideoFrame解析成功: {parsed.width}x{parsed.height}, data_len={len(parsed.data)}"
    )
else:
    print("❌ VideoFrame解析失败")

print("\n" + "=" * 60)
print("诊断完成")
print("=" * 60)

print("\n📋 总结:")
print("1. 如果'Parsed VideoFrame'日志不存在，说明webui没有收到或解析视频帧")
print("2. 如果'Disconnected from MOQ Relay'日志很多，说明连接不稳定")
print("3. 需要重启webui使修改生效: systemctl restart webui 或手动重启")
