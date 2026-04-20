#!/usr/bin/env python3
"""
验证webui集成 - 完整测试流程
"""

import subprocess
import sys
import time

print("=" * 60)
print("WebUI集成验证测试")
print("=" * 60)

# 1. 检查MOQ代码是否一致
print("\n[1/5] 检查MOQ代码一致性...")
result = subprocess.run(
    [
        "diff",
        "/home/acn/zqm/moq-modified/transport/quic_transport.py",
        "/root/lpx/webui/moq/transport/quic_transport.py",
    ],
    capture_output=True,
)
if result.returncode == 0:
    print("✅ transport/quic_transport.py: 一致")
else:
    print("❌ transport/quic_transport.py: 有差异")

result = subprocess.run(
    [
        "diff",
        "/home/acn/zqm/moq-modified/pub/publisher.py",
        "/root/lpx/webui/moq/pub/publisher.py",
    ],
    capture_output=True,
)
if result.returncode == 0:
    print("✅ pub/publisher.py: 一致")
else:
    print("❌ pub/publisher.py: 有差异")

result = subprocess.run(
    [
        "diff",
        "/home/acn/zqm/moq-modified/sub/subscriber.py",
        "/root/lpx/webui/moq/sub/subscriber.py",
    ],
    capture_output=True,
)
if result.returncode == 0:
    print("✅ sub/subscriber.py: 一致")
else:
    print("❌ sub/subscriber.py: 有差异")

# 2. 检查webui修改
print("\n[2/5] 检查webui修改...")
with open("/root/lpx/webui/backend/app/moq_video.py", "r") as f:
    content = f.read()
    if "_chunk_buffers" in content:
        print("✅ moq_video.py: 包含分块重组代码")
    else:
        print("❌ moq_video.py: 缺少分块重组代码")

    if "try_parse_video_frame" in content:
        print("✅ moq_video.py: 包含VideoFrame解析")
    else:
        print("❌ moq_video.py: 缺少VideoFrame解析")

import os

if os.path.exists("/root/lpx/webui/backend/app/video_frame_parser.py"):
    print("✅ video_frame_parser.py: 存在")
else:
    print("❌ video_frame_parser.py: 不存在")

# 3. 生成测试视频
print("\n[3/5] 生成测试视频...")
video_path = "/tmp/webui_test_1080p.mp4"
subprocess.run(
    [
        "ffmpeg",
        "-y",
        "-f",
        "lavfi",
        "-i",
        "testsrc=duration=10:size=1920x1080:rate=30",
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
    ],
    capture_output=True,
)
print(f"✅ 测试视频: {video_path}")

# 4. 停止旧进程
print("\n[4/5] 清理旧进程...")
subprocess.run(
    "pkill -f 'demo_task_initiator_video_production' 2>/dev/null", shell=True
)
subprocess.run("pkill -f 'uvicorn.*9005' 2>/dev/null", shell=True)
time.sleep(2)
print("✅ 旧进程已清理")

# 5. 启动webui
print("\n[5/5] 启动WebUI...")
subprocess.run(
    "mv /root/lpx/webui/logs/backend.log /root/lpx/webui/logs/backend.log.bak 2>/dev/null",
    shell=True,
)

webui_proc = subprocess.Popen(
    [
        "python3",
        "-m",
        "uvicorn",
        "app.main:app",
        "--host",
        "0.0.0.0",
        "--port",
        "9005",
        "--log-level",
        "info",
    ],
    cwd="/root/lpx/webui",
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
)
print(f"✅ WebUI启动 (PID: {webui_proc.pid})")

# 等待webui启动
time.sleep(5)

# 6. 启动发送端
print("\n" + "=" * 60)
print("启动视频发送端...")
print("=" * 60)

sender_proc = subprocess.Popen(
    [
        "python3",
        "/home/acn/zqm/acn_sdk/examples/demo_task_initiator_video_production_fixed.py",
        "--video",
        video_path,
        "--width",
        "1920",
        "--height",
        "1080",
        "--fps",
        "30",
        "--bitrate",
        "4M",
        "--config",
        "/home/acn/zqm/acn_sdk/camera_demo_config.yaml",
    ],
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
)

print(f"发送端启动 (PID: {sender_proc.pid})")

# 7. 等待并检查日志
print("\n[*] 等待10秒传输...")
time.sleep(10)

print("\n" + "=" * 60)
print("检查接收端日志...")
print("=" * 60)

# 检查webui日志
if os.path.exists("/root/lpx/webui/logs/backend.log"):
    result = subprocess.run(
        ["grep", "-c", "_on_object_received", "/root/lpx/webui/logs/backend.log"],
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        count = result.stdout.strip()
        print(f"✅ 收到 {count} 个视频对象")
    else:
        print("❌ 没有收到视频对象")

    # 检查Parsed VideoFrame
    result = subprocess.run(
        ["grep", "-c", "Parsed VideoFrame", "/root/lpx/webui/logs/backend.log"],
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        count = result.stdout.strip()
        print(f"✅ 解析了 {count} 个VideoFrame")
    else:
        print("❌ 没有解析VideoFrame")

    # 显示最后几条MOQ日志
    print("\n最后10条MOQ日志:")
    subprocess.run(
        ["grep", "MOQ", "/root/lpx/webui/logs/backend.log"], capture_output=False
    )
else:
    print("❌ webui日志文件不存在")

# 8. 清理
print("\n" + "=" * 60)
print("清理进程...")
print("=" * 60)
sender_proc.terminate()
webui_proc.terminate()
print("✅ 测试完成")

print("\n查看完整日志: tail -f /root/lpx/webui/logs/backend.log | grep MOQ")
