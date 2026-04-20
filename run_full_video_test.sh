#!/bin/bash
# 完整的视频传输测试流程

echo "============================================================"
echo "MOQ视频传输完整测试流程"
echo "============================================================"
echo ""

# 1. 检查视频文件
echo "[1/5] 检查测试视频..."
VIDEO_PATH="/tmp/test_1080p_30s.mp4"
if [ ! -f "$VIDEO_PATH" ]; then
    echo "生成测试视频..."
    ffmpeg -f lavfi -i testsrc=duration=30:size=1920x1080:rate=30 \
           -pix_fmt yuv420p -c:v libx264 -preset ultrafast -b:v 4M -an \
           "$VIDEO_PATH" -y 2>&1 | tail -5
fi
echo "✅ 视频文件: $VIDEO_PATH ($(du -h $VIDEO_PATH | cut -f1))"
echo ""

# 2. 停止所有相关进程
echo "[2/5] 清理旧进程..."
pkill -f "demo_task_initiator_video_production" 2>/dev/null
pkill -f "uvicorn.*app.main:app.*9005" 2>/dev/null
sleep 2
echo "✅ 旧进程已清理"
echo ""

# 3. 启动webui接收端
echo "[3/5] 启动WebUI接收端..."
cd /root/lpx/webui
source .venv/bin/activate

# 备份旧日志
mv logs/backend.log logs/backend.log.bak.$(date +%Y%m%d_%H%M%S) 2>/dev/null

# 启动webui
python3 -m uvicorn app.main:app --host 0.0.0.0 --port 9005 --log-level info > logs/backend.log 2>&1 &
WEBUI_PID=$!
echo "✅ WebUI已启动 (PID: $WEBUI_PID, 端口: 9005)"
sleep 3
echo ""

# 4. 启动发送端
echo "[4/5] 启动视频发送端..."
cd /home/acn/zqm/acn_sdk

echo "运行: demo_task_initiator_video_production_fixed.py"
echo "配置: 1920x1080@30fps, 4Mbps"
echo ""

# 在前台运行，可以看到输出
timeout 60 python3 examples/demo_task_initiator_video_production_fixed.py \
    --video "$VIDEO_PATH" \
    --width 1920 \
    --height 1080 \
    --fps 30 \
    --bitrate 4M \
    --config camera_demo_config.yaml 2>&1 &

SENDER_PID=$!
echo "✅ 发送端已启动 (PID: $SENDER_PID)"
echo ""

# 5. 监控日志
echo "[5/5] 监控传输..."
echo "等待10秒后检查日志..."
echo ""

sleep 10

echo "============================================================"
echo "发送端日志 (最后30行):"
echo "============================================================"
tail -30 /tmp/fixed_demo.log 2>/dev/null || echo "日志文件不存在"
echo ""

echo "============================================================"
echo "接收端日志 (MOQ相关):"
echo "============================================================"
grep -E "\[MOQ\]|subscribe|Track" /root/lpx/webui/logs/backend.log 2>/dev/null | tail -20 || echo "暂无MOQ日志"
echo ""

echo "============================================================"
echo "检查视频帧接收:"
echo "============================================================"
grep -E "_on_object_received|Parsed VideoFrame|MOQ.*object" /root/lpx/webui/logs/backend.log 2>/dev/null | head -10 || echo "暂无视频帧接收日志"
echo ""

# 6. 保持运行
echo "============================================================"
echo "测试运行中..."
echo "按 Ctrl+C 停止所有进程"
echo "============================================================"

wait $SENDER_PID 2>/dev/null

# 7. 清理
echo ""
echo "测试结束，清理进程..."
kill $WEBUI_PID 2>/dev/null
pkill -f "demo_task_initiator_video_production" 2>/dev/null

echo "✅ 测试完成"
echo ""
echo "查看完整日志:"
echo "  发送端: /tmp/fixed_demo.log"
echo "  接收端: /root/lpx/webui/logs/backend.log"
