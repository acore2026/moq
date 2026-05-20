# MoQ Rust 视频传输测试手册

本文档整理当前项目中 Rust MoQ 视频传输相关测试工具的使用方式。默认测试端口：

```text
Rust MoQ relay: 9007
浏览器 viewer: 9008
```

除非明确需要，不要使用 `9001`、`9002`、`9003`，这些端口属于当前 Agent GW
运行环境。

## 1. 测试内容

当前视频测试覆盖三类场景：

- 本机启动 Rust relay 和浏览器 viewer，看实时视频画面。
- 远端 Windows 使用摄像头推流，本机 relay 转发，本机 viewer 播放。
- 使用 ffmpeg 生成测试源，验证传输、解码、丢帧、延迟、抖动、内存占用。

视频热路径推荐使用 Rust CLI：

```text
Camera / ffmpeg -> moq-cli publish avc3 -> Rust moq-relay
Rust moq-relay -> moq-cli subscribe --output avc3 -> viewer/WebCodecs
```

Python 只负责启动、停止和管理进程，不进入逐帧视频热路径。

## 2. 前置条件

本机需要：

- 补丁版 `moq-relay`
- 补丁版 `moq-cli`
- Python 依赖
- `ffmpeg`

补丁版 Rust 二进制通常在：

```text
/home/acn/zqm/test/moq-rust/moq/target/release/moq-relay
/home/acn/zqm/test/moq-rust/moq/target/release/moq-cli
```

如果没有，需要先构建：

```bash
cd /home/acn/zqm/test/moq-rust/moq
cargo build --release --package moq-cli
cargo build --release --package moq-relay
```

确认 `ffmpeg` 可用：

```bash
ffmpeg -version
```

## 3. 启动本机 relay + viewer

在项目根目录运行：

```bash
cd /home/acn/zqm/test/moq-rust/moq/py/acn-moq-rust

MOQ_OFFICIAL_RELAY_BIN=/home/acn/zqm/test/moq-rust/moq/target/release/moq-relay \
python3 video/moq_live_video_viewer.py \
  --subscriber rust-avc3 \
  --moq-cli-bin /home/acn/zqm/test/moq-rust/moq/target/release/moq-cli \
  --relay-host 0.0.0.0 \
  --relay-port 9007 \
  --web-host 0.0.0.0 \
  --web-port 9008 \
  --no-https
```

打开：

```text
http://<本机IP>:9008/
```

如果需要远端浏览器访问，确认防火墙/安全组开放：

```text
TCP 9008
TCP 9007
UDP 9007
```

viewer 默认订阅：

```text
broadcast name: camera
subscriber path: rust-avc3
```

## 4. 远端 Windows 摄像头推流

远端 Windows 需要：

- `ffmpeg.exe`
- 补丁版 `moq-cli.exe`

先列出摄像头名称：

```powershell
ffmpeg -list_devices true -f dshow -i dummy
```

使用摄像头推流，示例：

```powershell
ffmpeg -f dshow `
  -video_size 1280x720 `
  -framerate 30 `
  -i video="HD Camera" `
  -an -threads 1 `
  -c:v libx264 -preset ultrafast -tune zerolatency `
  -profile:v baseline -level 3.1 `
  -x264-params keyint=30:min-keyint=30:scenecut=0 `
  -b:v 2500k -maxrate 2500k -bufsize 5000k `
  -pix_fmt yuv420p `
  -f h264 - `
| .\moq-cli.exe --log-level info --iroh-enabled=false publish `
  --client-bind 0.0.0.0:0 `
  --url http://<relay-ip>:9007/ `
  --name camera `
  avc3
```

注意：

- `--name camera` 要和 viewer 的 `--broadcast-name camera` 一致。
- 推荐使用 `avc3`，不要用前面测试过的 fMP4 低延迟路径。
- Windows 上如果 QUIC 绑定异常，保留 `--client-bind 0.0.0.0:0`。

## 5. 远端 ffmpeg 测试源推流

如果先不接真实摄像头，可以用测试源：

```powershell
ffmpeg -hide_banner -loglevel warning `
  -re -f lavfi -i testsrc2=size=640x360:rate=30 `
  -an -threads 1 `
  -c:v libx264 -preset ultrafast -tune zerolatency `
  -profile:v baseline -level 3.1 `
  -x264-params keyint=30:min-keyint=30:scenecut=0 `
  -b:v 800k -maxrate 800k -bufsize 1600k `
  -pix_fmt yuv420p `
  -f h264 - `
| .\moq-cli.exe --log-level info --iroh-enabled=false publish `
  --client-bind 0.0.0.0:0 `
  --url http://<relay-ip>:9007/ `
  --name camera `
  avc3
```

本机 viewer 如果正常，应能看到彩条测试源。

## 6. 使用 Python 视频发布 API

端侧安装 `moq_rust_video` wheel 后，可以用 `CameraPublisher` 管理 ffmpeg 和
moq-cli：

```python
from moq_rust_video import CameraPublisher

publisher = CameraPublisher(
    relay_url="http://<relay-ip>:9007/",
    name="camera",
    camera_name="HD Camera",
    platform="windows",
    width=1280,
    height=720,
    fps=30,
    bitrate="2500k",
)

publisher.start()

try:
    publisher.wait()
finally:
    publisher.stop()
```

如果 `moq-cli` 或 `ffmpeg` 不在 PATH 中：

```python
publisher = CameraPublisher(
    relay_url="http://<relay-ip>:9007/",
    name="camera",
    camera_name="HD Camera",
    platform="windows",
    moq_cli_path=r"D:\path\to\moq-cli.exe",
    ffmpeg_path=r"D:\path\to\ffmpeg.exe",
)
```

## 7. 自动化 ffmpeg 传输测试

脚本：

```text
video/moq_rust_relay_ffmpeg_video_test.py
```

它会：

- 启动临时 Rust relay
- 生成 H.264 测试源
- 通过 Python MoQ pub/sub 或兼容路径传输
- 保存源文件和接收文件
- 调用 ffmpeg 解码验证
- 输出 JSON 结果

建议使用 9007：

```bash
MOQ_OFFICIAL_RELAY_BIN=/home/acn/zqm/test/moq-rust/moq/target/release/moq-relay \
python3 video/moq_rust_relay_ffmpeg_video_test.py \
  --port 9007 \
  --width 1280 \
  --height 720 \
  --fps 30 \
  --duration 5 \
  --bitrate 2500k
```

输出目录默认在：

```text
/tmp/moq-rust-relay-ffmpeg-video-test
```

## 8. 性能测试

脚本：

```text
video/moq_rust_relay_video_perf.py
```

它会输出：

- 期望帧数
- 收到帧数
- 丢帧率
- 接收码率
- latency min/avg/p50/p95/max
- jitter p95
- relay RSS 内存

示例：

```bash
MOQ_OFFICIAL_RELAY_BIN=/home/acn/zqm/test/moq-rust/moq/target/release/moq-relay \
python3 video/moq_rust_relay_video_perf.py \
  --port 9007 \
  --fps 30 \
  --duration 10 \
  --payload-size 50000
```

如果要粗略模拟 1080p30，可把 payload 调大到约 100KB 到 150KB：

```bash
python3 video/moq_rust_relay_video_perf.py \
  --port 9007 \
  --fps 30 \
  --duration 10 \
  --payload-size 120000
```

## 9. 前端看不到视频的排查

按顺序检查：

1. `9007` relay 是否在运行。

   ```bash
   ss -ltnp | grep 9007
   ss -lunp | grep 9007
   ```

2. `9008` viewer 是否在运行。

   ```bash
   ss -ltnp | grep 9008
   ```

3. 推流端是否连接到正确 relay。

   ```text
   --url http://<relay-ip>:9007/
   --name camera
   ```

4. viewer 和 publisher 的 broadcast name 是否一致。

   ```text
   viewer:    --broadcast-name camera
   publisher: --name camera
   ```

5. 使用 `avc3` 而不是 fMP4。

   ```text
   moq-cli publish ... avc3
   moq-cli subscribe --output avc3
   ```

6. H.264 参数是否适合实时解码。

   推荐：

   ```text
   -preset ultrafast
   -tune zerolatency
   -profile:v baseline
   -x264-params keyint=30:min-keyint=30:scenecut=0
   -pix_fmt yuv420p
   -f h264 -
   ```

7. 浏览器是否支持 WebCodecs。

   Chrome/Edge 推荐；如果用 HTTPS viewer，需要信任临时证书。测试时可先用
   `--no-https`。

8. Windows QUIC 绑定问题。

   如果日志里出现地址绑定或 IPv6 mapped address 错误，publisher 命令加：

   ```text
   --client-bind 0.0.0.0:0
   ```

## 10. 停止测试服务

如果是前台启动，直接 `Ctrl+C`。

如果需要确认没有残留：

```bash
ps -eo pid,ppid,stat,cmd | grep -E 'moq_live_video_viewer|moq-cli|moq-relay|ffmpeg'
ss -ltnp | grep -E ':9007|:9008'
ss -lunp | grep -E ':9007|:9008'
```

当前项目约定：测试结束后不应遗留 `9007` / `9008` 监听。

## 11. 已知边界

- 当前低延迟实时视频推荐 `avc3` / WebCodecs 路径。
- 早期 fMP4 / MSE 路径可以运行，但低延迟效果不如 `avc3`，曾出现只显示几帧或缓冲过高。
- `moq_rust_video` v1 不内置 `ffmpeg`，端侧需要自行安装或传入 `ffmpeg_path`。
- 该视频方案依赖补丁版 `moq-cli`，普通官方未打补丁版本不具备所有当前测试能力。
