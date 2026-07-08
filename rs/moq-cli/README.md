# moq-cli

A command-line tool for publishing and subscribing to media over MoQ.
It works with FFmpeg for encoding.

## Install

```bash
cargo install moq-cli
```

### Docker

```bash
docker pull moqdev/moq-cli
```

Multi-arch images (`linux/amd64` and `linux/arm64`) are published to [Docker Hub](https://hub.docker.com/r/moqdev/moq-cli).

## Usage

### Publish a Video File

```bash
moq-cli publish --url https://relay.example.com/ --name my-stream fmp4 < video.mp4
```

### Publish from FFmpeg

```bash
ffmpeg -i input.mp4 -c:v libx264 -bsf:v h264_mp4toannexb -f h264 - \
  | moq-cli publish --url https://relay.example.com/ --name my-stream avc3
```
