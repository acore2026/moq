# MOQ Protocol Python Implementation - Project Documentation

## 项目概述

基于 Python 的 MOQ Transport (MOQT) 协议实现，符合 draft-ietf-moq-transport-17 规范。

## 文档索引

1. [README.md](README.md) - 项目介绍和基本使用
2. [QUICKSTART.md](QUICKSTART.md) - 快速开始指南
3. [ARCHITECTURE.md](ARCHITECTURE.md) - 系统架构设计文档
4. [API.md](API.md) - API 接口文档
5. [examples/README.md](examples/README.md) - 示例代码说明

## 关键特性

- 完整实现 MOQ Transport 协议 draft-17
- 支持 Publisher、Subscriber、Relay 三角色
- 智能双层缓存 (内存 + 磁盘)
- 断线重连和数据续传
- 完整的日志和指标系统
- SDK 化设计，易于集成

## 项目结构

```
moq-py/
├── moq/                  # 核心代码包
│   ├── protocol/         # 协议层
│   ├── transport/        # 传输层
│   ├── cache/            # 缓存层
│   └── utils/            # 工具函数
├── examples/             # 示例代码
├── tests/                # 测试代码
└── docs/                 # 文档
```

## 快速开始

```bash
# 安装依赖
pip install -r requirements.txt
pip install -e .

# 启动 Relay
python examples/relay/basic_relay.py

# 启动 Publisher
python examples/pub/basic_publisher.py

# 启动 Subscriber
python examples/sub/basic_subscriber.py
```

## 技术栈

- **语言**: Python 3.8+
- **QUIC 框架**: aioquic
- **异步框架**: asyncio
- **日志**: logging 模块
- **缓存**: SQLite + 文件系统

## 协议规范

- [draft-ietf-moq-transport-17](https://www.ietf.org/archive/id/draft-ietf-moq-transport-17.txt)

## 贡献指南

欢迎提交 Issue 和 Pull Request。

## 许可证

MIT License
