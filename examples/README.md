# 示例代码

本目录包含 MOQ 协议的使用示例。

## 目录结构

```
examples/
├── pub/                    # 发布者示例
│   └── basic_publisher.py
├── sub/                    # 订阅者示例
│   ├── basic_subscriber.py
│   └── reconnection_resume.py
└── relay/                  # 中继示例
    └── basic_relay.py
```

## 示例说明

### basic_publisher.py

基础发布者示例，展示如何:
- 连接到中继
- 声明命名空间
- 发布媒体对象

### basic_subscriber.py

基础订阅者示例，展示如何:
- 连接到中继
- 订阅轨道
- 接收媒体对象

### reconnection_resume.py

断线重连示例，展示如何:
- 订阅带 ABSOLUTE_START 过滤器
- 断开连接并保存恢复点
- 重连后从断点恢复

### basic_relay.py

基础中继服务器示例，展示如何:
- 启动中继服务器
- 处理发布者和订阅者连接
- 转发和缓存媒体对象

## 运行顺序

1. 先启动 Relay:
   ```bash
   python examples/relay/basic_relay.py
   ```

2. 然后启动 Publisher:
   ```bash
   python examples/pub/basic_publisher.py
   ```

3. 最后启动 Subscriber:
   ```bash
   python examples/sub/basic_subscriber.py
   ```
