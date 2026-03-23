# MOQT 连接迁移支持

本文档描述 MOQT 实现的连接迁移功能。

## 概述

连接迁移允许客户端在 IP 地址改变时（例如从 WiFi 切换到移动网络）保持 QUIC 连接不中断。这对于移动设备上的实时流媒体传输非常重要。

## 使用方法

### 1. 启用连接迁移

在创建 `DialerOptions` 时设置 `EnableMigration: true`：

```go
Options := moqt.DialerOptions{
    ALPNs: ALPNS,
    QuicConfig: &quic.Config{
        EnableDatagrams: true,
    },
    EnableMigration: true, // 启用连接迁移
}
```

### 2. 创建订阅者

```go
sub := api.NewMOQSub(Options, RELAY)
handler, err := sub.Connect()
if err != nil {
    log.Error().Msgf("Error - %s", err)
    return
}
```

### 3. 处理流

连接迁移对应用程序是透明的。当连接迁移发生时，流会自动继续接收数据：

```go
sub.OnStream(func(ss moqt.SubStream) {
    go handleStream(&ss)
})
```

## 工作原理

### 连接迁移流程

1. **初始连接**: 客户端使用默认网络接口建立连接
2. **网络监控**: 后台 goroutine 定期监控本地网络接口变化
3. **路径探测**: 当检测到新网络接口时，尝试添加新路径
4. **路径切换**: 新路径验证成功后，自动切换到新路径
5. **连接恢复**: 连接使用新的 IP 地址继续传输数据

### 关键特性

- **自动检测**: 自动检测网络接口变化（WiFi ↔ 移动网络）
- **无缝切换**: 在路径切换过程中，数据流不会中断
- **多路径支持**: 支持同时维护多个路径

## 示例

```go
package main

import (
    "github.com/DineshAdhi/moq-go/moqt"
    "github.com/DineshAdhi/moq-go/moqt/api"
    "github.com/quic-go/quic-go"
    "github.com/rs/zerolog/log"
)

func main() {
    Options := moqt.DialerOptions{
        ALPNs: []string{"moq-00"},
        QuicConfig: &quic.Config{
            EnableDatagrams: true,
        },
        EnableMigration: true, // 启用连接迁移
    }

    sub := api.NewMOQSub(Options, "relay.example.com:4443")
    handler, err := sub.Connect()
    if err != nil {
        log.Fatal().Err(err).Msg("Failed to connect")
    }

    // 订阅频道
    handler.Subscribe("live", "stream1", 0)

    // 处理流 - 即使 IP 改变也会继续接收
    sub.OnStream(func(ss moqt.SubStream) {
        // 处理流数据...
    })

    <-sub.Ctx.Done()
}
```

## 注意事项

1. **服务器支持**: 连接迁移需要服务器也支持该功能
2. **Connection ID**: 服务器需要配置非零长度的 Connection ID
3. **防火墙**: 某些防火墙可能会阻止连接迁移
4. **移动网络**: 在移动网络之间切换时，可能会有短暂的数据包丢失

## 故障排除

### 连接迁移不工作

- 检查服务器是否支持连接迁移
- 确保 `EnableMigration` 设置为 `true`
- 查看日志中是否有迁移相关的警告信息

### 切换后数据丢失

- 这是正常行为，QUIC 会在新路径上重传丢失的数据
- 应用程序应该能够处理重复或乱序的数据包

## 实现细节

连接迁移功能基于 quic-go 库的路径迁移实现：

- 使用 `quic.Conn.AddPath()` 添加新路径
- 使用 `path.Probe()` 验证路径可用性
- 使用 `path.Switch()` 切换到新路径

监控逻辑位于 `moqt/migratingconn.go` 文件中。