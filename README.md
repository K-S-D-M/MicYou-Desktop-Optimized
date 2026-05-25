# MicYou Desktop Optimizations

MicYou 电脑端优化代码

## 优化内容

1. **启动速度优化** - 异步初始化 LookAndFeel
2. **连接稳定性优化** - 添加 Pong 超时检测（15秒）
3. **TCP 连接优化** - 添加 keepAlive、noDelay、socketTimeout
4. **mDNS 优化** - 支持端口变化时自动更新广播

## 文件清单

- main.kt - 异步初始化 LookAndFeel
- ConnectionHandler.kt - Pong 超时检测、握手超时
- NetworkServer.kt - TCP socket 选项优化
- AudioEngine.jvm.kt - mDNS 延迟启动、端口变化支持

## 使用方法

将这些文件复制到原项目对应位置：

```bash
# 备份原文件
cp MicYou/composeApp/src/jvmMain/kotlin/com/lanrhyme/micyou/main.kt MicYou/composeApp/src/jvmMain/kotlin/com/lanrhyme/micyou/main.kt.bak
cp MicYou/composeApp/src/jvmMain/kotlin/com/lanrhyme/micyou/network/ConnectionHandler.kt MicYou/composeApp/src/jvmMain/kotlin/com/lanrhyme/micyou/network/ConnectionHandler.kt.bak
cp MicYou/composeApp/src/jvmMain/kotlin/com/lanrhyme/micyou/network/NetworkServer.kt MicYou/composeApp/src/jvmMain/kotlin/com/lanrhyme/micyou/network/NetworkServer.kt.bak
cp MicYou/composeApp/src/jvmMain/kotlin/com/lanrhyme/micyou/AudioEngine.jvm.kt MicYou/composeApp/src/jvmMain/kotlin/com/lanrhyme/micyou/AudioEngine.jvm.kt.bak

# 复制优化文件
cp composeApp/src/jvmMain/kotlin/com/lanrhyme/micyou/main.kt MicYou/composeApp/src/jvmMain/kotlin/com/lanrhyme/micyou/
cp composeApp/src/jvmMain/kotlin/com/lanrhyme/micyou/network/ConnectionHandler.kt MicYou/composeApp/src/jvmMain/kotlin/com/lanrhyme/micyou/network/
cp composeApp/src/jvmMain/kotlin/com/lanrhyme/micyou/network/NetworkServer.kt MicYou/composeApp/src/jvmMain/kotlin/com/lanrhyme/micyou/network/
cp composeApp/src/jvmMain/kotlin/com/lanrhyme/micyou/AudioEngine.jvm.kt MicYou/composeApp/src/jvmMain/kotlin/com/lanrhyme/micyou/
```

## 优化详情

### 1. main.kt
- 使用 `Dispatchers.IO` 异步初始化 LookAndFeel
- 减少启动时的主线程阻塞

### 2. ConnectionHandler.kt
- 添加 Pong 超时检测（15秒），及时发现客户端静默断开
- 握手操作添加 10 秒超时
- 重同步逻辑添加 1MB 扫描限制，防止无限循环

### 3. NetworkServer.kt
- TCP socket 启用 `keepAlive` 更快检测死连接
- 启用 `noDelay` 降低音频传输延迟
- 设置 `socketTimeout` 防止半开连接阻塞

### 4. AudioEngine.jvm.kt
- 延迟 mDNS 广播到 `start()` 时启动
- 支持端口变化时自动更新 mDNS 广播
- 停止时正确清理 mDNS 资源

## 原项目

https://github.com/LanRhyme/MicYou
