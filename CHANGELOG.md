# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [v0.1.0] - 2026-02-23

### Added
- `InitWithError` 与 `NewClientWithError/NewClient`，支持错误返回及多实例客户端。
- 新增实例客户端方法覆盖 (`Set/Get/Del/HSet/HGet/Exists/Publish/Subscribe/Stream*`)。
- 支持 `DefaultTimeout`（非阻塞操作默认超时）。
- 连接与池化配置项：`DialTimeout/ReadTimeout/WriteTimeout/PoolSize/MinIdleConns/MaxRetries/MinRetryBackoff/MaxRetryBackoff/TLSConfig`。
- Sentinel 支持 `MasterName` 配置。
- Pub/Sub 重连配置 `SubscribeRetryConfig`（backoff + jitter + `OnRetry`）。
- `SubscribeWithReady`（订阅建立/重连后的 ready 回调）。
- Streams 观测辅助：`StreamPendingSummary/StreamPendingList/StreamPendingCount`。
- Metrics hook (`MetricsCollector`) 与 Hook (`Hook`) 支持（含 context 传递）。
- OpenTelemetry、Hook、Metrics、订阅重连、DLQ、测试/基准等文档示例。
- 基准测试 `BenchmarkSet/BenchmarkGet/BenchmarkSetWithTimeout`。
- 集成测试脚手架：订阅重连、TLS、Cluster、Sentinel。
- `StreamClaim` 增加基于 `XAutoClaim` 的实现以防止潜在的并发竞态条件。
- 增加 `Close()` 函数以优雅地关闭客户端并释放底层连接资源。
- 增加日志接口 (`Logger`) 和日志级别 (`LogLevel`)，允许用户注入自定义的日志记录器。
- 新增 `LogLevelNone` 级别以完全禁用日志。
- 新增 `discardLogger` 实现，用于忽略所有日志输出。

### Changed
- `Subscribe` 支持自动重连与抖动回退策略。
- 非阻塞操作可自动附加默认超时（当 context 未设置 deadline）。
- Hook 接口支持 context 传递以便 tracing。
- 升级 Redis 客户端初始化逻辑：底层不再使用 `redis.NewClient` 或 `redis.NewClusterClient`，而是采用 `redis.NewUniversalClient` 自动适配单机、哨兵及集群模式。
- 对所有的读写及订阅 API (如 `Get`、`Set`、`Del`、`Publish`、`Subscribe`、`Stream*` 等) 增加了 `ctx context.Context` 参数支持，便于调用方做超时控制和链路取消。
- 重构了内置的默认日志记录器，以实现更灵活的日志级别控制。
- `Init` 函数现在支持日志记录器的配置。

## [v0.0.2] - 2025-11-25

### Added
- Pub/Sub (Publish-Subscribe) functionality with `Publish` and `Subscribe` methods.
- Comprehensive usage examples and API reference for Pub/Sub in `README.md`.
- Unit tests for Pub/Sub functionality.
