# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- 增加日志接口 (`Logger`) 和日志级别 (`LogLevel`)，允许用户注入自定义的日志记录器。
- 新增 `LogLevelNone` 级别以完全禁用日志。
- 新增 `discardLogger` 实现，用于忽略所有日志输出。

### Changed
- 重构了内置的默认日志记录器，以实现更灵活的日志级别控制。
- `Init` 函数现在支持日志记录器的配置。

## [v0.0.2] - 2025-11-25

### Added
- Pub/Sub (Publish-Subscribe) functionality with `Publish` and `Subscribe` methods.
- Comprehensive usage examples and API reference for Pub/Sub in `README.md`.
- Unit tests for Pub/Sub functionality.
