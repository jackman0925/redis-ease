# Redis-Ease v0.2.0 Stability Contract

This document defines the stability and maintenance contract for v0.2.0.

## Goals

- Keep every Go source and test file below 500 lines.
- Make the instance client the single implementation path.
- Keep package-level functions as compatibility wrappers around one default client.
- Make initialization, shutdown, subscriptions, and instrumentation safe under concurrency.
- Separate deterministic unit tests from opt-in Redis integration tests.
- Require `go vet`, unit tests, race tests, and the line-count check to pass before release.
- Keep statement coverage at or above 80% for the default unit-test suite.

## Client Lifecycle

- `NewClientWithError` is the recommended constructor.
- `InitWithError` configures the package-level compatibility client.
- Initializing the package-level client twice returns `ErrAlreadyInitialized`.
- `Close` is idempotent.
- `Close` cancels subscriptions and closes Redis connections without waiting on user callbacks.
- `Shutdown(ctx)` additionally waits for subscriber goroutines with a caller-controlled deadline.
- Commands racing with `Close` either complete before shutdown or return `ErrClientClosed`.

## Logging

- An omitted log level uses `LogLevelInfo`.
- `LogLevelNone` explicitly disables the built-in logger.
- User-provided logger, metrics, hook, retry, and ready callbacks are isolated from panics.
- Recovered callback panics are reported through the configured logger.

## Pub/Sub

- go-redis owns network reconnect and re-subscription after a subscription is established.
- `SubscribeRetryConfig` controls only initial subscription establishment retries.
- `SubscribeWithReady` calls `ready` after initial establishment and after go-redis emits a
  subsequent subscription event.
- The client lifecycle owns subscriber goroutines; `Close` cancels them and `Shutdown(ctx)`
  waits for them with a caller-controlled deadline.
- Pub/Sub is at-most-once and is not a replacement for Redis Streams when delivery guarantees
  are required.

## Testing

- `go test ./...` must never connect to or mutate an external Redis instance implicitly.
- Unit tests use an isolated in-process Redis-compatible server where supported.
- Integration tests require explicit environment variables and use unique key prefixes.
- No test may call `FLUSHDB` against a user-provided Redis endpoint.
- Missing integration dependencies must produce a skipped integration test, not skip unit tests.

## Release Gates

Run all of the following before publishing:

```sh
gofmt -w *.go
go vet ./...
go test ./...
go test -race ./...
./scripts/check_go_file_size.sh
```

The same gates can be run with `./scripts/check_quality.sh` and are enforced by CI.

Optional environment-backed integration tests are documented in `README.md` and
`README_CN.md`.
