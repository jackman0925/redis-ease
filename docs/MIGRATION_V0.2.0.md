# Migrating to v0.2.0

v0.2.0 focuses on lifecycle safety, truthful tests, and maintainable package structure.

## Compatible Usage

Existing package-level calls remain available:

```go
redis_ease.Init(config)
redis_ease.Set(ctx, "key", "value", 0)
value, err := redis_ease.Get(ctx, "key")
```

For production services, prefer explicit clients:

```go
client, err := redis_ease.NewClientWithError(config)
if err != nil {
    return err
}
defer client.Close()
```

## Behavior Changes

- Calling `InitWithError` after the default client is initialized returns
  `ErrAlreadyInitialized` instead of silently ignoring the new configuration.
- The zero log-level value now means `LogLevelDefault`, which resolves to `LogLevelInfo`.
- Use `LogLevelNone` to explicitly disable built-in logging.
- `SubscribeRetryConfig` applies to initial subscription establishment. Runtime network
  reconnect and re-subscription are handled by go-redis.
- `Close` stops managed subscriptions and is safe to call more than once.
- Use `Shutdown(ctx)` during service shutdown when the process must wait for subscribers to exit.

## Test Changes

Normal unit tests no longer require `localhost:6379` and never execute `FLUSHDB`.
Environment-backed tests are opt-in and use the `REDIS_E2E_*` variables documented in the
README files.
