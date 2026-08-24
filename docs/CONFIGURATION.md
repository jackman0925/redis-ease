# Configuration Reference

`Config` is shared by the package-level and instance constructors. Zero values delegate to
go-redis defaults unless documented otherwise.

## Connection

| Field | Purpose | Default |
| --- | --- | --- |
| `Addresses` | Required Redis, cluster seed, or Sentinel addresses | none |
| `Username` / `Password` | Redis ACL credentials | empty |
| `DB` | Database for single-node or Sentinel mode | `0` |
| `ClientName` | Redis connection name | empty |
| `MasterName` | Enables Sentinel mode and selects its master | empty |
| `SentinelUsername` / `SentinelPassword` | Sentinel credentials | empty |
| `IsClusterMode` | Forces cluster mode for one configuration endpoint | `false` |
| `TLSConfig` | Enables TLS | `nil` |

Redis Cluster supports only database zero. Use `DB: 0` for cluster deployments.

## Timeouts and Retries

| Field | Purpose | Default |
| --- | --- | --- |
| `InitTimeout` | Constructor `PING` deadline | 5 seconds |
| `DefaultTimeout` | Deadline for non-blocking wrappers when caller has none | disabled |
| `DialTimeout` / `ReadTimeout` / `WriteTimeout` | Socket limits | go-redis default |
| `MaxRetries` | Command retry count | go-redis default |
| `MinRetryBackoff` / `MaxRetryBackoff` | Command retry delay range | go-redis default |

`DefaultTimeout` does not apply to `Subscribe`, `StreamConsume`, or
`StreamConsumeAdvanced`; callers control those lifetimes with context and stream block settings.

## Pool

`PoolSize`, `PoolTimeout`, `MinIdleConns`, `MaxIdleConns`, `MaxActiveConns`,
`ConnMaxIdleTime`, and `ConnMaxLifetime` map directly to go-redis pool settings. Start with
go-redis defaults and tune from measured concurrency, latency, and saturation metrics.

## Observability

- `Logger` replaces the built-in logger.
- `LogLevelDefault` resolves to `LogLevelInfo`.
- `LogLevelNone` disables built-in logging.
- `Metrics` receives command name, duration, and original error.
- `Hook` can add tracing state to the command context.

Instrumentation callback panics are recovered and logged so they do not break Redis commands.

## Pub/Sub

`SubscribeRetry` applies only while establishing the initial subscription. Once established,
go-redis automatically reconnects and re-subscribes after network failures. `OnRetry` receives
the failed initial attempt, selected backoff, and original error.

Use Redis Streams rather than Pub/Sub when consumers must recover messages sent while offline.
