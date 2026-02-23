# Redis-Ease

中文文档：[README_CN.md](README_CN.md)

A lightweight, easy-to-use Go library for quick Redis integration. It acts as a simple wrapper around the powerful `go-redis/redis` client, allowing for effortless setup for both single-node and cluster deployments.

## ✨ Features

-   Simple, configuration-driven setup.
-   Supports both single-node and Redis Cluster.
-   Convenience wrappers for common commands (`Get`, `Set`, `Del`, etc.).
-   Reliable, persistent message queues via Redis Streams.
-   Built-in consumer group and failure recovery mechanisms.
-   Global client for easy access anywhere in your project.

## 🚀 Installation

```sh
go get github.com/jackman0925/redis-ease
```

## 💡 Usage Examples

### 1. Initialization

First, configure and initialize the library when your application starts. This only needs to be done once.

```go
import "github.com/jackman0925/redis-ease"

func main() {
    config := redis_ease.Config{
        Addresses: []string{"localhost:6379"}, // For cluster, add more addresses
        Password:  "",
        DB:        0, // For single-node only
    }
    if err := redis_ease.InitWithError(config); err != nil {
        panic(err)
    }

    // Your application logic starts here...
}
```

### 2. Basic Key-Value Operations (Quick Start)

After initialization, you can call the helper functions from anywhere in your project.

```go
import (
    "context"
    "fmt"
    "time"
    "github.com/jackman0925/redis-ease"
    "github.com/redis/go-redis/v9"
)

func someFunction() {
    ctx := context.Background()

    // Set a value
    err := redis_ease.Set(ctx, "user:1", "John Doe", 10*time.Minute)
    if err != nil {
        panic(err)
    }

    // Get a value
    name, err := redis_ease.Get(ctx, "user:1")
    if err != nil {
        if err == redis.Nil {
            fmt.Println("user:1 does not exist")
            return
        }
        panic(err)
    }
    fmt.Println("User 1 is:", name)
}
```

### 3. Reliable Message Queue

The library provides a robust message queue using Redis Streams.

#### Producer Example

```go
// Add a job to the 'orders' queue
msg := map[string]interface{}{
    "order_id": 12345,
    "user_id":  "user-abc",
}
msgID, err := redis_ease.StreamAdd(context.Background(), "orders", msg)
if err != nil {
    panic(err)
}
fmt.Printf("Message added to stream 'orders' with ID: %s\n", msgID)
```

#### Consumer Example

This would typically run in a background goroutine.
```go
func processOrders() {
    for {
        // 1. Read one message from the 'orders' stream.
        msg, err := redis_ease.StreamConsume(context.Background(), "orders", "processing_group", "consumer_1")
        if err != nil {
            fmt.Println("Error consuming from stream:", err)
            continue
        }

        // 2. Process the message
        fmt.Printf("Processing message %s: %v\n", msg.ID, msg.Values)

        // 3. Acknowledge the message so it's not processed again
        err = redis_ease.StreamAck(context.Background(), "orders", "processing_group", msg.ID)
        if err != nil {
            fmt.Println("Error acknowledging message:", err)
        }
    }
}
```

#### Handling Failures & Reliability

If a consumer crashes before acknowledging a message, it becomes "pending". A recovery process is needed to claim and re-process these messages to prevent data loss.

```go
// This recovery process can run periodically in a separate goroutine
func recoverFailedMessages() {
    ctx := context.Background()

    for {
        // Check for stale messages every 5 minutes
        time.Sleep(5 * time.Minute)

        // Claim messages that have been idle for at least 5 minutes
        staleMsgs, err := redis_ease.StreamClaim(ctx, "orders", "processing_group", "recovery_consumer", 5*time.Minute)
        if err != nil {
            fmt.Println("Error claiming messages:", err)
            continue
        }

        if len(staleMsgs) > 0 {
            fmt.Printf("Claimed %d stale messages\n", len(staleMsgs))
        }

        // Re-process the claimed messages
        for _, msg := range staleMsgs {
            fmt.Printf("Re-processing message %s: %v\n", msg.ID, msg.Values)
            // Your business logic here...
            redis_ease.StreamAck(ctx, "orders", "processing_group", msg.ID)
        }
    }
}
```

### 4. Pub/Sub (Publish-Subscribe)

Use the Pub/Sub functions for real-time messaging between different parts of your application.

#### Subscriber Example

The `Subscribe` function runs in a background goroutine. You can use a `context` to manage its lifecycle.

To enable automatic reconnect, configure `SubscribeRetry` in `Config`.

```go
import (
    "context"
    "fmt"
    "time"
    "github.com/jackman0925/redis-ease"
    "github.com/redis/go-redis/v9"
)

func listenForUpdates() {
    // Create a context that can be cancelled.
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    fmt.Println("Subscribing to 'news' channel...")
    
    // The handler function will be called for each message.
    handler := func(msg *redis.Message) {
        fmt.Printf("Received message from channel '%s': %s\n", msg.Channel, msg.Payload)
    }

    // Subscribe to the channel. This starts a non-blocking goroutine.
    // If you need a "ready" signal, use SubscribeWithReady.
    redis_ease.Subscribe(ctx, "news", handler)

    // Keep the main goroutine alive to listen for messages.
    // In a real application, this would be part of your application's lifecycle.
    time.Sleep(1 * time.Minute) 
}
```

#### Publisher Example

```go
import (
    "context"
    "github.com/jackman0925/redis-ease"
)

func sendUpdate() {
    err := redis_ease.Publish(context.Background(), "news", "A new blog post has been published!")
    if err != nil {
        panic(err)
    }
}
```

### 5. Logging Configuration

By default, the library logs informational messages and errors to standard output. You can customize this behavior.

#### Changing the Log Level

You can change the verbosity of the default logger by setting the `LogLevel`.

```go
config := redis_ease.Config{
    // ... other settings
    LogLevel: redis_ease.LogLevelWarn, // Only log warnings and errors
}
if err := redis_ease.InitWithError(config); err != nil {
    panic(err)
}
```

Available levels: `LogLevelNone`, `LogLevelError`, `LogLevelWarn`, `LogLevelInfo`, `LogLevelDebug`.

#### Using a Custom Logger

For more advanced logging, you can provide your own logger that implements the `redis_ease.Logger` interface. This is useful for integrating with your application's existing logging setup (e.g., using `logrus`, `zap`, etc.).

```go
import (
    "github.com/sirupsen/logrus"
    "github.com/jackman0925/redis-ease"
)

// Create a custom logger that wraps logrus
type MyLogger struct {
    *logrus.Logger
}
func (l *MyLogger) Errorf(format string, v ...interface{}) { l.Errorf(format, v...) }
func (l *MyLogger) Warnf(format string, v ...interface{})  { l.Warnf(format, v...) }
func (l *MyLogger) Infof(format string, v ...interface{})  { l.Infof(format, v...) }
func (l *MyLogger) Debugf(format string, v ...interface{}) { l.Debugf(format, v...) }


func main() {
    // Create a new logrus logger
    logrusLogger := logrus.New()
    logrusLogger.SetFormatter(&logrus.JSONFormatter{})

    config := redis_ease.Config{
        // ... other settings
        Logger: &MyLogger{logrusLogger},
    }
    if err := redis_ease.InitWithError(config); err != nil {
        panic(err)
    }
}
```

#### Disabling Logging

To disable logging completely, set the log level to `LogLevelNone`.

```go
config := redis_ease.Config{
    // ... other settings
    LogLevel: redis_ease.LogLevelNone,
}
if err := redis_ease.InitWithError(config); err != nil {
    panic(err)
}
```

### 6. Connection & Pool Configuration

You can tune timeouts, retry behavior, connection pooling, and TLS.

```go
config := redis_ease.Config{
    Addresses: []string{"localhost:6379"},
    DB:        0,
    DialTimeout:     5 * time.Second,
    ReadTimeout:     2 * time.Second,
    WriteTimeout:    2 * time.Second,
    PoolSize:        50,
    MinIdleConns:    10,
    MaxRetries:      3,
    MinRetryBackoff: 100 * time.Millisecond,
    MaxRetryBackoff: 2 * time.Second,
    DefaultTimeout:  2 * time.Second,
    // TLSConfig: &tls.Config{...},
    // Metrics: &MyMetrics{},
    // Hook: &MyHook{},
}
if err := redis_ease.InitWithError(config); err != nil {
    panic(err)
}
```

### 7. Instance Clients (Recommended for Larger Projects)

You can create dedicated clients instead of relying on the global singleton. This is useful for multi-tenant apps, tests, or different Redis configs within the same process.

```go
config := redis_ease.Config{
    Addresses: []string{"localhost:6379"},
    DB:        1,
}

client, err := redis_ease.NewClientWithError(config)
if err != nil {
    panic(err)
}
defer client.Close()

_ = client.Set(context.Background(), "k", "v", 0)
```

You can also use SubscribeWithReady on instance clients:

```go
ready := make(chan struct{}, 1)
client.SubscribeWithReady(ctx, "news", handler, func() { ready <- struct{}{} })
<-ready
```

Default timeout only applies to non-blocking calls. It does not affect `Subscribe` or `StreamConsume*` to avoid unintended cancellations.

## 📚 API Reference

### Initialization

-   `Init(config Config)`: Initializes the global Redis client. Must be called once at application start.
-   `InitWithError(config Config) error`: Initializes the global Redis client and returns an error on failure.
-   `NewClient(config Config) *Client`: Creates a new instance client (panics on failure).
-   `NewClientWithError(config Config) (*Client, error)`: Creates a new instance client with error return.
-   `Close() error`: Closes the Redis client, releasing connections.
    -   `(*Client).Close() error`: Closes the instance client, releasing connections.

### Key-Value Functions

-   `Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error`
-   `Get(ctx context.Context, key string) (string, error)`
-   `Del(ctx context.Context, keys ...string) (int64, error)`
-   `HSet(ctx context.Context, key string, values ...interface{}) (int64, error)`
-   `HGet(ctx context.Context, key, field string) (string, error)`
-   `Exists(ctx context.Context, keys ...string) (int64, error)`
    -   `(*Client).Set(...)`, `(*Client).Get(...)`, `(*Client).Del(...)`, `(*Client).HSet(...)`, `(*Client).HGet(...)`, `(*Client).Exists(...)`

### Pub/Sub Functions

-   `Publish(ctx context.Context, channel string, message interface{}) error`: Publishes a message to a channel.
-   `Subscribe(ctx context.Context, channel string, handler func(msg *redis.Message))`: Subscribes to a channel and processes messages with a handler function. Runs in a background goroutine.
-   `SubscribeWithReady(ctx context.Context, channel string, handler func(msg *redis.Message), ready func())`: Like `Subscribe`, but invokes `ready` after each successful (re)subscription.
    -   `(*Client).Publish(...)`, `(*Client).Subscribe(...)`
    -   `(*Client).SubscribeWithReady(...)`

### Stream (Queue) Functions

-   `StreamAdd(ctx context.Context, streamName string, values map[string]interface{}) (string, error)`: Adds a message to the stream.
-   `StreamConsume(ctx context.Context, streamName, groupName, consumerName string) (*redis.XMessage, error)`: Reads a single message, blocking forever until one is available.
-   `StreamConsumeAdvanced(ctx context.Context, streamName, groupName, consumerName string, block time.Duration, count int64) ([]redis.XMessage, error)`: Reads multiple messages with a specific blocking timeout.
-   `StreamAck(ctx context.Context, streamName, groupName, messageID string) error`: Acknowledges a message as successfully processed.
-   `StreamClaim(ctx context.Context, streamName, groupName, consumerName string, minIdleTime time.Duration) ([]redis.XMessage, error)`: Claims messages that were left pending by a failed consumer.
-   `StreamPendingSummary(ctx context.Context, streamName, groupName string) (*redis.XPending, error)`: Returns pending summary for a group.
-   `StreamPendingList(ctx context.Context, streamName, groupName, start, end string, count int64, consumer string) ([]redis.XPendingExt, error)`: Returns pending entries for a group.
-   `StreamPendingCount(ctx context.Context, streamName, groupName string) (int64, error)`: Returns number of pending entries for a group.
    -   `(*Client).StreamAdd(...)`, `(*Client).StreamConsume(...)`, `(*Client).StreamConsumeAdvanced(...)`, `(*Client).StreamAck(...)`, `(*Client).StreamClaim(...)`
    -   `(*Client).StreamPendingSummary(...)`, `(*Client).StreamPendingList(...)`, `(*Client).StreamPendingCount(...)`

### Advanced Usage

-   `GetClient() redis.UniversalClient`: For advanced use cases, you can retrieve the underlying `go-redis` client to access features like transactions and scripting.

## ✅ Production Guidance

-   Connection pool sizing: size `PoolSize` based on peak concurrent in-flight Redis commands per instance, not CPU cores.
-   Timeouts: set `DefaultTimeout` for non-blocking operations, and keep `ReadTimeout`/`WriteTimeout` aligned with your SLO.
-   Retries: avoid high `MaxRetries` on write-heavy workloads to prevent thundering herds during Redis outages.
-   Streams: make message handling idempotent; on crash recovery, use `StreamPendingSummary`/`StreamPendingList` to inspect backlog.
-   Backlog management: periodically `StreamClaim` stale messages and consider a dead-letter stream for poisoned messages.
-   Observability: export metrics for latency, error rate, and pending backlog size per consumer group.

## ☠️ Dead-Letter Streams & Replay

When messages repeatedly fail processing, move them to a dedicated dead-letter stream (DLQ) instead of retrying forever.

Example pattern:

```go
const (
    mainStream = "orders"
    dlqStream  = "orders:dlq"
    maxRetries = 5
)

func process(msg *redis.XMessage) error {
    // your business logic
    return nil
}

func handleMessage(msg *redis.XMessage) {
    err := process(msg)
    if err == nil {
        _ = redis_ease.StreamAck(context.Background(), mainStream, "processing_group", msg.ID)
        return
    }

    // simple retry counter stored in message field
    retries, _ := msg.Values["retries"].(string)
    if retries == "" {
        retries = "0"
    }
    // parse & increment (left as an exercise)

    if /* retries >= maxRetries */ false {
        // move to DLQ
        _, _ = redis_ease.StreamAdd(context.Background(), dlqStream, map[string]interface{}{
            "original_id": msg.ID,
            "error":       err.Error(),
            "payload":     msg.Values,
        })
        _ = redis_ease.StreamAck(context.Background(), mainStream, "processing_group", msg.ID)
        return
    }

    // otherwise keep pending; it will be reclaimed later
}
```

Replay from DLQ (manual or scheduled job):

```go
func replayDLQ() {
    ctx := context.Background()
    // read from DLQ, re-validate, then re-add to main stream
    msg, err := redis_ease.StreamConsume(ctx, dlqStream, "dlq_group", "replayer")
    if err != nil {
        return
    }
    _, _ = redis_ease.StreamAdd(ctx, mainStream, msg.Values)
    _ = redis_ease.StreamAck(ctx, dlqStream, "dlq_group", msg.ID)
}
```

## 🧪 Test & Bench Automation

Default unit tests:

```sh
go test ./...
```

Benchmarks (opt-in):

```sh
REDIS_BENCH=1 go test -run '^$' -bench . ./...
```

Reconnect integration test:

```sh
REDIS_E2E_RECONNECT=1 \\
REDIS_E2E_RECONNECT_CMD="redis-cli shutdown; redis-server --daemonize yes" \\
go test -run TestSubscribeReconnectIntegration ./...
```

TLS integration test:

```sh
REDIS_E2E_TLS=1 \\
REDIS_E2E_TLS_ADDR=localhost:6380 \\
REDIS_E2E_TLS_CA=/path/to/ca.pem \\
go test -run TestTLSIntegration ./...
```

Cluster integration test:

```sh
REDIS_E2E_CLUSTER=1 \\
REDIS_E2E_CLUSTER_ADDRS="localhost:7000,localhost:7001,localhost:7002" \\
go test -run TestClusterIntegration ./...
```

Sentinel integration test:

```sh
REDIS_E2E_SENTINEL=1 \\
REDIS_E2E_SENTINEL_ADDRS="localhost:26379,localhost:26380" \\
REDIS_E2E_SENTINEL_MASTER=mymaster \\
go test -run TestSentinelIntegration ./...
```

## 📈 Metrics Hook

You can inject a metrics collector to observe latency and error results.

```go
type MyMetrics struct{}

func (m *MyMetrics) ObserveDuration(op string, d time.Duration, err error) {
    // Export to Prometheus, OpenTelemetry, etc.
}

config := redis_ease.Config{
    Addresses: []string{"localhost:6379"},
    SubscribeRetry: redis_ease.SubscribeRetryConfig{
        Enabled:    true,
        MinBackoff: 200 * time.Millisecond,
        MaxBackoff: 5 * time.Second,
        MaxRetries: 0, // 0 means unlimited
        Jitter:     0.2,
        OnRetry: func(attempt int, wait time.Duration, err error) {
            _ = attempt
            _ = wait
            _ = err
        },
    },
    Metrics:   &MyMetrics{},
}
if err := redis_ease.InitWithError(config); err != nil {
    panic(err)
}
```

## 🧩 Hook (Before/After)

Use hooks to wrap operations, for example to start and finish tracing spans.

```go
type MyHook struct{}

func (h *MyHook) Before(op string) {
    // Start span
}

func (h *MyHook) After(op string, err error, d time.Duration) {
    // End span and record error
}

config := redis_ease.Config{
    Addresses: []string{"localhost:6379"},
    Hook:      &MyHook{},
}
if err := redis_ease.InitWithError(config); err != nil {
    panic(err)
}
```

## 🔎 OpenTelemetry Example

This shows how to use `Hook` to wrap operations with spans using OpenTelemetry.

```go
import (
    "context"
    "time"
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/attribute"
    "go.opentelemetry.io/otel/trace"
)

type OtelHook struct {
    tracer trace.Tracer
}

type spanKey struct{}

func NewOtelHook() *OtelHook {
    return &OtelHook{tracer: otel.Tracer("redis-ease")}
}

func (h *OtelHook) Before(ctx context.Context, op string) context.Context {
    ctx, span := h.tracer.Start(ctx, "redis."+op)
    return trace.ContextWithSpan(ctx, span)
}

func (h *OtelHook) After(ctx context.Context, op string, err error, d time.Duration) {
    span := trace.SpanFromContext(ctx)
    if !span.IsRecording() {
        return
    }
    span.SetAttributes(
        attribute.String("db.system", "redis"),
        attribute.String("redis.operation", op),
        attribute.Int64("redis.duration_ms", d.Milliseconds()),
    )
    if err != nil {
        span.RecordError(err)
    }
    span.End()
}

config := redis_ease.Config{
    Addresses: []string{"localhost:6379"},
    Hook:      NewOtelHook(),
}
if err := redis_ease.InitWithError(config); err != nil {
    panic(err)
}
```

## 🧵 Context Propagation Example

If you attach trace/span to the context in your application, the Hook will receive it:

```go
ctx := trace.ContextWithSpan(context.Background(), span)
redis_ease.Set(ctx, "k", "v", 0)
```

## 📄 License

This project is licensed under the [LICENSE](LICENSE) file.
