# Redis-Ease

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
        IsCluster: false,
    }
    redis_ease.Init(config)

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

The `Subscribe` function runs in a background goroutine and will automatically reconnect if the connection is lost. You can use a `context` to manage its lifecycle.

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
redis_ease.Init(config)
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
    redis_ease.Init(config)
}
```

#### Disabling Logging

To disable logging completely, set the log level to `LogLevelNone`.

```go
config := redis_ease.Config{
    // ... other settings
    LogLevel: redis_ease.LogLevelNone,
}
redis_ease.Init(config)
```

## 📚 API Reference

### Initialization

-   `Init(config Config)`: Initializes the global Redis client. Must be called once at application start.
-   `Close() error`: Closes the Redis client, releasing connections.

### Key-Value Functions

-   `Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error`
-   `Get(ctx context.Context, key string) (string, error)`
-   `Del(ctx context.Context, keys ...string) (int64, error)`
-   `HSet(ctx context.Context, key string, values ...interface{}) (int64, error)`
-   `HGet(ctx context.Context, key, field string) (string, error)`
-   `Exists(ctx context.Context, keys ...string) (int64, error)`

### Pub/Sub Functions

-   `Publish(ctx context.Context, channel string, message interface{}) error`: Publishes a message to a channel.
-   `Subscribe(ctx context.Context, channel string, handler func(msg *redis.Message))`: Subscribes to a channel and processes messages with a handler function. Runs in a background goroutine.

### Stream (Queue) Functions

-   `StreamAdd(ctx context.Context, streamName string, values map[string]interface{}) (string, error)`: Adds a message to the stream.
-   `StreamConsume(ctx context.Context, streamName, groupName, consumerName string) (*redis.XMessage, error)`: Reads a single message, blocking forever until one is available.
-   `StreamConsumeAdvanced(ctx context.Context, streamName, groupName, consumerName string, block time.Duration, count int64) ([]redis.XMessage, error)`: Reads multiple messages with a specific blocking timeout.
-   `StreamAck(ctx context.Context, streamName, groupName, messageID string) error`: Acknowledges a message as successfully processed.
-   `StreamClaim(ctx context.Context, streamName, groupName, consumerName string, minIdleTime time.Duration) ([]redis.XMessage, error)`: Claims messages that were left pending by a failed consumer.

### Advanced Usage

-   `GetClient() redis.UniversalClient`: For advanced use cases, you can retrieve the underlying `go-redis` client to access features like transactions and scripting.

## 📄 License

This project is licensed under the [LICENSE](LICENSE) file.