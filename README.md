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
    "fmt"
    "github.com/jackman0925/redis-ease"
    "github.com/redis/go-redis/v9"
)

func someFunction() {
    // Set a value
    err := redis_ease.Set("user:1", "John Doe", 10*time.Minute)
    if err != nil {
        panic(err)
    }

    // Get a value
    name, err := redis_ease.Get("user:1")
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
msgID, err := redis_ease.StreamAdd("orders", msg)
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
        msg, err := redis_ease.StreamConsume("orders", "processing_group", "consumer_1")
        if err != nil {
            fmt.Println("Error consuming from stream:", err)
            continue
        }

        // 2. Process the message
        fmt.Printf("Processing message %s: %v\n", msg.ID, msg.Values)

        // 3. Acknowledge the message so it's not processed again
        err = redis_ease.StreamAck("orders", "processing_group", msg.ID)
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
    for {
        // Check for stale messages every 5 minutes
        time.Sleep(5 * time.Minute)

        // Claim messages that have been idle for at least 5 minutes
        staleMsgs, err := redis_ease.StreamClaim("orders", "processing_group", "recovery_consumer", 5*time.Minute)
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
            redis_ease.StreamAck("orders", "processing_group", msg.ID)
        }
    }
}
```

## 📚 API Reference

### Initialization

-   `Init(config Config)`: Initializes the global Redis client. Must be called once at application start.

### Key-Value Functions

-   `Set(key string, value interface{}, expiration time.Duration) error`
-   `Get(key string) (string, error)`
-   `Del(keys ...string) (int64, error)`
-   `HSet(key string, values ...interface{}) (int64, error)`
-   `HGet(key, field string) (string, error)`
-   `Exists(keys ...string) (int64, error)`

### Stream (Queue) Functions

-   `StreamAdd(streamName string, values map[string]interface{}) (string, error)`: Adds a message to the stream.
-   `StreamConsume(streamName, groupName, consumerName string) (*redis.XMessage, error)`: Reads a single message, blocking forever until one is available.
-   `StreamConsumeAdvanced(streamName, groupName, consumerName string, block time.Duration, count int64) ([]redis.XMessage, error)`: Reads multiple messages with a specific blocking timeout.
-   `StreamAck(streamName, groupName, messageID string) error`: Acknowledges a message as successfully processed.
-   `StreamClaim(streamName, groupName, consumerName string, minIdleTime time.Duration) ([]redis.XMessage, error)`: Claims messages that were left pending by a failed consumer.

### Advanced Usage

-   `GetClient() redis.Cmdable`: For advanced use cases, you can retrieve the underlying `go-redis` client to access features like transactions and scripting.

## 📄 License

This project is licensed under the [LICENSE](LICENSE) file.