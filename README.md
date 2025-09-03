# Redis-Ease

A lightweight, easy-to-use Go library for quick Redis integration. It acts as a simple wrapper around the powerful `go-redis/redis` client, allowing for effortless setup for both single-node and cluster deployments.

## Features

-   Simple, configuration-driven setup.
-   Supports both single-node and Redis Cluster.
-   **Convenience wrappers** for common commands (`Get`, `Set`, `Del`, etc.).
-   Provides a global client for easy access anywhere in your project.
-   Built on the reliable `go-redis/redis/v9`.

## Installation

To get started, add the library to your Go project. First, initialize Go modules if you haven't already:
```sh
go mod init your-project-name
```

Then, get the library:
```sh
go get redis-ease
```
*(Note: This assumes the module path is `redis-ease`. The actual import path will depend on the final repository location.)*

## Quick Start

After initializing the library, you can use the simple helper functions for most operations.

```go
package main

import (
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"redis-ease" // Use your module path here
)

func main() {
	// Configure and initialize once
	config := redis_ease.Config{
		Addresses: []string{"localhost:6379"},
		IsCluster: false,
	}
	redis_ease.Init(config)

	// ---

	// Set a value
	err := redis_ease.Set("name", "Tester", 10*time.Minute)
	if err != nil {
		panic(err)
	}
	fmt.Println("Set 'name' to 'Tester'")

	// Get a value
	name, err := redis_ease.Get("name")
	if err != nil {
		// Handle key not found
		if err == redis.Nil {
			fmt.Println("'name' does not exist.")
			return
		}
		panic(err)
	}
	fmt.Println("Got 'name':", name)

	// Delete a key
	keysDeleted, err := redis_ease.Del("name")
	if err != nil {
		panic(err)
	}
	fmt.Printf("Deleted %d key(s)\n", keysDeleted)
    
    // Check if a key exists
    count, err := redis_ease.Exists("name")
    if err != nil {
        panic(err)
    }
    if count == 0 {
        fmt.Println("'name' no longer exists.")
    }
}
```

## API

### `Init(config Config)`

Initializes the global Redis client. This function should be called once when your application starts. It will panic if the configuration is invalid or it fails to connect to the Redis server(s).

The `Config` struct has the following fields:

-   `Addresses []string`: A slice of `host:port` strings. Provide one for single-node, multiple for a cluster.
-   `Password string`: The Redis password. Leave empty if none.
-   `DB int`: The database number to use (only for single-node mode).
-   `IsCluster bool`: Set to `true` for a Redis Cluster connection.

### Convenience Functions

These are the recommended way to interact with Redis for most common use cases.

-   `Set(key string, value interface{}, expiration time.Duration) error`
-   `Get(key string) (string, error)`
-   `Del(keys ...string) (int64, error)`
-   `HSet(key string, values ...interface{}) (int64, error)`
-   `HGet(key, field string) (string, error)`
-   `Exists(keys ...string) (int64, error)`

### `GetClient() redis.Cmdable`

For advanced use cases, you can retrieve the underlying `go-redis` client. This gives you access to all of its features, such as transactions and scripting.

It will panic if `Init()` has not been called first.

**Example:**
```go
// Get the underlying client for a transaction
client := redis_ease.GetClient()

ctx := context.Background()
pipe := client.TxPipeline()
pipe.Incr(ctx, "counter")
pipe.Expire(ctx, "counter", time.Hour)
_, err := pipe.Exec(ctx)
if err != nil {
    panic(err)
}
```
