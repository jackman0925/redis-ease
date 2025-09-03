package redis_ease

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Config holds the configuration for the Redis client.
type Config struct {
	// A slice of host:port addresses of the redis servers.
	// For single node, just provide one address.
	Addresses []string
	// Password for the redis server.
	Password string
	// Database to be selected after connecting.
	DB int
	// IsCluster indicates whether to connect to a Redis Cluster.
	IsCluster bool
}

var (
	client redis.Cmdable
	once   sync.Once
)

// Init initializes the Redis client. It should be called only once at the start of the application.
// It panics if the configuration is invalid or if it fails to connect to Redis.
func Init(cfg Config) {
	once.Do(func() {
		var err error
		if cfg.IsCluster {
			if len(cfg.Addresses) == 0 {
				panic("redis-ease: cluster mode requires at least one address")
			}
			clusterClient := redis.NewClusterClient(&redis.ClusterOptions{
				Addrs:    cfg.Addresses,
				Password: cfg.Password,
			})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err = clusterClient.Ping(ctx).Err(); err != nil {
				panic(fmt.Sprintf("redis-ease: failed to connect to redis cluster: %v", err))
			}
			client = clusterClient
		} else {
			if len(cfg.Addresses) != 1 {
				panic("redis-ease: single node mode requires exactly one address")
			}
			singleClient := redis.NewClient(&redis.Options{
				Addr:     cfg.Addresses[0],
				Password: cfg.Password,
				DB:       cfg.DB,
			})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err = singleClient.Ping(ctx).Err(); err != nil {
				panic(fmt.Sprintf("redis-ease: failed to connect to redis: %v", err))
			}
			client = singleClient
		}
	})
}

// GetClient returns the initialized Redis client.
// It will panic if the client has not been initialized with Init().
func GetClient() redis.Cmdable {
	if client == nil {
		panic("redis-ease: client not initialized. Call Init() first.")
	}
	return client
}

// --- Convenience Wrappers ---

// Set is a convenience wrapper for the Set command.
func Set(key string, value interface{}, expiration time.Duration) error {
	return GetClient().Set(context.Background(), key, value, expiration).Err()
}

// Get is a convenience wrapper for the Get command.
// It returns redis.Nil error if the key does not exist.
func Get(key string) (string, error) {
	return GetClient().Get(context.Background(), key).Result()
}

// Del is a convenience wrapper for the Del command.
// It returns the number of keys that were removed.
func Del(keys ...string) (int64, error) {
	return GetClient().Del(context.Background(), keys...).Result()
}

// HSet is a convenience wrapper for the HSet command.
func HSet(key string, values ...interface{}) (int64, error) {
	return GetClient().HSet(context.Background(), key, values...).Result()
}

// HGet is a convenience wrapper for the HGet command.
// It returns redis.Nil error if the key or field does not exist.
func HGet(key, field string) (string, error) {
	return GetClient().HGet(context.Background(), key, field).Result()
}

// Exists is a convenience wrapper for the Exists command.
// It returns the number of keys that exist.
func Exists(keys ...string) (int64, error) {
	return GetClient().Exists(context.Background(), keys...).Result()
}