package redis_ease

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// Set stores a value using the instance client.
func (c *Client) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	_, err := runOperation(ctx, c, "Set", true, func(ctx context.Context, rc redis.UniversalClient) (struct{}, error) {
		return struct{}{}, rc.Set(ctx, key, value, expiration).Err()
	})
	return err
}

// Get returns a string value. It returns redis.Nil when the key does not exist.
func (c *Client) Get(ctx context.Context, key string) (string, error) {
	return runOperation(ctx, c, "Get", true, func(ctx context.Context, rc redis.UniversalClient) (string, error) {
		return rc.Get(ctx, key).Result()
	})
}

// Del deletes keys and returns the number removed.
func (c *Client) Del(ctx context.Context, keys ...string) (int64, error) {
	return runOperation(ctx, c, "Del", true, func(ctx context.Context, rc redis.UniversalClient) (int64, error) {
		return rc.Del(ctx, keys...).Result()
	})
}

// HSet sets hash fields and returns the number of fields added.
func (c *Client) HSet(ctx context.Context, key string, values ...interface{}) (int64, error) {
	return runOperation(ctx, c, "HSet", true, func(ctx context.Context, rc redis.UniversalClient) (int64, error) {
		return rc.HSet(ctx, key, values...).Result()
	})
}

// HGet returns a hash field. It returns redis.Nil when missing.
func (c *Client) HGet(ctx context.Context, key, field string) (string, error) {
	return runOperation(ctx, c, "HGet", true, func(ctx context.Context, rc redis.UniversalClient) (string, error) {
		return rc.HGet(ctx, key, field).Result()
	})
}

// Exists returns the number of keys that exist.
func (c *Client) Exists(ctx context.Context, keys ...string) (int64, error) {
	return runOperation(ctx, c, "Exists", true, func(ctx context.Context, rc redis.UniversalClient) (int64, error) {
		return rc.Exists(ctx, keys...).Result()
	})
}

// Publish sends a message to a channel.
func (c *Client) Publish(ctx context.Context, channel string, message interface{}) error {
	_, err := runOperation(ctx, c, "Publish", true, func(ctx context.Context, rc redis.UniversalClient) (struct{}, error) {
		return struct{}{}, rc.Publish(ctx, channel, message).Err()
	})
	return err
}
