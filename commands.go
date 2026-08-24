package redis_ease

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// Set 使用实例客户端存储键值。
func (c *Client) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	_, err := runOperation(ctx, c, "Set", true, func(ctx context.Context, rc redis.UniversalClient) (struct{}, error) {
		return struct{}{}, rc.Set(ctx, key, value, expiration).Err()
	})
	return err
}

// Get 返回字符串值；键不存在时返回 redis.Nil。
func (c *Client) Get(ctx context.Context, key string) (string, error) {
	return runOperation(ctx, c, "Get", true, func(ctx context.Context, rc redis.UniversalClient) (string, error) {
		return rc.Get(ctx, key).Result()
	})
}

// Del 删除键并返回成功删除的数量。
func (c *Client) Del(ctx context.Context, keys ...string) (int64, error) {
	return runOperation(ctx, c, "Del", true, func(ctx context.Context, rc redis.UniversalClient) (int64, error) {
		return rc.Del(ctx, keys...).Result()
	})
}

// HSet 设置哈希字段并返回新增字段数量。
func (c *Client) HSet(ctx context.Context, key string, values ...interface{}) (int64, error) {
	return runOperation(ctx, c, "HSet", true, func(ctx context.Context, rc redis.UniversalClient) (int64, error) {
		return rc.HSet(ctx, key, values...).Result()
	})
}

// HGet 返回哈希字段值；字段不存在时返回 redis.Nil。
func (c *Client) HGet(ctx context.Context, key, field string) (string, error) {
	return runOperation(ctx, c, "HGet", true, func(ctx context.Context, rc redis.UniversalClient) (string, error) {
		return rc.HGet(ctx, key, field).Result()
	})
}

// Exists 返回存在的键数量。
func (c *Client) Exists(ctx context.Context, keys ...string) (int64, error) {
	return runOperation(ctx, c, "Exists", true, func(ctx context.Context, rc redis.UniversalClient) (int64, error) {
		return rc.Exists(ctx, keys...).Result()
	})
}

// Publish 向频道发送消息。
func (c *Client) Publish(ctx context.Context, channel string, message interface{}) error {
	_, err := runOperation(ctx, c, "Publish", true, func(ctx context.Context, rc redis.UniversalClient) (struct{}, error) {
		return struct{}{}, rc.Publish(ctx, channel, message).Err()
	})
	return err
}
