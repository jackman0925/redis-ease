package redis_ease

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var defaultState struct {
	sync.RWMutex
	client *Client
}

// Init 初始化包级兼容客户端，失败时触发 panic。
func Init(cfg Config) {
	if err := InitWithError(cfg); err != nil {
		panic(err)
	}
}

// InitWithError 初始化一次包级兼容客户端。
func InitWithError(cfg Config) error {
	defaultState.Lock()
	defer defaultState.Unlock()
	if defaultState.client != nil {
		return ErrAlreadyInitialized
	}
	c, err := NewClientWithError(cfg)
	if err != nil {
		return err
	}
	defaultState.client = c
	return nil
}

// GetClient 返回包级客户端底层的 go-redis 客户端。
func GetClient() redis.UniversalClient {
	c := mustDefaultClient()
	rc, err := c.rawClient()
	if err != nil {
		panic(err)
	}
	return rc
}

// Close 关闭并清除包级客户端；该操作是幂等的。
func Close() error {
	defaultState.Lock()
	c := defaultState.client
	defaultState.client = nil
	defaultState.Unlock()
	if c == nil {
		return nil
	}
	return c.Close()
}

// Shutdown 关闭包级客户端并等待订阅退出。
func Shutdown(ctx context.Context) error {
	defaultState.Lock()
	c := defaultState.client
	defaultState.client = nil
	defaultState.Unlock()
	if c == nil {
		return nil
	}
	return c.Shutdown(ctx)
}

func mustDefaultClient() *Client {
	c, err := defaultClient()
	if err != nil {
		panic(err)
	}
	return c
}

func defaultClient() (*Client, error) {
	defaultState.RLock()
	c := defaultState.client
	defaultState.RUnlock()
	if c == nil {
		return nil, ErrNotInitialized
	}
	return c, nil
}

func Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	c, err := defaultClient()
	if err != nil {
		return err
	}
	return c.Set(ctx, key, value, expiration)
}

func Get(ctx context.Context, key string) (string, error) {
	c, err := defaultClient()
	if err != nil {
		return "", err
	}
	return c.Get(ctx, key)
}

func Del(ctx context.Context, keys ...string) (int64, error) {
	c, err := defaultClient()
	if err != nil {
		return 0, err
	}
	return c.Del(ctx, keys...)
}

func HSet(ctx context.Context, key string, values ...interface{}) (int64, error) {
	c, err := defaultClient()
	if err != nil {
		return 0, err
	}
	return c.HSet(ctx, key, values...)
}

func HGet(ctx context.Context, key, field string) (string, error) {
	c, err := defaultClient()
	if err != nil {
		return "", err
	}
	return c.HGet(ctx, key, field)
}

func Exists(ctx context.Context, keys ...string) (int64, error) {
	c, err := defaultClient()
	if err != nil {
		return 0, err
	}
	return c.Exists(ctx, keys...)
}

func Publish(ctx context.Context, channel string, message interface{}) error {
	c, err := defaultClient()
	if err != nil {
		return err
	}
	return c.Publish(ctx, channel, message)
}

func Subscribe(ctx context.Context, channel string, handler func(msg *redis.Message)) {
	mustDefaultClient().Subscribe(ctx, channel, handler)
}

func SubscribeWithReady(ctx context.Context, channel string, handler func(msg *redis.Message), ready func()) {
	mustDefaultClient().SubscribeWithReady(ctx, channel, handler, ready)
}

func StreamAdd(ctx context.Context, streamName string, values map[string]interface{}) (string, error) {
	c, err := defaultClient()
	if err != nil {
		return "", err
	}
	return c.StreamAdd(ctx, streamName, values)
}

func StreamConsume(ctx context.Context, streamName, groupName, consumerName string) (*redis.XMessage, error) {
	c, err := defaultClient()
	if err != nil {
		return nil, err
	}
	return c.StreamConsume(ctx, streamName, groupName, consumerName)
}

func StreamConsumeAdvanced(ctx context.Context, streamName, groupName, consumerName string, block time.Duration, count int64) ([]redis.XMessage, error) {
	c, err := defaultClient()
	if err != nil {
		return nil, err
	}
	return c.StreamConsumeAdvanced(ctx, streamName, groupName, consumerName, block, count)
}

func StreamAck(ctx context.Context, streamName, groupName, messageID string) error {
	c, err := defaultClient()
	if err != nil {
		return err
	}
	return c.StreamAck(ctx, streamName, groupName, messageID)
}

func StreamClaim(ctx context.Context, streamName, groupName, consumerName string, minIdleTime time.Duration) ([]redis.XMessage, error) {
	c, err := defaultClient()
	if err != nil {
		return nil, err
	}
	return c.StreamClaim(ctx, streamName, groupName, consumerName, minIdleTime)
}

func StreamPendingSummary(ctx context.Context, streamName, groupName string) (*redis.XPending, error) {
	c, err := defaultClient()
	if err != nil {
		return nil, err
	}
	return c.StreamPendingSummary(ctx, streamName, groupName)
}

func StreamPendingList(ctx context.Context, streamName, groupName, start, end string, count int64, consumer string) ([]redis.XPendingExt, error) {
	c, err := defaultClient()
	if err != nil {
		return nil, err
	}
	return c.StreamPendingList(ctx, streamName, groupName, start, end, count, consumer)
}

func StreamPendingCount(ctx context.Context, streamName, groupName string) (int64, error) {
	c, err := defaultClient()
	if err != nil {
		return 0, err
	}
	return c.StreamPendingCount(ctx, streamName, groupName)
}
