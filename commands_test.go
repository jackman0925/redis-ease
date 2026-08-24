package redis_ease

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestStringCommands(t *testing.T) {
	client, _ := newTestClient(t)
	ctx := context.Background()

	require.NoError(t, client.Set(ctx, "key", "value", time.Minute))
	value, err := client.Get(ctx, "key")
	require.NoError(t, err)
	require.Equal(t, "value", value)

	count, err := client.Exists(ctx, "key", "missing")
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	deleted, err := client.Del(ctx, "key")
	require.NoError(t, err)
	require.Equal(t, int64(1), deleted)
	_, err = client.Get(ctx, "key")
	require.ErrorIs(t, err, redis.Nil)
}

func TestHashCommands(t *testing.T) {
	client, _ := newTestClient(t)
	ctx := context.Background()

	added, err := client.HSet(ctx, "hash", "field", "value")
	require.NoError(t, err)
	require.Equal(t, int64(1), added)
	value, err := client.HGet(ctx, "hash", "field")
	require.NoError(t, err)
	require.Equal(t, "value", value)
	_, err = client.HGet(ctx, "hash", "missing")
	require.ErrorIs(t, err, redis.Nil)
}

func TestDefaultTimeoutAndCallerDeadline(t *testing.T) {
	client, _ := newTestClient(t, func(cfg *Config) { cfg.DefaultTimeout = time.Nanosecond })
	_, err := client.Get(context.Background(), "key")
	require.ErrorIs(t, err, context.DeadlineExceeded)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = client.Get(ctx, "key")
	require.ErrorIs(t, err, context.Canceled)
}
