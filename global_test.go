package redis_ease

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestGlobalCompatibilityWrappers(t *testing.T) {
	resetDefaultClient(t)
	server := miniredis.RunT(t)
	require.NoError(t, InitWithError(Config{Addresses: []string{server.Addr()}, LogLevel: LogLevelNone}))
	ctx := context.Background()

	require.NotNil(t, GetClient())
	require.NoError(t, Set(ctx, "key", "value", 0))
	value, err := Get(ctx, "key")
	require.NoError(t, err)
	require.Equal(t, "value", value)
	exists, err := Exists(ctx, "key")
	require.NoError(t, err)
	require.Equal(t, int64(1), exists)

	_, err = HSet(ctx, "hash", "field", "value")
	require.NoError(t, err)
	value, err = HGet(ctx, "hash", "field")
	require.NoError(t, err)
	require.Equal(t, "value", value)
	deleted, err := Del(ctx, "key", "hash")
	require.NoError(t, err)
	require.Equal(t, int64(2), deleted)

	id, err := StreamAdd(ctx, "stream", map[string]interface{}{"value": "1"})
	require.NoError(t, err)
	message, err := StreamConsume(ctx, "stream", "group", "consumer")
	require.NoError(t, err)
	require.Equal(t, id, message.ID)
	summary, err := StreamPendingSummary(ctx, "stream", "group")
	require.NoError(t, err)
	require.Equal(t, int64(1), summary.Count)
	items, err := StreamPendingList(ctx, "stream", "group", "-", "+", 10, "")
	require.NoError(t, err)
	require.Len(t, items, 1)
	count, err := StreamPendingCount(ctx, "stream", "group")
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
	require.NoError(t, StreamAck(ctx, "stream", "group", id))
}

func TestGlobalPubSubWrappers(t *testing.T) {
	resetDefaultClient(t)
	server := miniredis.RunT(t)
	require.NoError(t, InitWithError(Config{Addresses: []string{server.Addr()}, LogLevel: LogLevelNone}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan struct{}, 1)
	received := make(chan string, 1)
	SubscribeWithReady(ctx, "global-events", func(msg *redis.Message) { received <- msg.Payload }, func() { ready <- struct{}{} })
	waitSignal(t, ready, time.Second, "global subscription")
	require.NoError(t, Publish(ctx, "global-events", "value"))
	waitSignal(t, received, time.Second, "global message")
}

func TestGlobalInitAndShutdown(t *testing.T) {
	resetDefaultClient(t)
	server := miniredis.RunT(t)
	require.NotPanics(t, func() { Init(Config{Addresses: []string{server.Addr()}, LogLevel: LogLevelNone}) })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, Shutdown(ctx))
	require.Panics(t, func() { GetClient() })
}
