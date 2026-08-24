package redis_ease

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStreamConsumeAckAndPending(t *testing.T) {
	client, _ := newTestClient(t)
	ctx := context.Background()
	id, err := client.StreamAdd(ctx, "orders", map[string]interface{}{"order": "1"})
	require.NoError(t, err)

	msg, err := client.StreamConsume(ctx, "orders", "workers", "consumer-1")
	require.NoError(t, err)
	require.Equal(t, id, msg.ID)
	count, err := client.StreamPendingCount(ctx, "orders", "workers")
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	items, err := client.StreamPendingList(ctx, "orders", "workers", "-", "+", 10, "")
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.NoError(t, client.StreamAck(ctx, "orders", "workers", id))
	count, err = client.StreamPendingCount(ctx, "orders", "workers")
	require.NoError(t, err)
	require.Zero(t, count)
}

func TestStreamConsumeAdvanced(t *testing.T) {
	client, _ := newTestClient(t)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		_, err := client.StreamAdd(ctx, "batch", map[string]interface{}{"n": i})
		require.NoError(t, err)
	}
	messages, err := client.StreamConsumeAdvanced(ctx, "batch", "workers", "consumer", time.Millisecond, 3)
	require.NoError(t, err)
	require.Len(t, messages, 3)
}

func TestStreamInputValidation(t *testing.T) {
	client, _ := newTestClient(t)
	_, err := client.StreamConsumeAdvanced(context.Background(), "stream", "group", "consumer", 0, 0)
	require.Error(t, err)
	_, err = client.StreamPendingList(context.Background(), "stream", "group", "-", "+", 0, "")
	require.Error(t, err)
}
