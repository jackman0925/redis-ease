package redis_ease

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
	}{
		{name: "missing address", cfg: Config{}},
		{name: "empty address", cfg: Config{Addresses: []string{""}}},
		{name: "negative pool", cfg: Config{Addresses: []string{"localhost:1"}, PoolSize: -1}},
		{name: "invalid jitter", cfg: Config{Addresses: []string{"localhost:1"}, SubscribeRetry: SubscribeRetryConfig{Jitter: 2}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Error(t, validateConfig(tt.cfg))
		})
	}
}

func TestDefaultLoggerIsInfo(t *testing.T) {
	logger := buildLogger(Config{})
	levelled, ok := logger.(*leveledLogger)
	require.True(t, ok)
	require.Equal(t, LogLevelInfo, levelled.level)
	require.IsType(t, &discardLogger{}, buildLogger(Config{LogLevel: LogLevelNone}))
}

func TestClientCloseIsIdempotent(t *testing.T) {
	client, _ := newTestClient(t)
	require.NoError(t, client.Close())
	require.NoError(t, client.Close())
	_, err := client.Get(context.Background(), "closed")
	require.ErrorIs(t, err, ErrClientClosed)
}

func TestDefaultClientLifecycle(t *testing.T) {
	resetDefaultClient(t)
	serverClient, server := newTestClient(t)
	_ = serverClient.Close()

	cfg := Config{Addresses: []string{server.Addr()}, LogLevel: LogLevelNone}
	require.NoError(t, InitWithError(cfg))
	require.ErrorIs(t, InitWithError(cfg), ErrAlreadyInitialized)
	require.NoError(t, Close())
	require.ErrorIs(t, Set(context.Background(), "key", "value", 0), ErrNotInitialized)
}

func TestCloseRacingWithCommands(t *testing.T) {
	client, _ := newTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var workers sync.WaitGroup
	for i := 0; i < 8; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for ctx.Err() == nil {
				err := client.Set(ctx, "race", "value", 0)
				if err != nil && !errors.Is(err, ErrClientClosed) && !errors.Is(err, context.Canceled) {
					t.Errorf("unexpected command error: %v", err)
					return
				}
			}
		}()
	}
	require.NoError(t, client.Close())
	cancel()
	workers.Wait()
}
