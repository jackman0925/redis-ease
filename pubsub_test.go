package redis_ease

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestSubscribeReadyAndMessage(t *testing.T) {
	client, _ := newTestClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan struct{}, 2)
	received := make(chan string, 1)

	client.SubscribeWithReady(ctx, "events", func(msg *redis.Message) {
		received <- msg.Payload
	}, func() { ready <- struct{}{} })

	select {
	case <-ready:
	case <-time.After(time.Second):
		t.Fatal("subscription did not become ready")
	}
	require.NoError(t, client.Publish(ctx, "events", "payload"))
	select {
	case payload := <-received:
		require.Equal(t, "payload", payload)
	case <-time.After(time.Second):
		t.Fatal("message not received")
	}
}

func TestShutdownWaitsForSubscription(t *testing.T) {
	client, _ := newTestClient(t, func(cfg *Config) {
		cfg.SubscribeRetry = SubscribeRetryConfig{Enabled: true}
	})
	ready := make(chan struct{}, 1)
	client.SubscribeWithReady(context.Background(), "events", func(*redis.Message) {}, func() { ready <- struct{}{} })
	select {
	case <-ready:
	case <-time.After(time.Second):
		t.Fatal("subscription did not become ready")
	}
	done := make(chan error, 1)
	go func() { done <- client.Shutdown(context.Background()) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Close did not wait for subscription shutdown")
	}
}

func TestHandlerCanCloseClientWithoutDeadlock(t *testing.T) {
	client, _ := newTestClient(t)
	ready := make(chan struct{}, 1)
	closed := make(chan error, 1)
	client.SubscribeWithReady(context.Background(), "close-events", func(*redis.Message) {
		closed <- client.Close()
	}, func() { ready <- struct{}{} })
	waitSignal(t, ready, time.Second, "subscription ready")
	require.NoError(t, client.Publish(context.Background(), "close-events", "close"))
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Close deadlocked inside handler")
	}
}

func TestRetryHelpers(t *testing.T) {
	require.Equal(t, 80*time.Millisecond, applyJitter(100*time.Millisecond, 0.2, 0))
	require.Equal(t, 120*time.Millisecond, applyJitter(100*time.Millisecond, 0.2, 1))
	retry := normalizeSubscribeRetry(SubscribeRetryConfig{Enabled: true, MinBackoff: 10 * time.Millisecond, MaxBackoff: 20 * time.Millisecond})
	require.Equal(t, 10*time.Millisecond, retry.Next())
	require.Equal(t, 20*time.Millisecond, retry.Next())
}

func TestRetryCallbackPanicIsContained(t *testing.T) {
	logger := buildLogger(Config{LogLevel: LogLevelNone})
	require.NotPanics(t, func() {
		safeRetryCallback(logger, func(int, time.Duration, error) { panic("retry") }, 1, time.Millisecond, errors.New("failed"))
	})
}

func TestInitialSubscriptionRetryReportsError(t *testing.T) {
	rc := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 10 * time.Millisecond})
	defer rc.Close()
	client := newClient(rc, Config{
		LogLevel:       LogLevelNone,
		SubscribeRetry: SubscribeRetryConfig{Enabled: true, MaxRetries: 1, MinBackoff: time.Millisecond, MaxBackoff: time.Millisecond},
	}, buildLogger(Config{LogLevel: LogLevelNone}))
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := client.establishSubscription(ctx, rc, "events")
	require.Error(t, err)
}

func TestSleepWithContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.False(t, sleepWithContext(ctx, time.Second))
}
