package redis_ease

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func newTestClient(t *testing.T, configure ...func(*Config)) (*Client, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	cfg := Config{Addresses: []string{server.Addr()}, LogLevel: LogLevelNone}
	for _, fn := range configure {
		fn(&cfg)
	}
	client, err := NewClientWithError(cfg)
	if err != nil {
		t.Fatalf("NewClientWithError: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client, server
}

func waitSignal[T any](t *testing.T, channel <-chan T, timeout time.Duration, description string) {
	t.Helper()
	select {
	case <-channel:
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func resetDefaultClient(t *testing.T) {
	t.Helper()
	_ = Close()
	t.Cleanup(func() { _ = Close() })
}
