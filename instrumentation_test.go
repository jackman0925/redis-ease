package redis_ease

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

type recordingMetrics struct {
	sync.Mutex
	op  string
	err error
}

func (m *recordingMetrics) ObserveDuration(op string, _ time.Duration, err error) {
	m.Lock()
	defer m.Unlock()
	m.op, m.err = op, err
}

type contextKey struct{}

type recordingHook struct {
	sync.Mutex
	before, after bool
	afterValue    bool
}

func (h *recordingHook) Before(ctx context.Context, _ string) context.Context {
	h.Lock()
	h.before = true
	h.Unlock()
	return context.WithValue(ctx, contextKey{}, true)
}

func (h *recordingHook) After(ctx context.Context, _ string, _ error, _ time.Duration) {
	h.Lock()
	defer h.Unlock()
	h.after = true
	h.afterValue, _ = ctx.Value(contextKey{}).(bool)
}

func TestInstrumentationReceivesResultAndContext(t *testing.T) {
	metrics := &recordingMetrics{}
	hook := &recordingHook{}
	client, _ := newTestClient(t, func(cfg *Config) { cfg.Metrics, cfg.Hook = metrics, hook })

	_, err := client.Get(context.Background(), "missing")
	require.ErrorIs(t, err, redis.Nil)
	metrics.Lock()
	require.Equal(t, "Get", metrics.op)
	require.ErrorIs(t, metrics.err, redis.Nil)
	metrics.Unlock()
	hook.Lock()
	require.True(t, hook.before)
	require.True(t, hook.after)
	require.True(t, hook.afterValue)
	hook.Unlock()
}

type panicMetrics struct{}

func (*panicMetrics) ObserveDuration(string, time.Duration, error) { panic("metrics panic") }

type panicHook struct{}

func (*panicHook) Before(context.Context, string) context.Context      { panic("before panic") }
func (*panicHook) After(context.Context, string, error, time.Duration) { panic("after panic") }

func TestInstrumentationPanicsAreContained(t *testing.T) {
	client, _ := newTestClient(t, func(cfg *Config) {
		cfg.Metrics = &panicMetrics{}
		cfg.Hook = &panicHook{}
	})
	require.NotPanics(t, func() {
		require.NoError(t, client.Set(context.Background(), "key", "value", 0))
	})
}

type panicLogger struct{}

func (*panicLogger) Errorf(string, ...interface{}) { panic("logger") }
func (*panicLogger) Warnf(string, ...interface{})  { panic("logger") }
func (*panicLogger) Infof(string, ...interface{})  { panic("logger") }
func (*panicLogger) Debugf(string, ...interface{}) { panic("logger") }

func TestCustomLoggerPanicsAreContained(t *testing.T) {
	require.NotPanics(t, func() { buildLogger(Config{Logger: &panicLogger{}}).Infof("test") })
}
