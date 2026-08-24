package redis_ease

import (
	"context"
	"math/rand"
	"time"
)

// SubscribeRetryConfig 控制首次建立订阅时的重试策略。
type SubscribeRetryConfig struct {
	Enabled    bool
	MinBackoff time.Duration
	MaxBackoff time.Duration
	MaxRetries int // 0 表示无限重试。
	Jitter     float64
	OnRetry    func(attempt int, wait time.Duration, err error)
}

type retryState struct {
	SubscribeRetryConfig
	current time.Duration
}

func normalizeSubscribeRetry(cfg SubscribeRetryConfig) retryState {
	if cfg.MinBackoff <= 0 {
		cfg.MinBackoff = 200 * time.Millisecond
	}
	if cfg.MaxBackoff <= 0 || cfg.MaxBackoff < cfg.MinBackoff {
		cfg.MaxBackoff = 5 * time.Second
	}
	return retryState{SubscribeRetryConfig: cfg, current: cfg.MinBackoff}
}

func (r *retryState) Next() time.Duration {
	wait := applyJitter(r.current, r.Jitter, rand.Float64())
	r.current *= 2
	if r.current > r.MaxBackoff {
		r.current = r.MaxBackoff
	}
	return wait
}

func applyJitter(d time.Duration, jitter, random float64) time.Duration {
	if jitter <= 0 {
		return d
	}
	factor := 1 - jitter + random*2*jitter
	return time.Duration(float64(d) * factor)
}

func sleepWithContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
