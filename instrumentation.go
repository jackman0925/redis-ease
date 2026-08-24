package redis_ease

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// MetricsCollector 用于观测命令耗时和原始 Redis 错误。
type MetricsCollector interface {
	ObserveDuration(op string, d time.Duration, err error)
}

// Hook 包装命令，并可通过 context 传递链路追踪状态。
type Hook interface {
	Before(ctx context.Context, op string) context.Context
	After(ctx context.Context, op string, err error, duration time.Duration)
}

func runOperation[T any](ctx context.Context, c *Client, op string, useDefaultTimeout bool, fn func(context.Context, redis.UniversalClient) (T, error)) (result T, err error) {
	ctx = safeHookBefore(ctx, c, op)
	start := time.Now()
	if useDefaultTimeout {
		var cancel context.CancelFunc
		ctx, cancel = withDefaultTimeout(ctx, c.defaultTimeout)
		if cancel != nil {
			defer cancel()
		}
	}
	rc, release, err := c.acquire()
	if err == nil {
		result, err = fn(ctx, rc)
		release()
	}
	safeObserve(c, op, start, err)
	safeHookAfter(ctx, c, op, start, err)
	return result, err
}

func safeHookBefore(ctx context.Context, c *Client, op string) (result context.Context) {
	result = ctx
	if c.hook == nil {
		return result
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			c.logger.Errorf("hook Before panic for %s: %v", op, recovered)
			result = ctx
		}
	}()
	if next := c.hook.Before(ctx, op); next != nil {
		result = next
	}
	return result
}

func safeHookAfter(ctx context.Context, c *Client, op string, start time.Time, err error) {
	if c.hook == nil {
		return
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			c.logger.Errorf("hook After panic for %s: %v", op, recovered)
		}
	}()
	c.hook.After(ctx, op, err, time.Since(start))
}

func safeObserve(c *Client, op string, start time.Time, err error) {
	if c.metrics == nil {
		return
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			c.logger.Errorf("metrics panic for %s: %v", op, recovered)
		}
	}()
	c.metrics.ObserveDuration(op, time.Since(start), err)
}

func safeCallback(logger Logger, name string, callback func()) {
	if callback == nil {
		return
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			logger.Errorf("%s callback panic: %s", name, fmt.Sprint(recovered))
		}
	}()
	callback()
}

func withDefaultTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return ctx, nil
	}
	if _, ok := ctx.Deadline(); ok {
		return ctx, nil
	}
	return context.WithTimeout(ctx, timeout)
}
