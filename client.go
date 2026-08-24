package redis_ease

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Client is an instance-scoped Redis client.
type Client struct {
	mu              sync.RWMutex
	redis           redis.UniversalClient
	closed          bool
	logger          Logger
	defaultTimeout  time.Duration
	metrics         MetricsCollector
	hook            Hook
	subRetry        SubscribeRetryConfig
	lifecycleCtx    context.Context
	lifecycleCancel context.CancelFunc
	subscriptions   sync.WaitGroup
}

func newClient(rc redis.UniversalClient, cfg Config, logger Logger) *Client {
	ctx, cancel := context.WithCancel(context.Background())
	return &Client{
		redis: rc, logger: logger, defaultTimeout: cfg.DefaultTimeout,
		metrics: cfg.Metrics, hook: cfg.Hook, subRetry: cfg.SubscribeRetry,
		lifecycleCtx: ctx, lifecycleCancel: cancel,
	}
}

// Close cancels managed subscriptions and closes Redis connections. It is idempotent.
func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.lifecycleCancel()
	rc := c.redis
	c.mu.Unlock()

	return rc.Close()
}

// Shutdown closes the client and waits for subscription goroutines or ctx cancellation.
func (c *Client) Shutdown(ctx context.Context) error {
	err := c.Close()
	done := make(chan struct{})
	go func() {
		c.subscriptions.Wait()
		close(done)
	}()
	select {
	case <-done:
		return err
	case <-ctx.Done():
		return errors.Join(err, ctx.Err())
	}
}

func (c *Client) acquire() (redis.UniversalClient, func(), error) {
	if c == nil {
		return nil, nil, ErrClientClosed
	}
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return nil, nil, ErrClientClosed
	}
	return c.redis, c.mu.RUnlock, nil
}

func (c *Client) startSubscription(ctx context.Context, run func(context.Context, redis.UniversalClient)) error {
	if c == nil {
		return ErrClientClosed
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrClientClosed
	}
	rc := c.redis
	c.subscriptions.Add(1)
	c.mu.Unlock()

	go func() {
		defer c.subscriptions.Done()
		workerCtx, cancel := context.WithCancel(ctx)
		stop := context.AfterFunc(c.lifecycleCtx, cancel)
		defer stop()
		defer cancel()
		run(workerCtx, rc)
	}()
	return nil
}

func (c *Client) rawClient() (redis.UniversalClient, error) {
	rc, release, err := c.acquire()
	if err != nil {
		return nil, err
	}
	release()
	return rc, nil
}
