package redis_ease

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

var errSubscriptionClosed = errors.New("redis-ease: subscription channel closed")

// Subscribe starts a managed asynchronous subscription.
func (c *Client) Subscribe(ctx context.Context, channel string, handler func(msg *redis.Message)) {
	c.SubscribeWithReady(ctx, channel, handler, nil)
}

// SubscribeWithReady calls ready after initial subscription and subsequent resubscriptions.
func (c *Client) SubscribeWithReady(ctx context.Context, channel string, handler func(msg *redis.Message), ready func()) {
	if handler == nil {
		c.logger.Errorf("subscription handler for %s must not be nil", channel)
		return
	}
	err := c.startSubscription(ctx, func(workerCtx context.Context, rc redis.UniversalClient) {
		c.runSubscription(workerCtx, rc, channel, handler, ready)
	})
	if err != nil {
		c.logger.Errorf("failed to start subscription for %s: %v", channel, err)
	}
}

func (c *Client) runSubscription(ctx context.Context, rc redis.UniversalClient, channel string, handler func(*redis.Message), ready func()) {
	pubsub, err := c.establishSubscription(ctx, rc, channel)
	if err != nil {
		if !errors.Is(err, context.Canceled) && !errors.Is(err, ErrClientClosed) {
			c.logger.Errorf("failed to subscribe to %s: %v", channel, err)
		}
		return
	}
	defer pubsub.Close()
	safeCallback(c.logger, "subscription ready", ready)

	events := pubsub.ChannelWithSubscriptions()
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-events:
			if !ok {
				if ctx.Err() == nil {
					c.logger.Warnf("subscription channel %s closed: %v", channel, errSubscriptionClosed)
				}
				return
			}
			switch value := event.(type) {
			case *redis.Message:
				safeMessageHandler(c.logger, channel, handler, value)
			case *redis.Subscription:
				if value.Kind == "subscribe" && value.Channel == channel {
					safeCallback(c.logger, "subscription ready", ready)
				}
			}
		}
	}
}

func (c *Client) establishSubscription(ctx context.Context, rc redis.UniversalClient, channel string) (*redis.PubSub, error) {
	retry := normalizeSubscribeRetry(c.subRetry)
	for attempt := 1; ; attempt++ {
		pubsub := rc.Subscribe(ctx, channel)
		if _, err := pubsub.Receive(ctx); err == nil {
			return pubsub, nil
		} else {
			_ = pubsub.Close()
			if !retry.Enabled || (retry.MaxRetries > 0 && attempt > retry.MaxRetries) {
				return nil, err
			}
			wait := retry.Next()
			safeRetryCallback(c.logger, retry.OnRetry, attempt, wait, err)
			if !sleepWithContext(ctx, wait) {
				return nil, ctx.Err()
			}
		}
	}
}

func safeMessageHandler(logger Logger, channel string, handler func(*redis.Message), msg *redis.Message) {
	defer func() {
		if recovered := recover(); recovered != nil {
			logger.Errorf("message handler panic for %s: %v", channel, recovered)
		}
	}()
	handler(msg)
}

func safeRetryCallback(logger Logger, callback func(int, time.Duration, error), attempt int, wait time.Duration, err error) {
	if callback == nil {
		return
	}
	safeCallback(logger, "subscription retry", func() { callback(attempt, wait, err) })
}
