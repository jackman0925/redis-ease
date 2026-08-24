package redis_ease

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// StreamAdd appends a message and returns its Redis-generated ID.
func (c *Client) StreamAdd(ctx context.Context, streamName string, values map[string]interface{}) (string, error) {
	return runOperation(ctx, c, "StreamAdd", true, func(ctx context.Context, rc redis.UniversalClient) (string, error) {
		return rc.XAdd(ctx, &redis.XAddArgs{Stream: streamName, Values: values}).Result()
	})
}

// StreamConsume blocks until one new consumer-group message is available.
func (c *Client) StreamConsume(ctx context.Context, streamName, groupName, consumerName string) (*redis.XMessage, error) {
	return runOperation(ctx, c, "StreamConsume", false, func(ctx context.Context, rc redis.UniversalClient) (*redis.XMessage, error) {
		if err := ensureGroup(ctx, rc, c.logger, streamName, groupName); err != nil {
			return nil, err
		}
		streams, err := rc.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: groupName, Consumer: consumerName,
			Streams: []string{streamName, ">"}, Count: 1, Block: 0,
		}).Result()
		if err != nil {
			return nil, err
		}
		if len(streams) == 0 || len(streams[0].Messages) == 0 {
			return nil, redis.Nil
		}
		return &streams[0].Messages[0], nil
	})
}

// StreamConsumeAdvanced reads up to count new messages and returns nil, nil on timeout.
func (c *Client) StreamConsumeAdvanced(ctx context.Context, streamName, groupName, consumerName string, block time.Duration, count int64) ([]redis.XMessage, error) {
	if count <= 0 {
		return nil, errors.New("redis-ease: stream consume count must be positive")
	}
	return runOperation(ctx, c, "StreamConsumeAdvanced", false, func(ctx context.Context, rc redis.UniversalClient) ([]redis.XMessage, error) {
		if err := ensureGroup(ctx, rc, c.logger, streamName, groupName); err != nil {
			return nil, err
		}
		streams, err := rc.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: groupName, Consumer: consumerName,
			Streams: []string{streamName, ">"}, Count: count, Block: block,
		}).Result()
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		if err != nil || len(streams) == 0 {
			return nil, err
		}
		return streams[0].Messages, nil
	})
}

// StreamAck acknowledges a processed message.
func (c *Client) StreamAck(ctx context.Context, streamName, groupName, messageID string) error {
	_, err := runOperation(ctx, c, "StreamAck", true, func(ctx context.Context, rc redis.UniversalClient) (struct{}, error) {
		return struct{}{}, rc.XAck(ctx, streamName, groupName, messageID).Err()
	})
	return err
}

// StreamClaim claims up to 100 stale pending messages for consumerName.
func (c *Client) StreamClaim(ctx context.Context, streamName, groupName, consumerName string, minIdleTime time.Duration) ([]redis.XMessage, error) {
	return runOperation(ctx, c, "StreamClaim", true, func(ctx context.Context, rc redis.UniversalClient) ([]redis.XMessage, error) {
		messages, _, err := rc.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream: streamName, Group: groupName, Consumer: consumerName,
			MinIdle: minIdleTime, Start: "0-0", Count: 100,
		}).Result()
		return messages, err
	})
}

// StreamPendingSummary returns pending-message aggregate information.
func (c *Client) StreamPendingSummary(ctx context.Context, streamName, groupName string) (*redis.XPending, error) {
	return runOperation(ctx, c, "StreamPendingSummary", true, func(ctx context.Context, rc redis.UniversalClient) (*redis.XPending, error) {
		return rc.XPending(ctx, streamName, groupName).Result()
	})
}

// StreamPendingList returns pending entries, optionally filtered by consumer.
func (c *Client) StreamPendingList(ctx context.Context, streamName, groupName, start, end string, count int64, consumer string) ([]redis.XPendingExt, error) {
	if count <= 0 {
		return nil, errors.New("redis-ease: pending list count must be positive")
	}
	return runOperation(ctx, c, "StreamPendingList", true, func(ctx context.Context, rc redis.UniversalClient) ([]redis.XPendingExt, error) {
		return rc.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: streamName, Group: groupName, Start: start, End: end,
			Count: count, Consumer: consumer,
		}).Result()
	})
}

// StreamPendingCount returns the number of pending entries.
func (c *Client) StreamPendingCount(ctx context.Context, streamName, groupName string) (int64, error) {
	return runOperation(ctx, c, "StreamPendingCount", true, func(ctx context.Context, rc redis.UniversalClient) (int64, error) {
		summary, err := rc.XPending(ctx, streamName, groupName).Result()
		if err != nil {
			return 0, err
		}
		return summary.Count, nil
	})
}

func ensureGroup(ctx context.Context, rc redis.UniversalClient, logger Logger, streamName, groupName string) error {
	err := rc.XGroupCreateMkStream(ctx, streamName, groupName, "0").Err()
	if err == nil || isBusyGroupError(err) {
		return nil
	}
	logger.Warnf("failed to create consumer group %s for stream %s: %v", groupName, streamName, err)
	return err
}

func isBusyGroupError(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), "BUSYGROUP")
}
