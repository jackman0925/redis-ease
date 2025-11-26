package redis_ease

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)


// Config holds the configuration for the Redis client.
type Config struct {
	// A slice of host:port addresses of the redis servers.
	// For single node, just provide one address.
	Addresses []string
	// Password for the redis server.
	Password string
	// Database to be selected after connecting.
	DB int
	// IsCluster indicates whether to connect to a Redis Cluster.
	IsCluster bool
	// Logger is the logger to use. If nil, a default logger that writes to stdout is used.
	Logger Logger
	// LogLevel is the level of logging to use. Defaults to LogLevelInfo.
	LogLevel LogLevel
}

var (
	client redis.Cmdable
	once   sync.Once
	// Provide a default logger out of the box.
	logger Logger = &leveledLogger{level: LogLevelInfo}
)

// Init initializes the Redis client. It should be called only once at the start of the application.
// It panics if the configuration is invalid or if it fails to connect to Redis.
func Init(cfg Config) {
	once.Do(func() {
		// Set up the logger
		if cfg.Logger != nil {
			logger = cfg.Logger
		} else {
			if cfg.LogLevel == LogLevelNone {
				logger = &discardLogger{}
			} else if l, ok := logger.(*leveledLogger); ok {
				// If user didn't provide a logger, set the level on the default leveled logger
				if cfg.LogLevel > LogLevelDebug {
					l.level = LogLevelInfo // Default to info if not set or invalid
				} else {
					l.level = cfg.LogLevel
				}
			}
		}

		logger.Infof("Initializing redis-ease...")

		var err error
		if cfg.IsCluster {
			logger.Debugf("Cluster mode enabled with addresses: %v", cfg.Addresses)
			if len(cfg.Addresses) == 0 {
				logger.Errorf("Cluster mode requires at least one address")
				panic("redis-ease: cluster mode requires at least one address")
			}
			clusterClient := redis.NewClusterClient(&redis.ClusterOptions{
				Addrs:    cfg.Addresses,
				Password: cfg.Password,
			})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err = clusterClient.Ping(ctx).Err(); err != nil {
				logger.Errorf("Failed to connect to redis cluster: %v", err)
				panic(fmt.Sprintf("redis-ease: failed to connect to redis cluster: %v", err))
			}
			client = clusterClient
		} else {
			if len(cfg.Addresses) != 1 {
				logger.Errorf("Single node mode requires exactly one address, but %d were provided", len(cfg.Addresses))
				panic("redis-ease: single node mode requires exactly one address")
			}
			logger.Debugf("Single-node mode enabled with address: %s", cfg.Addresses[0])
			singleClient := redis.NewClient(&redis.Options{
				Addr:     cfg.Addresses[0],
				Password: cfg.Password,
				DB:       cfg.DB,
			})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err = singleClient.Ping(ctx).Err(); err != nil {
				logger.Errorf("Failed to connect to redis: %v", err)
				panic(fmt.Sprintf("redis-ease: failed to connect to redis: %v", err))
			}
			client = singleClient
		}
		logger.Infof("redis-ease initialized successfully.")
	})
}

// GetClient returns the initialized Redis client.
// It will panic if the client has not been initialized with Init().
func GetClient() redis.Cmdable {
	if client == nil {
		panic("redis-ease: client not initialized. Call Init() first.")
	}
	return client
}

// --- Convenience Wrappers ---

// Set is a convenience wrapper for the Set command.
func Set(key string, value interface{}, expiration time.Duration) error {
	return GetClient().Set(context.Background(), key, value, expiration).Err()
}

// Get is a convenience wrapper for the Get command.
// It returns redis.Nil error if the key does not exist.
func Get(key string) (string, error) {
	return GetClient().Get(context.Background(), key).Result()
}

// Del is a convenience wrapper for the Del command.
// It returns the number of keys that were removed.
func Del(keys ...string) (int64, error) {
	return GetClient().Del(context.Background(), keys...).Result()
}

// HSet is a convenience wrapper for the HSet command.
func HSet(key string, values ...interface{}) (int64, error) {
	return GetClient().HSet(context.Background(), key, values...).Result()
}

// HGet is a convenience wrapper for the HGet command.
// It returns redis.Nil error if the key or field does not exist.
func HGet(key, field string) (string, error) {
	return GetClient().HGet(context.Background(), key, field).Result()
}

// Exists is a convenience wrapper for the Exists command.
// It returns the number of keys that exist.
func Exists(keys ...string) (int64, error) {
	return GetClient().Exists(context.Background(), keys...).Result()
}

// --- Pub/Sub Functions ---

// Publish sends a message to a given channel.
func Publish(channel string, message interface{}) error {
	return GetClient().Publish(context.Background(), channel, message).Err()
}

// Subscribe listens for messages on a given channel and calls the handler function for each message.
// This function starts a new goroutine for the subscription. The provided context can be used to
// cancel the subscription and exit the goroutine.
// Note: This function will panic if the underlying client is not a *redis.Client or *redis.ClusterClient.
func Subscribe(ctx context.Context, channel string, handler func(msg *redis.Message)) {
	c := GetClient()
	logger.Debugf("Subscribing to channel: %s", channel)

	var pubsub *redis.PubSub
	switch c := c.(type) {
	case *redis.Client:
		pubsub = c.Subscribe(ctx, channel)
	case *redis.ClusterClient:
		pubsub = c.Subscribe(ctx, channel)
	default:
		// This should not happen with the current Init logic
		errMsg := "redis-ease: unsupported client type for Subscribe"
		logger.Errorf(errMsg)
		panic(errMsg)
	}

	go func() {
		defer pubsub.Close()
		ch := pubsub.Channel()
		logger.Infof("Started subscriber goroutine for channel: %s", channel)

		for {
			select {
			case <-ctx.Done():
				// Context cancelled, time to exit.
				logger.Infof("Context cancelled for channel %s. Shutting down subscriber.", channel)
				return
			case msg, ok := <-ch:
				if !ok {
					// Channel was closed.
					logger.Warnf("Pub/Sub channel %s was closed.", channel)
					return
				}
				logger.Debugf("Received message on channel %s", msg.Channel)
				handler(msg)
			}
		}
	}()
}

// --- Stream (Queue) Functions ---

// StreamAdd adds a message to a Redis Stream.
// 'values' is a map of key-value pairs for the message content.
// Returns the message ID assigned by Redis.
func StreamAdd(streamName string, values map[string]interface{}) (string, error) {
	client := GetClient()
	return client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: streamName,
		Values: values,
	}).Result()
}

// StreamConsume reads a single message from a stream using a consumer group.
// It will automatically create the stream and consumer group if they don't exist.
// It blocks until a new message is available.
// Returns the message or an error (e.g., redis.Nil if no message is returned).
func StreamConsume(streamName, groupName, consumerName string) (*redis.XMessage, error) {
	client := GetClient()
	ctx := context.Background()

	// Attempt to create the group. Ignore error if it already exists.
	// The MKSTREAM option creates the stream if it doesn't exist.
	_ = client.XGroupCreateMkStream(ctx, streamName, groupName, "0").Err()

	// Read from the stream.
	streams, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: consumerName,
		Streams:  []string{streamName, ">"}, // ">" means new messages only
		Count:    1,                         // Read one message at a time
		Block:    0,                         // Block forever
	}).Result()

	if err != nil {
		if errors.Is(err, redis.Nil) {
			logger.Debugf("StreamConsume on %s timed out as expected.", streamName)
			return nil, err
		}
		logger.Warnf("Error consuming from stream %s: %v", streamName, err)
		return nil, err
	}

	// We requested one stream and one message, so the structure is predictable.
	if len(streams) > 0 && len(streams[0].Messages) > 0 {
		return &streams[0].Messages[0], nil
	}

	// This case should ideally not be reached with Block: 0, but as a safeguard.
	return nil, redis.Nil
}

// StreamConsumeAdvanced provides more control over stream consumption.
// It allows specifying the number of messages to read (count) and how long to block (block).
// It automatically creates the stream and consumer group if they don't exist.
// Returns a slice of messages or an error. Returns (nil, nil) on timeout.
func StreamConsumeAdvanced(streamName, groupName, consumerName string, block time.Duration, count int64) ([]redis.XMessage, error) {
	client := GetClient()
	ctx := context.Background()

	// Attempt to create the group. Ignore error if it already exists.
	_ = client.XGroupCreateMkStream(ctx, streamName, groupName, "0").Err()

	// Read from the stream.
	streams, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: consumerName,
		Streams:  []string{streamName, ">"}, // ">" means new messages only
		Count:    count,
		Block:    block,
	}).Result()

	if err != nil {
		if errors.Is(err, redis.Nil) {
			logger.Debugf("StreamConsumeAdvanced on %s timed out as expected.", streamName)
			return nil, nil // A timeout occurred, return as documented
		}
		logger.Warnf("Error in StreamConsumeAdvanced for stream %s: %v", streamName, err)
		return nil, err // Other, unexpected errors
	}

	if len(streams) > 0 {
		return streams[0].Messages, nil
	}

	return nil, nil // Return nil, nil if no messages are returned (e.g., on timeout)
}

// StreamAck acknowledges the successful processing of a message.
func StreamAck(streamName, groupName, messageID string) error {
	client := GetClient()
	return client.XAck(context.Background(), streamName, groupName, messageID).Err()
}

// StreamClaim finds and claims pending messages that have been idle for longer than minIdleTime.
// This is used to recover messages from crashed consumers.
func StreamClaim(streamName, groupName, consumerName string, minIdleTime time.Duration) ([]redis.XMessage, error) {
	client := GetClient()
	ctx := context.Background()

	// 1. Find all pending messages for the group.
	// Note: In a high-throughput system, you might want to process this in batches.
	pendingResult, err := client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: streamName,
		Group:  groupName,
		Start:  "-", // Start of time
		End:    "+", // End of time
		Count:  100, // Max number of pending messages to check at once
	}).Result()
	if err != nil {
		logger.Errorf("Failed to check for pending messages in stream %s: %v", streamName, err)
		return nil, err
	}

	var staleIDs []string
	for _, p := range pendingResult {
		if p.Idle >= minIdleTime {
			staleIDs = append(staleIDs, p.ID)
		}
	}

	// If no stale messages, we're done.
	if len(staleIDs) == 0 {
		logger.Debugf("No stale messages to claim in stream %s.", streamName)
		return nil, nil
	}
	logger.Infof("Found %d stale messages to claim in stream %s.", len(staleIDs), streamName)

	// 2. Claim the stale messages.
	claimResult, err := client.XClaim(ctx, &redis.XClaimArgs{
		Stream:   streamName,
		Group:    groupName,
		Consumer: consumerName,
		Messages: staleIDs,
	}).Result()

	if err != nil {
		logger.Errorf("Failed to claim stale messages in stream %s: %v", streamName, err)
		return nil, err
	}

	logger.Infof("Successfully claimed %d messages for consumer %s.", len(claimResult), consumerName)
	return claimResult, nil
}
