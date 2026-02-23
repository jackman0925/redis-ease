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
	client redis.UniversalClient
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

		if len(cfg.Addresses) == 0 {
			logger.Errorf("Init requires at least one address")
			panic("redis-ease: Init requires at least one address")
		}

		// Use UniversalClient which automatically handles single node, sentinel, and cluster based on the addresses.
		client = redis.NewUniversalClient(&redis.UniversalOptions{
			Addrs:    cfg.Addresses,
			Password: cfg.Password,
			DB:       cfg.DB,
		})

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := client.Ping(ctx).Err(); err != nil {
			logger.Errorf("Failed to connect to redis: %v", err)
			panic(fmt.Sprintf("redis-ease: failed to connect to redis: %v", err))
		}

		logger.Infof("redis-ease initialized successfully.")
	})
}

// GetClient returns the initialized Redis client.
// It will panic if the client has not been initialized with Init().
func GetClient() redis.UniversalClient {
	if client == nil {
		panic("redis-ease: client not initialized. Call Init() first.")
	}
	return client
}

// Close closes the Redis client, releasing any open resources.
func Close() error {
	if client != nil {
		return client.Close()
	}
	return nil
}

// --- Convenience Wrappers ---

// Set is a convenience wrapper for the Set command.
func Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	return GetClient().Set(ctx, key, value, expiration).Err()
}

// Get is a convenience wrapper for the Get command.
// It returns redis.Nil error if the key does not exist.
func Get(ctx context.Context, key string) (string, error) {
	return GetClient().Get(ctx, key).Result()
}

// Del is a convenience wrapper for the Del command.
// It returns the number of keys that were removed.
func Del(ctx context.Context, keys ...string) (int64, error) {
	return GetClient().Del(ctx, keys...).Result()
}

// HSet is a convenience wrapper for the HSet command.
func HSet(ctx context.Context, key string, values ...interface{}) (int64, error) {
	return GetClient().HSet(ctx, key, values...).Result()
}

// HGet is a convenience wrapper for the HGet command.
// It returns redis.Nil error if the key or field does not exist.
func HGet(ctx context.Context, key, field string) (string, error) {
	return GetClient().HGet(ctx, key, field).Result()
}

// Exists is a convenience wrapper for the Exists command.
// It returns the number of keys that exist.
func Exists(ctx context.Context, keys ...string) (int64, error) {
	return GetClient().Exists(ctx, keys...).Result()
}

// --- Pub/Sub Functions ---

// Publish sends a message to a given channel.
func Publish(ctx context.Context, channel string, message interface{}) error {
	return GetClient().Publish(ctx, channel, message).Err()
}

// Subscribe listens for messages on a given channel and calls the handler function for each message.
// This function starts a new goroutine for the subscription. The provided context can be used to
// cancel the subscription and exit the goroutine.
func Subscribe(ctx context.Context, channel string, handler func(msg *redis.Message)) {
	c := GetClient()
	logger.Debugf("Subscribing to channel: %s", channel)

	pubsub := c.Subscribe(ctx, channel)

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
func StreamAdd(ctx context.Context, streamName string, values map[string]interface{}) (string, error) {
	client := GetClient()
	return client.XAdd(ctx, &redis.XAddArgs{
		Stream: streamName,
		Values: values,
	}).Result()
}

// StreamConsume reads a single message from a stream using a consumer group.
// It will automatically create the stream and consumer group if they don't exist.
// It blocks until a new message is available.
// Returns the message or an error (e.g., redis.Nil if no message is returned).
func StreamConsume(ctx context.Context, streamName, groupName, consumerName string) (*redis.XMessage, error) {
	client := GetClient()

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
func StreamConsumeAdvanced(ctx context.Context, streamName, groupName, consumerName string, block time.Duration, count int64) ([]redis.XMessage, error) {
	client := GetClient()

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
func StreamAck(ctx context.Context, streamName, groupName, messageID string) error {
	client := GetClient()
	return client.XAck(ctx, streamName, groupName, messageID).Err()
}

// StreamClaim finds and claims pending messages that have been idle for longer than minIdleTime.
// This is used to recover messages from crashed consumers.
func StreamClaim(ctx context.Context, streamName, groupName, consumerName string, minIdleTime time.Duration) ([]redis.XMessage, error) {
	client := GetClient()

	// Use XAutoClaim for an atomic, efficient implementation of claim recovery.
	messages, _, err := client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   streamName,
		Group:    groupName,
		Consumer: consumerName,
		MinIdle:  minIdleTime,
		Start:    "0-0",
		Count:    100, // Max number of pending messages to claim at once
	}).Result()

	if err != nil {
		logger.Errorf("Failed to auto-claim messages in stream %s: %v", streamName, err)
		return nil, err
	}

	if len(messages) == 0 {
		logger.Debugf("No stale messages to claim in stream %s.", streamName)
		return nil, nil
	}

	logger.Infof("Successfully claimed %d messages for consumer %s.", len(messages), consumerName)
	return messages, nil
}
