package redis_ease

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math/rand"
	"strings"
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
	// DefaultTimeout applies to non-blocking operations when context has no deadline.
	// It is not applied to Subscribe or StreamConsume*.
	DefaultTimeout time.Duration
	// DialTimeout is the dial timeout for new connections.
	DialTimeout time.Duration
	// ReadTimeout is the timeout for socket reads.
	ReadTimeout time.Duration
	// WriteTimeout is the timeout for socket writes.
	WriteTimeout time.Duration
	// PoolSize is the maximum number of socket connections.
	PoolSize int
	// MinIdleConns is the minimum number of idle connections.
	MinIdleConns int
	// MaxRetries is the maximum number of retries before giving up.
	MaxRetries int
	// MinRetryBackoff is the minimum backoff between retries.
	MinRetryBackoff time.Duration
	// MaxRetryBackoff is the maximum backoff between retries.
	MaxRetryBackoff time.Duration
	// TLSConfig enables TLS if set.
	TLSConfig *tls.Config
	// MasterName is the sentinel master name (if using sentinel).
	MasterName string
	// Metrics allows plugging in metrics collection for operations.
	Metrics MetricsCollector
	// Hook allows attaching before/after instrumentation for operations.
	Hook Hook
	// SubscribeRetry configures reconnect strategy for Subscribe.
	SubscribeRetry SubscribeRetryConfig
	// Logger is the logger to use. If nil, a default logger that writes to stdout is used.
	Logger Logger
	// LogLevel is the level of logging to use. Defaults to LogLevelInfo.
	LogLevel LogLevel
}

var (
	client               redis.UniversalClient
	once                 sync.Once
	initMu               sync.Mutex
	globalDefaultTimeout time.Duration
	globalMetrics        MetricsCollector
	globalHook           Hook
	globalSubscribeRetry SubscribeRetryConfig
	randFloat64Source    = rand.Float64
	// Provide a default logger out of the box.
	logger Logger = &leveledLogger{level: LogLevelInfo}
)

// Client provides an instance-scoped Redis client with its own logger.
type Client struct {
	client         redis.UniversalClient
	logger         Logger
	defaultTimeout time.Duration
	metrics        MetricsCollector
	hook           Hook
	subRetry       SubscribeRetryConfig
}

// MetricsCollector allows observing latency and errors for operations.
type MetricsCollector interface {
	ObserveDuration(op string, d time.Duration, err error)
}

// Hook allows hooks around operations, e.g., for tracing spans.
type Hook interface {
	Before(ctx context.Context, op string) context.Context
	After(ctx context.Context, op string, err error, duration time.Duration)
}

// SubscribeRetryConfig controls reconnect behavior for Subscribe.
type SubscribeRetryConfig struct {
	Enabled    bool
	MinBackoff time.Duration
	MaxBackoff time.Duration
	MaxRetries int // 0 means unlimited
	// Jitter is a fraction [0,1] applied to backoff to avoid retry storms.
	Jitter float64
	// OnRetry is called before a retry sleep. The error is from the last attempt.
	OnRetry func(attempt int, wait time.Duration, err error)
}

// Init initializes the Redis client. It should be called only once at the start of the application.
// It panics if the configuration is invalid or if it fails to connect to Redis.
func Init(cfg Config) {
	if err := InitWithError(cfg); err != nil {
		panic(err)
	}
}

// InitWithError initializes the Redis client and returns an error on failure.
// It allows retrying after a failure and returns nil if already initialized.
func InitWithError(cfg Config) error {
	initMu.Lock()
	defer initMu.Unlock()

	if client != nil {
		return nil
	}

	var initErr error
	once.Do(func() {
		c, err := NewClientWithError(cfg)
		if err != nil {
			initErr = err
			return
		}
		logger = c.logger
		client = c.client
		globalDefaultTimeout = c.defaultTimeout
		globalMetrics = c.metrics
		globalHook = c.hook
		globalSubscribeRetry = c.subRetry
	})

	if initErr != nil {
		once = sync.Once{}
	}

	return initErr
}

// NewClient constructs a new instance client and panics on failure.
func NewClient(cfg Config) *Client {
	c, err := NewClientWithError(cfg)
	if err != nil {
		panic(err)
	}
	return c
}

// NewClientWithError constructs a new instance client and returns an error on failure.
func NewClientWithError(cfg Config) (*Client, error) {
	l := buildLogger(cfg)
	l.Infof("Initializing redis-ease client...")

	if len(cfg.Addresses) == 0 {
		l.Errorf("Init requires at least one address")
		return nil, errors.New("redis-ease: Init requires at least one address")
	}

	opts := &redis.UniversalOptions{
		Addrs:           cfg.Addresses,
		Password:        cfg.Password,
		DB:              cfg.DB,
		MasterName:      cfg.MasterName,
		DialTimeout:     cfg.DialTimeout,
		ReadTimeout:     cfg.ReadTimeout,
		WriteTimeout:    cfg.WriteTimeout,
		PoolSize:        cfg.PoolSize,
		MinIdleConns:    cfg.MinIdleConns,
		MaxRetries:      cfg.MaxRetries,
		MinRetryBackoff: cfg.MinRetryBackoff,
		MaxRetryBackoff: cfg.MaxRetryBackoff,
		TLSConfig:       cfg.TLSConfig,
	}

	rc := redis.NewUniversalClient(opts)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rc.Ping(ctx).Err(); err != nil {
		l.Errorf("Failed to connect to redis: %v", err)
		_ = rc.Close()
		return nil, fmt.Errorf("redis-ease: failed to connect to redis: %w", err)
	}

	l.Infof("redis-ease client initialized successfully.")
	return &Client{
		client:         rc,
		logger:         l,
		defaultTimeout: cfg.DefaultTimeout,
		metrics:        cfg.Metrics,
		hook:           cfg.Hook,
		subRetry:       cfg.SubscribeRetry,
	}, nil
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
	initMu.Lock()
	defer initMu.Unlock()
	if client != nil {
		err := client.Close()
		client = nil
		globalDefaultTimeout = 0
		globalMetrics = nil
		globalHook = nil
		globalSubscribeRetry = SubscribeRetryConfig{}
		once = sync.Once{}
		return err
	}
	return nil
}

// Close closes the instance client, releasing any open resources.
func (c *Client) Close() error {
	if c == nil || c.client == nil {
		return nil
	}
	return c.client.Close()
}

// --- Convenience Wrappers ---

// Set is a convenience wrapper for the Set command.
func Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	op := "Set"
	ctx = hookBefore(ctx, globalHook, op)
	start := time.Now()
	ctx, cancel := withDefaultTimeout(ctx, globalDefaultTimeout)
	if cancel != nil {
		defer cancel()
	}
	err := GetClient().Set(ctx, key, value, expiration).Err()
	observe(globalMetrics, op, start, err)
	hookAfter(ctx, globalHook, op, err, start)
	return err
}

// Get is a convenience wrapper for the Get command.
// It returns redis.Nil error if the key does not exist.
func Get(ctx context.Context, key string) (string, error) {
	op := "Get"
	ctx = hookBefore(ctx, globalHook, op)
	start := time.Now()
	ctx, cancel := withDefaultTimeout(ctx, globalDefaultTimeout)
	if cancel != nil {
		defer cancel()
	}
	res, err := GetClient().Get(ctx, key).Result()
	observe(globalMetrics, op, start, err)
	hookAfter(ctx, globalHook, op, err, start)
	return res, err
}

// Del is a convenience wrapper for the Del command.
// It returns the number of keys that were removed.
func Del(ctx context.Context, keys ...string) (int64, error) {
	op := "Del"
	ctx = hookBefore(ctx, globalHook, op)
	start := time.Now()
	ctx, cancel := withDefaultTimeout(ctx, globalDefaultTimeout)
	if cancel != nil {
		defer cancel()
	}
	res, err := GetClient().Del(ctx, keys...).Result()
	observe(globalMetrics, op, start, err)
	hookAfter(ctx, globalHook, op, err, start)
	return res, err
}

// HSet is a convenience wrapper for the HSet command.
func HSet(ctx context.Context, key string, values ...interface{}) (int64, error) {
	op := "HSet"
	ctx = hookBefore(ctx, globalHook, op)
	start := time.Now()
	ctx, cancel := withDefaultTimeout(ctx, globalDefaultTimeout)
	if cancel != nil {
		defer cancel()
	}
	res, err := GetClient().HSet(ctx, key, values...).Result()
	observe(globalMetrics, op, start, err)
	hookAfter(ctx, globalHook, op, err, start)
	return res, err
}

// HGet is a convenience wrapper for the HGet command.
// It returns redis.Nil error if the key or field does not exist.
func HGet(ctx context.Context, key, field string) (string, error) {
	op := "HGet"
	ctx = hookBefore(ctx, globalHook, op)
	start := time.Now()
	ctx, cancel := withDefaultTimeout(ctx, globalDefaultTimeout)
	if cancel != nil {
		defer cancel()
	}
	res, err := GetClient().HGet(ctx, key, field).Result()
	observe(globalMetrics, op, start, err)
	hookAfter(ctx, globalHook, op, err, start)
	return res, err
}

// Exists is a convenience wrapper for the Exists command.
// It returns the number of keys that exist.
func Exists(ctx context.Context, keys ...string) (int64, error) {
	op := "Exists"
	ctx = hookBefore(ctx, globalHook, op)
	start := time.Now()
	ctx, cancel := withDefaultTimeout(ctx, globalDefaultTimeout)
	if cancel != nil {
		defer cancel()
	}
	res, err := GetClient().Exists(ctx, keys...).Result()
	observe(globalMetrics, op, start, err)
	hookAfter(ctx, globalHook, op, err, start)
	return res, err
}

// Set is a convenience wrapper for the Set command on an instance client.
func (c *Client) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	op := "Set"
	ctx = hookBefore(ctx, c.hook, op)
	start := time.Now()
	ctx, cancel := withDefaultTimeout(ctx, c.defaultTimeout)
	if cancel != nil {
		defer cancel()
	}
	err := c.client.Set(ctx, key, value, expiration).Err()
	observe(c.metrics, op, start, err)
	hookAfter(ctx, c.hook, op, err, start)
	return err
}

// Get is a convenience wrapper for the Get command on an instance client.
// It returns redis.Nil error if the key does not exist.
func (c *Client) Get(ctx context.Context, key string) (string, error) {
	op := "Get"
	ctx = hookBefore(ctx, c.hook, op)
	start := time.Now()
	ctx, cancel := withDefaultTimeout(ctx, c.defaultTimeout)
	if cancel != nil {
		defer cancel()
	}
	res, err := c.client.Get(ctx, key).Result()
	observe(c.metrics, op, start, err)
	hookAfter(ctx, c.hook, op, err, start)
	return res, err
}

// Del is a convenience wrapper for the Del command on an instance client.
// It returns the number of keys that were removed.
func (c *Client) Del(ctx context.Context, keys ...string) (int64, error) {
	op := "Del"
	ctx = hookBefore(ctx, c.hook, op)
	start := time.Now()
	ctx, cancel := withDefaultTimeout(ctx, c.defaultTimeout)
	if cancel != nil {
		defer cancel()
	}
	res, err := c.client.Del(ctx, keys...).Result()
	observe(c.metrics, op, start, err)
	hookAfter(ctx, c.hook, op, err, start)
	return res, err
}

// HSet is a convenience wrapper for the HSet command on an instance client.
func (c *Client) HSet(ctx context.Context, key string, values ...interface{}) (int64, error) {
	op := "HSet"
	ctx = hookBefore(ctx, c.hook, op)
	start := time.Now()
	ctx, cancel := withDefaultTimeout(ctx, c.defaultTimeout)
	if cancel != nil {
		defer cancel()
	}
	res, err := c.client.HSet(ctx, key, values...).Result()
	observe(c.metrics, op, start, err)
	hookAfter(ctx, c.hook, op, err, start)
	return res, err
}

// HGet is a convenience wrapper for the HGet command on an instance client.
// It returns redis.Nil error if the key or field does not exist.
func (c *Client) HGet(ctx context.Context, key, field string) (string, error) {
	op := "HGet"
	ctx = hookBefore(ctx, c.hook, op)
	start := time.Now()
	ctx, cancel := withDefaultTimeout(ctx, c.defaultTimeout)
	if cancel != nil {
		defer cancel()
	}
	res, err := c.client.HGet(ctx, key, field).Result()
	observe(c.metrics, op, start, err)
	hookAfter(ctx, c.hook, op, err, start)
	return res, err
}

// Exists is a convenience wrapper for the Exists command on an instance client.
// It returns the number of keys that exist.
func (c *Client) Exists(ctx context.Context, keys ...string) (int64, error) {
	op := "Exists"
	ctx = hookBefore(ctx, c.hook, op)
	start := time.Now()
	ctx, cancel := withDefaultTimeout(ctx, c.defaultTimeout)
	if cancel != nil {
		defer cancel()
	}
	res, err := c.client.Exists(ctx, keys...).Result()
	observe(c.metrics, op, start, err)
	hookAfter(ctx, c.hook, op, err, start)
	return res, err
}

// --- Pub/Sub Functions ---

// Publish sends a message to a given channel.
func Publish(ctx context.Context, channel string, message interface{}) error {
	op := "Publish"
	ctx = hookBefore(ctx, globalHook, op)
	start := time.Now()
	ctx, cancel := withDefaultTimeout(ctx, globalDefaultTimeout)
	if cancel != nil {
		defer cancel()
	}
	err := GetClient().Publish(ctx, channel, message).Err()
	observe(globalMetrics, op, start, err)
	hookAfter(ctx, globalHook, op, err, start)
	return err
}

// Subscribe listens for messages on a given channel and calls the handler function for each message.
// This function starts a new goroutine for the subscription. The provided context can be used to
// cancel the subscription and exit the goroutine.
func Subscribe(ctx context.Context, channel string, handler func(msg *redis.Message)) {
	subscribeWithLogger(ctx, GetClient(), logger, globalSubscribeRetry, channel, handler, nil)
}

// SubscribeWithReady is like Subscribe, but calls ready() after the subscription is established.
// The ready callback is called once per successful (re)subscription.
func SubscribeWithReady(ctx context.Context, channel string, handler func(msg *redis.Message), ready func()) {
	subscribeWithLogger(ctx, GetClient(), logger, globalSubscribeRetry, channel, handler, ready)
}

// Publish sends a message to a given channel on an instance client.
func (c *Client) Publish(ctx context.Context, channel string, message interface{}) error {
	op := "Publish"
	ctx = hookBefore(ctx, c.hook, op)
	start := time.Now()
	ctx, cancel := withDefaultTimeout(ctx, c.defaultTimeout)
	if cancel != nil {
		defer cancel()
	}
	err := c.client.Publish(ctx, channel, message).Err()
	observe(c.metrics, op, start, err)
	hookAfter(ctx, c.hook, op, err, start)
	return err
}

// Subscribe listens for messages on a given channel using an instance client.
func (c *Client) Subscribe(ctx context.Context, channel string, handler func(msg *redis.Message)) {
	subscribeWithLogger(ctx, c.client, c.logger, c.subRetry, channel, handler, nil)
}

// SubscribeWithReady is like Subscribe, but calls ready() after the subscription is established.
// The ready callback is called once per successful (re)subscription.
func (c *Client) SubscribeWithReady(ctx context.Context, channel string, handler func(msg *redis.Message), ready func()) {
	subscribeWithLogger(ctx, c.client, c.logger, c.subRetry, channel, handler, ready)
}

func subscribeWithLogger(ctx context.Context, c redis.UniversalClient, l Logger, retry SubscribeRetryConfig, channel string, handler func(msg *redis.Message), ready func()) {
	l.Debugf("Subscribing to channel: %s", channel)

	go func() {
		backoff := normalizeSubscribeRetry(retry)
		var attempts int
		var lastErr error

		for {
			if ctx.Err() != nil {
				l.Infof("Context cancelled for channel %s. Shutting down subscriber.", channel)
				return
			}

			pubsub := c.Subscribe(ctx, channel)
			if _, err := pubsub.Receive(ctx); err != nil {
				l.Errorf("Failed to subscribe to channel %s: %v", channel, err)
				lastErr = err
				_ = pubsub.Close()
				if !backoff.Enabled {
					return
				}
				attempts++
				if backoff.MaxRetries > 0 && attempts > backoff.MaxRetries {
					l.Errorf("Exceeded max retries for channel %s.", channel)
					return
				}
				wait := backoff.Next()
				if backoff.OnRetry != nil {
					backoff.OnRetry(attempts, wait, lastErr)
				}
				l.Warnf("Retrying subscription to %s in %s...", channel, wait)
				if !sleepWithContext(ctx, wait) {
					return
				}
				continue
			}

			attempts = 0
			ch := pubsub.Channel()
			l.Infof("Started subscriber goroutine for channel: %s", channel)
			if ready != nil {
				ready()
			}

			closed := false
			for !closed {
				select {
				case <-ctx.Done():
					l.Infof("Context cancelled for channel %s. Shutting down subscriber.", channel)
					closed = true
				case msg, ok := <-ch:
					if !ok {
						l.Warnf("Pub/Sub channel %s was closed.", channel)
						closed = true
						break
					}
					l.Debugf("Received message on channel %s", msg.Channel)
					func() {
						defer func() {
							if r := recover(); r != nil {
								l.Errorf("Recovered from panic in pubsub handler for channel %s: %v", channel, r)
							}
						}()
						handler(msg)
					}()
				}
			}

			_ = pubsub.Close()
			if !backoff.Enabled || ctx.Err() != nil {
				return
			}
			attempts++
			if backoff.MaxRetries > 0 && attempts > backoff.MaxRetries {
				l.Errorf("Exceeded max retries for channel %s.", channel)
				return
			}
			wait := backoff.Next()
			if backoff.OnRetry != nil {
				backoff.OnRetry(attempts, wait, lastErr)
			}
			l.Warnf("Retrying subscription to %s in %s...", channel, wait)
			if !sleepWithContext(ctx, wait) {
				return
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
	op := "StreamAdd"
	ctx = hookBefore(ctx, globalHook, op)
	start := time.Now()
	ctx, cancel := withDefaultTimeout(ctx, globalDefaultTimeout)
	if cancel != nil {
		defer cancel()
	}
	res, err := client.XAdd(ctx, &redis.XAddArgs{
		Stream: streamName,
		Values: values,
	}).Result()
	observe(globalMetrics, op, start, err)
	hookAfter(ctx, globalHook, op, err, start)
	return res, err
}

// StreamAdd adds a message to a Redis Stream using an instance client.
func (c *Client) StreamAdd(ctx context.Context, streamName string, values map[string]interface{}) (string, error) {
	op := "StreamAdd"
	ctx = hookBefore(ctx, c.hook, op)
	start := time.Now()
	ctx, cancel := withDefaultTimeout(ctx, c.defaultTimeout)
	if cancel != nil {
		defer cancel()
	}
	res, err := c.client.XAdd(ctx, &redis.XAddArgs{
		Stream: streamName,
		Values: values,
	}).Result()
	observe(c.metrics, op, start, err)
	hookAfter(ctx, c.hook, op, err, start)
	return res, err
}

// StreamConsume reads a single message from a stream using a consumer group.
// It will automatically create the stream and consumer group if they don't exist.
// It blocks until a new message is available.
// Returns the message or an error (e.g., redis.Nil if no message is returned).
func StreamConsume(ctx context.Context, streamName, groupName, consumerName string) (*redis.XMessage, error) {
	client := GetClient()

	// Attempt to create the group. Ignore error if it already exists.
	// The MKSTREAM option creates the stream if it doesn't exist.
	if err := ensureGroup(ctx, client, logger, streamName, groupName); err != nil {
		return nil, err
	}

	// Read from the stream.
	op := "StreamConsume"
	ctx = hookBefore(ctx, globalHook, op)
	start := time.Now()
	streams, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: consumerName,
		Streams:  []string{streamName, ">"}, // ">" means new messages only
		Count:    1,                         // Read one message at a time
		Block:    0,                         // Block forever
	}).Result()
	observe(globalMetrics, op, start, err)
	hookAfter(ctx, globalHook, op, err, start)

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
	if err := ensureGroup(ctx, client, logger, streamName, groupName); err != nil {
		return nil, err
	}

	// Read from the stream.
	op := "StreamConsumeAdvanced"
	ctx = hookBefore(ctx, globalHook, op)
	start := time.Now()
	streams, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: consumerName,
		Streams:  []string{streamName, ">"}, // ">" means new messages only
		Count:    count,
		Block:    block,
	}).Result()
	observe(globalMetrics, op, start, err)
	hookAfter(ctx, globalHook, op, err, start)

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
	op := "StreamAck"
	ctx = hookBefore(ctx, globalHook, op)
	start := time.Now()
	ctx, cancel := withDefaultTimeout(ctx, globalDefaultTimeout)
	if cancel != nil {
		defer cancel()
	}
	err := client.XAck(ctx, streamName, groupName, messageID).Err()
	observe(globalMetrics, op, start, err)
	hookAfter(ctx, globalHook, op, err, start)
	return err
}

// StreamConsume reads a single message from a stream using an instance client.
func (c *Client) StreamConsume(ctx context.Context, streamName, groupName, consumerName string) (*redis.XMessage, error) {
	if err := ensureGroup(ctx, c.client, c.logger, streamName, groupName); err != nil {
		return nil, err
	}

	op := "StreamConsume"
	ctx = hookBefore(ctx, c.hook, op)
	start := time.Now()
	streams, err := c.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: consumerName,
		Streams:  []string{streamName, ">"},
		Count:    1,
		Block:    0,
	}).Result()
	observe(c.metrics, op, start, err)
	hookAfter(ctx, c.hook, op, err, start)

	if err != nil {
		if errors.Is(err, redis.Nil) {
			c.logger.Debugf("StreamConsume on %s timed out as expected.", streamName)
			return nil, err
		}
		c.logger.Warnf("Error consuming from stream %s: %v", streamName, err)
		return nil, err
	}

	if len(streams) > 0 && len(streams[0].Messages) > 0 {
		return &streams[0].Messages[0], nil
	}

	return nil, redis.Nil
}

// StreamConsumeAdvanced provides more control over stream consumption using an instance client.
func (c *Client) StreamConsumeAdvanced(ctx context.Context, streamName, groupName, consumerName string, block time.Duration, count int64) ([]redis.XMessage, error) {
	if err := ensureGroup(ctx, c.client, c.logger, streamName, groupName); err != nil {
		return nil, err
	}

	op := "StreamConsumeAdvanced"
	ctx = hookBefore(ctx, c.hook, op)
	start := time.Now()
	streams, err := c.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: consumerName,
		Streams:  []string{streamName, ">"},
		Count:    count,
		Block:    block,
	}).Result()
	observe(c.metrics, op, start, err)
	hookAfter(ctx, c.hook, op, err, start)

	if err != nil {
		if errors.Is(err, redis.Nil) {
			c.logger.Debugf("StreamConsumeAdvanced on %s timed out as expected.", streamName)
			return nil, nil
		}
		c.logger.Warnf("Error in StreamConsumeAdvanced for stream %s: %v", streamName, err)
		return nil, err
	}

	if len(streams) > 0 {
		return streams[0].Messages, nil
	}

	return nil, nil
}

// StreamAck acknowledges the successful processing of a message using an instance client.
func (c *Client) StreamAck(ctx context.Context, streamName, groupName, messageID string) error {
	op := "StreamAck"
	ctx = hookBefore(ctx, c.hook, op)
	start := time.Now()
	ctx, cancel := withDefaultTimeout(ctx, c.defaultTimeout)
	if cancel != nil {
		defer cancel()
	}
	err := c.client.XAck(ctx, streamName, groupName, messageID).Err()
	observe(c.metrics, op, start, err)
	hookAfter(ctx, c.hook, op, err, start)
	return err
}

// StreamClaim finds and claims pending messages that have been idle for longer than minIdleTime.
// This is used to recover messages from crashed consumers.
func StreamClaim(ctx context.Context, streamName, groupName, consumerName string, minIdleTime time.Duration) ([]redis.XMessage, error) {
	client := GetClient()
	op := "StreamClaim"
	ctx = hookBefore(ctx, globalHook, op)
	start := time.Now()
	ctx, cancel := withDefaultTimeout(ctx, globalDefaultTimeout)
	if cancel != nil {
		defer cancel()
	}

	// Use XAutoClaim for an atomic, efficient implementation of claim recovery.
	messages, _, err := client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   streamName,
		Group:    groupName,
		Consumer: consumerName,
		MinIdle:  minIdleTime,
		Start:    "0-0",
		Count:    100, // Max number of pending messages to claim at once
	}).Result()
	observe(globalMetrics, op, start, err)
	hookAfter(ctx, globalHook, op, err, start)

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

// StreamPendingSummary returns a summary of pending messages for a group.
func StreamPendingSummary(ctx context.Context, streamName, groupName string) (*redis.XPending, error) {
	client := GetClient()
	op := "StreamPendingSummary"
	ctx = hookBefore(ctx, globalHook, op)
	start := time.Now()
	ctx, cancel := withDefaultTimeout(ctx, globalDefaultTimeout)
	if cancel != nil {
		defer cancel()
	}
	res, err := client.XPending(ctx, streamName, groupName).Result()
	observe(globalMetrics, op, start, err)
	hookAfter(ctx, globalHook, op, err, start)
	return res, err
}

// StreamClaim finds and claims pending messages using an instance client.
func (c *Client) StreamClaim(ctx context.Context, streamName, groupName, consumerName string, minIdleTime time.Duration) ([]redis.XMessage, error) {
	op := "StreamClaim"
	ctx = hookBefore(ctx, c.hook, op)
	start := time.Now()
	ctx, cancel := withDefaultTimeout(ctx, c.defaultTimeout)
	if cancel != nil {
		defer cancel()
	}
	messages, _, err := c.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   streamName,
		Group:    groupName,
		Consumer: consumerName,
		MinIdle:  minIdleTime,
		Start:    "0-0",
		Count:    100,
	}).Result()
	observe(c.metrics, op, start, err)
	hookAfter(ctx, c.hook, op, err, start)

	if err != nil {
		c.logger.Errorf("Failed to auto-claim messages in stream %s: %v", streamName, err)
		return nil, err
	}

	if len(messages) == 0 {
		c.logger.Debugf("No stale messages to claim in stream %s.", streamName)
		return nil, nil
	}

	c.logger.Infof("Successfully claimed %d messages for consumer %s.", len(messages), consumerName)
	return messages, nil
}

// StreamPendingSummary returns a summary of pending messages for a group using an instance client.
func (c *Client) StreamPendingSummary(ctx context.Context, streamName, groupName string) (*redis.XPending, error) {
	op := "StreamPendingSummary"
	ctx = hookBefore(ctx, c.hook, op)
	start := time.Now()
	ctx, cancel := withDefaultTimeout(ctx, c.defaultTimeout)
	if cancel != nil {
		defer cancel()
	}
	res, err := c.client.XPending(ctx, streamName, groupName).Result()
	observe(c.metrics, op, start, err)
	hookAfter(ctx, c.hook, op, err, start)
	return res, err
}

// StreamPendingList returns a list of pending entries for a group.
// Pass an empty consumer string to fetch across all consumers.
func StreamPendingList(ctx context.Context, streamName, groupName, start, end string, count int64, consumer string) ([]redis.XPendingExt, error) {
	client := GetClient()
	op := "StreamPendingList"
	ctx = hookBefore(ctx, globalHook, op)
	startTime := time.Now()
	ctx, cancel := withDefaultTimeout(ctx, globalDefaultTimeout)
	if cancel != nil {
		defer cancel()
	}
	res, err := client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream:   streamName,
		Group:    groupName,
		Start:    start,
		End:      end,
		Count:    count,
		Consumer: consumer,
	}).Result()
	observe(globalMetrics, op, startTime, err)
	hookAfter(ctx, globalHook, op, err, startTime)
	return res, err
}

// StreamPendingCount returns the number of pending entries for a group.
func StreamPendingCount(ctx context.Context, streamName, groupName string) (int64, error) {
	summary, err := StreamPendingSummary(ctx, streamName, groupName)
	if err != nil {
		return 0, err
	}
	return summary.Count, nil
}

// StreamPendingCount returns the number of pending entries for a group using an instance client.
func (c *Client) StreamPendingCount(ctx context.Context, streamName, groupName string) (int64, error) {
	summary, err := c.StreamPendingSummary(ctx, streamName, groupName)
	if err != nil {
		return 0, err
	}
	return summary.Count, nil
}

// StreamPendingList returns a list of pending entries for a group using an instance client.
// Pass an empty consumer string to fetch across all consumers.
func (c *Client) StreamPendingList(ctx context.Context, streamName, groupName, start, end string, count int64, consumer string) ([]redis.XPendingExt, error) {
	op := "StreamPendingList"
	ctx = hookBefore(ctx, c.hook, op)
	startTime := time.Now()
	ctx, cancel := withDefaultTimeout(ctx, c.defaultTimeout)
	if cancel != nil {
		defer cancel()
	}
	res, err := c.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream:   streamName,
		Group:    groupName,
		Start:    start,
		End:      end,
		Count:    count,
		Consumer: consumer,
	}).Result()
	observe(c.metrics, op, startTime, err)
	hookAfter(ctx, c.hook, op, err, startTime)
	return res, err
}

func observe(m MetricsCollector, op string, start time.Time, err error) {
	if m == nil {
		return
	}
	m.ObserveDuration(op, time.Since(start), err)
}

func hookBefore(ctx context.Context, h Hook, op string) context.Context {
	if h == nil {
		return ctx
	}
	next := h.Before(ctx, op)
	if next == nil {
		return ctx
	}
	return next
}

func hookAfter(ctx context.Context, h Hook, op string, err error, start time.Time) {
	if h == nil {
		return
	}
	h.After(ctx, op, err, time.Since(start))
}

type retryState struct {
	Enabled    bool
	MinBackoff time.Duration
	MaxBackoff time.Duration
	MaxRetries int
	Jitter     float64
	OnRetry    func(attempt int, wait time.Duration, err error)
	current    time.Duration
}

func normalizeSubscribeRetry(cfg SubscribeRetryConfig) retryState {
	state := retryState{
		Enabled:    cfg.Enabled,
		MinBackoff: cfg.MinBackoff,
		MaxBackoff: cfg.MaxBackoff,
		MaxRetries: cfg.MaxRetries,
		Jitter:     cfg.Jitter,
		OnRetry:    cfg.OnRetry,
	}
	if !state.Enabled {
		return state
	}
	if state.MinBackoff <= 0 {
		state.MinBackoff = 200 * time.Millisecond
	}
	if state.MaxBackoff <= 0 || state.MaxBackoff < state.MinBackoff {
		state.MaxBackoff = 5 * time.Second
	}
	state.current = state.MinBackoff
	return state
}

func (r *retryState) Next() time.Duration {
	if r.current <= 0 {
		r.current = r.MinBackoff
	}
	wait := r.current
	wait = applyJitter(wait, r.Jitter)
	next := r.current * 2
	if next > r.MaxBackoff {
		next = r.MaxBackoff
	}
	r.current = next
	return wait
}

func applyJitter(d time.Duration, jitter float64) time.Duration {
	if jitter <= 0 {
		return d
	}
	if jitter > 1 {
		jitter = 1
	}
	// random in [1-jitter, 1+jitter]
	factor := 1 - jitter + (randFloat64() * 2 * jitter)
	if factor < 0 {
		factor = 0
	}
	return time.Duration(float64(d) * factor)
}

func randFloat64() float64 {
	return randFloat64Source()
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

func ensureGroup(ctx context.Context, client redis.UniversalClient, l Logger, streamName, groupName string) error {
	err := client.XGroupCreateMkStream(ctx, streamName, groupName, "0").Err()
	if err == nil {
		return nil
	}
	if isBusyGroupError(err) {
		return nil
	}
	l.Warnf("Failed to create consumer group %s for stream %s: %v", groupName, streamName, err)
	return err
}

func isBusyGroupError(err error) bool {
	if err == nil {
		return false
	}
	return strings.HasPrefix(err.Error(), "BUSYGROUP")
}

func buildLogger(cfg Config) Logger {
	if cfg.Logger != nil {
		return cfg.Logger
	}
	if cfg.LogLevel == LogLevelNone {
		return &discardLogger{}
	}
	if cfg.LogLevel > LogLevelDebug {
		return &leveledLogger{level: LogLevelInfo}
	}
	return &leveledLogger{level: cfg.LogLevel}
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
