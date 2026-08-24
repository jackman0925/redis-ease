package redis_ease

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const defaultInitTimeout = 5 * time.Second

var (
	ErrAlreadyInitialized = errors.New("redis-ease: default client already initialized")
	ErrClientClosed       = errors.New("redis-ease: client is closed")
	ErrNotInitialized     = errors.New("redis-ease: default client is not initialized")
)

// Config holds Redis connection and redis-ease behavior settings.
type Config struct {
	// Addresses contains single-node, cluster seed, or Sentinel addresses.
	Addresses []string
	// Username and Password configure Redis ACL authentication.
	Username string
	Password string
	// DB selects the database for single-node and Sentinel clients.
	DB int
	// ClientName is assigned to each Redis connection.
	ClientName string

	// DefaultTimeout applies when a non-blocking command context has no deadline.
	DefaultTimeout time.Duration
	// InitTimeout limits the constructor health check. The default is five seconds.
	InitTimeout time.Duration
	// DialTimeout, ReadTimeout, and WriteTimeout configure socket operations.
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	// PoolSize is the maximum base connection-pool size.
	PoolSize int
	// PoolTimeout limits how long a caller waits for an available connection.
	PoolTimeout time.Duration
	// MinIdleConns and MaxIdleConns bound idle pool connections.
	MinIdleConns int
	MaxIdleConns int
	// MaxActiveConns limits active connections; zero uses the go-redis default.
	MaxActiveConns int
	// ConnMaxIdleTime and ConnMaxLifetime control connection recycling.
	ConnMaxIdleTime time.Duration
	ConnMaxLifetime time.Duration

	// MaxRetries configures go-redis command retries.
	MaxRetries int
	// MinRetryBackoff and MaxRetryBackoff bound command retry delays.
	MinRetryBackoff time.Duration
	MaxRetryBackoff time.Duration

	// TLSConfig enables TLS when non-nil.
	TLSConfig *tls.Config
	// MasterName enables Sentinel failover mode.
	MasterName string
	// SentinelUsername and SentinelPassword authenticate to Sentinel.
	SentinelUsername string
	SentinelPassword string
	// IsClusterMode forces cluster mode when only one seed address is supplied.
	IsClusterMode bool

	// Metrics and Hook provide optional instrumentation.
	Metrics MetricsCollector
	Hook    Hook
	// SubscribeRetry controls initial Pub/Sub establishment retries.
	SubscribeRetry SubscribeRetryConfig
	// Logger overrides the built-in logger.
	Logger Logger
	// LogLevel defaults to LogLevelInfo; LogLevelNone disables built-in logging.
	LogLevel LogLevel
}

// NewClient constructs a client and panics if construction fails.
func NewClient(cfg Config) *Client {
	c, err := NewClientWithError(cfg)
	if err != nil {
		panic(err)
	}
	return c
}

// NewClientWithError constructs and verifies an instance-scoped client.
func NewClientWithError(cfg Config) (*Client, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}

	l := buildLogger(cfg)
	rc := redis.NewUniversalClient(universalOptions(cfg))
	initTimeout := cfg.InitTimeout
	if initTimeout <= 0 {
		initTimeout = defaultInitTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), initTimeout)
	defer cancel()
	if err := rc.Ping(ctx).Err(); err != nil {
		_ = rc.Close()
		return nil, fmt.Errorf("redis-ease: failed to connect to redis: %w", err)
	}

	l.Infof("redis-ease client initialized successfully")
	return newClient(rc, cfg, l), nil
}

func validateConfig(cfg Config) error {
	if len(cfg.Addresses) == 0 {
		return errors.New("redis-ease: at least one address is required")
	}
	for _, addr := range cfg.Addresses {
		if strings.TrimSpace(addr) == "" {
			return errors.New("redis-ease: addresses must not contain empty values")
		}
	}
	if cfg.DB < 0 || cfg.PoolSize < 0 || cfg.MinIdleConns < 0 || cfg.MaxIdleConns < 0 || cfg.MaxActiveConns < 0 {
		return errors.New("redis-ease: DB and pool sizes must not be negative")
	}
	if cfg.SubscribeRetry.MaxRetries < 0 {
		return errors.New("redis-ease: subscribe max retries must not be negative")
	}
	if cfg.SubscribeRetry.Jitter < 0 || cfg.SubscribeRetry.Jitter > 1 {
		return errors.New("redis-ease: subscribe jitter must be between 0 and 1")
	}
	return nil
}

func universalOptions(cfg Config) *redis.UniversalOptions {
	return &redis.UniversalOptions{
		Addrs: cfg.Addresses, ClientName: cfg.ClientName, DB: cfg.DB,
		Username: cfg.Username, Password: cfg.Password,
		SentinelUsername: cfg.SentinelUsername, SentinelPassword: cfg.SentinelPassword,
		MasterName: cfg.MasterName, IsClusterMode: cfg.IsClusterMode,
		DialTimeout: cfg.DialTimeout, ReadTimeout: cfg.ReadTimeout, WriteTimeout: cfg.WriteTimeout,
		PoolSize: cfg.PoolSize, PoolTimeout: cfg.PoolTimeout,
		MinIdleConns: cfg.MinIdleConns, MaxIdleConns: cfg.MaxIdleConns,
		MaxActiveConns: cfg.MaxActiveConns, ConnMaxIdleTime: cfg.ConnMaxIdleTime,
		ConnMaxLifetime: cfg.ConnMaxLifetime,
		MaxRetries:      cfg.MaxRetries, MinRetryBackoff: cfg.MinRetryBackoff,
		MaxRetryBackoff: cfg.MaxRetryBackoff, TLSConfig: cfg.TLSConfig,
	}
}
