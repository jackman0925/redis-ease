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

// Config 包含 Redis 连接参数和 redis-ease 行为配置。
type Config struct {
	// Addresses 包含单机地址、集群种子地址或 Sentinel 地址。
	Addresses []string
	// Username 和 Password 用于配置 Redis ACL 认证。
	Username string
	Password string
	// DB 为单机和 Sentinel 客户端选择数据库。
	DB int
	// ClientName 会设置到每个 Redis 连接。
	ClientName string

	// DefaultTimeout 用于未设置截止时间的非阻塞命令 context。
	DefaultTimeout time.Duration
	// InitTimeout 限制构造阶段的健康检查时间，默认为五秒。
	InitTimeout time.Duration
	// DialTimeout、ReadTimeout 和 WriteTimeout 用于配置套接字操作超时。
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	// PoolSize 是基础连接池的最大连接数。
	PoolSize int
	// PoolTimeout 限制调用方等待可用连接的时间。
	PoolTimeout time.Duration
	// MinIdleConns 和 MaxIdleConns 限制连接池中的空闲连接数。
	MinIdleConns int
	MaxIdleConns int
	// MaxActiveConns 限制活跃连接数；零值使用 go-redis 默认配置。
	MaxActiveConns int
	// ConnMaxIdleTime 和 ConnMaxLifetime 控制连接回收。
	ConnMaxIdleTime time.Duration
	ConnMaxLifetime time.Duration

	// MaxRetries 配置 go-redis 命令重试次数。
	MaxRetries int
	// MinRetryBackoff 和 MaxRetryBackoff 限制命令重试等待时间。
	MinRetryBackoff time.Duration
	MaxRetryBackoff time.Duration

	// TLSConfig 非空时启用 TLS。
	TLSConfig *tls.Config
	// MasterName 用于启用 Sentinel 故障转移模式。
	MasterName string
	// SentinelUsername 和 SentinelPassword 用于 Sentinel 认证。
	SentinelUsername string
	SentinelPassword string
	// IsClusterMode 在仅提供一个种子地址时强制使用集群模式。
	IsClusterMode bool

	// Metrics 和 Hook 提供可选的可观测能力。
	Metrics MetricsCollector
	Hook    Hook
	// SubscribeRetry 控制首次建立 Pub/Sub 订阅时的重试策略。
	SubscribeRetry SubscribeRetryConfig
	// Logger 用于替换内置日志实现。
	Logger Logger
	// LogLevel 默认为 LogLevelInfo；LogLevelNone 禁用内置日志。
	LogLevel LogLevel
}

// NewClient 创建客户端，创建失败时触发 panic。
func NewClient(cfg Config) *Client {
	c, err := NewClientWithError(cfg)
	if err != nil {
		panic(err)
	}
	return c
}

// NewClientWithError 创建并验证实例级客户端。
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
