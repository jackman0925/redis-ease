# Redis-Ease（中文说明）

Redis-Ease 是一个轻量、易用的 Go Redis 封装库，基于 `go-redis`，提供统一的初始化、多实例客户端、常用命令封装、Streams 队列能力以及生产级可观测与重连配置。

## 特性

- 简单配置，支持单机 / Sentinel / Cluster（UniversalClient）
- 便捷封装：`Get/Set/Del/HSet/HGet/Exists` 等
- Streams 队列：消费组、ACK、自动认领、pending 观测
- Pub/Sub 支持重连、抖动回退、重连回调
- 默认超时、连接池与重试配置
- Metrics 与 Hook（Tracing）接入
- 多实例客户端（推荐生产使用）

## 安装

```sh
go get github.com/jackman0925/redis-ease
```

## 快速开始

### 初始化（推荐使用 InitWithError）

```go
import "github.com/jackman0925/redis-ease"

func main() {
    config := redis_ease.Config{
        Addresses: []string{"localhost:6379"},
        DB:        0,
    }
    if err := redis_ease.InitWithError(config); err != nil {
        panic(err)
    }
}
```

### 基本 KV 操作

```go
ctx := context.Background()

_ = redis_ease.Set(ctx, "user:1", "Alice", 10*time.Minute)
val, err := redis_ease.Get(ctx, "user:1")
if err == redis.Nil {
    // key 不存在
}
_ = val
```

## Streams 队列

### 生产者

```go
msgID, err := redis_ease.StreamAdd(context.Background(), "orders", map[string]interface{}{
    "order_id": 123,
})
_ = msgID
_ = err
```

### 消费者

```go
msg, err := redis_ease.StreamConsume(context.Background(), "orders", "group1", "consumer-1")
if err == nil && msg != nil {
    // 处理消息
    _ = redis_ease.StreamAck(context.Background(), "orders", "group1", msg.ID)
}
```

### Pending 观测

```go
summary, _ := redis_ease.StreamPendingSummary(context.Background(), "orders", "group1")
list, _ := redis_ease.StreamPendingList(context.Background(), "orders", "group1", "-", "+", 10, "")
count, _ := redis_ease.StreamPendingCount(context.Background(), "orders", "group1")
_ = summary
_ = list
_ = count
```

## Pub/Sub（支持重连）

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

handler := func(msg *redis.Message) {
    fmt.Println(msg.Payload)
}

redis_ease.Subscribe(ctx, "news", handler)
```

如需订阅建立后回调：

```go
ready := make(chan struct{}, 1)
redis_ease.SubscribeWithReady(ctx, "news", handler, func() { ready <- struct{}{} })
<-ready
```

重连配置：

```go
config := redis_ease.Config{
    Addresses: []string{"localhost:6379"},
    SubscribeRetry: redis_ease.SubscribeRetryConfig{
        Enabled:    true,
        MinBackoff: 200 * time.Millisecond,
        MaxBackoff: 5 * time.Second,
        MaxRetries: 0,
        Jitter:     0.2,
        OnRetry: func(attempt int, wait time.Duration, err error) {
            _ = attempt
            _ = wait
            _ = err
        },
    },
}
_ = redis_ease.InitWithError(config)
```

## 多实例客户端（推荐生产）

```go
client, err := redis_ease.NewClientWithError(redis_ease.Config{
    Addresses: []string{"localhost:6379"},
    DB:        1,
})
if err != nil {
    panic(err)
}
defer client.Close()

_ = client.Set(context.Background(), "k", "v", 0)
```

## 连接/超时/池化配置

```go
config := redis_ease.Config{
    Addresses:      []string{"localhost:6379"},
    DB:             0,
    DefaultTimeout: 2 * time.Second,
    DialTimeout:    5 * time.Second,
    ReadTimeout:    2 * time.Second,
    WriteTimeout:   2 * time.Second,
    PoolSize:       50,
    MinIdleConns:   10,
    MaxRetries:     3,
    TLSConfig:      &tls.Config{MinVersion: tls.VersionTLS12},
}
_ = redis_ease.InitWithError(config)
```

## Metrics 与 Hook（Tracing）

```go
type MyMetrics struct{}
func (m *MyMetrics) ObserveDuration(op string, d time.Duration, err error) {}

type MyHook struct{}
func (h *MyHook) Before(ctx context.Context, op string) context.Context { return ctx }
func (h *MyHook) After(ctx context.Context, op string, err error, d time.Duration) {}

config := redis_ease.Config{
    Addresses: []string{"localhost:6379"},
    Metrics:   &MyMetrics{},
    Hook:      &MyHook{},
}
_ = redis_ease.InitWithError(config)
```

## 测试与基准

```sh
go test ./...

REDIS_BENCH=1 go test -run '^$' -bench . ./...
```

更多集成测试与环境变量请参考英文 `README.md` 中的 “Test & Bench Automation” 章节。

---

如需我补充更多中文示例或部署建议，请告诉我你的运行环境。 
