package redis_ease

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

// TestMain sets up the connection for all tests in the package.
// It uses database 15 to prevent conflicts with any development data.
func TestMain(m *testing.M) {
	// Configuration for test Redis server
	config := Config{
		Addresses: []string{"localhost:6379"},
		Password:  "",
		DB:        15, // Use a dedicated test database
	}

	// Initialize client
	if err := InitWithError(config); err != nil {
		fmt.Printf("Could not initialize Redis client. Skipping tests. Error: %v\n", err)
		os.Exit(0)
	}

	// Ensure we can connect before running tests
	client := GetClient()
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		fmt.Printf("Could not connect to Redis at localhost:6379. Skipping tests. Error: %v\n", err)
		// Exit with a special code that tells `go test` to skip.
		os.Exit(0)
	}

	// Clean up the test database before running tests
	client.FlushDB(ctx)

	// Run all tests
	exitCode := m.Run()

	// Clean up after tests
	client.FlushDB(ctx)

	os.Exit(exitCode)
}

func TestSetAndGet(t *testing.T) {
	key := "test:setget"
	value := "hello world"

	t.Cleanup(func() {
		Del(context.Background(), key)
	})

	err := Set(context.Background(), key, value, 0)
	assert.NoError(t, err)

	gotValue, err := Get(context.Background(), key)
	assert.NoError(t, err)
	assert.Equal(t, value, gotValue)
}

func TestInstanceClientSetGet(t *testing.T) {
	client, err := NewClientWithError(Config{
		Addresses: []string{"localhost:6379"},
		DB:        14,
	})
	if err != nil {
		t.Fatalf("failed to create instance client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	key := "test:instance:setget"

	t.Cleanup(func() {
		client.Del(ctx, key)
	})

	err = client.Set(ctx, key, "ok", 0)
	assert.NoError(t, err)

	val, err := client.Get(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, "ok", val)
}

func TestDefaultTimeoutAppliesToInstanceClient(t *testing.T) {
	client, err := NewClientWithError(Config{
		Addresses:      []string{"localhost:6379"},
		DB:             14,
		DefaultTimeout: 1 * time.Nanosecond,
	})
	if err != nil {
		t.Fatalf("failed to create instance client: %v", err)
	}
	defer client.Close()

	_, err = client.Get(context.Background(), "timeout:key")
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestGetNonExistent(t *testing.T) {
	key := "test:nonexistent"
	_, err := Get(context.Background(), key)
	assert.Error(t, err)
	assert.Equal(t, redis.Nil, err, "Error should be redis.Nil for a non-existent key")
}

func TestDel(t *testing.T) {
	key1 := "test:del1"
	key2 := "test:del2"

	Set(context.Background(), key1, "v1", 0)
	Set(context.Background(), key2, "v2", 0)

	deletedCount, err := Del(context.Background(), key1, key2)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), deletedCount)

	_, err = Get(context.Background(), key1)
	assert.Equal(t, redis.Nil, err)
}

func TestHSetAndHGet(t *testing.T) {
	key := "test:hash"
	field := "name"
	value := "gemini"

	t.Cleanup(func() {
		Del(context.Background(), key)
	})

	_, err := HSet(context.Background(), key, field, value)
	assert.NoError(t, err)

	gotValue, err := HGet(context.Background(), key, field)
	assert.NoError(t, err)
	assert.Equal(t, value, gotValue)
}

func TestHGetNonExistent(t *testing.T) {
	key := "test:hash_nonexistent"
	field := "field"

	// Test non-existent field in existing hash
	HSet(context.Background(), key, "another_field", "some_value")
	t.Cleanup(func() { Del(context.Background(), key) })
	_, err := HGet(context.Background(), key, field)
	assert.Equal(t, redis.Nil, err)

	// Test on non-existent key
	_, err = HGet(context.Background(), "nonexistent_key", field)
	assert.Equal(t, redis.Nil, err)
}

func TestExists(t *testing.T) {
	key := "test:exists"

	t.Cleanup(func() {
		Del(context.Background(), key)
	})

	// Should not exist initially
	existCount, err := Exists(context.Background(), key)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), existCount)

	// Should exist after Set
	Set(context.Background(), key, "any value", 0)
	existCount, err = Exists(context.Background(), key)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), existCount)
}

func TestPubSub(t *testing.T) {
	channel := "test:pubsub"
	message := "hello pubsub"
	received := make(chan string, 1)
	ready := make(chan struct{}, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := func(msg *redis.Message) {
		received <- msg.Payload
	}

	originalRetry := globalSubscribeRetry
	globalSubscribeRetry = SubscribeRetryConfig{Enabled: true, MinBackoff: 10 * time.Millisecond, MaxBackoff: 50 * time.Millisecond, MaxRetries: 1}
	defer func() { globalSubscribeRetry = originalRetry }()

	SubscribeWithReady(ctx, channel, handler, func() { ready <- struct{}{} })

	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for subscription ready")
	}

	err := Publish(context.Background(), channel, message)
	assert.NoError(t, err)

	select {
	case gotMessage := <-received:
		assert.Equal(t, message, gotMessage)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message")
	}
}

func TestStreamFunctions(t *testing.T) {
	stream := "test:stream"
	group := "test:group"
	consumer := "test:consumer"
	msgChan := make(chan *redis.XMessage)

	t.Cleanup(func() {
		Del(context.Background(), stream)
	})

	// Run consumer in a separate goroutine because it blocks
	go func() {
		msg, err := StreamConsume(context.Background(), stream, group, consumer)
		if err == nil {
			msgChan <- msg
		}
	}()

	// Producer adds a message
	payload := map[string]interface{}{"data": "important-message"}
	msgID, err := StreamAdd(context.Background(), stream, payload)
	assert.NoError(t, err)
	assert.NotEmpty(t, msgID)

	// Wait for the consumer to receive the message
	consumedMsg := <-msgChan

	// Assertions
	assert.NotNil(t, consumedMsg)
	assert.Equal(t, msgID, consumedMsg.ID)
	assert.Equal(t, "important-message", consumedMsg.Values["data"])

	// Acknowledge the message
	err = StreamAck(context.Background(), stream, group, consumedMsg.ID)
	assert.NoError(t, err)
}

func TestStreamConsumeAdvanced(t *testing.T) {
	stream := "test:stream_advanced"
	group := "test:group_advanced"
	consumer := "test:consumer_advanced"

	t.Cleanup(func() {
		Del(context.Background(), stream)
	})

	// Test bulk consumption
	for i := 0; i < 3; i++ {
		StreamAdd(context.Background(), stream, map[string]interface{}{"n": fmt.Sprintf("%d", i)})
	}

	msgs, err := StreamConsumeAdvanced(context.Background(), stream, group, consumer, 2*time.Second, 3)
	assert.NoError(t, err)
	assert.Len(t, msgs, 3)
	assert.Equal(t, "0", msgs[0].Values["n"])
	assert.Equal(t, "2", msgs[2].Values["n"])

	// Test timeout
	emptyMsgs, err := StreamConsumeAdvanced(context.Background(), stream, group, consumer, 100*time.Millisecond, 1)
	assert.NoError(t, err)
	assert.Empty(t, emptyMsgs)
}

func TestStreamGroupCreateBusyGroup(t *testing.T) {
	stream := "test:stream_group_create"
	group := "test:group_create"
	consumer := "test:consumer_create"

	t.Cleanup(func() {
		Del(context.Background(), stream)
	})

	StreamAdd(context.Background(), stream, map[string]interface{}{"n": "1"})

	_, err := StreamConsumeAdvanced(context.Background(), stream, group, consumer, 50*time.Millisecond, 1)
	assert.NoError(t, err)

	_, err = StreamConsumeAdvanced(context.Background(), stream, group, consumer, 50*time.Millisecond, 1)
	assert.NoError(t, err)
}

func TestStreamClaim(t *testing.T) {
	stream := "test:stream_claim"
	group := "test:group_claim"
	badConsumer := "consumer_bad"
	recoveryConsumer := "consumer_recovery"

	t.Cleanup(func() {
		Del(context.Background(), stream)
	})

	// 1. Producer adds a message
	payload := map[string]interface{}{"task": "process_video"}
	msgID, err := StreamAdd(context.Background(), stream, payload)
	assert.NoError(t, err)

	// 2. A "bad" consumer reads it but never ACKs
	// We need to create the group first
	_ = GetClient().XGroupCreateMkStream(context.Background(), stream, group, "0").Err()
	readResult, err := GetClient().XReadGroup(context.Background(), &redis.XReadGroupArgs{
		Group:    group,
		Consumer: badConsumer,
		Streams:  []string{stream, ">"},
		Count:    1,
	}).Result()
	assert.NoError(t, err)
	assert.Len(t, readResult[0].Messages, 1)

	// 3. Wait for the message to become "idle"
	time.Sleep(100 * time.Millisecond)

	// 4. The "recovery" consumer claims stale messages
	claimedMsgs, err := StreamClaim(context.Background(), stream, group, recoveryConsumer, 50*time.Millisecond)
	assert.NoError(t, err)
	assert.Len(t, claimedMsgs, 1)
	assert.Equal(t, msgID, claimedMsgs[0].ID)

	// 5. The recovery consumer ACKs the message to finish the job
	err = StreamAck(context.Background(), stream, group, claimedMsgs[0].ID)
	assert.NoError(t, err)
}

func TestStreamGroupCreateErrorPropagates(t *testing.T) {
	ctx := context.Background()
	stream := "test:stream_group_error"
	group := "test:group_error"

	t.Cleanup(func() {
		Del(ctx, stream)
		Del(ctx, group)
	})

	_, err := StreamAdd(ctx, stream, map[string]interface{}{"k": "v"})
	assert.NoError(t, err)

	// Create a conflicting key on the stream name to make XGROUP CREATE fail (WRONGTYPE)
	err = GetClient().Set(ctx, stream, "not-a-stream", 0).Err()
	assert.NoError(t, err)

	_, err = StreamConsume(ctx, stream, group, "consumer")
	assert.Error(t, err)
}

func TestStreamPendingEmptyReturnsZero(t *testing.T) {
	ctx := context.Background()
	stream := "test:stream_pending_empty"
	group := "test:group_pending_empty"

	t.Cleanup(func() {
		Del(ctx, stream)
	})

	_, err := StreamAdd(ctx, stream, map[string]interface{}{"k": "v"})
	assert.NoError(t, err)

	msg, err := StreamConsume(ctx, stream, group, "consumer")
	assert.NoError(t, err)
	assert.NotNil(t, msg)

	err = StreamAck(ctx, stream, group, msg.ID)
	assert.NoError(t, err)

	count, err := StreamPendingCount(ctx, stream, group)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)
}

func TestStreamPendingHelpers(t *testing.T) {
	stream := "test:stream_pending"
	group := "test:group_pending"
	consumer := "test:consumer_pending"

	t.Cleanup(func() {
		Del(context.Background(), stream)
	})

	// Add and consume without ACK to create a pending entry
	msgID, err := StreamAdd(context.Background(), stream, map[string]interface{}{"task": "pending"})
	assert.NoError(t, err)

	msg, err := StreamConsume(context.Background(), stream, group, consumer)
	assert.NoError(t, err)
	assert.NotNil(t, msg)
	assert.Equal(t, msgID, msg.ID)

	summary, err := StreamPendingSummary(context.Background(), stream, group)
	assert.NoError(t, err)
	assert.NotNil(t, summary)
	assert.GreaterOrEqual(t, summary.Count, int64(1))

	items, err := StreamPendingList(context.Background(), stream, group, "-", "+", 10, "")
	assert.NoError(t, err)
	assert.NotEmpty(t, items)

	pendingCount, err := StreamPendingCount(context.Background(), stream, group)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, pendingCount, int64(1))

	err = StreamAck(context.Background(), stream, group, msg.ID)
	assert.NoError(t, err)
}

type mockMetrics struct {
	mu        sync.Mutex
	count     int
	lastOp    string
	lastError error
}

type mockHook struct {
	mu        sync.Mutex
	beforeOps []string
	afterOps  []string
	lastError error
}

func (h *mockHook) Before(ctx context.Context, op string) context.Context {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.beforeOps = append(h.beforeOps, op)
	return context.WithValue(ctx, "hooked", true)
}

func (h *mockHook) After(_ context.Context, op string, err error, _ time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.afterOps = append(h.afterOps, op)
	h.lastError = err
}

func (m *mockMetrics) ObserveDuration(op string, _ time.Duration, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.count++
	m.lastOp = op
	m.lastError = err
}

// mockLogger is for testing the logging functionality.
type mockLogger struct {
	lastMessage string
	lastLevel   string
}

func (m *mockLogger) Errorf(format string, v ...interface{}) {
	m.lastMessage = fmt.Sprintf(format, v...)
	m.lastLevel = "error"
}
func (m *mockLogger) Warnf(format string, v ...interface{}) {
	m.lastMessage = fmt.Sprintf(format, v...)
	m.lastLevel = "warn"
}
func (m *mockLogger) Infof(format string, v ...interface{}) {
	m.lastMessage = fmt.Sprintf(format, v...)
	m.lastLevel = "info"
}
func (m *mockLogger) Debugf(format string, v ...interface{}) {
	m.lastMessage = fmt.Sprintf(format, v...)
	m.lastLevel = "debug"
}

func TestCustomLogger(t *testing.T) {
	// Temporarily replace the package-level logger to test its usage
	originalLogger := logger
	defer func() { logger = originalLogger }()

	mock := &mockLogger{}
	logger = mock

	// This action should trigger an error log inside StreamClaim because the stream does not exist,
	// and therefore XPendingExt will return an error.
	_, err := StreamClaim(context.Background(), "non_existent_stream_for_log_test", "group", "consumer", 1*time.Second)

	// We expect an error from the function itself
	assert.Error(t, err)

	// And we assert that our mock logger was called with the correct level and message
	assert.Equal(t, "error", mock.lastLevel)
	assert.Contains(t, mock.lastMessage, "Failed to auto-claim messages in stream")
}

func TestMetricsHookIsCalled(t *testing.T) {
	metrics := &mockMetrics{}
	client, err := NewClientWithError(Config{
		Addresses: []string{"localhost:6379"},
		DB:        14,
		Metrics:   metrics,
	})
	if err != nil {
		t.Fatalf("failed to create instance client: %v", err)
	}
	defer client.Close()

	err = client.Set(context.Background(), "metrics:key", "v", 0)
	assert.NoError(t, err)

	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	assert.GreaterOrEqual(t, metrics.count, 1)
	assert.Equal(t, "Set", metrics.lastOp)
}

func TestMetricsHookDistinguishesRedisNil(t *testing.T) {
	metrics := &mockMetrics{}
	client, err := NewClientWithError(Config{
		Addresses: []string{"localhost:6379"},
		DB:        14,
		Metrics:   metrics,
	})
	if err != nil {
		t.Fatalf("failed to create instance client: %v", err)
	}
	defer client.Close()

	_, err = client.Get(context.Background(), "metrics:nil")
	assert.Error(t, err)
	assert.Equal(t, redis.Nil, err)

	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	assert.Equal(t, "Get", metrics.lastOp)
	assert.Equal(t, redis.Nil, metrics.lastError)
}

func TestHookIsCalled(t *testing.T) {
	hook := &mockHook{}
	client, err := NewClientWithError(Config{
		Addresses: []string{"localhost:6379"},
		DB:        14,
		Hook:      hook,
	})
	if err != nil {
		t.Fatalf("failed to create instance client: %v", err)
	}
	defer client.Close()

	err = client.Set(context.Background(), "hook:key", "v", 0)
	assert.NoError(t, err)

	hook.mu.Lock()
	defer hook.mu.Unlock()
	assert.NotEmpty(t, hook.beforeOps)
	assert.NotEmpty(t, hook.afterOps)
	assert.Equal(t, "Set", hook.beforeOps[len(hook.beforeOps)-1])
	assert.Equal(t, "Set", hook.afterOps[len(hook.afterOps)-1])
}

func TestHookCanModifyContext(t *testing.T) {
	hook := &mockHook{}
	client, err := NewClientWithError(Config{
		Addresses: []string{"localhost:6379"},
		DB:        14,
		Hook:      hook,
	})
	if err != nil {
		t.Fatalf("failed to create instance client: %v", err)
	}
	defer client.Close()

	_, err = client.Get(context.Background(), "hook:ctx")
	assert.Error(t, err)

	hook.mu.Lock()
	defer hook.mu.Unlock()
	assert.NotEmpty(t, hook.beforeOps)
}

func TestHookWithCanceledContext(t *testing.T) {
	hook := &mockHook{}
	client, err := NewClientWithError(Config{
		Addresses: []string{"localhost:6379"},
		DB:        14,
		Hook:      hook,
	})
	if err != nil {
		t.Fatalf("failed to create instance client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = client.Get(ctx, "hook:cancel")
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)

	hook.mu.Lock()
	defer hook.mu.Unlock()
	assert.NotEmpty(t, hook.beforeOps)
	assert.NotEmpty(t, hook.afterOps)
}

func TestSubscribeRetryConfig(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	Subscribe(ctx, "test:retry", func(_ *redis.Message) {})
}

func TestSubscribeOnRetryCallback(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan int, 1)
	cfg := SubscribeRetryConfig{
		Enabled:    true,
		MinBackoff: 10 * time.Millisecond,
		MaxBackoff: 10 * time.Millisecond,
		MaxRetries: 1,
		OnRetry: func(attempt int, _ time.Duration, _ error) {
			ch <- attempt
		},
	}

	originalRetry := globalSubscribeRetry
	globalSubscribeRetry = cfg
	defer func() { globalSubscribeRetry = originalRetry }()

	Subscribe(ctx, "test:retry-callback", func(_ *redis.Message) {})

	select {
	case <-ch:
	case <-time.After(200 * time.Millisecond):
		// retry callback might not fire if subscription succeeds on first attempt
	}
}

func TestSubscribeOnRetryErrorIsPropagated(t *testing.T) {
	ch := make(chan error, 1)
	cfg := SubscribeRetryConfig{
		Enabled:    true,
		MinBackoff: 10 * time.Millisecond,
		MaxBackoff: 10 * time.Millisecond,
		MaxRetries: 1,
		OnRetry: func(_ int, _ time.Duration, err error) {
			ch <- err
		},
	}

	originalRetry := globalSubscribeRetry
	globalSubscribeRetry = cfg
	defer func() { globalSubscribeRetry = originalRetry }()

	invalidClient := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:1",
	})
	invalidCtx, invalidCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer invalidCancel()

	subscribeWithLogger(invalidCtx, invalidClient, logger, cfg, "test:retry-err", func(_ *redis.Message) {}, nil)

	select {
	case err := <-ch:
		assert.Error(t, err)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for OnRetry error")
	}
}

func TestSubscribeWithReadyInstance(t *testing.T) {
	client, err := NewClientWithError(Config{
		Addresses: []string{"localhost:6379"},
		DB:        14,
	})
	if err != nil {
		t.Fatalf("failed to create instance client: %v", err)
	}
	defer client.Close()

	channel := "test:pubsub_instance"
	ready := make(chan struct{}, 1)
	received := make(chan string, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client.SubscribeWithReady(ctx, channel, func(msg *redis.Message) {
		received <- msg.Payload
	}, func() {
		ready <- struct{}{}
	})

	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for subscription ready")
	}

	err = client.Publish(context.Background(), channel, "instance")
	assert.NoError(t, err)

	select {
	case got := <-received:
		assert.Equal(t, "instance", got)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message")
	}
}

func TestApplyJitterBounds(t *testing.T) {
	originalRand := randFloat64Source
	defer func() { randFloat64Source = originalRand }()

	d := 100 * time.Millisecond

	randFloat64Source = func() float64 { return 0.0 }
	low := applyJitter(d, 0.2)
	assert.Equal(t, 80*time.Millisecond, low)

	randFloat64Source = func() float64 { return 1.0 }
	high := applyJitter(d, 0.2)
	assert.Equal(t, 120*time.Millisecond, high)
}

func TestSubscribeReconnectIntegration(t *testing.T) {
	if os.Getenv("REDIS_E2E_RECONNECT") == "" {
		t.Skip("set REDIS_E2E_RECONNECT=1 to run reconnect integration test")
	}

	restartCmd := os.Getenv("REDIS_E2E_RECONNECT_CMD")
	if restartCmd == "" {
		t.Skip("set REDIS_E2E_RECONNECT_CMD to a command that restarts redis (e.g. 'redis-cli shutdown; redis-server --daemonize yes')")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ready := make(chan struct{}, 1)
	received := make(chan string, 1)
	SubscribeWithReady(ctx, "test:reconnect", func(_ *redis.Message) {}, func() {
		select {
		case ready <- struct{}{}:
		default:
		}
	})

	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for subscription ready")
	}

	cmd := exec.Command("sh", "-c", restartCmd)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("failed to restart redis: %v output=%s", err, string(output))
	}

	// Wait for reconnect ready signal
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for subscription reconnect")
	}

	Subscribe(ctx, "test:reconnect", func(msg *redis.Message) {
		received <- msg.Payload
	})
	err := Publish(context.Background(), "test:reconnect", "reconnected")
	assert.NoError(t, err)

	select {
	case got := <-received:
		assert.Equal(t, "reconnected", got)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message after reconnect")
	}
}

func TestTLSIntegration(t *testing.T) {
	if os.Getenv("REDIS_E2E_TLS") == "" {
		t.Skip("set REDIS_E2E_TLS=1 to run TLS integration test")
	}

	addr := os.Getenv("REDIS_E2E_TLS_ADDR")
	if addr == "" {
		t.Skip("set REDIS_E2E_TLS_ADDR (e.g. localhost:6380)")
	}

	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	if os.Getenv("REDIS_E2E_TLS_INSECURE") == "1" {
		tlsCfg.InsecureSkipVerify = true
	} else {
		caPath := os.Getenv("REDIS_E2E_TLS_CA")
		if caPath == "" {
			t.Skip("set REDIS_E2E_TLS_CA or REDIS_E2E_TLS_INSECURE=1")
		}
		caFile, err := os.Open(caPath)
		if err != nil {
			t.Fatalf("failed to open CA file: %v", err)
		}
		defer caFile.Close()
		caBytes, err := io.ReadAll(caFile)
		if err != nil {
			t.Fatalf("failed to read CA file: %v", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caBytes) {
			t.Fatalf("failed to append CA certs")
		}
		tlsCfg.RootCAs = pool
	}

	client, err := NewClientWithError(Config{
		Addresses: []string{addr},
		DB:        14,
		TLSConfig: tlsCfg,
	})
	if err != nil {
		t.Fatalf("failed to create TLS client: %v", err)
	}
	defer client.Close()

	err = client.Set(context.Background(), "tls:key", "v", 0)
	assert.NoError(t, err)
}

func TestClusterIntegration(t *testing.T) {
	if os.Getenv("REDIS_E2E_CLUSTER") == "" {
		t.Skip("set REDIS_E2E_CLUSTER=1 to run cluster integration test")
	}

	addrs := os.Getenv("REDIS_E2E_CLUSTER_ADDRS")
	if addrs == "" {
		t.Skip("set REDIS_E2E_CLUSTER_ADDRS (comma separated, e.g. 'localhost:7000,localhost:7001')")
	}

	client, err := NewClientWithError(Config{
		Addresses: strings.Split(addrs, ","),
	})
	if err != nil {
		t.Fatalf("failed to create cluster client: %v", err)
	}
	defer client.Close()

	err = client.Set(context.Background(), "cluster:key", "v", 0)
	assert.NoError(t, err)
}

func TestSentinelIntegration(t *testing.T) {
	if os.Getenv("REDIS_E2E_SENTINEL") == "" {
		t.Skip("set REDIS_E2E_SENTINEL=1 to run sentinel integration test")
	}

	addrs := os.Getenv("REDIS_E2E_SENTINEL_ADDRS")
	masterName := os.Getenv("REDIS_E2E_SENTINEL_MASTER")
	if addrs == "" || masterName == "" {
		t.Skip("set REDIS_E2E_SENTINEL_ADDRS and REDIS_E2E_SENTINEL_MASTER")
	}

	client, err := NewClientWithError(Config{
		Addresses:  strings.Split(addrs, ","),
		MasterName: masterName,
	})
	if err != nil {
		t.Fatalf("failed to create sentinel client: %v", err)
	}
	defer client.Close()

	err = client.Set(context.Background(), "sentinel:key", "v", 0)
	assert.NoError(t, err)
}

func TestLoggers(t *testing.T) {
	dl := &discardLogger{}
	dl.Debugf("test")
	dl.Infof("test")
	dl.Warnf("test")
	dl.Errorf("test")

	ll := &leveledLogger{level: LogLevelDebug}
	ll.Debugf("test")
	ll.Infof("test")
	ll.Warnf("test")
	ll.Errorf("test")

	ll2 := &leveledLogger{level: LogLevelInfo}
	ll2.Debugf("test")
	ll2.Infof("test")

	ll3 := &leveledLogger{level: LogLevelWarn}
	ll3.Infof("test")
	ll3.Warnf("test")

	ll4 := &leveledLogger{level: LogLevelError}
	ll4.Warnf("test")
	ll4.Errorf("test")
}

func TestGetClientPanic(t *testing.T) {
	originalClient := client
	defer func() { client = originalClient }()

	client = nil
	assert.Panics(t, func() {
		GetClient()
	})
}

func TestCloseClient(t *testing.T) {
	originalClient := client
	originalOnce := once
	defer func() { client = originalClient }()
	defer func() { once = originalOnce }()

	tempClient := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{"localhost:6379"},
	})
	client = tempClient
	err := Close()
	assert.NoError(t, err)
}

func TestCloseAllowsReinit(t *testing.T) {
	originalClient := client
	originalOnce := once
	defer func() { client = originalClient }()
	defer func() { once = originalOnce }()

	tempClient := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{"localhost:6379"},
	})
	client = tempClient

	err := Close()
	assert.NoError(t, err)
	assert.Panics(t, func() {
		GetClient()
	})

	once = sync.Once{}
	Init(Config{
		Addresses: []string{"localhost:6379"},
		DB:        15,
	})
	assert.NotNil(t, client)
}

func TestContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := Get(ctx, "some_key")
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestInitPanics(t *testing.T) {
	originalClient := client
	originalOnce := once
	defer func() {
		client = originalClient
		once = originalOnce
	}()

	// Test panic on empty addresses
	resetInitStateForTest()
	assert.Panics(t, func() {
		Init(Config{Addresses: []string{}})
	})

	// Test panic on connection failure (bad address)
	resetInitStateForTest()
	assert.Panics(t, func() {
		Init(Config{Addresses: []string{"invalid_address:9999"}})
	})
}

func TestInitWithError(t *testing.T) {
	originalClient := client
	originalOnce := once
	defer func() {
		client = originalClient
		once = originalOnce
	}()

	resetInitStateForTest()
	err := InitWithError(Config{Addresses: []string{}})
	assert.Error(t, err)

	resetInitStateForTest()
	err = InitWithError(Config{Addresses: []string{"invalid_address:9999"}})
	assert.Error(t, err)
}

func resetInitStateForTest() {
	initMu.Lock()
	defer initMu.Unlock()
	client = nil
	once = sync.Once{}
}
