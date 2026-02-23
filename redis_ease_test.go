package redis_ease

import (
	"context"
	"fmt"
	"os"
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
		IsCluster: false,
	}

	// Initialize client
	Init(config)

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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := func(msg *redis.Message) {
		received <- msg.Payload
	}

	Subscribe(ctx, channel, handler)

	// It might take a moment for the subscription to activate.
	// In a real-world scenario with many subscribers, you might need a more robust synchronization mechanism.
	time.Sleep(50 * time.Millisecond)

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
	defer func() { client = originalClient }()

	tempClient := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{"localhost:6379"},
	})
	client = tempClient
	err := Close()
	assert.NoError(t, err)
}

func TestContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := Get(ctx, "some_key")
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestInitPanics(t *testing.T) {
	originalOnce := once
	originalClient := client
	defer func() {
		once = originalOnce
		client = originalClient
	}()

	// Test panic on empty addresses
	once = sync.Once{}
	assert.Panics(t, func() {
		Init(Config{Addresses: []string{}})
	})

	// Test panic on connection failure (bad address)
	once = sync.Once{}
	assert.Panics(t, func() {
		Init(Config{Addresses: []string{"invalid_address:9999"}})
	})
}
