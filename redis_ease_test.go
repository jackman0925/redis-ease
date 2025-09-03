package redis_ease

import (
	"context"
	"fmt"
	"os"
	"testing"
	

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
		Del(key)
	})

	err := Set(key, value, 0)
	assert.NoError(t, err)

	gotValue, err := Get(key)
	assert.NoError(t, err)
	assert.Equal(t, value, gotValue)
}

func TestGetNonExistent(t *testing.T) {
	key := "test:nonexistent"
	_, err := Get(key)
	assert.Error(t, err)
	assert.Equal(t, redis.Nil, err, "Error should be redis.Nil for a non-existent key")
}

func TestDel(t *testing.T) {
	key1 := "test:del1"
	key2 := "test:del2"

	Set(key1, "v1", 0)
	Set(key2, "v2", 0)

	deletedCount, err := Del(key1, key2)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), deletedCount)

	_, err = Get(key1)
	assert.Equal(t, redis.Nil, err)
}

func TestHSetAndHGet(t *testing.T) {
	key := "test:hash"
	field := "name"
	value := "gemini"

	t.Cleanup(func() {
		Del(key)
	})

	_, err := HSet(key, field, value)
	assert.NoError(t, err)

	gotValue, err := HGet(key, field)
	assert.NoError(t, err)
	assert.Equal(t, value, gotValue)
}

func TestHGetNonExistent(t *testing.T) {
	key := "test:hash_nonexistent"
	field := "field"

	// Test non-existent field in existing hash
	HSet(key, "another_field", "some_value")
	t.Cleanup(func() { Del(key) })
	_, err := HGet(key, field)
	assert.Equal(t, redis.Nil, err)

	// Test on non-existent key
	_, err = HGet("nonexistent_key", field)
	assert.Equal(t, redis.Nil, err)
}

func TestExists(t *testing.T) {
	key := "test:exists"

	t.Cleanup(func() {
		Del(key)
	})

	// Should not exist initially
	existCount, err := Exists(key)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), existCount)

	// Should exist after Set
	Set(key, "any value", 0)
	existCount, err = Exists(key)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), existCount)
}
