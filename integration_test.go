package redis_ease

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestRedisIntegration(t *testing.T) {
	addr := requireIntegrationEnv(t, "REDIS_E2E_ADDR")
	client := integrationClient(t, Config{Addresses: []string{addr}})
	key := integrationKey(t, "string")
	require.NoError(t, client.Set(context.Background(), key, "value", time.Minute))
	value, err := client.Get(context.Background(), key)
	require.NoError(t, err)
	require.Equal(t, "value", value)
	t.Cleanup(func() { _, _ = client.Del(context.Background(), key) })
}

func TestSubscribeReconnectIntegration(t *testing.T) {
	addr := requireIntegrationEnv(t, "REDIS_E2E_RECONNECT_ADDR")
	restart := requireIntegrationEnv(t, "REDIS_E2E_RECONNECT_CMD")
	client := integrationClient(t, Config{Addresses: []string{addr}})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ready := make(chan struct{}, 4)
	received := make(chan string, 1)
	channel := integrationKey(t, "reconnect")
	client.SubscribeWithReady(ctx, channel, func(msg *redis.Message) { received <- msg.Payload }, func() {
		select {
		case ready <- struct{}{}:
		default:
		}
	})
	waitSignal(t, ready, 5*time.Second, "initial subscription")

	cmd := exec.Command("sh", "-c", restart)
	output, err := cmd.CombinedOutput()
	require.NoErrorf(t, err, "restart output: %s", output)
	waitSignal(t, ready, 15*time.Second, "resubscription")

	require.Eventually(t, func() bool {
		return client.Publish(ctx, channel, "reconnected") == nil
	}, 10*time.Second, 200*time.Millisecond)
	waitSignal(t, received, 5*time.Second, "message after reconnect")
}

func TestTLSIntegration(t *testing.T) {
	addr := requireIntegrationEnv(t, "REDIS_E2E_TLS_ADDR")
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if caPath := os.Getenv("REDIS_E2E_TLS_CA"); caPath != "" {
		pem, err := os.ReadFile(caPath)
		require.NoError(t, err)
		pool := x509.NewCertPool()
		require.True(t, pool.AppendCertsFromPEM(pem))
		tlsConfig.RootCAs = pool
	} else if os.Getenv("REDIS_E2E_TLS_INSECURE") == "1" {
		tlsConfig.InsecureSkipVerify = true // Explicit opt-in for local integration only.
	} else {
		t.Skip("set REDIS_E2E_TLS_CA or REDIS_E2E_TLS_INSECURE=1")
	}
	client := integrationClient(t, Config{Addresses: []string{addr}, TLSConfig: tlsConfig})
	require.NoError(t, client.Set(context.Background(), integrationKey(t, "tls"), "value", time.Minute))
}

func TestClusterIntegration(t *testing.T) {
	addresses := requireIntegrationEnv(t, "REDIS_E2E_CLUSTER_ADDRS")
	client := integrationClient(t, Config{Addresses: splitAddresses(addresses), IsClusterMode: true})
	require.NoError(t, client.Set(context.Background(), integrationKey(t, "cluster"), "value", time.Minute))
}

func TestSentinelIntegration(t *testing.T) {
	addresses := requireIntegrationEnv(t, "REDIS_E2E_SENTINEL_ADDRS")
	master := requireIntegrationEnv(t, "REDIS_E2E_SENTINEL_MASTER")
	client := integrationClient(t, Config{Addresses: splitAddresses(addresses), MasterName: master})
	require.NoError(t, client.Set(context.Background(), integrationKey(t, "sentinel"), "value", time.Minute))
}

func integrationClient(t *testing.T, cfg Config) *Client {
	t.Helper()
	cfg.LogLevel = LogLevelNone
	client, err := NewClientWithError(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func requireIntegrationEnv(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		t.Skipf("set %s to run this integration test", name)
	}
	return value
}

func integrationKey(t *testing.T, suffix string) string {
	return fmt.Sprintf("redis-ease:e2e:%d:%s:%s", os.Getpid(), strings.ReplaceAll(t.Name(), "/", "_"), suffix)
}

func splitAddresses(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}
