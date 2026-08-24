package redis_ease

import (
	"context"
	"os"
	"testing"
	"time"
)

// 基准测试默认不运行，避免影响常规测试。
// 设置 REDIS_BENCH=1 后启用。

func benchClient(b *testing.B) *Client {
	addr := os.Getenv("REDIS_BENCH_ADDR")
	if addr == "" {
		b.Skip("set REDIS_BENCH_ADDR to run benchmarks")
	}
	client, err := NewClientWithError(Config{Addresses: []string{addr}, LogLevel: LogLevelNone})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = client.Close() })
	return client
}

func BenchmarkSet(b *testing.B) {
	client := benchClient(b)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := client.Set(ctx, "bench:set", "v", 0); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	client := benchClient(b)
	ctx := context.Background()
	_ = client.Set(ctx, "bench:get", "v", 0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := client.Get(ctx, "bench:get"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSetWithTimeout(b *testing.B) {
	client := benchClient(b)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx2, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		if err := client.Set(ctx2, "bench:set_timeout", "v", 0); err != nil {
			cancel()
			b.Fatal(err)
		}
		cancel()
	}
}
