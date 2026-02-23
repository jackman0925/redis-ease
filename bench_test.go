package redis_ease

import (
	"context"
	"os"
	"testing"
	"time"
)

// Benchmarks are opt-in to avoid impacting default test runs.
// Set REDIS_BENCH=1 to enable.

func requireBenchRedis(b *testing.B) {
	if os.Getenv("REDIS_BENCH") == "" {
		b.Skip("set REDIS_BENCH=1 to run benchmarks")
	}
}

func BenchmarkSet(b *testing.B) {
	requireBenchRedis(b)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := Set(ctx, "bench:set", "v", 0); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	requireBenchRedis(b)
	ctx := context.Background()
	_ = Set(ctx, "bench:get", "v", 0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Get(ctx, "bench:get"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSetWithTimeout(b *testing.B) {
	requireBenchRedis(b)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx2, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		if err := Set(ctx2, "bench:set_timeout", "v", 0); err != nil {
			cancel()
			b.Fatal(err)
		}
		cancel()
	}
}
