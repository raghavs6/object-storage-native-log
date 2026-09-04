package storage

import (
	"bytes"
	"context"
	"os"
	"testing"
)

// rangeHeader is where the half-open/inclusive conversion lives, so it gets a
// test that needs no network. The " world" case is the one from step 3.
func TestRangeHeader(t *testing.T) {
	cases := []struct {
		start, end int
		want       string
	}{
		{0, 1, "bytes=0-0"},   // one byte, not zero
		{5, 11, "bytes=5-10"}, // " world" in "hello world, ..."
		{0, 30, "bytes=0-29"},
	}
	for _, c := range cases {
		if got := rangeHeader(c.start, c.end); got != c.want {
			t.Errorf("rangeHeader(%d, %d) = %q, want %q", c.start, c.end, got, c.want)
		}
	}
}

// testS3Config points at the local Compose stack.
//
// These credentials are not secrets — they are the dev-only values in
// docker-compose.yml, already committed. Real credentials arrive through the
// environment and never through a file in this repo.
func testS3Config() S3Config {
	return S3Config{
		Endpoint:  envOr("OBJ_S3_ENDPOINT", "http://localhost:9000"),
		Region:    envOr("OBJ_S3_REGION", "us-east-1"),
		Bucket:    envOr("OBJ_S3_BUCKET", "obj"),
		AccessKey: envOr("OBJ_S3_ACCESS_KEY", "minioadmin"),
		SecretKey: envOr("OBJ_S3_SECRET_KEY", "minioadmin"),
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// Requires `docker compose up -d`. It fails rather than skips when MinIO is
// unreachable: a test that silently skips is a test that quietly stops running.
func TestS3StoreRoundTrip(t *testing.T) {
	ctx := context.Background()

	store, err := NewS3Store(ctx, testS3Config())
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	// Fixed key, so re-running overwrites instead of littering the bucket.
	const key = "test/objectstore-roundtrip"
	data := []byte("hello world, this is an object")

	if err := store.Put(ctx, key, data); err != nil {
		t.Fatalf("put (is `docker compose up -d` running?): %v", err)
	}

	t.Run("whole object", func(t *testing.T) {
		got, err := store.GetRange(ctx, key, 0, len(data))
		if err != nil {
			t.Fatalf("get range: %v", err)
		}
		if !bytes.Equal(got, data) {
			t.Errorf("got %q, want %q", got, data)
		}
	})

	t.Run("slice from the middle", func(t *testing.T) {
		got, err := store.GetRange(ctx, key, 5, 11)
		if err != nil {
			t.Fatalf("get range: %v", err)
		}
		// Compared against the Go slice, so the half-open contract is what is
		// actually asserted — not just "six bytes came back".
		if want := data[5:11]; !bytes.Equal(got, want) {
			t.Errorf("got %q, want %q", got, want)
		}
	})

	t.Run("empty range", func(t *testing.T) {
		got, err := store.GetRange(ctx, key, 7, 7)
		if err != nil {
			t.Fatalf("get range: %v", err)
		}
		if len(got) != 0 {
			t.Errorf("got %q, want empty", got)
		}
	})

	t.Run("inverted range is rejected", func(t *testing.T) {
		if _, err := store.GetRange(ctx, key, 11, 5); err == nil {
			t.Error("want error for [11, 5), got nil")
		}
	})
}
