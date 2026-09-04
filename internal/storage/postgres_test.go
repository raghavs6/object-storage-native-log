package storage

import (
	"context"
	"testing"
)

// testPostgresConfig points at the local Compose stack.
//
// Dev-only credentials, already committed in docker-compose.yml. Real ones
// arrive through the environment and never through a file in this repo.
func testPostgresConfig() PostgresConfig {
	return PostgresConfig{
		DSN: envOr("OBJ_POSTGRES_DSN", "postgres://obj:obj@localhost:5433/obj"),
	}
}

// Requires `docker compose up -d`. Fails rather than skips when Postgres is
// unreachable, for the same reason the object store test does.
func TestPostgresStoreSegmentRoundTrip(t *testing.T) {
	ctx := context.Background()

	store, err := NewPostgresStore(ctx, testPostgresConfig())
	if err != nil {
		t.Fatalf("new store (is `docker compose up -d` running?): %v", err)
	}
	// Close via t.Cleanup, NOT defer. Deferred calls run when the test function
	// returns; t.Cleanup functions run after that. A deferred Close would shut
	// the pool before the DELETE below ever gets to use it, and the rows would
	// survive the test. Cleanups run last-registered-first, so registering
	// Close here means it runs after the DELETE.
	t.Cleanup(store.Close)

	// The test owns this topic name and deletes only its own rows, so it can
	// never wipe something you were inspecting by hand.
	const topic = "test-segment-round-trip"
	t.Cleanup(func() {
		if _, err := store.pool.Exec(context.Background(),
			`DELETE FROM segments WHERE topic = $1`, topic); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	})

	// Inserted out of order, so the ORDER BY is actually being tested rather
	// than coinciding with insertion order.
	second := Segment{
		ObjectKey: "obj-b", Topic: topic, Partition: 0,
		StartOffset: 3, EndOffset: 5, ByteStart: 42, ByteEnd: 70,
	}
	first := Segment{
		ObjectKey: "obj-a", Topic: topic, Partition: 0,
		StartOffset: 0, EndOffset: 3, ByteStart: 0, ByteEnd: 42,
	}
	// Same topic, different partition — must not appear in partition 0's results.
	other := Segment{
		ObjectKey: "obj-a", Topic: topic, Partition: 1,
		StartOffset: 0, EndOffset: 9, ByteStart: 42, ByteEnd: 99,
	}
	for _, s := range []Segment{second, first, other} {
		if err := store.InsertSegment(ctx, s); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	got, err := store.Segments(ctx, topic, 0)
	if err != nil {
		t.Fatalf("segments: %v", err)
	}

	// Segment is comparable, so == checks every field. A column swapped in
	// either the INSERT or the SELECT shows up here.
	want := []Segment{first, second}
	if len(got) != len(want) {
		t.Fatalf("got %d segments, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("segment %d:\n got %+v\nwant %+v", i, got[i], want[i])
		}
	}

	t.Run("unknown partition returns nothing", func(t *testing.T) {
		got, err := store.Segments(ctx, topic, 99)
		if err != nil {
			t.Fatalf("segments: %v", err)
		}
		if len(got) != 0 {
			t.Errorf("got %d segments, want 0", len(got))
		}
	})
}
