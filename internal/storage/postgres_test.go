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
	const otherTopic = "test-segment-round-trip-other"
	t.Cleanup(func() {
		if _, err := store.pool.Exec(context.Background(),
			`DELETE FROM segments WHERE topic IN ($1, $2)`, topic, otherTopic); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	})

	// Inserted out of order, so the ORDER BY is actually being tested rather
	// than coinciding with insertion order.
	second := Segment{
		ObjectKey: "obj-b", Topic: topic, Partition: 0,
		StartOffset: 3, EndOffset: 6, ByteStart: 42, ByteEnd: 70,
	}
	first := Segment{
		ObjectKey: "obj-a", Topic: topic, Partition: 0,
		StartOffset: 0, EndOffset: 3, ByteStart: 0, ByteEnd: 42,
	}
	third := Segment{
		ObjectKey: "obj-c", Topic: topic, Partition: 0,
		StartOffset: 6, EndOffset: 8, ByteStart: 0, ByteEnd: 28,
	}
	// Same topic, different partition — must not appear in partition 0's results.
	other := Segment{
		ObjectKey: "obj-a", Topic: topic, Partition: 1,
		StartOffset: 0, EndOffset: 9, ByteStart: 42, ByteEnd: 99,
	}
	otherTopicSegment := first
	otherTopicSegment.Topic = otherTopic
	for _, s := range []Segment{second, third, first, other, otherTopicSegment} {
		if err := store.InsertSegment(ctx, s); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	cases := []struct {
		name       string
		topic      string
		partition  int
		fromOffset int64
		want       []Segment
	}{
		{"all", topic, 0, 0, []Segment{first, second, third}},
		{"segment start", topic, 0, 3, []Segment{second, third}},
		{"inside segment", topic, 0, 4, []Segment{second, third}},
		{"last segment", topic, 0, 6, []Segment{third}},
		{"at end", topic, 0, 8, nil},
		{"beyond end", topic, 0, 9, nil},
		{"unknown partition", topic, 99, 0, nil},
		{"unknown topic", "test-segment-round-trip-unknown", 0, 0, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := store.Segments(ctx, tc.topic, tc.partition, tc.fromOffset)
			if err != nil {
				t.Fatalf("segments: %v", err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("got %d segments, want %d: %+v", len(got), len(tc.want), got)
			}
			// Compare every field to catch swapped SQL columns as well as ordering.
			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Errorf("segment %d:\n got %+v\nwant %+v", i, got[i], tc.want[i])
				}
			}
		})
	}
}

func TestSegmentsRejectNegativeOffset(t *testing.T) {
	// No pool: invalid input must be rejected before querying the database.
	store := &PostgresStore{}
	got, err := store.Segments(t.Context(), "topic", 0, -1)
	if err == nil || got != nil {
		t.Fatalf("got %+v, %v; want no segments and an error", got, err)
	}
}
