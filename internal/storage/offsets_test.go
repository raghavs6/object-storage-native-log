package storage

import (
	"context"
	"sync"
	"testing"
)

// newOffsetTestStore opens a store and arranges for topic's rows to be removed
// when the test ends.
//
// Close is registered FIRST so it runs LAST: t.Cleanup functions run in
// last-registered-first order, and the DELETE needs a live pool. Registering
// Close with defer instead would shut the pool before the DELETE ran, and the
// cleanup would silently do nothing.
func newOffsetTestStore(t *testing.T, topic string) *PostgresStore {
	t.Helper()

	store, err := NewPostgresStore(context.Background(), testPostgresConfig())
	if err != nil {
		t.Fatalf("new store (is `docker compose up -d` running?): %v", err)
	}
	t.Cleanup(store.Close)
	t.Cleanup(func() {
		if _, err := store.pool.Exec(context.Background(),
			`DELETE FROM partition_offsets WHERE topic = $1`, topic); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	})
	return store
}

// nextOffset reads the counter directly, to check where a partition ended up.
func nextOffset(t *testing.T, store *PostgresStore, topic string, partition int) int64 {
	t.Helper()

	var n int64
	if err := store.pool.QueryRow(context.Background(),
		`SELECT next_offset FROM partition_offsets WHERE topic = $1 AND partition = $2`,
		topic, partition).Scan(&n); err != nil {
		t.Fatalf("read next_offset: %v", err)
	}
	return n
}

func TestAssignOffsetsSequential(t *testing.T) {
	ctx := context.Background()
	const topic = "test-assign-sequential"
	store := newOffsetTestStore(t, topic)

	// The first call also covers the fresh-partition case: no row exists yet,
	// which is exactly where a bare UPDATE would assign nothing and report no
	// error.
	want := []int64{0, 2, 4}
	for i, w := range want {
		got, err := AssignOffsets(ctx, store.pool, topic, 0, 2)
		if err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
		if got != w {
			t.Errorf("call %d: got start %d, want %d", i, got, w)
		}
	}

	if got := nextOffset(t, store, topic, 0); got != 6 {
		t.Errorf("next_offset = %d, want 6", got)
	}
}

func TestAssignOffsetsRejectsNonPositive(t *testing.T) {
	ctx := context.Background()
	const topic = "test-assign-non-positive"
	store := newOffsetTestStore(t, topic)

	for _, n := range []int{0, -1} {
		if _, err := AssignOffsets(ctx, store.pool, topic, 0, n); err == nil {
			t.Errorf("n=%d: want error, got nil", n)
		}
	}
}

// The point of the whole step: concurrent callers must never be handed the same
// offsets, and none may be lost.
//
// Honest about what this proves. pgxpool caps connections at roughly
// max(4, NumCPU), so these goroutines queue rather than all running at once,
// and it is one process rather than two brokers. It shows the statement is
// sound under contention; open question #1 — isolation levels across real
// concurrent brokers — still needs M2.
func TestAssignOffsetsConcurrent(t *testing.T) {
	ctx := context.Background()
	const topic = "test-assign-concurrent"
	const callers, each = 50, 2
	store := newOffsetTestStore(t, topic)

	starts := make([]int64, callers)
	errs := make([]error, callers)

	var wg sync.WaitGroup
	for i := range callers {
		wg.Go(func() {
			starts[i], errs[i] = AssignOffsets(ctx, store.pool, topic, 0, each)
		})
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("caller %d: %v", i, err)
		}
	}

	// Every reservation distinct, and together they must tile [0, 100) exactly
	// — no offset handed out twice, none skipped.
	seen := make(map[int64]bool, callers)
	for _, s := range starts {
		if seen[s] {
			t.Errorf("start offset %d handed out more than once", s)
		}
		seen[s] = true
	}
	for want := int64(0); want < callers*each; want += each {
		if !seen[want] {
			t.Errorf("start offset %d was never handed out", want)
		}
	}

	// The blunt check a lost update cannot survive: anything dropped leaves the
	// final counter short.
	if got := nextOffset(t, store, topic, 0); got != callers*each {
		t.Errorf("next_offset = %d, want %d — offsets were lost", got, callers*each)
	}
}
