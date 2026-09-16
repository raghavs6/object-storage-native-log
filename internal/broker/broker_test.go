package broker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/raghavs6/object-storage-native-log/internal/storage"
	objv1 "github.com/raghavs6/object-storage-native-log/proto/obj/v1"
)

type commitResult struct {
	segments []storage.Segment
	err      error
}

type commitCall struct {
	ctx    context.Context
	groups map[storage.PartitionKey][]*objv1.Record
	result chan commitResult
}

type controlledStore struct {
	calls chan commitCall
}

func (s *controlledStore) Commit(ctx context.Context, groups map[storage.PartitionKey][]*objv1.Record) ([]storage.Segment, error) {
	call := commitCall{ctx: ctx, groups: groups, result: make(chan commitResult, 1)}
	select {
	case s.calls <- call:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case result := <-call.result:
		return result.segments, result.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func startTestBroker(t *testing.T, ctx context.Context) (*Broker, chan time.Time, *controlledStore) {
	t.Helper()
	ticks := make(chan time.Time, 1)
	store := &controlledStore{calls: make(chan commitCall, 1)}
	b := newBroker(ctx, store, ticks, func() {})
	t.Cleanup(func() { _ = b.Close() })
	return b, ticks, store
}

func appendAsync(b *Broker, ctx context.Context, key storage.PartitionKey, payload string) <-chan appendResult {
	result := make(chan appendResult, 1)
	go func() {
		offset, err := b.Append(ctx, key, []*objv1.Record{{Payload: []byte(payload)}})
		result <- appendResult{offset: offset, err: err}
	}()
	return result
}

func receiveCommit(t *testing.T, store *controlledStore) commitCall {
	t.Helper()
	select {
	case call := <-store.calls:
		return call
	case <-time.After(5 * time.Second):
		t.Fatal("commit did not start")
		return commitCall{}
	}
}

func assertNoCommit(t *testing.T, store *controlledStore) {
	t.Helper()
	select {
	case call := <-store.calls:
		t.Fatalf("unexpected commit: %v", call.groups)
	default:
	}
}

func TestBrokerFlushOrdering(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b, ticks, store := startTestBroker(t, t.Context())
		key := storage.PartitionKey{Topic: "topic"}
		ticks <- time.Now()
		synctest.Wait()
		assertNoCommit(t, store) // Empty ticks do no storage work.
		first := appendAsync(b, t.Context(), key, "first")
		synctest.Wait()
		second := appendAsync(b, t.Context(), key, "second")
		synctest.Wait()
		assertPending(t, first)
		assertPending(t, second)
		assertNoCommit(t, store)
		ticks <- time.Now()
		call := receiveCommit(t, store)
		if len(call.groups) != 1 || len(call.groups[key]) != 2 ||
			string(call.groups[key][0].Payload) != "first" || string(call.groups[key][1].Payload) != "second" {
			t.Fatalf("unexpected batch: %v", call.groups)
		}
		third := appendAsync(b, t.Context(), key, "third")
		synctest.Wait()
		ticks <- time.Now()
		synctest.Wait()
		assertNoCommit(t, store) // Queued tick cannot start a second commit yet.
		assertPending(t, first)
		assertPending(t, second)
		assertPending(t, third)
		call.result <- commitResult{segments: []storage.Segment{{Topic: "topic", StartOffset: 10, EndOffset: 12}}}
		for i, receipt := range []<-chan appendResult{first, second} {
			if got := readReceipt(t, receipt); got.err != nil || got.offset != int64(10+i) {
				t.Fatalf("append %d: %+v", i, got)
			}
		}
		next := receiveCommit(t, store)
		if len(next.groups[key]) != 1 || string(next.groups[key][0].Payload) != "third" {
			t.Fatalf("next batch: %v", next.groups)
		}
		assertPending(t, third)
		next.result <- commitResult{segments: []storage.Segment{{Topic: "topic", StartOffset: 12, EndOffset: 13}}}
		if got := readReceipt(t, third); got.err != nil || got.offset != 12 {
			t.Fatalf("third append: %+v", got)
		}
	})
}

func TestBrokerCallerCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b, ticks, store := startTestBroker(t, t.Context())
		key := storage.PartitionKey{Topic: "topic"}
		ctx, cancel := context.WithCancel(t.Context())
		first := appendAsync(b, ctx, key, "canceled caller")
		synctest.Wait()
		second := appendAsync(b, t.Context(), key, "waiting caller")
		synctest.Wait()
		ticks <- time.Now()
		call := receiveCommit(t, store)
		cancel()
		if got := readReceipt(t, first); !errors.Is(got.err, context.Canceled) {
			t.Fatalf("canceled append: %+v", got)
		}
		if call.ctx.Err() != nil || len(call.groups[key]) != 2 {
			t.Fatal("caller cancellation affected shared batch")
		}
		assertPending(t, second)
		call.result <- commitResult{segments: []storage.Segment{{Topic: "topic", StartOffset: 0, EndOffset: 2}}}
		if got := readReceipt(t, second); got.err != nil || got.offset != 1 {
			t.Fatalf("other caller: %+v", got)
		}
	})
}

func TestBrokerCommitFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b, ticks, store := startTestBroker(t, t.Context())
		key := storage.PartitionKey{Topic: "topic"}
		first := appendAsync(b, t.Context(), key, "in flight")
		synctest.Wait()
		ticks <- time.Now()
		call := receiveCommit(t, store)
		pending := appendAsync(b, t.Context(), key, "pending")
		synctest.Wait()
		want := errors.New("storage unavailable")
		call.result <- commitResult{err: want}
		for _, receipt := range []<-chan appendResult{first, pending} {
			if got := readReceipt(t, receipt); !errors.Is(got.err, want) {
				t.Fatalf("failed append: %+v", got)
			}
		}
		if err := b.Wait(); !errors.Is(err, want) {
			t.Fatalf("Wait: %v", err)
		}
		if _, err := b.Append(t.Context(), key, []*objv1.Record{{}}); !errors.Is(err, want) {
			t.Fatalf("new append after failure: %v", err)
		}
		if err := b.Close(); !errors.Is(err, want) {
			t.Fatalf("Close lost failure: %v", err)
		}
		assertNoCommit(t, store)
	})
}

func TestBrokerShutdown(t *testing.T) {
	for _, parentCancel := range []bool{false, true} {
		name := "close"
		if parentCancel {
			name = "parent cancellation"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				b, ticks, store := startTestBroker(t, ctx)
				key := storage.PartitionKey{Topic: "topic"}
				first := appendAsync(b, t.Context(), key, "in flight")
				synctest.Wait()
				ticks <- time.Now()
				call := receiveCommit(t, store)
				pending := appendAsync(b, t.Context(), key, "pending")
				synctest.Wait()
				want := ErrClosed
				if parentCancel {
					want = context.Canceled
					cancel()
					if err := b.Wait(); !errors.Is(err, want) {
						t.Fatalf("Wait: %v", err)
					}
				} else if err := b.Close(); err != nil {
					t.Fatal(err)
				}
				if got := readReceipt(t, first); !errors.Is(got.err, context.Canceled) {
					t.Fatalf("in-flight result: %+v", got)
				}
				if got := readReceipt(t, pending); !errors.Is(got.err, want) {
					t.Fatalf("pending result: %+v", got)
				}
				if call.ctx.Err() == nil {
					t.Fatal("storage context not canceled")
				}
				if _, err := b.Append(t.Context(), key, []*objv1.Record{{}}); !errors.Is(err, want) {
					t.Fatalf("new append: %v", err)
				}
				_ = b.Close() // Repeated shutdown is safe.
				assertNoCommit(t, store)
			})
		})
	}
}

// Two producers appending to one partition concurrently must not interleave:
// each request's records stay adjacent and in order, so its offsets run
// contiguously from the base offset Append returns. Admitting one record at a
// time would allow a0 b0 a1 b1, which both splits a request's offsets and can
// reorder its own records.
func TestBrokerBatchStaysContiguous(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b, ticks, store := startTestBroker(t, t.Context())
		key := storage.PartitionKey{Topic: "topic"}
		appendBatch := func(prefix string) <-chan appendResult {
			result := make(chan appendResult, 1)
			go func() {
				records := make([]*objv1.Record, 3)
				for i := range records {
					records[i] = &objv1.Record{Payload: fmt.Appendf(nil, "%s%d", prefix, i)}
				}
				offset, err := b.Append(t.Context(), key, records)
				result <- appendResult{offset: offset, err: err}
			}()
			return result
		}
		first := appendBatch("a")
		second := appendBatch("b")
		synctest.Wait()
		ticks <- time.Now()
		call := receiveCommit(t, store)
		payloads := make([]string, 0, 6)
		for _, record := range call.groups[key] {
			payloads = append(payloads, string(record.Payload))
		}
		order := strings.Join(payloads, " ")
		if order != "a0 a1 a2 b0 b1 b2" && order != "b0 b1 b2 a0 a1 a2" {
			t.Fatalf("requests interleaved in one batch: %s", order)
		}
		call.result <- commitResult{segments: []storage.Segment{{Topic: "topic", StartOffset: 0, EndOffset: 6}}}
		bases := map[int64]bool{}
		for _, receipt := range []<-chan appendResult{first, second} {
			got := readReceipt(t, receipt)
			if got.err != nil {
				t.Fatalf("batch append: %+v", got)
			}
			bases[got.offset] = true
		}
		// One request owns [0,3), the other [3,6); neither starts at 1 or 2.
		if !bases[0] || !bases[3] {
			t.Fatalf("base offsets %v, want 0 and 3", bases)
		}
	})
}

func TestBrokerValidationAndTicker(t *testing.T) {
	for _, interval := range []time.Duration{0, -time.Second} {
		if b, err := New(t.Context(), nil, interval); b != nil || err == nil {
			t.Fatalf("interval %v: got %v, %v", interval, b, err)
		}
	}
	synctest.Test(t, func(t *testing.T) {
		store := &controlledStore{calls: make(chan commitCall, 1)}
		b, err := New(t.Context(), store, DefaultFlushInterval)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = b.Close() })
		key := storage.PartitionKey{Topic: "topic"}
		if _, err := b.Append(t.Context(), key, []*objv1.Record{nil}); err == nil {
			t.Fatal("nil record accepted")
		}
		if _, err := b.Append(t.Context(), key, nil); err == nil {
			t.Fatal("empty request accepted")
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, err := b.Append(ctx, key, []*objv1.Record{{}}); !errors.Is(err, context.Canceled) {
			t.Fatalf("pre-canceled append: %v", err)
		}
		result := appendAsync(b, t.Context(), key, "record")
		synctest.Wait()
		assertPending(t, result)
		assertNoCommit(t, store)
		// Only synctest's virtual clock advances; this is not a wall-clock sleep.
		time.Sleep(DefaultFlushInterval)
		call := receiveCommit(t, store)
		if len(call.groups[key]) != 1 {
			t.Fatalf("invalid requests entered batch: %v", call.groups)
		}
		call.result <- commitResult{segments: []storage.Segment{{Topic: "topic", EndOffset: 1}}}
		if got := readReceipt(t, result); got.err != nil || got.offset != 0 {
			t.Fatalf("timed append: %+v", got)
		}
	})
}
