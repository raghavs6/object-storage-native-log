package broker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/raghavs6/object-storage-native-log/internal/storage"
	objv1 "github.com/raghavs6/object-storage-native-log/proto/obj/v1"
)

var ErrClosed = errors.New("broker closed")

// Config bounds a batch from two directions. A flush happens on whichever comes
// first, so FlushInterval caps how long a record waits and FlushBytes caps how
// large one object gets. Both are initial settings, not measured optima.
type Config struct {
	FlushInterval time.Duration
	// FlushBytes is encoded size, the same measurement the buffer counts, so it
	// is comparable to the object a flush would produce.
	FlushBytes int
}

// DefaultConfig is the starting point for both bounds. Below roughly a megabyte
// an object pays object storage's per-request price for very little data; far
// above this one PUT is slow enough to hurt every record inside it. M3 replaces
// both numbers with a measured curve.
func DefaultConfig() Config {
	return Config{FlushInterval: 250 * time.Millisecond, FlushBytes: 4 << 20}
}

// committer is implemented by storage.Store and controlled test stores.
type committer interface {
	Commit(context.Context, map[storage.PartitionKey][]*objv1.Record) ([]storage.Segment, error)
}

// Broker collects appends and commits one batch at a time, flushing on whichever
// of its two bounds comes first. It holds no durable state and does not own its
// storage dependencies. Pending memory is unbounded.
type Broker struct {
	mu     sync.Mutex // serializes admission, draining, and stopping
	buffer buffer
	err    error
	store  committer
	cfg    Config
	// flush carries at most one pending size trigger; see Append.
	flush  chan struct{}
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

// New starts flushing on cfg's bounds; DefaultConfig supplies initial settings.
// Cancel ctx or call Close to stop; the store must honor context cancellation.
func New(ctx context.Context, store committer, cfg Config) (*Broker, error) {
	if cfg.FlushInterval <= 0 {
		return nil, fmt.Errorf("flush interval must be positive, got %s", cfg.FlushInterval)
	}
	if cfg.FlushBytes <= 0 {
		return nil, fmt.Errorf("flush bytes must be positive, got %d", cfg.FlushBytes)
	}
	ticker := time.NewTicker(cfg.FlushInterval)
	return newBroker(ctx, store, cfg, ticker.C, ticker.Stop), nil
}

func newBroker(ctx context.Context, store committer, cfg Config, ticks <-chan time.Time, stopTicks func()) *Broker {
	ctx, cancel := context.WithCancel(ctx)
	b := &Broker{store: store, cfg: cfg, flush: make(chan struct{}, 1),
		ctx: ctx, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(b.done)
		defer stopTicks()
		b.run(ticks)
	}()
	return b
}

// Append copies records and waits for their commit, returning the first offset.
// One call's records share a batch in the given order, so their offsets run
// contiguously from the returned value. Cancellation stops only this caller's
// wait; accepted records may still commit. An error is not proof that they are
// absent. If completion races cancellation, either result may be observed. The
// caller must not mutate records until Append returns.
func (b *Broker) Append(ctx context.Context, key storage.PartitionKey, records []*objv1.Record) (int64, error) {
	b.mu.Lock()
	if err := ctx.Err(); err != nil {
		b.mu.Unlock()
		return 0, err
	}
	if b.err != nil {
		err := b.err
		b.mu.Unlock()
		return 0, err
	}
	if err := b.ctx.Err(); err != nil {
		b.mu.Unlock()
		return 0, err
	}
	receipts, buffered, err := b.buffer.add(key, records)
	if err == nil && buffered >= b.cfg.FlushBytes {
		// A doorbell rather than a queue: the worker drains the whole buffer when
		// it wakes, so a signal dropped because one is already pending would only
		// have produced a second wake-up with nothing to do. Never blocks, which
		// is why holding mu here is safe; a signal sent while the worker is
		// mid-commit waits in the channel and fires the moment it returns.
		select {
		case b.flush <- struct{}{}:
		default:
		}
	}
	b.mu.Unlock()
	if err != nil {
		return 0, err
	}
	// Wait for every receipt rather than only the first, so this does not depend
	// on same-batch receipts always completing together.
	var base int64
	for i, receipt := range receipts {
		select {
		case result := <-receipt:
			if result.err != nil {
				return 0, result.err
			}
			if i == 0 {
				base = result.offset
			}
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	return base, nil
}

func (b *Broker) run(ticks <-chan time.Time) {
	for {
		select {
		case <-b.ctx.Done():
			b.stop(b.ctx.Err())
			return
		case <-ticks:
			if !b.flushOnce() {
				return
			}
		case <-b.flush:
			if !b.flushOnce() {
				return
			}
		}
	}
}

// flushOnce commits whatever is pending and reports whether the worker should
// keep running. Both triggers share it: a size flush and a timed flush differ
// only in what woke the worker, and separate copies would drift the first time
// one of them was fixed.
func (b *Broker) flushOnce() bool {
	b.mu.Lock()
	if b.err != nil || b.ctx.Err() != nil {
		b.mu.Unlock()
		b.stop(b.ctx.Err())
		return false
	}
	batch := b.buffer.drain()
	b.mu.Unlock()
	if batch == nil {
		return true
	}
	segments, err := b.store.Commit(b.ctx, batch.groups)
	if err != nil {
		err = fmt.Errorf("commit batch: %w", err)
		b.stop(err)
		batch.complete(nil, err)
		return false
	}
	batch.complete(segments, nil)
	return true
}

// stop owns completion of the pending batch; run owns the in-flight batch.
// Retain the first stop reason, including when Close races a commit failure.
func (b *Broker) stop(err error) {
	b.mu.Lock()
	if b.err == nil {
		b.err = err
	}
	err = b.err
	b.cancel()
	pending := b.buffer.drain()
	b.mu.Unlock()
	if pending != nil {
		pending.complete(nil, err)
	}
}

// Wait waits for the worker to exit and returns its terminal error. Explicit
// closure is normal termination; parent cancellation and commit errors surface.
func (b *Broker) Wait() error {
	<-b.done
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.err == ErrClosed {
		return nil
	}
	return b.err
}

// Close rejects new writes, cancels storage work, and waits for the worker.
// It does not flush pending records or close storage dependencies. Repeated and
// concurrent calls are safe. A successfully finished commit may still report
// success to its callers during shutdown.
func (b *Broker) Close() error {
	b.stop(ErrClosed)
	return b.Wait()
}
