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

const DefaultFlushInterval = 250 * time.Millisecond

var ErrClosed = errors.New("broker closed")

// committer is implemented by storage.Store and controlled test stores.
type committer interface {
	Commit(context.Context, map[storage.PartitionKey][]*objv1.Record) ([]storage.Segment, error)
}

// Broker collects appends and commits one batch at a time. It holds no durable
// state and does not own its storage dependencies. Pending memory is unbounded.
type Broker struct {
	mu     sync.Mutex // serializes admission, draining, and stopping
	buffer buffer
	err    error
	store  committer
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

// New starts timed flushing. Use DefaultFlushInterval as the initial setting.
// Cancel ctx or call Close to stop; the store must honor context cancellation.
func New(ctx context.Context, store committer, interval time.Duration) (*Broker, error) {
	if interval <= 0 {
		return nil, fmt.Errorf("flush interval must be positive, got %s", interval)
	}
	ticker := time.NewTicker(interval)
	return newBroker(ctx, store, ticker.C, ticker.Stop), nil
}

func newBroker(ctx context.Context, store committer, ticks <-chan time.Time, stopTicks func()) *Broker {
	ctx, cancel := context.WithCancel(ctx)
	b := &Broker{store: store, ctx: ctx, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(b.done)
		defer stopTicks()
		b.run(ticks)
	}()
	return b
}

// Append copies a record and waits for its committed offset. Cancellation stops
// only this caller's wait; an accepted record may still commit. An error is not
// proof that a record is absent. If completion races cancellation, either result
// may be observed. The caller must not mutate record until Append returns.
func (b *Broker) Append(ctx context.Context, key storage.PartitionKey, record *objv1.Record) (int64, error) {
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
	receipt, err := b.buffer.add(key, record)
	b.mu.Unlock()
	if err != nil {
		return 0, err
	}
	select {
	case result := <-receipt:
		return result.offset, result.err
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (b *Broker) run(ticks <-chan time.Time) {
	for {
		select {
		case <-b.ctx.Done():
			b.stop(b.ctx.Err())
			return
		case <-ticks:
			b.mu.Lock()
			if b.err != nil || b.ctx.Err() != nil {
				b.mu.Unlock()
				b.stop(b.ctx.Err())
				return
			}
			batch := b.buffer.drain()
			b.mu.Unlock()
			if batch == nil {
				continue
			}
			segments, err := b.store.Commit(b.ctx, batch.groups)
			if err != nil {
				err = fmt.Errorf("commit batch: %w", err)
				b.stop(err)
				batch.complete(nil, err)
				return
			}
			batch.complete(segments, nil)
		}
	}
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
