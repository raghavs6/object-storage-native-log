package broker

import (
	"fmt"
	"sync"

	"github.com/raghavs6/object-storage-native-log/internal/storage"
	objv1 "github.com/raghavs6/object-storage-native-log/proto/obj/v1"
	"google.golang.org/protobuf/proto"
)

// buffer collects records for a future storage commit. Its zero value is ready
// to use. Records within a partition follow insertion order under mu.
type buffer struct {
	mu      sync.Mutex
	pending *batch
}

// appendResult's offset is meaningful only when err is nil.
type appendResult struct {
	offset int64
	err    error
}

type batch struct {
	groups   map[storage.PartitionKey][]*objv1.Record
	receipts map[storage.PartitionKey][]chan appendResult
}

// complete must be called exactly once by the batch owner, with the result of
// committing this batch. Capacity-one receipts allow callers to stop waiting.
// Each receipt has one consumer and one result; channels are not closed.
func (b *batch) complete(segments []storage.Segment, err error) {
	if err != nil {
		for _, receipts := range b.receipts {
			for _, receipt := range receipts {
				receipt <- appendResult{err: err}
			}
		}
		return
	}
	for _, segment := range segments {
		key := storage.PartitionKey{Topic: segment.Topic, Partition: segment.Partition}
		for i, receipt := range b.receipts[key] {
			receipt <- appendResult{offset: segment.StartOffset + int64(i)}
		}
	}
}

// add copies record into memory; success does not mean the record is durable.
// The caller may modify record after add returns, but not while it is copying.
// The returned receipt delivers one eventual commit result. Abandoning it does
// not remove the record or cancel the batch.
func (b *buffer) add(key storage.PartitionKey, record *objv1.Record) (<-chan appendResult, error) {
	if record == nil {
		return nil, fmt.Errorf("buffer %s/%d: nil record", key.Topic, key.Partition)
	}
	copy := proto.Clone(record).(*objv1.Record)
	receipt := make(chan appendResult, 1)
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.pending == nil {
		b.pending = &batch{
			groups:   make(map[storage.PartitionKey][]*objv1.Record),
			receipts: make(map[storage.PartitionKey][]chan appendResult),
		}
	}
	b.pending.groups[key] = append(b.pending.groups[key], copy)
	b.pending.receipts[key] = append(b.pending.receipts[key], receipt)
	return receipt, nil
}

// drain transfers ownership of the current batch to the caller. New additions
// use a separate map, so storage can process the batch without holding mu.
// An empty buffer returns nil.
func (b *buffer) drain() *batch {
	b.mu.Lock()
	defer b.mu.Unlock()
	pending := b.pending
	b.pending = nil
	return pending
}
