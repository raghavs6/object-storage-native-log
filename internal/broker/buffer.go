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
	mu     sync.Mutex
	groups map[storage.PartitionKey][]*objv1.Record
}

// add copies record into memory; success does not mean the record is durable.
// The caller may modify record after add returns, but not while it is copying.
func (b *buffer) add(key storage.PartitionKey, record *objv1.Record) error {
	if record == nil {
		return fmt.Errorf("buffer %s/%d: nil record", key.Topic, key.Partition)
	}
	copy := proto.Clone(record).(*objv1.Record)
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.groups == nil {
		b.groups = make(map[storage.PartitionKey][]*objv1.Record)
	}
	b.groups[key] = append(b.groups[key], copy)
	return nil
}

// drain transfers ownership of the current batch to the caller. New additions
// use a separate map, so storage can process the batch without holding mu.
// An empty buffer returns nil.
func (b *buffer) drain() map[storage.PartitionKey][]*objv1.Record {
	b.mu.Lock()
	defer b.mu.Unlock()
	groups := b.groups
	b.groups = nil
	return groups
}
