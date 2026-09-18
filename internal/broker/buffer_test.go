package broker

import (
	"fmt"
	"sync"
	"testing"

	"github.com/raghavs6/object-storage-native-log/internal/storage"
	objv1 "github.com/raghavs6/object-storage-native-log/proto/obj/v1"
	"google.golang.org/protobuf/proto"
)

// addOne adds a single record, which most tests want; add itself takes a group.
func addOne(b *buffer, key storage.PartitionKey, record *objv1.Record) (<-chan appendResult, error) {
	receipts, _, err := b.add(key, []*objv1.Record{record})
	if err != nil {
		return nil, err
	}
	return receipts[0], nil
}

func TestBufferGroupingAndOrder(t *testing.T) {
	var b buffer
	alpha := storage.PartitionKey{Topic: "alpha", Partition: 0}
	otherPartition := storage.PartitionKey{Topic: "alpha", Partition: 1}
	beta := storage.PartitionKey{Topic: "beta", Partition: 0}
	inputs := []struct {
		key     storage.PartitionKey
		payload string
	}{
		{alpha, "first"}, {beta, "other topic"},
		{otherPartition, "other partition"}, {alpha, ""}, {alpha, "last"},
	}
	for _, input := range inputs {
		if _, err := addOne(&b, input.key, &objv1.Record{Payload: []byte(input.payload)}); err != nil {
			t.Fatal(err)
		}
	}
	got := b.drain().groups
	want := map[storage.PartitionKey][]string{
		alpha: {"first", "", "last"}, otherPartition: {"other partition"}, beta: {"other topic"},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d groups, want %d", len(got), len(want))
	}
	for key, payloads := range want {
		if len(got[key]) != len(payloads) {
			t.Fatalf("%v: got %d records, want %d", key, len(got[key]), len(payloads))
		}
		for i, payload := range payloads {
			if got[key][i] == nil || string(got[key][i].Payload) != payload {
				t.Errorf("%v record %d: got %v, want %q", key, i, got[key][i], payload)
			}
		}
	}
}

func TestBufferCopiesRecords(t *testing.T) {
	var b buffer
	key := storage.PartitionKey{Topic: "topic"}
	record := &objv1.Record{Payload: []byte("original")}
	if _, err := addOne(&b, key, record); err != nil {
		t.Fatal(err)
	}
	record.Payload[0] = 'X'
	record.Payload = []byte("replacement")
	if got := b.drain().groups[key][0]; !proto.Equal(got, &objv1.Record{Payload: []byte("original")}) {
		t.Fatalf("buffered record changed with caller: %v", got)
	}
}

func TestBufferDrainOwnership(t *testing.T) {
	var b buffer
	key := storage.PartitionKey{Topic: "topic"}
	if got := b.drain(); got != nil {
		t.Fatalf("initial drain = %v, want nil", got)
	}
	if receipt, err := addOne(&b, key, nil); err == nil || receipt != nil {
		t.Fatalf("nil record: got receipt %v, error %v", receipt, err)
	}
	if got := b.drain(); got != nil {
		t.Fatalf("rejected record created a batch: %v", got)
	}
	if _, err := addOne(&b, key, &objv1.Record{Payload: []byte("old")}); err != nil {
		t.Fatal(err)
	}
	old := b.drain().groups
	if got := b.drain(); got != nil {
		t.Fatalf("repeated drain = %v, want nil", got)
	}
	if _, err := addOne(&b, key, &objv1.Record{Payload: []byte("new")}); err != nil {
		t.Fatal(err)
	}
	if len(old[key]) != 1 || string(old[key][0].Payload) != "old" {
		t.Fatalf("new addition changed old batch: %v", old)
	}
	old[key][0].Payload[0] = 'X'
	delete(old, key)
	got := b.drain().groups
	if len(got[key]) != 1 || string(got[key][0].Payload) != "new" {
		t.Fatalf("old batch mutation changed new batch: %v", got)
	}
}

func TestBufferConcurrentAddAndDrain(t *testing.T) {
	var b buffer
	const writers, perWriter = 8, 100
	var receipts [writers][perWriter]<-chan appendResult
	start := make(chan struct{})
	var wg sync.WaitGroup
	for writer := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := range perWriter {
				id := fmt.Sprintf("%d/%d", writer, i)
				key := storage.PartitionKey{Topic: "topic", Partition: writer % 2}
				receipt, err := addOne(&b, key, &objv1.Record{Payload: []byte(id)})
				if err != nil {
					t.Errorf("add %s: %v", id, err)
				}
				receipts[writer][i] = receipt
			}
		}()
	}
	// One collector owns the results; it drains concurrently with the writers.
	batches := make(chan *batch, perWriter)
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for range perWriter {
			batches <- b.drain()
		}
	}()
	close(start)
	wg.Wait()
	close(batches)
	seen := make(map[string]int)
	expected := make(map[string]int64)
	next := make(map[storage.PartitionKey]int64)
	collect := func(batch *batch) {
		if batch == nil {
			return
		}
		var segments []storage.Segment
		for key, records := range batch.groups {
			segments = append(segments, storage.Segment{Topic: key.Topic, Partition: key.Partition,
				StartOffset: next[key], EndOffset: next[key] + int64(len(records))})
			for i, record := range records {
				id := string(record.Payload)
				seen[id]++
				expected[id] = next[key] + int64(i)
				var writer, sequence int
				if _, err := fmt.Sscanf(id, "%d/%d", &writer, &sequence); err != nil ||
					key.Topic != "topic" || key.Partition != writer%2 {
					t.Errorf("record %q in wrong group %v", id, key)
				}
			}
			next[key] += int64(len(records))
		}
		completeWithoutReceiver(t, batch, segments, nil)
	}
	for batch := range batches {
		collect(batch)
	}
	collect(b.drain())
	if len(seen) != writers*perWriter {
		t.Errorf("got %d unique records, want %d", len(seen), writers*perWriter)
	}
	for writer := range writers {
		for i := range perWriter {
			id := fmt.Sprintf("%d/%d", writer, i)
			if seen[id] != 1 {
				t.Errorf("record %s appeared %d times, want 1", id, seen[id])
			}
			result := readReceipt(t, receipts[writer][i])
			if result.err != nil || result.offset != expected[id] {
				t.Errorf("record %s: result %+v, want offset %d", id, result, expected[id])
			}
			assertPending(t, receipts[writer][i])
		}
	}
}

// TestBufferByteAccounting pins the buffer's running total to the length of the
// object its records would encode to. The identity is what a size-triggered
// flush depends on: it is only meaningful to compare the total against an object
// size threshold if the two are the same measurement.
func TestBufferByteAccounting(t *testing.T) {
	var b buffer
	if b.bytes != 0 {
		t.Fatalf("fresh buffer holds %d bytes, want 0", b.bytes)
	}
	alpha := storage.PartitionKey{Topic: "alpha", Partition: 0}
	otherPartition := storage.PartitionKey{Topic: "alpha", Partition: 7}
	beta := storage.PartitionKey{Topic: "beta", Partition: 0}
	// An empty payload is a real 4-byte frame, and a payload past 127 bytes needs
	// a two-byte protobuf length — both are natural places for the arithmetic to
	// drift from the encoder.
	groups := []struct {
		key      storage.PartitionKey
		payloads []string
	}{
		{alpha, []string{"first", ""}},
		{beta, []string{string(make([]byte, 300))}},
		{otherPartition, []string{"x"}},
		{alpha, []string{"second", "third"}},
	}
	total := 0
	for _, group := range groups {
		records := make([]*objv1.Record, len(group.payloads))
		for i, payload := range group.payloads {
			records[i] = &objv1.Record{Payload: []byte(payload)}
		}
		var err error
		if _, total, err = b.add(group.key, records); err != nil {
			t.Fatal(err)
		}
	}
	// A rejected call must leave the total where a successful one left it, the
	// same all-or-nothing rule the batch itself follows.
	if _, got, err := b.add(alpha, []*objv1.Record{{Payload: []byte("ok")}, nil}); err == nil {
		t.Fatalf("nil record accepted, total %d", got)
	}
	if b.bytes != total {
		t.Fatalf("rejected add changed the total: %d, want %d", b.bytes, total)
	}

	drained := b.drain()
	data, _, err := storage.EncodeObject(drained.groups)
	if err != nil {
		t.Fatal(err)
	}
	if total != len(data) {
		t.Errorf("buffer counted %d bytes, encoded object is %d", total, len(data))
	}
	if b.bytes != 0 {
		t.Errorf("drained buffer holds %d bytes, want 0", b.bytes)
	}
}
