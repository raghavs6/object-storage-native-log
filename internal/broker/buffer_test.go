package broker

import (
	"fmt"
	"sync"
	"testing"

	"github.com/raghavs6/object-storage-native-log/internal/storage"
	objv1 "github.com/raghavs6/object-storage-native-log/proto/obj/v1"
	"google.golang.org/protobuf/proto"
)

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
		if err := b.add(input.key, &objv1.Record{Payload: []byte(input.payload)}); err != nil {
			t.Fatal(err)
		}
	}
	got := b.drain()
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
	if err := b.add(key, record); err != nil {
		t.Fatal(err)
	}
	record.Payload[0] = 'X'
	record.Payload = []byte("replacement")
	if got := b.drain()[key][0]; !proto.Equal(got, &objv1.Record{Payload: []byte("original")}) {
		t.Fatalf("buffered record changed with caller: %v", got)
	}
}

func TestBufferDrainOwnership(t *testing.T) {
	var b buffer
	key := storage.PartitionKey{Topic: "topic"}
	if got := b.drain(); got != nil {
		t.Fatalf("initial drain = %v, want nil", got)
	}
	if err := b.add(key, nil); err == nil {
		t.Fatal("nil record accepted")
	}
	if got := b.drain(); got != nil {
		t.Fatalf("rejected record created a batch: %v", got)
	}
	if err := b.add(key, &objv1.Record{Payload: []byte("old")}); err != nil {
		t.Fatal(err)
	}
	old := b.drain()
	if got := b.drain(); got != nil {
		t.Fatalf("repeated drain = %v, want nil", got)
	}
	if err := b.add(key, &objv1.Record{Payload: []byte("new")}); err != nil {
		t.Fatal(err)
	}
	if len(old[key]) != 1 || string(old[key][0].Payload) != "old" {
		t.Fatalf("new addition changed old batch: %v", old)
	}
	old[key][0].Payload[0] = 'X'
	delete(old, key)
	got := b.drain()
	if len(got[key]) != 1 || string(got[key][0].Payload) != "new" {
		t.Fatalf("old batch mutation changed new batch: %v", got)
	}
}

func TestBufferConcurrentAddAndDrain(t *testing.T) {
	var b buffer
	const writers, perWriter = 8, 100
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
				if err := b.add(key, &objv1.Record{Payload: []byte(id)}); err != nil {
					t.Errorf("add %s: %v", id, err)
				}
			}
		}()
	}
	// One collector owns the results; it drains concurrently with the writers.
	batches := make(chan map[storage.PartitionKey][]*objv1.Record, perWriter)
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
	collect := func(batch map[storage.PartitionKey][]*objv1.Record) {
		for key, records := range batch {
			for _, record := range records {
				id := string(record.Payload)
				seen[id]++
				var writer, sequence int
				if _, err := fmt.Sscanf(id, "%d/%d", &writer, &sequence); err != nil ||
					key.Topic != "topic" || key.Partition != writer%2 {
					t.Errorf("record %q in wrong group %v", id, key)
				}
			}
		}
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
		}
	}
}
