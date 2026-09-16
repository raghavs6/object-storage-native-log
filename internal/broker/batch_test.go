package broker

import (
	"errors"
	"testing"
	"time"

	"github.com/raghavs6/object-storage-native-log/internal/storage"
	objv1 "github.com/raghavs6/object-storage-native-log/proto/obj/v1"
)

func assertPending(t *testing.T, receipt <-chan appendResult) {
	t.Helper()
	select {
	case result := <-receipt:
		t.Fatalf("unexpected receipt result: %+v", result)
	default:
	}
}

func readReceipt(t *testing.T, receipt <-chan appendResult) appendResult {
	t.Helper()
	select {
	case result := <-receipt:
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("receipt did not complete")
		return appendResult{}
	}
}

func completeWithoutReceiver(t *testing.T, batch *batch, segments []storage.Segment, err error) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		batch.complete(segments, err)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("completion blocked without a receiver")
	}
}

func TestBatchCompletion(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "success"
		if fail {
			name = "failure"
		}
		t.Run(name, func(t *testing.T) {
			var b buffer
			alpha := storage.PartitionKey{Topic: "alpha", Partition: 0}
			other := storage.PartitionKey{Topic: "alpha", Partition: 1}
			beta := storage.PartitionKey{Topic: "beta", Partition: 0}
			var receipts []<-chan appendResult
			for _, key := range []storage.PartitionKey{alpha, beta, alpha, other} {
				receipt, err := addOne(&b, key, &objv1.Record{})
				if err != nil {
					t.Fatal(err)
				}
				assertPending(t, receipt)
				receipts = append(receipts, receipt)
			}
			first := b.drain()
			for _, receipt := range receipts {
				assertPending(t, receipt)
			}
			nextReceipt, err := addOne(&b, alpha, &objv1.Record{})
			if err != nil {
				t.Fatal(err)
			}
			second := b.drain()
			// Deliberately different from insertion and sorted partition order.
			segments := []storage.Segment{
				{Topic: "beta", Partition: 0, StartOffset: 30, EndOffset: 31},
				{Topic: "alpha", Partition: 1, StartOffset: 20, EndOffset: 21},
				{Topic: "alpha", Partition: 0, StartOffset: 10, EndOffset: 12},
			}
			var commitErr error
			if fail {
				commitErr = errors.New("commit failed")
				segments = nil
			}
			completeWithoutReceiver(t, first, segments, commitErr)
			assertPending(t, nextReceipt)
			for i, want := range []int64{10, 30, 11, 20} {
				got := readReceipt(t, receipts[i])
				if !errors.Is(got.err, commitErr) || (!fail && got.offset != want) {
					t.Errorf("receipt %d: got %+v, want offset %d / error %v", i, got, want, commitErr)
				}
				assertPending(t, receipts[i])
			}
			completeWithoutReceiver(t, second, []storage.Segment{
				{Topic: "alpha", Partition: 0, StartOffset: 12, EndOffset: 13},
			}, nil)
			if got := readReceipt(t, nextReceipt); got.err != nil || got.offset != 12 {
				t.Fatalf("next batch result: %+v", got)
			}
		})
	}
}
