package storage

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	objv1 "github.com/raghavs6/object-storage-native-log/proto/obj/v1"
)

type rangeCall struct {
	key        string
	start, end int
}

type fetchObjectStore struct {
	memoryObjectStore
	reads []rangeCall
	read  func(context.Context, string, int, int) ([]byte, error)
}

func (s *fetchObjectStore) GetRange(ctx context.Context, key string, start, end int) ([]byte, error) {
	s.reads = append(s.reads, rangeCall{key, start, end})
	if s.read != nil {
		return s.read(ctx, key, start, end)
	}
	return s.memoryObjectStore.GetRange(ctx, key, start, end)
}

func assertFetchedRecords(t *testing.T, got, want []*objv1.Record) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d records, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] == nil || !bytes.Equal(got[i].Payload, want[i].Payload) {
			t.Errorf("record %d: got %v, want %v", i, got[i], want[i])
		}
	}
}

func TestFetchRangesAndOffsets(t *testing.T) {
	pg := newCommitTestPostgres(t)
	objects := &fetchObjectStore{}
	store := NewStore(objects, pg)
	key := PartitionKey{Topic: "target", Partition: 0}
	want := []*objv1.Record{{Payload: []byte("first")}, {}, {Payload: []byte{0, 0xff, '\n'}}, {Payload: []byte("last")}}
	var ranges []rangeCall
	for _, batch := range [][]*objv1.Record{want[:2], want[2:]} {
		segments, err := store.Commit(t.Context(), map[PartitionKey][]*objv1.Record{
			key:                             batch,
			{Topic: "alpha", Partition: 0}:  {{Payload: []byte("other topic")}},
			{Topic: "target", Partition: 1}: {{Payload: []byte("other partition")}},
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range segments {
			if s.Topic == key.Topic && s.Partition == key.Partition {
				ranges = append(ranges, rangeCall{s.ObjectKey, s.ByteStart, s.ByteEnd})
			}
		}
	}
	cases := []struct {
		name, topic string
		partition   int
		from        int64
		want        []*objv1.Record
		reads       []rangeCall
	}{
		{"all", "target", 0, 0, want, ranges},
		{"inside first", "target", 0, 1, want[1:], ranges},
		{"boundary", "target", 0, 2, want[2:], ranges[1:]},
		{"inside last", "target", 0, 3, want[3:], ranges[1:]},
		{"at end", "target", 0, 4, nil, nil},
		{"beyond end", "target", 0, 5, nil, nil},
		{"unknown topic", "unknown", 0, 0, nil, nil},
		{"unknown partition", "target", 99, 0, nil, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			objects.reads = nil
			got, err := store.Fetch(t.Context(), tc.topic, tc.partition, tc.from)
			if err != nil {
				t.Fatal(err)
			}
			assertFetchedRecords(t, got, tc.want)
			if !reflect.DeepEqual(objects.reads, tc.reads) {
				t.Errorf("reads = %+v, want %+v", objects.reads, tc.reads)
			}
		})
	}
	t.Run("canceled lookup", func(t *testing.T) {
		objects.reads = nil
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		got, err := store.Fetch(ctx, key.Topic, key.Partition, 0)
		if got != nil || !errors.Is(err, context.Canceled) || len(objects.reads) != 0 {
			t.Fatalf("got %v, %v, reads %v; want cancellation before object reads", got, err, objects.reads)
		}
	})
}

func TestFetchRejectNegativeOffset(t *testing.T) {
	objects := &fetchObjectStore{}
	got, err := NewStore(objects, &PostgresStore{}).Fetch(t.Context(), "topic", 0, -1)
	if got != nil || err == nil || len(objects.reads) != 0 {
		t.Fatalf("got %v, %v, reads %v; want rejection without I/O", got, err, objects.reads)
	}
}

func TestFetchDiscardsPartialResults(t *testing.T) {
	pg := newCommitTestPostgres(t)
	objects := &fetchObjectStore{}
	store := NewStore(objects, pg)
	for range 2 {
		_, err := store.Commit(t.Context(), map[PartitionKey][]*objv1.Record{
			{Topic: "topic"}: {{Payload: []byte("record")}, {}},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	readErr := errors.New("injected read failure")
	cases := []struct {
		name string
		data []byte
		err  error
	}{
		{"read failure", nil, readErr},
		{"malformed frame", []byte{0}, nil},
		// One valid empty record, but metadata promises two records.
		{"record count mismatch", []byte{0, 0, 0, 0}, nil},
		{"canceled read", nil, context.Canceled},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			objects.reads = nil
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			objects.read = func(readCtx context.Context, key string, start, end int) ([]byte, error) {
				if readCtx != ctx {
					t.Fatal("fetch did not forward caller context")
				}
				if len(objects.reads) == 1 {
					return objects.memoryObjectStore.GetRange(readCtx, key, start, end)
				}
				if tc.err == context.Canceled {
					cancel()
					return nil, readCtx.Err()
				}
				return tc.data, tc.err
			}
			got, err := store.Fetch(ctx, "topic", 0, 1)
			if got != nil || err == nil || len(objects.reads) != 2 {
				t.Fatalf("got %v, %v, reads %v; want failure on second read with no records", got, err, objects.reads)
			}
			if !strings.Contains(err.Error(), objects.reads[1].key) {
				t.Errorf("error omits failing object: %v", err)
			}
			if tc.err != nil && !errors.Is(err, tc.err) {
				t.Errorf("underlying error lost: %v", err)
			}
		})
	}
}

func TestFetchMinIORoundTrip(t *testing.T) {
	pg := newCommitTestPostgres(t)
	s3Store, err := NewS3Store(t.Context(), testS3Config())
	if err != nil {
		t.Fatal(err)
	}
	store := NewStore(cleanupS3Store{S3Store: s3Store, t: t}, pg)
	want := []*objv1.Record{{Payload: []byte("skip")}, {}, {Payload: []byte{0, 0xff, '\n'}}}
	for _, batch := range [][]*objv1.Record{want[:2], want[2:]} {
		if _, err := store.Commit(t.Context(), map[PartitionKey][]*objv1.Record{
			{Topic: "target"}: batch,
			{Topic: "alpha"}:  {{Payload: []byte("unrelated")}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := store.Fetch(t.Context(), "target", 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	assertFetchedRecords(t, got, want[1:])
}
