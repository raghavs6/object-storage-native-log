package broker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/raghavs6/object-storage-native-log/internal/storage"
	objv1 "github.com/raghavs6/object-storage-native-log/proto/obj/v1"
)

// sequentialStore commits without storage, handing out offsets in order so a
// produce can be checked against the offsets it reports.
type sequentialStore struct {
	mu   sync.Mutex
	next map[storage.PartitionKey]int64
}

func (s *sequentialStore) Commit(_ context.Context, groups map[storage.PartitionKey][]*objv1.Record) ([]storage.Segment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.next == nil {
		s.next = make(map[storage.PartitionKey]int64)
	}
	segments := make([]storage.Segment, 0, len(groups))
	for key, records := range groups {
		start := s.next[key]
		s.next[key] = start + int64(len(records))
		segments = append(segments, storage.Segment{
			Topic: key.Topic, Partition: key.Partition,
			StartOffset: start, EndOffset: s.next[key],
		})
	}
	return segments, nil
}

// stubFetcher records what the server asked for and returns a fixed result.
type stubFetcher struct {
	mu        sync.Mutex
	records   []*objv1.Record
	err       error
	topic     string
	partition int
	offset    int64
}

func (f *stubFetcher) Fetch(_ context.Context, topic string, partition int, fromOffset int64) ([]*objv1.Record, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.topic, f.partition, f.offset = topic, partition, fromOffset
	return f.records, f.err
}

func (f *stubFetcher) asked() (string, int, int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.topic, f.partition, f.offset
}

func testRecords(prefix string, n int) []*objv1.Record {
	records := make([]*objv1.Record, n)
	for i := range records {
		records[i] = &objv1.Record{Payload: fmt.Appendf(nil, "%s%d", prefix, i)}
	}
	return records
}

// startTestServer serves one broker over a real gRPC connection on a local port.
func startTestServer(t *testing.T, records fetcher, interval time.Duration) (objv1.LogClient, *Broker) {
	t.Helper()
	b, err := New(t.Context(), &sequentialStore{}, interval)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	objv1.RegisterLogServer(server, NewServer(b, records))
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return objv1.NewLogClient(conn), b
}

func TestServerProduceAndFetch(t *testing.T) {
	want := []*objv1.Record{{Payload: []byte("first")}, {Payload: nil}}
	stub := &stubFetcher{records: want}
	client, _ := startTestServer(t, stub, time.Millisecond)
	for i, size := range []int{3, 2} {
		resp, err := client.Produce(t.Context(), &objv1.ProduceRequest{
			Topic: "topic", Records: testRecords("a", size),
		})
		if err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
		// Offsets are contiguous per request, so the second starts after the first.
		if wantBase := int64(i * 3); resp.GetBaseOffset() != wantBase {
			t.Errorf("produce %d: base offset %d, want %d", i, resp.GetBaseOffset(), wantBase)
		}
	}
	resp, err := client.Fetch(t.Context(), &objv1.FetchRequest{Topic: "topic", Partition: 2, Offset: 7})
	if err != nil {
		t.Fatal(err)
	}
	if topic, partition, offset := stub.asked(); topic != "topic" || partition != 2 || offset != 7 {
		t.Errorf("fetcher asked for %s/%d from %d, want topic/2 from 7", topic, partition, offset)
	}
	if len(resp.GetRecords()) != len(want) {
		t.Fatalf("got %d records, want %d", len(resp.GetRecords()), len(want))
	}
	for i, record := range resp.GetRecords() {
		if string(record.GetPayload()) != string(want[i].GetPayload()) {
			t.Errorf("record %d: got %q, want %q", i, record.GetPayload(), want[i].GetPayload())
		}
	}
}

func TestServerRejectsInvalidRequests(t *testing.T) {
	client, _ := startTestServer(t, &stubFetcher{}, time.Millisecond)
	one := testRecords("a", 1)
	produces := map[string]*objv1.ProduceRequest{
		"empty topic":        {Records: one},
		"negative partition": {Topic: "topic", Partition: -1, Records: one},
		"no records":         {Topic: "topic"},
	}
	for name, req := range produces {
		if _, err := client.Produce(t.Context(), req); status.Code(err) != codes.InvalidArgument {
			t.Errorf("produce %s: got %v, want InvalidArgument", name, err)
		}
	}
	fetches := map[string]*objv1.FetchRequest{
		"empty topic":        {},
		"negative partition": {Topic: "topic", Partition: -1},
		"negative offset":    {Topic: "topic", Offset: -1},
	}
	for name, req := range fetches {
		if _, err := client.Fetch(t.Context(), req); status.Code(err) != codes.InvalidArgument {
			t.Errorf("fetch %s: got %v, want InvalidArgument", name, err)
		}
	}
}

func TestServerErrorCodes(t *testing.T) {
	t.Run("stopped broker is unavailable", func(t *testing.T) {
		client, b := startTestServer(t, &stubFetcher{}, time.Millisecond)
		if err := b.Close(); err != nil {
			t.Fatal(err)
		}
		_, err := client.Produce(t.Context(), &objv1.ProduceRequest{
			Topic: "topic", Records: testRecords("a", 1),
		})
		if status.Code(err) != codes.Unavailable {
			t.Errorf("produce after close: got %v, want Unavailable", err)
		}
	})

	t.Run("read failure is internal", func(t *testing.T) {
		stub := &stubFetcher{err: errors.New("metadata unreachable")}
		client, _ := startTestServer(t, stub, time.Millisecond)
		_, err := client.Fetch(t.Context(), &objv1.FetchRequest{Topic: "topic"})
		if status.Code(err) != codes.Internal {
			t.Errorf("failed fetch: got %v, want Internal", err)
		}
	})

	t.Run("caller timeout does not become the server's fault", func(t *testing.T) {
		// An hour-long flush interval means this produce never commits in time.
		client, _ := startTestServer(t, &stubFetcher{}, time.Hour)
		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()
		_, err := client.Produce(ctx, &objv1.ProduceRequest{
			Topic: "topic", Records: testRecords("a", 1),
		})
		// The record may still commit later; a timeout is not proof of absence.
		if status.Code(err) != codes.DeadlineExceeded {
			t.Errorf("timed out produce: got %v, want DeadlineExceeded", err)
		}
	})
}
