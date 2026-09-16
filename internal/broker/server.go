package broker

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/raghavs6/object-storage-native-log/internal/storage"
	objv1 "github.com/raghavs6/object-storage-native-log/proto/obj/v1"
)

// fetcher is implemented by storage.Store and controlled test stores.
type fetcher interface {
	Fetch(ctx context.Context, topic string, partition int, fromOffset int64) ([]*objv1.Record, error)
}

// Server exposes one broker and one read path over gRPC. It does not own either
// dependency: the caller constructs them and shuts the broker down.
type Server struct {
	objv1.UnimplementedLogServer
	broker  *Broker
	records fetcher
}

func NewServer(broker *Broker, records fetcher) *Server {
	return &Server{broker: broker, records: records}
}

// Produce appends every record to one partition and returns the first offset.
// It returns after the records are durable, so callers observe the flush
// interval as latency.
func (s *Server) Produce(ctx context.Context, req *objv1.ProduceRequest) (*objv1.ProduceResponse, error) {
	key, err := partitionKey(req.GetTopic(), req.GetPartition())
	if err != nil {
		return nil, err
	}
	// An empty request has no offset to report, so it is a client error rather
	// than a no-op. Nil records cannot arrive over the wire; the buffer rejects
	// them, and that surfaces as Internal because it would be a caller's bug.
	if len(req.GetRecords()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "produce: no records")
	}
	base, err := s.broker.Append(ctx, key, req.GetRecords())
	if err != nil {
		return nil, rpcError(ctx, err)
	}
	return &objv1.ProduceResponse{BaseOffset: base}, nil
}

// Fetch returns records from the requested offset onward, in order. Offsets at
// or beyond the end of the partition return no records.
func (s *Server) Fetch(ctx context.Context, req *objv1.FetchRequest) (*objv1.FetchResponse, error) {
	key, err := partitionKey(req.GetTopic(), req.GetPartition())
	if err != nil {
		return nil, err
	}
	if req.GetOffset() < 0 {
		return nil, status.Errorf(codes.InvalidArgument, "fetch: negative offset %d", req.GetOffset())
	}
	records, err := s.records.Fetch(ctx, key.Topic, key.Partition, req.GetOffset())
	if err != nil {
		return nil, rpcError(ctx, err)
	}
	return &objv1.FetchResponse{Records: records}, nil
}

// partitionKey validates the addressing fields both calls share. Partitions are
// int32 on the wire and int in storage.
func partitionKey(topic string, partition int32) (storage.PartitionKey, error) {
	if topic == "" {
		return storage.PartitionKey{}, status.Error(codes.InvalidArgument, "empty topic")
	}
	if partition < 0 {
		return storage.PartitionKey{}, status.Errorf(codes.InvalidArgument, "negative partition %d", partition)
	}
	return storage.PartitionKey{Topic: topic, Partition: int(partition)}, nil
}

// rpcError gives the client a code it can act on. A caller that cancelled or
// timed out learns that first: a stopped broker also reports context.Canceled,
// and reporting that as the caller's own cancellation would be misleading.
// A stopped broker is Unavailable because restarting one is the fix, while
// storage and metadata failures are ours to own.
func rpcError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return status.FromContextError(ctx.Err()).Err()
	}
	if errors.Is(err, ErrClosed) || errors.Is(err, context.Canceled) {
		return status.Error(codes.Unavailable, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}
