package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	objv1 "github.com/raghavs6/object-storage-native-log/proto/obj/v1"
)

// Store commits record bytes to object storage and their locations to Postgres.
type Store struct {
	objects  ObjectStore
	postgres *PostgresStore
}

// NewStore uses caller-owned dependencies; it does not open or close them.
func NewStore(objects ObjectStore, postgres *PostgresStore) *Store {
	return &Store{objects: objects, postgres: postgres}
}

// Commit uploads one object, then assigns offsets and inserts all segment rows
// in one transaction. Results are returned in topic/partition order after commit.
// Empty batches do no I/O. Errors return no segments, but an uploaded object may
// remain, and a connection failure during commit can leave its outcome unknown.
// Commit does not retry or delete objects.
func (s *Store) Commit(ctx context.Context, groups map[PartitionKey][]*objv1.Record) ([]Segment, error) {
	data, ranges, err := EncodeObject(groups)
	if err != nil {
		return nil, fmt.Errorf("encode batch: %w", err)
	}
	if len(ranges) == 0 {
		return nil, nil
	}
	id, err := uuid.NewRandom()
	if err != nil {
		return nil, fmt.Errorf("generate object key: %w", err)
	}
	key := id.String()
	if err := s.objects.Put(ctx, key, data); err != nil {
		return nil, fmt.Errorf("upload object %s: %w", key, err)
	}

	// Upload first so slow storage does not hold database locks. Postgres alone
	// cannot undo the upload if this transaction fails.
	tx, err := s.postgres.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin metadata for object %s: %w", key, err)
	}
	defer func() {
		// Cleanup still runs if the caller's context has been cancelled.
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(cleanupCtx)
	}()

	segments := make([]Segment, 0, len(ranges))
	for _, r := range ranges {
		start, err := AssignOffsets(ctx, tx, r.Topic, r.Partition, r.RecordCount)
		if err != nil {
			return nil, fmt.Errorf("reserve offsets for object %s: %w", key, err)
		}
		segment := Segment{
			ObjectKey: key, Topic: r.Topic, Partition: r.Partition,
			StartOffset: start, EndOffset: start + int64(r.RecordCount),
			ByteStart: r.ByteStart, ByteEnd: r.ByteEnd,
		}
		if err := insertSegment(ctx, tx, segment); err != nil {
			return nil, fmt.Errorf("write metadata for object %s: %w", key, err)
		}
		segments = append(segments, segment)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit metadata for object %s: %w", key, err)
	}
	return segments, nil
}
