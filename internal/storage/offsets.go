package storage

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// querier is the subset of pgx that assigning offsets needs: run one query,
// read one row. Both *pgxpool.Pool and pgx.Tx declare exactly this method, so
// both satisfy it with no adapter.
//
// Named here rather than taking a concrete type because step 10 must assign
// offsets inside the same transaction that inserts the segment rows — see
// AssignOffsets.
type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// AssignOffsets reserves n offsets for a partition and returns the first one.
// The caller owns [start, start+n).
//
// A free function taking a querier rather than a method on PostgresStore, which
// is deliberately unlike InsertSegment and Segments. Assigning offsets and
// writing the segment rows must be one all-or-nothing unit: assign 0-2, crash
// before the rows land, and next_offset says 3 while nothing claims 0, 1 or 2 —
// a permanent hole that nothing will ever fill. So the caller supplies the
// transaction, and this cannot be wired to the pool.
func AssignOffsets(ctx context.Context, q querier, topic string, partition, n int) (int64, error) {
	// A flush with no records for a partition should not reach here at all.
	// Erroring beats silently returning an empty reservation that looks valid.
	if n <= 0 {
		return 0, fmt.Errorf("assign offsets %s/%d: n must be positive, got %d", topic, partition, n)
	}

	// ONE statement, not a SELECT then an UPDATE. Read-then-write leaves a gap:
	// two writers both read 0, both write n, and both believe they own the same
	// offsets. Here Postgres does the reading and writing itself, holding a row
	// lock throughout — a second writer waits, then re-reads the fresh value
	// before adding to it, so nothing is lost at READ COMMITTED.
	//
	// INSERT ... ON CONFLICT, not a bare UPDATE: a partition nobody has written
	// yet has no row, and UPDATE would match zero rows and assign nothing at all
	// without reporting an error. The upsert handles first write and every write
	// after it in the same statement.
	//
	// RETURNING next_offset - $3 hands back the START of the reservation, so the
	// caller never does the arithmetic.
	var start int64
	err := q.QueryRow(ctx, `
		INSERT INTO partition_offsets (topic, partition, next_offset)
		VALUES ($1, $2, $3)
		ON CONFLICT (topic, partition)
		DO UPDATE SET next_offset = partition_offsets.next_offset + $3
		RETURNING next_offset - $3`,
		topic, partition, n).Scan(&start)
	if err != nil {
		return 0, fmt.Errorf("assign %d offsets for %s/%d: %w", n, topic, partition, err)
	}
	return start, nil
}
