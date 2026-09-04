package storage

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Segment records where one partition's records live inside one object.
//
// A single object holds records for many (topic, partition) pairs, so one PUT
// produces one Segment per partition present in that object. Ranges are
// half-open at both ends: offsets are [StartOffset, EndOffset), bytes are
// [ByteStart, ByteEnd).
//
// No ID or CreatedAt field. The database assigns both; nothing here reads them.
type Segment struct {
	ObjectKey   string
	Topic       string
	Partition   int
	StartOffset int64 // BIGINT
	EndOffset   int64 // exclusive, so EndOffset-StartOffset is the record count
	ByteStart   int   // INT, and matches GetRange(start, end int)
	ByteEnd     int   // exclusive
}

// PostgresConfig locates the metadata database.
//
// One DSN string rather than separate host/port/user/password fields: pgx
// parses it, and it is one value to carry through the environment. Passed in
// rather than read from os.Getenv here, for the same reason as S3Config.
type PostgresConfig struct {
	DSN string // postgres://user:password@host:port/database
}

// PostgresStore is the offset -> byte-range index.
//
// No interface over it: only one implementation exists and none is scheduled,
// which is the bar ObjectStore had to clear.
type PostgresStore struct {
	pool *pgxpool.Pool
}

// NewPostgresStore opens a connection pool.
//
// A pool, not a single connection, because a single pgx.Conn is not safe for
// concurrent use — and by the flush loop in step 13 a background ticker and
// request handlers will both be talking to Postgres at once. Same amount of
// code today; a data race later otherwise.
func NewPostgresStore(ctx context.Context, cfg PostgresConfig) (*PostgresStore, error) {
	pool, err := pgxpool.New(ctx, cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("open postgres pool: %w", err)
	}

	// pgxpool.New validates the DSN but connects lazily, so an unreachable
	// server would not surface until the first query, somewhere confusing.
	// Ping here so a bad address fails at construction.
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	return &PostgresStore{pool: pool}, nil
}

func (p *PostgresStore) Close() { p.pool.Close() }

// InsertSegment writes one row.
//
// Singular on purpose. Step 10 inserts several at once — one object yields one
// row per partition — but that also needs the transaction, which step 7 settles.
func (p *PostgresStore) InsertSegment(ctx context.Context, s Segment) error {
	// $1..$7 are placeholders, not string formatting. The values travel to
	// Postgres separately from the query text, so a topic name containing SQL
	// is data and can never become part of the statement.
	_, err := p.pool.Exec(ctx, `
		INSERT INTO segments
			(object_key, topic, partition, start_offset, end_offset, byte_start, byte_end)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		s.ObjectKey, s.Topic, s.Partition,
		s.StartOffset, s.EndOffset, s.ByteStart, s.ByteEnd)
	if err != nil {
		return fmt.Errorf("insert segment %s/%d [%d, %d): %w",
			s.Topic, s.Partition, s.StartOffset, s.EndOffset, err)
	}
	return nil
}

// Segments returns one partition's segments in offset order.
//
// The (topic, partition, start_offset) index exists for exactly this shape.
// Step 11 adds a "from this offset onward" filter; a consumer reading from the
// middle of a partition has no reason to fetch what came before.
func (p *PostgresStore) Segments(ctx context.Context, topic string, partition int) ([]Segment, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT object_key, topic, partition, start_offset, end_offset, byte_start, byte_end
		FROM segments
		WHERE topic = $1 AND partition = $2
		ORDER BY start_offset`, topic, partition)
	if err != nil {
		return nil, fmt.Errorf("query segments %s/%d: %w", topic, partition, err)
	}
	defer rows.Close()

	var segments []Segment
	for rows.Next() {
		var s Segment
		if err := rows.Scan(&s.ObjectKey, &s.Topic, &s.Partition,
			&s.StartOffset, &s.EndOffset, &s.ByteStart, &s.ByteEnd); err != nil {
			return nil, fmt.Errorf("scan segment %s/%d: %w", topic, partition, err)
		}
		segments = append(segments, s)
	}

	// Required, not optional. A failure partway through iteration ends the loop
	// normally and is reported only here — skip it and a truncated result set
	// looks like a complete one.
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read segments %s/%d: %w", topic, partition, err)
	}
	return segments, nil
}
