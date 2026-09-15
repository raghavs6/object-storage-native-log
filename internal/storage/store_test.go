package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	objv1 "github.com/raghavs6/object-storage-native-log/proto/obj/v1"
)

type memoryObjectStore struct {
	objects map[string][]byte
	puts    int
	putErr  error
	lastKey string
}

func (m *memoryObjectStore) Put(_ context.Context, key string, data []byte) error {
	m.puts++
	m.lastKey = key
	if m.putErr != nil {
		return m.putErr
	}
	if m.objects == nil {
		m.objects = make(map[string][]byte)
	}
	m.objects[key] = bytes.Clone(data)
	return nil
}

func (m *memoryObjectStore) GetRange(_ context.Context, key string, start, end int) ([]byte, error) {
	data, ok := m.objects[key]
	if !ok || start < 0 || end < start || end > len(data) {
		return nil, fmt.Errorf("invalid test object range %s [%d,%d)", key, start, end)
	}
	return bytes.Clone(data[start:end]), nil
}

// Each test owns a schema so failure triggers and cleanup cannot affect other
// tests or application data. Apply the real migration rather than a copied schema.
func newCommitTestPostgres(t *testing.T) *PostgresStore {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	admin, err := NewPostgresStore(ctx, testPostgresConfig())
	if err != nil {
		t.Fatalf("postgres (is docker compose up -d running?): %v", err)
	}
	t.Cleanup(admin.Close)
	schema := "test_commit_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.pool.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := admin.pool.Exec(ctx, "DROP SCHEMA "+quoted+" CASCADE"); err != nil {
			t.Errorf("drop test schema: %v", err)
		}
	})
	cfg, err := pgxpool.ParseConfig(testPostgresConfig().DSN)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	sql, err := os.ReadFile("../../migrations/0001_init.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(sql)); err != nil {
		t.Fatal(err)
	}
	return &PostgresStore{pool: pool}
}

func assertCommitRecords(t *testing.T, objects ObjectStore, segment Segment, want []*objv1.Record) {
	t.Helper()
	data, err := objects.GetRange(t.Context(), segment.ObjectKey, segment.ByteStart, segment.ByteEnd)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeRecords(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("decoded %d records, want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i].Payload, want[i].Payload) {
			t.Errorf("record %d: got %x, want %x", i, got[i].Payload, want[i].Payload)
		}
	}
}

func assertEmptyCommitMetadata(t *testing.T, pg *PostgresStore) {
	t.Helper()
	var segments, counters int
	err := pg.pool.QueryRow(t.Context(), `SELECT
		(SELECT count(*) FROM segments), (SELECT count(*) FROM partition_offsets)`).Scan(&segments, &counters)
	if err != nil {
		t.Fatal(err)
	}
	if segments != 0 || counters != 0 {
		t.Fatalf("got %d segments and %d counters, want none", segments, counters)
	}
}

func TestCommitRoundTrip(t *testing.T) {
	pg := newCommitTestPostgres(t)
	objects := &memoryObjectStore{}
	store := NewStore(objects, pg)
	groups := map[PartitionKey][]*objv1.Record{
		{Topic: "alpha", Partition: 2}:  {{Payload: []byte("hi")}, {}, {Payload: []byte("bye")}},
		{Topic: "alpha", Partition: 10}: {{Payload: []byte("z")}},
		{Topic: "beta", Partition: 0}:   {{Payload: []byte("hi")}},
		{Topic: "empty", Partition: 0}:  {},
	}
	segments, err := store.Commit(t.Context(), groups)
	if err != nil {
		t.Fatal(err)
	}
	if len(segments) != 3 || objects.puts != 1 || len(objects.objects) != 1 {
		t.Fatalf("got %d segments, %d PUTs, %d objects", len(segments), objects.puts, len(objects.objects))
	}
	key := segments[0].ObjectKey
	id, err := uuid.Parse(key)
	if err != nil || id.Version() != 4 {
		t.Fatalf("key %q is not a random UUID: %v", key, err)
	}
	want := []Segment{
		{ObjectKey: key, Topic: "alpha", Partition: 2, StartOffset: 0, EndOffset: 3, ByteStart: 0, ByteEnd: 21},
		{ObjectKey: key, Topic: "alpha", Partition: 10, StartOffset: 0, EndOffset: 1, ByteStart: 21, ByteEnd: 28},
		{ObjectKey: key, Topic: "beta", Partition: 0, StartOffset: 0, EndOffset: 1, ByteStart: 28, ByteEnd: 36},
	}
	if !reflect.DeepEqual(segments, want) {
		t.Fatalf("segments = %+v, want %+v", segments, want)
	}
	if len(objects.objects[key]) != 36 {
		t.Fatalf("object length = %d, want 36", len(objects.objects[key]))
	}
	for _, segment := range segments {
		rows, err := pg.Segments(t.Context(), segment.Topic, segment.Partition, 0)
		if err != nil || !reflect.DeepEqual(rows, []Segment{segment}) {
			t.Fatalf("stored segments = %+v, %v", rows, err)
		}
		if got := nextOffset(t, pg, segment.Topic, segment.Partition); got != segment.EndOffset {
			t.Errorf("next offset = %d, want %d", got, segment.EndOffset)
		}
		assertCommitRecords(t, objects, segment, groups[PartitionKey{Topic: segment.Topic, Partition: segment.Partition}])
	}

	second, err := store.Commit(t.Context(), groups)
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 3 || objects.puts != 2 || len(objects.objects) != 2 {
		t.Fatalf("second commit: %d segments, %d PUTs, %d objects", len(second), objects.puts, len(objects.objects))
	}
	for i, segment := range second {
		previous := segments[i]
		if segment.ObjectKey == key || segment.ObjectKey != second[0].ObjectKey ||
			segment.StartOffset != previous.EndOffset || segment.EndOffset != previous.EndOffset*2 {
			t.Errorf("second segment = %+v after %+v", segment, previous)
		}
		rows, err := pg.Segments(t.Context(), segment.Topic, segment.Partition, 0)
		if err != nil || !reflect.DeepEqual(rows, []Segment{previous, segment}) {
			t.Fatalf("repeated commit rows = %+v, %v", rows, err)
		}
	}
	var emptyCounters int
	if err := pg.pool.QueryRow(t.Context(), "SELECT count(*) FROM partition_offsets WHERE topic = 'empty'").Scan(&emptyCounters); err != nil {
		t.Fatal(err)
	}
	if emptyCounters != 0 {
		t.Error("empty group reserved offsets")
	}
}

func TestCommitEmptyAndMalformed(t *testing.T) {
	cases := []struct {
		name   string
		groups map[PartitionKey][]*objv1.Record
		bad    bool
	}{
		{"nil", nil, false},
		{"empty map", map[PartitionKey][]*objv1.Record{}, false},
		{"empty groups", map[PartitionKey][]*objv1.Record{{Topic: "empty"}: {}}, false},
		{"nil record after valid group", map[PartitionKey][]*objv1.Record{
			{Topic: "alpha"}: {{Payload: []byte("hi")}}, {Topic: "beta"}: {nil},
		}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			objects := &memoryObjectStore{}
			// A nil database makes accidental database work fail immediately.
			got, err := NewStore(objects, nil).Commit(t.Context(), tc.groups)
			if (err != nil) != tc.bad || got != nil || objects.puts != 0 {
				t.Fatalf("got %+v, %v, %d PUTs", got, err, objects.puts)
			}
		})
	}
}

func TestCommitUploadFailure(t *testing.T) {
	pg := newCommitTestPostgres(t)
	wantErr := errors.New("upload failed")
	objects := &memoryObjectStore{putErr: wantErr}
	got, err := NewStore(objects, pg).Commit(t.Context(), map[PartitionKey][]*objv1.Record{
		{Topic: "alpha"}: {{Payload: []byte("hi")}},
	})
	if got != nil || !errors.Is(err, wantErr) || objects.puts != 1 {
		t.Fatalf("got %+v, %v, %d PUTs", got, err, objects.puts)
	}
	if !strings.Contains(err.Error(), objects.lastKey) {
		t.Errorf("error omits object key: %v", err)
	}
	assertEmptyCommitMetadata(t, pg)
}

func TestCommitRollback(t *testing.T) {
	for _, seeded := range []bool{false, true} {
		t.Run(fmt.Sprintf("existing=%t", seeded), func(t *testing.T) {
			pg := newCommitTestPostgres(t)
			objects := &memoryObjectStore{}
			store := NewStore(objects, pg)
			groups := map[PartitionKey][]*objv1.Record{
				{Topic: "alpha", Partition: 0}: {{Payload: []byte("hi")}, {}},
				{Topic: "alpha", Partition: 1}: {{Payload: []byte("bye")}},
			}
			var baseline []Segment
			if seeded {
				var err error
				baseline, err = store.Commit(t.Context(), groups)
				if err != nil {
					t.Fatal(err)
				}
			}
			_, err := pg.pool.Exec(t.Context(), `
				CREATE FUNCTION reject_second_segment() RETURNS trigger LANGUAGE plpgsql AS $$
				BEGIN
					IF NEW.partition = 1 THEN
						IF NOT EXISTS (SELECT 1 FROM segments WHERE object_key = NEW.object_key AND partition = 0) THEN
							RAISE EXCEPTION 'first segment was not inserted';
						END IF;
						RAISE EXCEPTION 'forced second segment failure';
					END IF;
					RETURN NEW;
				END $$;
				CREATE TRIGGER reject_second BEFORE INSERT ON segments
				FOR EACH ROW EXECUTE FUNCTION reject_second_segment();`)
			if err != nil {
				t.Fatal(err)
			}
			putsBefore := objects.puts
			got, err := store.Commit(t.Context(), groups)
			var pgErr *pgconn.PgError
			if got != nil || !errors.As(err, &pgErr) || pgErr.Message != "forced second segment failure" {
				t.Fatalf("got %+v, %v; want second-insert failure", got, err)
			}
			if !strings.Contains(err.Error(), objects.lastKey) {
				t.Errorf("error omits object key: %v", err)
			}
			if objects.puts != putsBefore+1 || len(objects.objects) != objects.puts {
				t.Fatalf("uploaded object should remain after rollback: %d PUTs, %d objects", objects.puts, len(objects.objects))
			}
			if !seeded {
				assertEmptyCommitMetadata(t, pg)
			} else {
				for _, segment := range baseline {
					rows, err := pg.Segments(t.Context(), segment.Topic, segment.Partition, 0)
					if err != nil || !reflect.DeepEqual(rows, []Segment{segment}) {
						t.Fatalf("rollback changed existing rows: %+v, %v", rows, err)
					}
					if got := nextOffset(t, pg, segment.Topic, segment.Partition); got != segment.EndOffset {
						t.Errorf("rollback advanced counter to %d, want %d", got, segment.EndOffset)
					}
				}
			}
		})
	}
}

type cleanupS3Store struct {
	*S3Store
	t *testing.T
}

func (s cleanupS3Store) Put(ctx context.Context, key string, data []byte) error {
	s.t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
		if err != nil {
			s.t.Errorf("delete test object %s: %v", key, err)
		}
	})
	return s.S3Store.Put(ctx, key, data)
}

func TestCommitMinIORoundTrip(t *testing.T) {
	pg := newCommitTestPostgres(t)
	s3Store, err := NewS3Store(t.Context(), testS3Config())
	if err != nil {
		t.Fatal(err)
	}
	objects := cleanupS3Store{S3Store: s3Store, t: t}
	groups := map[PartitionKey][]*objv1.Record{
		{Topic: "alpha", Partition: 0}: {{Payload: []byte("hi")}, {}, {Payload: []byte("bye")}},
		{Topic: "beta", Partition: 0}:  {{Payload: []byte{0, 0xff, '\n'}}},
	}
	segments, err := NewStore(objects, pg).Commit(t.Context(), groups)
	if err != nil {
		t.Fatal(err)
	}
	if len(segments) != 2 {
		t.Fatalf("got %d segments, want 2", len(segments))
	}
	for _, segment := range segments {
		rows, err := pg.Segments(t.Context(), segment.Topic, segment.Partition, 0)
		if err != nil || !reflect.DeepEqual(rows, []Segment{segment}) {
			t.Fatalf("stored rows = %+v, %v", rows, err)
		}
		assertCommitRecords(t, objects, segment, groups[PartitionKey{Topic: segment.Topic, Partition: segment.Partition}])
	}
}
