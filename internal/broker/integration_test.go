package broker

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/raghavs6/object-storage-native-log/internal/storage"
	objv1 "github.com/raghavs6/object-storage-native-log/proto/obj/v1"
)

type integrationObjects struct {
	storage.ObjectStore
	t      *testing.T
	client *s3.Client
	bucket string
}

func (s integrationObjects) Put(ctx context.Context, key string, data []byte) error {
	s.t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(s.bucket), Key: aws.String(key),
		}); err != nil {
			s.t.Errorf("delete test object %s: %v", key, err)
		}
	})
	return s.ObjectStore.Put(ctx, key, data)
}

func integrationEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

// Uses the local Compose services and fails loudly if they are unavailable.
// Each run owns a unique topic and deletes only its own rows and object keys.
func TestBrokerAppendFetchIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	dsn := integrationEnv("OBJ_POSTGRES_DSN", "postgres://obj:obj@localhost:5433/obj")
	pg, err := storage.NewPostgresStore(ctx, storage.PostgresConfig{DSN: dsn})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pg.Close)
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	key := storage.PartitionKey{Topic: "test-broker-" + uuid.NewString()}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		defer admin.Close(ctx)
		for _, query := range []string{
			"DELETE FROM segments WHERE topic = $1",
			"DELETE FROM partition_offsets WHERE topic = $1",
		} {
			if _, err := admin.Exec(ctx, query, key.Topic); err != nil {
				t.Errorf("clean test metadata: %v", err)
			}
		}
	})
	cfg := storage.S3Config{
		Endpoint:  integrationEnv("OBJ_S3_ENDPOINT", "http://localhost:9000"),
		Region:    integrationEnv("OBJ_S3_REGION", "us-east-1"),
		Bucket:    integrationEnv("OBJ_S3_BUCKET", "obj"),
		AccessKey: integrationEnv("OBJ_S3_ACCESS_KEY", "minioadmin"),
		SecretKey: integrationEnv("OBJ_S3_SECRET_KEY", "minioadmin"),
	}
	objects, err := storage.NewS3Store(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	cleanupClient := s3.NewFromConfig(aws.Config{
		Region: cfg.Region, Credentials: credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""),
	}, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			o.UsePathStyle = true
		}
	})
	store := storage.NewStore(integrationObjects{objects, t, cleanupClient, cfg.Bucket}, pg)
	// Shorten the interval for the test; no latency claim is derived from it.
	b, err := New(ctx, store, 5*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close() // Stop the worker before test cleanup deletes objects and rows.
	payloads := [][]byte{[]byte("first"), {}, {0, 0xff, '\n'}}
	for i, payload := range payloads {
		offset, err := b.Append(ctx, key, &objv1.Record{Payload: payload})
		if err != nil || offset != int64(i) {
			t.Fatalf("append %d: offset %d, error %v", i, offset, err)
		}
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	// Closing a broker does not close its caller-owned storage dependencies.
	got, err := store.Fetch(ctx, key.Topic, key.Partition, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d records, want 2", len(got))
	}
	for i, record := range got {
		if !bytes.Equal(record.Payload, payloads[i+1]) {
			t.Errorf("record %d: got %x, want %x", i, record.Payload, payloads[i+1])
		}
	}
}
