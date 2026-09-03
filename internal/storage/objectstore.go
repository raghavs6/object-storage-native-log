// Package storage is the durability layer: object storage for record bytes,
// Postgres for the offset -> byte-range index.
package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// ObjectStore is the durable byte store. Objects are written whole and read in
// slices, because one object holds records for many (topic, partition) pairs
// and a consumer wants only its own.
//
// Deliberately minimal. Whole-object Get is absent because GetRange subsumes
// it and nothing here wants a whole object; Delete and List are absent until
// M5's reconciliation actually needs them. Every method is a promise.
type ObjectStore interface {
	// Put writes data under key, replacing any existing object.
	Put(ctx context.Context, key string, data []byte) error

	// GetRange returns bytes [start, end) — half-open, as Go slices are.
	GetRange(ctx context.Context, key string, start, end int) ([]byte, error)
}

// Config is everything needed to reach an S3-compatible endpoint.
//
// Passed in explicitly rather than read from the environment here: a package
// that reaches into os.Getenv is awkward to test and surprising to call.
// Reading the environment is the program's job, not the library's — which is
// also what keeps real AWS credentials out of every file in this repo.
type Config struct {
	Endpoint  string // http://localhost:9000 for MinIO; empty for real AWS
	Region    string
	Bucket    string
	AccessKey string
	SecretKey string
}

// S3Store talks to anything speaking the S3 API. Named for the protocol, not
// for MinIO: MinIO is not a different system, just a different endpoint.
type S3Store struct {
	client *s3.Client
	bucket string
}

// Compile-time assertion that S3Store satisfies ObjectStore. Go's interface
// satisfaction is implicit, so without this a signature drift would only
// surface at the call site, with a worse error message.
var _ ObjectStore = (*S3Store)(nil)

func NewS3Store(ctx context.Context, cfg Config) (*S3Store, error) {
	awsCfg, err := config.LoadDefaultConfig(ctx,
		// Region is an input to the request signing algorithm, so it must be
		// set even against MinIO, which ignores it entirely.
		config.WithRegion(cfg.Region),
		// Static credentials instead of the SDK's default chain (env -> shared
		// config file -> instance metadata), so a missing value fails here
		// rather than after a confusing hunt for real AWS credentials.
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)

			// Path-style:  http://host:9000/<bucket>/<key>
			// Virtual-host (default): http://<bucket>.host:9000/<key>
			//
			// Required for MinIO. Virtual-host style appears to work against
			// localhost only because macOS resolves *.localhost to 127.0.0.1;
			// it breaks the moment the endpoint is anything else, e.g.
			// http://minio:9000 from inside Compose.
			o.UsePathStyle = true
		}
	})

	return &S3Store{client: client, bucket: cfg.Bucket}, nil
}

// Put takes []byte rather than io.Reader on purpose. The commit path cannot
// stream: computing each partition's byte range requires the whole object laid
// out in memory first, so a Reader would advertise a capability this design
// can never use.
func (s *S3Store) Put(ctx context.Context, key string, data []byte) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		return fmt.Errorf("put %s: %w", key, err)
	}
	return nil
}

// GetRange returns bytes [start, end).
//
// Returns []byte rather than the SDK's response body, which is an open network
// connection the caller would have to close. Forgetting leaks it — invisible in
// a script, fatal in a broker serving thousands of reads a second. The cost is
// that a range must fit in memory, which is fine: ranges are bounded by the
// flush size.
func (s *S3Store) GetRange(ctx context.Context, key string, start, end int) ([]byte, error) {
	// Guards a real bug, not a hypothetical one: byte-range bookkeeping is the
	// likeliest place for an off-by-one in this project, and "bytes=5-4" is a
	// malformed header servers answer inconsistently rather than rejecting.
	if start < 0 || end < start {
		return nil, fmt.Errorf("invalid range [%d, %d) for %s", start, end, key)
	}
	if start == end {
		return nil, nil
	}

	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Range:  aws.String(rangeHeader(start, end)),
	})
	if err != nil {
		return nil, fmt.Errorf("get %s [%d, %d): %w", key, start, end, err)
	}
	defer out.Body.Close()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("read %s [%d, %d): %w", key, start, end, err)
	}
	return data, nil
}

// rangeHeader converts a half-open [start, end) range into an HTTP Range
// header value.
//
// The single place in this project where the two conventions meet. HTTP ranges
// are INCLUSIVE on both ends — "bytes=5-10" returns six bytes, 5 through 10 —
// while Go slices are half-open — data[5:10] returns five. Everything inside
// this system speaks half-open, including segments.byte_start/byte_end. The -1
// lives here and nowhere else.
func rangeHeader(start, end int) string {
	return fmt.Sprintf("bytes=%d-%d", start, end-1)
}
