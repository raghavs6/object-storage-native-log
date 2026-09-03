// Command scratch is a throwaway program for learning the S3 API against MinIO.
//
// It is deleted at the end of Group 1. Nothing imports it, and it deliberately
// contains no abstraction — the point is to see the raw SDK calls with nothing
// in the way. The real client lands in step 4 behind the ObjectStore interface.
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Scratch-only constants. Real configuration moves to environment variables in
// step 4. Never do this in code that outlives a learning exercise.
const (
	endpoint  = "http://localhost:9000"
	accessKey = "minioadmin"
	secretKey = "minioadmin"

	// MinIO ignores the region entirely, but the SDK will not build a client
	// without one: region is an input to the request signing algorithm, so it
	// must be set even when the server on the other end doesn't care.
	region = "us-east-1"

	bucket = "obj"

	// A fixed key, so re-running overwrites instead of littering the bucket.
	//
	// The slash creates no folder. Object storage is one flat list of
	// name -> data; the MinIO console merely *renders* a slash as if it were a
	// directory. Real object keys in this project are meaningless UUIDs,
	// because a single object holds records for many topics and partitions at
	// once — there is no one topic it could be named after. All the structure
	// lives in Postgres instead.
	key = "scratch/hello.txt"
)

func main() {
	ctx := context.Background()
	log.SetFlags(0)

	client := newClient(ctx)
	original := []byte("hello world, this is an object")

	// ---- PUT ---------------------------------------------------------------
	//
	// Body wants an io.Reader, but we keep []byte as the real input and wrap it
	// only at the call site. The commit path can never truly stream: working out
	// each partition's byte range requires the whole object to be laid out in
	// memory already. A streaming interface would advertise a capability this
	// design cannot use.
	if _, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(original),
	}); err != nil {
		log.Fatalf("put: %v", err)
	}
	fmt.Printf("PUT  %s/%s  (%d bytes)\n", bucket, key, len(original))

	// ---- GET ---------------------------------------------------------------
	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		log.Fatalf("get: %v", err)
	}
	// Body is an open network connection, not a buffer. Failing to close it
	// leaks that connection — invisible in a script, fatal in a broker serving
	// thousands of reads a second.
	defer out.Body.Close()

	retrieved, err := io.ReadAll(out.Body)
	if err != nil {
		log.Fatalf("read body: %v", err)
	}
	fmt.Printf("GET  %s/%s  (%d bytes)\n", bucket, key, len(retrieved))
	fmt.Printf("     %q\n", retrieved)

	// ---- verify ------------------------------------------------------------
	//
	// Compared, not eyeballed. By step 8 this payload is binary protobuf, where
	// corrupted garbage looks exactly like correct garbage on a terminal.
	if !bytes.Equal(original, retrieved) {
		log.Fatalf("MISMATCH\n  wrote %q\n  read  %q", original, retrieved)
	}
	fmt.Println("round-trip OK: bytes identical")
}

func newClient(ctx context.Context) *s3.Client {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		// Static credentials, rather than the SDK's default chain (env vars ->
		// shared config file -> EC2 instance metadata). Without this the SDK
		// would go hunting for real AWS credentials and fail confusingly.
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
		),
	)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)

		// Path-style addressing:      http://localhost:9000/<bucket>/<key>
		// Virtual-host (the default): http://<bucket>.localhost:9000/<key>
		//
		// Real S3 uses virtual-host style; MinIO expects path-style.
		//
		// Note it is easy to get away without this locally on macOS, because
		// *.localhost resolves to 127.0.0.1 and so virtual-host requests
		// happen to work. It breaks as soon as the endpoint is not localhost
		// — e.g. http://minio:9000 from inside Compose, where obj.minio does
		// not resolve at all.
		o.UsePathStyle = true
	})
}
