// Command scratch is a throwaway program for learning the S3 API against MinIO.
//
// It is deleted at the end of Group 1. Nothing imports it, and it deliberately
// contains no abstraction — the point is to see the raw SDK calls with nothing
// in the way. The real client lands in step 4 behind the ObjectStore interface.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

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
)

func main() {
	ctx := context.Background()

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

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
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

	// ListBuckets is the simplest authenticated call there is, which makes it a
	// good smoke test: if it succeeds, then the network path, the credentials,
	// the endpoint, the addressing mode, and request signing are all correct.
	out, err := client.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		log.Fatalf("list buckets: %v", err)
	}

	fmt.Printf("connected to %s\n", endpoint)
	fmt.Printf("%d bucket(s):\n", len(out.Buckets))
	for _, b := range out.Buckets {
		fmt.Printf("  %-16s created %s\n",
			aws.ToString(b.Name), b.CreationDate.Format(time.RFC3339))
	}
}
