// Command broker serves one object-storage-native log over gRPC.
//
// This is where os.Getenv finally appears. Packages under internal/ take their
// configuration as values so they stay testable and keep real credentials out
// of the library code; reading the environment is the program's job.
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/raghavs6/object-storage-native-log/internal/broker"
	"github.com/raghavs6/object-storage-native-log/internal/storage"
	objv1 "github.com/raghavs6/object-storage-native-log/proto/obj/v1"
)

const (
	// Bounds the wait for in-flight RPCs. A commit blocked on object storage
	// must not leave a broker that cannot be stopped with Ctrl-C.
	shutdownGrace = 10 * time.Second
	// Bounds reaching Postgres at startup, so a wrong DSN fails here rather
	// than hanging before the listener is even open.
	startupTimeout = 10 * time.Second
)

func main() {
	// run() so deferred closes actually happen: log.Fatal calls os.Exit, which
	// skips them.
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Derived from sigCtx so Ctrl-C works during a slow startup. Safe because
	// pgxpool does not retain its constructor's context — verified in
	// pgxpool.NewWithConfig, which passes it only to the initial connections.
	startupCtx, cancel := context.WithTimeout(sigCtx, startupTimeout)
	defer cancel()

	// Pings, so an unreachable database fails here instead of at the first produce.
	pg, err := storage.NewPostgresStore(startupCtx, storage.PostgresConfig{
		DSN: env("OBJ_POSTGRES_DSN", "postgres://obj:obj@localhost:5433/obj"),
	})
	if err != nil {
		return err
	}
	defer pg.Close()

	cfg := storage.S3Config{
		Endpoint:  env("OBJ_S3_ENDPOINT", "http://localhost:9000"),
		Region:    env("OBJ_S3_REGION", "us-east-1"),
		Bucket:    env("OBJ_S3_BUCKET", "obj"),
		AccessKey: env("OBJ_S3_ACCESS_KEY", "minioadmin"),
		SecretKey: env("OBJ_S3_SECRET_KEY", "minioadmin"),
	}
	objects, err := storage.NewS3Store(startupCtx, cfg)
	if err != nil {
		return err
	}

	// One Store serves both roles: the broker's committer on the write path and
	// the server's fetcher on the read path.
	store := storage.NewStore(objects, pg)

	// context.Background(), deliberately not sigCtx. A signal must not cancel
	// commits that are mid-flight; shutdown below stops the broker only after
	// GracefulStop has let those produces finish.
	b, err := broker.New(context.Background(), store, broker.DefaultFlushInterval)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer()
	objv1.RegisterLogServer(grpcServer, broker.NewServer(b, store))

	addr := env("OBJ_LISTEN", "127.0.0.1:9092")
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- grpcServer.Serve(listener) }()
	log.Printf("broker listening on %s; bucket %s at %s; flushing every %s",
		listener.Addr(), cfg.Bucket, cfg.Endpoint, broker.DefaultFlushInterval)

	var serveFailure error
	select {
	case err := <-serveErr:
		// Serve returns nil once stopped, so reaching here means it failed on
		// its own and no signal is coming.
		if err != nil {
			serveFailure = fmt.Errorf("serve: %w", err)
		}
	case <-sigCtx.Done():
		log.Print("shutting down")
		gracefulStop(grpcServer)
	}

	// Only now: the broker does not flush on close, so anything still parked in
	// Append would have been told Unavailable.
	if err := b.Close(); err != nil {
		// Worth a line rather than discarding: this is a terminal commit
		// failure, which means appends have been failing since it happened.
		log.Printf("broker: %v", err)
	}
	return serveFailure
}

// gracefulStop stops accepting connections and waits for in-flight RPCs, so a
// Produce parked on the next flush tick still returns its real offset instead
// of Unavailable. The timeout keeps a hung storage call from making the process
// unkillable by Ctrl-C.
func gracefulStop(grpcServer *grpc.Server) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		grpcServer.GracefulStop()
	}()
	select {
	case <-done:
	case <-time.After(shutdownGrace):
		log.Printf("requests still in flight after %s; stopping now", shutdownGrace)
		grpcServer.Stop()
		<-done
	}
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
