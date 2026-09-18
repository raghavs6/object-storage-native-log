// Command objctl produces records to a broker and fetches them back.
//
// It exists so manual verification is cheap enough to actually do. grpcurl can
// call the same two RPCs, but only with base64-encoded JSON payloads, and M5's
// crash-correctness work means producing, killing the broker, and fetching
// again over and over. Friction there turns into checks that never get run.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	objv1 "github.com/raghavs6/object-storage-native-log/proto/obj/v1"
)

const (
	// Bounds one call. Produce does not return until its batch is durable, so
	// this cannot be short; it exists only so a wedged broker does not hang the
	// shell forever.
	callTimeout = 30 * time.Second

	// Matches cmd/broker's OBJ_LISTEN default. Deliberately not read from
	// OBJ_LISTEN itself: that variable means "where to listen" and may sensibly
	// be 0.0.0.0:9092, which is a poor thing to dial.
	defaultAddr = "127.0.0.1:9092"
)

func main() {
	// No timestamps. cmd/broker keeps them because a server's log is a
	// timeline; a CLI's output is an answer to the command just typed.
	log.SetFlags(0)
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "produce":
		return produce(os.Args[2:])
	case "consume":
		return consume(os.Args[2:])
	default:
		// Exit 2 for a usage error, matching what flag itself does on a bad
		// flag. Nothing is open yet, so there are no defers to skip here.
		usage()
		os.Exit(2)
		return nil
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `objctl talks to a broker at %s unless --addr says otherwise.

  objctl produce --topic T --partition N --data VALUE [--data VALUE ...]
  objctl consume --topic T --partition N [--from OFFSET]

Run a subcommand with -h for its flags.
`, defaultAddr)
}

// repeatedString collects one value per occurrence of a flag. flag.Value's Set
// is documented as called once per occurrence in command-line order, so the
// records reach the broker in the order they were typed — which is the order
// their offsets will run in.
//
// No nil-receiver guard in String: flag's docs warn that it may call String on
// a zero-valued receiver, but a probe showed it passing a non-nil pointer to an
// empty slice every time, never a nil pointer.
type repeatedString []string

func (r *repeatedString) String() string { return strings.Join(*r, ",") }

func (r *repeatedString) Set(value string) error {
	*r = append(*r, value)
	return nil
}

func produce(args []string) error {
	fs := flag.NewFlagSet("produce", flag.ExitOnError)
	addr := fs.String("addr", defaultAddr, "broker address")
	topic := fs.String("topic", "", "topic name")
	partition := fs.Int("partition", 0, "partition number")
	var data repeatedString
	fs.Var(&data, "data", "record payload; repeat for more records, in order")
	if err := fs.Parse(args); err != nil {
		return err
	}
	// Refused here rather than at the broker. The server answers an empty
	// produce with InvalidArgument and server_test.go covers that path, so a
	// round trip would only make the shell wait to be told what the flags
	// already say.
	if len(data) == 0 {
		return errors.New("produce: no --data, so there is nothing to append")
	}
	id, err := partitionID(*partition)
	if err != nil {
		return err
	}

	records := make([]*objv1.Record, len(data))
	for i, payload := range data {
		// Payloads are bytes on the wire; the CLI can only carry what a shell
		// can type, which is text. --data-base64 is the escape hatch if a
		// binary payload ever matters.
		records[i] = &objv1.Record{Payload: []byte(payload)}
	}

	conn, client, err := dial(*addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	resp, err := client.Produce(ctx, &objv1.ProduceRequest{
		Topic: *topic, Partition: id, Records: records,
	})
	if err != nil {
		return fmt.Errorf("produce: %w", err)
	}
	// Only base_offset comes back: a request's offsets are contiguous from it.
	fmt.Printf("base offset: %d\n", resp.GetBaseOffset())
	return nil
}

func consume(args []string) error {
	fs := flag.NewFlagSet("consume", flag.ExitOnError)
	addr := fs.String("addr", defaultAddr, "broker address")
	topic := fs.String("topic", "", "topic name")
	partition := fs.Int("partition", 0, "partition number")
	from := fs.Int64("from", 0, "first offset to return")
	if err := fs.Parse(args); err != nil {
		return err
	}
	id, err := partitionID(*partition)
	if err != nil {
		return err
	}

	conn, client, err := dial(*addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	resp, err := client.Fetch(ctx, &objv1.FetchRequest{
		Topic: *topic, Partition: id, Offset: *from,
	})
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	records := resp.GetRecords()
	// Printing nothing at all is indistinguishable from a crash. Say so on
	// stderr, leaving stdout as pure data.
	if len(records) == 0 {
		fmt.Fprintf(os.Stderr, "no records at offset %d\n", *from)
		return nil
	}
	// Fetch sends no per-record offsets, because records are contiguous from
	// the offset asked for — so number them here. A gap would show up as an
	// offset that never appears, which is exactly M5's check.
	for i, record := range records {
		fmt.Printf("%d: %s\n", *from+int64(i), record.GetPayload())
	}
	return nil
}

func dial(addr string) (*grpc.ClientConn, objv1.LogClient, error) {
	// NewClient performs no I/O, so a wrong --addr does NOT fail here: it
	// surfaces at the first RPC. Deliberately unlike NewPostgresStore, which
	// pings so a bad DSN fails at construction. Do not "fix" this by adding a
	// connection check; for a one-shot CLI the very next thing is the RPC.
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("client for %s: %w", addr, err)
	}
	return conn, objv1.NewLogClient(conn), nil
}

// partitionID converts a command-line partition to its wire type. Checked in
// one place because the silent failure is confusing: a typo above the int32
// range wraps to a negative number, and the broker then rejects "a negative
// partition" naming a value the user never typed.
//
// Duplicating the three common flags across both subcommands instead of
// sharing a registration helper follows the same call docs/DECISIONS.md records for
// cmd/broker's env helper: a new shared surface is not worth saving six lines.
func partitionID(n int) (int32, error) {
	if n < 0 || n > math.MaxInt32 {
		return 0, fmt.Errorf("partition %d out of range", n)
	}
	return int32(n), nil
}
