# Decisions

Settled calls and why. Read before proposing something different — if a decision here is wrong,
argue against it, don't quietly work around it. Append a line when a new one is settled.

## Storage

- **`Store.Fetch` reads all records from a requested offset** (step 11b). One
  metadata lookup selects the segments; sequential range reads and decoding preserve
  partition order. Decode the whole containing segment, then trim earlier records:
  metadata indexes segments, not individual record byte positions. Check decoded
  counts against offset ranges before slicing. Errors return no partial records and
  preserve underlying causes; the caller's context reaches database and object reads.
  Unknown partitions and offsets at/beyond the end return no records; negative offsets
  are rejected by the lookup. Results fit in memory; limits and pagination are deferred.
- **Segment lookup accepts a starting offset** (step 11a). `Segments` filters by
  topic, partition, and `end_offset > fromOffset`, ordered by `start_offset`.
  This includes a segment starting before the requested offset if it still contains
  that record, and excludes a segment ending exactly there. Zero returns all segments;
  negative offsets are rejected. Unknown partitions and offsets at or beyond the end
  return no segments. Fetch trims earlier records after decoding.
- **`Store.Commit` uploads one object before opening a metadata transaction** (step 10).
  It reuses `EncodeObject`, then reserves offsets and inserts every segment through
  the same `READ COMMITTED` transaction, in sorted topic/partition order. It returns
  `[]Segment` only after commit succeeds. Empty batches do no I/O; encoding failures
  do not upload. `Store` receives caller-owned storage dependencies and does not close them.
- **Upload and database commit are separate durability boundaries.** Uploading first
  avoids holding database locks during object storage I/O. A database failure can leave
  an unreferenced object; an upload error does not prove the object is absent, and a
  lost connection during commit can leave its outcome unknown. No automatic retry or
  object deletion is added in step 10. Deferred rollback uses a separate five-second
  context so request cancellation does not prevent cleanup.
- **Generate object keys with `github.com/google/uuid` v1.6.0.** Use the error-returning
  `NewRandom` function for a version-4 UUID rather than maintaining our own UUID format.
  One key names the whole batch, regardless of how many partitions it contains.
- **Segment insertion shares one private `Exec` helper.** Both the existing standalone
  `InsertSegment` method and the commit transaction use the same SQL. Transaction tests
  use the real migration in isolated schemas; a test-only trigger rejects a later insert
  to verify rollback without changing application tables or constraints.
- **Object layout groups framed records by `(topic, partition)`** (step 9).
  `EncodeObject` accepts a map keyed by `PartitionKey`, matching the planned buffer,
  and packs nonempty groups in topic order, then numeric partition order. Sorting
  adds a little work but makes layout repeatable. Record order within each group
  is preserved; there is no cross-partition record-order guarantee.
- **Layout returns `PartitionRange`, not a partially filled `Segment`.** It records
  topic, partition, record count, and a half-open byte range including framing.
  The commit path adds the object key and assigns logical offsets after encoding.
  Empty groups produce no range; empty-payload records still count and occupy their
  frames. Encoding failures return no partial object or ranges.
- **Record framing is a 4-byte big-endian length prefix + protobuf**, concatenated
  in input order (step 8b). The unsigned length counts the complete protobuf body,
  excluding the prefix. Protobuf will also be used by the planned gRPC API.
  The prefix preserves record boundaries inside an object or a complete-frame byte range.
- **Malformed frames return an error and no partial records.** Incomplete prefixes,
  truncated bodies, and protobuf decoding errors identify the frame's byte position.
  The decoder checks available bytes before slicing and does not allocate from an
  advertised length. Empty input means zero records; a zero-length frame means one
  empty record. Encoding rejects nil record pointers, while empty payloads are valid.
- **Byte ranges are half-open `[start, end)` everywhere**, matching Go slices — including
  `segments.byte_start` / `byte_end`. HTTP `Range` is inclusive on both ends, so the `-1` conversion
  is confined to `rangeHeader()` in `internal/storage/objectstore.go` and appears nowhere else.
- **MinIO for development, real S3 for M3's headline numbers.** MinIO keeps the test loop at ~2ms
  instead of ~300ms. Its latency profile is fiction, and latency is exactly what this architecture
  trades away, so benchmark numbers must come from real S3.
- **Not S3 Express One Zone.** It's single-AZ. Free cross-AZ durability is the project's entire
  economic premise.
- **Object keys are opaque UUIDs.** One object spans many `(topic, partition)` pairs by design, so
  there is no topic it could be named after. All structure lives in Postgres.
- **`ObjectStore` is an interface** for M3's latency-injecting decorator and the in-memory test fake
  in steps 10–11 — *not* for the MinIO/S3 swap, which is the same client with a different config and
  needs no interface at all.
- **`Put` takes `[]byte`, not `io.Reader`.** The commit path cannot stream: computing each
  partition's byte range requires the whole object laid out in memory first.
- **`GetRange` returns `[]byte`, not the SDK's response body.** The body is an open connection the
  caller must close, and forgetting leaks it. Cost: a range must fit in memory, which is fine because
  ranges are bounded by the flush size.
- **Offsets are half-open too: `[start_offset, end_offset)`.** `partition_offsets.next_offset`
  already means "the number the next record will get", which is an excluded end. Matching it makes
  the last segment's `end_offset` and `next_offset` the same number; the other choice leaves the two
  tables one apart forever. `end_offset - start_offset` is the record count.
- **The half-open convention is recorded in the database itself**, via `COMMENT ON COLUMN`. The
  person who needs it is reading `\d+ segments` in psql, not the Go source.
- **Offset assignment is ONE statement — an upsert, not a bare `UPDATE`.**

  ```sql
  INSERT INTO partition_offsets (topic, partition, next_offset) VALUES ($1, $2, $3)
  ON CONFLICT (topic, partition)
  DO UPDATE SET next_offset = partition_offsets.next_offset + $3
  RETURNING next_offset - $3
  ```

  Not a SELECT then an UPDATE: the gap between them is where two writers both read the same number
  and both believe they own the same offsets. One statement holds a row lock throughout, so a second
  writer waits and then re-reads the fresh value — no lost updates at `READ COMMITTED`.

  Not the bare `UPDATE` earlier versions of the plan called for: a partition nobody has written has
  no row, so `UPDATE` matches zero rows and assigns nothing *without reporting an error*. Verified
  against the live database, and the concurrency test was verified to catch the naive version
  (it lost 49 of 50 reservations).
- **`AssignOffsets` is a free function taking a one-method `querier` interface**, not a method on
  `PostgresStore`. Step 10 must assign offsets inside the same transaction that writes the segment
  rows — assign 0-2, crash before the rows land, and `next_offset` says 3 while nothing claims 0, 1
  or 2. Both `*pgxpool.Pool` and `pgx.Tx` satisfy `querier` unchanged.

## Broker buffering

- **One `Append` call's records are admitted as a unit** (step 15a). `add` takes a
  slice and holds the buffer mutex once for the whole group, so the records sit
  adjacent in one batch and `complete` gives them contiguous offsets in request
  order. `Append` returns the first. Per-record admission allowed two producers to
  interleave — verified, a temporary test produced `b2 a1 a0 b1 a2 b0`, which both
  splits a request's offsets and reorders its own records. The wire contract's
  `base_offset` and per-partition producer order both depend on this.
  `Append` waits on every receipt rather than only the first, so it does not rely
  on same-batch receipts completing together.
- **The broker runs one timed commit at a time** (step 13b). `New` starts the
  worker with a positive flush interval; `DefaultFlushInterval` is 250ms, an initial
  setting rather than a measured optimum. Empty ticks do no storage work. Appends
  can accumulate in the next batch during a commit; memory remains unbounded.
- **`Append` waits for its receipts' committed offsets or caller cancellation.**
  Success follows both the object upload and PostgreSQL commit. Commits use the
  broker lifetime context, not a producer's context. Canceling a caller's wait does
  not remove its accepted record; a timeout/error does not prove the record absent.
  When completion races cancellation, either result may be observed.
- **A commit error stops the broker; batches are not retried automatically.**
  The in-flight and pending callers receive errors, later appends are rejected,
  and `Wait` exposes the terminal failure. Retain the first terminal reason if
  failure races shutdown. This favors explicit failure over uncertain duplicate writes.
- **`Close` cancels work and waits for the worker, without a final flush.**
  Pending callers receive `ErrClosed`; in-flight storage receives cancellation and
  reports its own result, including success if it finished successfully. Repeated
  or concurrent closure is safe. Parent cancellation similarly stops admission and
  releases waiters. Storage must honor cancellation; the broker does not close its
  caller-owned storage dependencies. Explicit closure makes `Wait` return nil;
  commit failures and parent cancellation surface as errors.
- **Every buffered record has a capacity-one completion receipt** (step 13a).
  `add` returns a receive-only channel; records and matching receipt slices enter
  and leave the buffer together under its mutex. A drained batch exposes record
  groups directly to `Store.Commit`. The owner calls `complete` exactly once with
  that commit's segments or error, outside the buffer lock. Success assigns each
  receipt its partition's starting offset plus record index; failure delivers the
  original error to all receipts. Channels carry one result and are not closed.
  Completion does not wait for receivers; abandoning a receipt neither removes a
  record nor cancels a batch. The flush worker completes each detached batch.
- **The private buffer copies records on entry** (step 12). Its zero value is
  usable; `add` rejects empty slices and any nil record, and clones valid protobuf
  records before taking the mutex. A rejected call buffers nothing, so a bad record
  cannot leave part of its request behind. Empty payloads are valid, and an empty
  payload differs from an absent record. Callers may reuse records after `add` returns,
  but must not mutate them during copying. Success means buffered, not durable.
- **`drain` transfers ownership of the accumulated batch and resets the buffer.**
  Additions and drains share one mutex; an addition belongs to exactly one batch.
  Records within each partition follow insertion order under the lock, with no
  predetermined order for concurrent callers or across partitions. Later additions
  cannot mutate a drained batch. Empty drains return nil. Storage I/O, timed flushing,
  producer acknowledgments, retries, and size limits are outside this step; memory
  remains unbounded until later batching controls are added.

## Network API

- **The gRPC server lives in `internal/broker/server.go`** (step 15b), beside the
  broker rather than in its own package. Keeping it here lets its test reuse the
  package's existing fakes instead of forcing `Broker` behind an interface just to
  be mocked from outside. `Server` embeds `objv1.UnimplementedLogServer`, takes a
  one-method `fetcher` interface mirroring `committer`, and owns neither
  dependency: the caller builds the broker and closes it.
- **Errors map to three codes, and the caller's context is checked first.**
  Validation failures are `InvalidArgument`; a stopped broker is `Unavailable`
  because restarting one is the fix; storage and metadata failures are `Internal`.
  The context check comes first because a stopped broker also reports
  `context.Canceled`, and reporting that as the caller's own cancellation would
  blame the wrong party. Without any mapping gRPC reports `Unknown`, which tells a
  client nothing; the tests assert the specific codes.
- **A produce with no records is `InvalidArgument`, not a no-op.** With no records
  there is no `base_offset` to return, and a zero would read as "your records start
  at 0". Step 14's `obj.proto` comment said an empty list appends nothing; it was
  corrected in this step. Nil records are not separately validated: proto3 cannot
  decode one over the wire, so the buffer's rejection surfacing as `Internal` is
  right — it would be a bug in an in-process caller, not client input.
- **Tested over a real gRPC connection on a local port**, not an in-memory pipe, so
  registration, serialization, and status codes are all exercised. The server is
  tested against fakes; real MinIO and Postgres behind it are covered by
  `TestBrokerAppendFetchIntegration`, and the two meet end to end in step 16.
- **The service lives in `obj.proto`, separate from `record.proto`** (step 14).
  Wire field numbers describe requests in flight and can change freely; field `1`
  in `record.proto` is written into every stored object and must never be
  renumbered, so that file stays small and rarely opened. One `protoc` run names
  both plugins and produces `obj.pb.go` and `obj_grpc.pb.go`, leaving
  `record.pb.go` untouched. Both share one Go package, so importing `record.proto`
  only lets the service reference `Record`. Generation is byte-repeatable.
- **`Produce` takes repeated records, not one.** Each `Append` blocks for its
  flush, so one record per call would make M3 measure round-trips rather than the
  storage path — a scheduled consumer, not a hypothetical one. `ProduceResponse`
  carries only `base_offset` because a request's offsets are contiguous. An empty
  record list appends nothing. Step 15a made the broker admit one request's records
  as a unit, which is what makes the contiguity promise true.
- **`Fetch` is unary and returns every record in one response.** `Store.Fetch`
  already loads all matching records into memory, so a stream would put a second
  concurrency model in front of a non-streaming implementation. Records are
  contiguous from the requested offset, so no per-record offsets are sent.
  Revisit in M4 with measured read amplification.
- **Partitions are `int32` on the wire** and `int` in storage; step 15 converts.

## Process and infrastructure

- **`cmd/broker` calls `GracefulStop` before `Broker.Close`** (step 16a). A `Produce` is parked
  inside `Append` waiting for the next flush tick, so at Ctrl-C there are almost always RPCs that
  are about to succeed. `GracefulStop` stops accepting and waits for those, and the ticker is still
  running, so they commit and return real offsets; only then does `Close` stop a broker with
  nothing pending. Closing first tells them `Unavailable` for records milliseconds from durable,
  because `Close` does not flush. Verified by A/B against a live broker, signalling the instant a
  client connection was established: reversed order returned `Unavailable` in 2 of 3 runs, correct
  order returned offsets in 3 of 3. An earlier attempt using fixed sleeps passed under *both*
  orderings and proved nothing — the requests had all completed before the signal.
- **The broker's parent context is `context.Background()`, not the signal context.** This is what
  makes the ordering above mean anything: deriving it from the signal would cancel commits
  mid-PUT the moment Ctrl-C arrives, and the graceful wait would buy nothing. `Close` is the only
  thing that stops the broker. `GracefulStop` is bounded at ten seconds, falling back to `Stop`,
  so a hung storage call cannot make the process unkillable.
- **`cmd/` reads the environment; libraries still do not.** `cmd/broker` reuses the variable names
  the integration test already uses — `OBJ_POSTGRES_DSN`, `OBJ_S3_ENDPOINT`/`REGION`/`BUCKET`/
  `ACCESS_KEY`/`SECRET_KEY` — so one set of values points both the tests and the binary at the same
  services, plus `OBJ_LISTEN` (default `127.0.0.1:9092`). The six-line `env` helper is duplicated
  between the test and the binary rather than exported; sharing it would mean a new exported
  surface to save six lines. No flush-interval variable: M3 adds one when it has a measurement to
  attach to it.
- **The wire is verified with `grpcurl -import-path . -proto proto/obj/v1/obj.proto`**, not server
  reflection. The `.proto` is in the repo and the flags cost two arguments; reflection would expose
  the service list on every connection to save typing. Revisit if `objctl` makes the flags annoying.

- **`objctl` takes payloads as a repeatable `--data` flag of UTF-8 text** (step 16b). One flag, one
  record, in command-line order — `flag.Value.Set` is documented as called once per occurrence in
  that order, so the records reach the broker in the order typed. Arbitrary binary is therefore not
  expressible, and nothing in M1-M5 needs it; `--data-base64` is a new flag if M6 does. Rejected:
  stdin-per-line, which makes the newline a delimiter and so forbids a newline *inside* a record;
  and base64, which reintroduces exactly the grpcurl friction this step removes. A produce with no
  `--data` fails locally without dialling, because the server's `InvalidArgument` is already covered
  by `server_test.go` and a round trip would only make the shell wait to be told what the flags say.
- **`objctl consume` prints offset-prefixed lines**, `0: a`. `Fetch` sends no per-record offsets
  because records are contiguous from the requested offset, so the client numbers them itself. M5's
  check is "no gaps among the acked records", and a gap is invisible in a bare payload list but
  obvious as a missing number. Costs raw pipe-ability, which nothing here uses; `--raw` if that
  changes. An empty result writes one line to stderr — printing nothing at all is indistinguishable
  from a crash — leaving stdout pure data. No `--follow`: `Fetch` is unary, so it would be a poll
  loop guessing an interval, and streaming is already deferred to M4 behind measured read
  amplification.
- **`objctl` takes `--addr` and does not read `OBJ_LISTEN`.** `cmd/` reads the environment, but that
  variable means *where to listen* and may sensibly be `0.0.0.0:9092`, which is a poor thing to dial.
  Same string, different meaning. The three shared flags are registered separately in each
  subcommand rather than through a helper, on the same grounds as the duplicated `env` helper above.
- **`grpc.NewClient`, not the deprecated `grpc.Dial` — and it performs no I/O.** A wrong `--addr`
  does not fail at construction; it surfaces at the first RPC as `Unavailable ... Error while
  dialing`. Deliberately unlike `NewPostgresStore`, which pings so a bad DSN fails early: a server
  validates its dependencies at startup, while a one-shot CLI's very next act is the RPC itself.
  Verified both ways against a dead port.

- **Step 8 is split into definition/generation (8a) and framing (8b).** Protobuf
  tooling was not already present through gRPC. Step 8a defines `obj.v1.Record`
  with only `bytes payload = 1`; empty and absent payloads mean the same thing.
  Topic, partition, and offsets stay in metadata. Use the generated Go type
  directly, without a handwritten wrapper.
- **Pin `protoc-gen-go-grpc` v1.6.2 beside `protoc-gen-go`** (step 14). Messages and
  services come from different generators: `protoc-gen-go` ignores a `service` block
  entirely. The new plugin joins the ignored `bin/` directory and the README recipe.
  `google.golang.org/grpc` v1.83.2 becomes a runtime dependency because the generated
  service code imports it; satisfying it also bumped `golang.org/x/text` and
  `golang.org/x/sync` and added `golang.org/x/sys` as indirect requirements.
- **Commit generated protobuf Go code; pin the generation tools.** `protoc` 36.1
  and `protoc-gen-go` v1.36.12 live under ignored `bin/`, with installation and
  generation commands beside the definition. The Go protobuf dependency is also
  v1.36.12. This adds generated code to the repo but keeps ordinary builds from
  needing the generators.
- **Postgres on host port 5433.** A `drift-postgres` container from another project owns 5432.
  Colliding risks a connection *succeeding* against the wrong database. Container still listens on
  5432 internally.
- **Pinned image tags, never `latest`.** An image update must never be confusable with a code bug.
- **Libraries never read the environment.** `Config` is passed in; reading env vars is `cmd/`'s job.
  This is also what keeps real credentials out of every file in the repo.
- **Integration tests fail loudly when MinIO is down; they don't skip.** A test that silently skips
  is a test that quietly stops running.
- **The schema is applied by Postgres itself**, from `migrations/` mounted at
  `/docker-entrypoint-initdb.d`. It runs only on first start with an empty data directory, so a
  schema change means `docker compose down -v && docker compose up -d`. That reset is free in M1:
  `minio-init` recreates the bucket and the tests re-PUT their objects. Real migration tooling
  (`goose`, `golang-migrate`) waits until there is data worth keeping.
- **No `CREATE TABLE IF NOT EXISTS`.** It silently does nothing when a table of that name exists with
  a *different* shape, which looks exactly like success. Plain `CREATE TABLE` errors loudly, leaving
  one way to apply the schema instead of two.

## Considered and declined

Proposed and turned down. Don't re-pitch these unprompted; revisit only if something concrete makes
the case.

- **CHECK constraints on segment ranges** (`end_offset > start_offset`, `byte_end > byte_start`),
  step 5. Revisit if step 9 or 10 produces a real range bug.
- **A UNIQUE index on `(topic, partition, start_offset)`**, step 5. The index stays plain. Revisit if
  M2's concurrent brokers make duplicate offset assignment a live problem.

## Settled, not yet built

- **Code stays partition-general** even though M1 only exercises one partition. Costs nearly
  nothing, stops M2 from being a rewrite.
