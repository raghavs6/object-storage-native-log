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

- **The broker runs one timed commit at a time** (step 13b). `New` starts the
  worker with a positive flush interval; `DefaultFlushInterval` is 250ms, an initial
  setting rather than a measured optimum. Empty ticks do no storage work. Appends
  can accumulate in the next batch during a commit; memory remains unbounded.
- **`Append` waits for its receipt's committed offset or caller cancellation.**
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
  usable; `add` rejects nil records and clones valid protobuf records before taking
  the mutex. Empty payloads are valid. Callers may reuse records after `add` returns,
  but must not mutate them during copying. Success means buffered, not durable.
- **`drain` transfers ownership of the accumulated batch and resets the buffer.**
  Additions and drains share one mutex; an addition belongs to exactly one batch.
  Records within each partition follow insertion order under the lock, with no
  predetermined order for concurrent callers or across partitions. Later additions
  cannot mutate a drained batch. Empty drains return nil. Storage I/O, timed flushing,
  producer acknowledgments, retries, and size limits are outside this step; memory
  remains unbounded until later batching controls are added.

## Process and infrastructure

- **Step 8 is split into definition/generation (8a) and framing (8b).** Protobuf
  tooling was not already present through gRPC. Step 8a defines `obj.v1.Record`
  with only `bytes payload = 1`; empty and absent payloads mean the same thing.
  Topic, partition, and offsets stay in metadata. Use the generated Go type
  directly, without a handwritten wrapper.
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
