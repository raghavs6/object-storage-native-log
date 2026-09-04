# Decisions

Settled calls and why. Read before proposing something different — if a decision here is wrong,
argue against it, don't quietly work around it. Append a line when a new one is settled.

## Storage

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

## Process and infrastructure

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
- **Record framing is a 4-byte big-endian length prefix + protobuf**, concatenated (step 8).
  Protobuf because gRPC already brings it in. The length prefix is what makes a byte range
  self-describing.
- **`Append` does not return until its flush has committed to Postgres** (step 13). Early-acking
  would make the system look fast and be wrong, and would hide the latency M3 exists to measure.
