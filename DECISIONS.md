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

## Process and infrastructure

- **Postgres on host port 5433.** A `drift-postgres` container from another project owns 5432.
  Colliding risks a connection *succeeding* against the wrong database. Container still listens on
  5432 internally.
- **Pinned image tags, never `latest`.** An image update must never be confusable with a code bug.
- **Libraries never read the environment.** `Config` is passed in; reading env vars is `cmd/`'s job.
  This is also what keeps real credentials out of every file in the repo.
- **Integration tests fail loudly when MinIO is down; they don't skip.** A test that silently skips
  is a test that quietly stops running.

## Settled, not yet built

- **Code stays partition-general** even though M1 only exercises one partition. Costs nearly
  nothing, stops M2 from being a rewrite.
- **Record framing is a 4-byte big-endian length prefix + protobuf**, concatenated (step 8).
  Protobuf because gRPC already brings it in. The length prefix is what makes a byte range
  self-describing.
- **Offset assignment uses one statement**, `UPDATE partition_offsets SET next_offset =
  next_offset + $1 ... RETURNING next_offset` — not a SELECT then an UPDATE (step 7). The
  single-statement form takes a row lock and re-reads, so it is free of lost updates at
  `READ COMMITTED`.
- **`Append` does not return until its flush has committed to Postgres** (step 13). Early-acking
  would make the system look fast and be wrong, and would hide the latency M3 exists to measure.
