
## What I'm building

A Kafka-style append-only log where S3-compatible object storage is the source of truth and the servers hold no durable state.

Prior art: WarpStream, AutoMQ, Bufstream. I'm building a scaled-down version to understand the architecture, not to compete with them.

---

## The problem this solves

Kafka stores partition data on brokers' local disks and replicates each record three times across availability zones for durability. Two consequences:

**Cost.** Cloud providers bill for cross-AZ network transfer (~$0.01–0.02/GB, both directions). Writing 1 GB means paying for 3–4 GB of zone-crossing traffic, before consumer fetches. At scale this line item often exceeds compute and storage combined.

**Operational pain.** Brokers are stateful. Adding capacity means physically copying partitions onto the new node while serving traffic. Losing a node means urgent re-replication.

**The observation:** S3 already replicates across AZs, and doesn't bill for that replication or for same-region traffic to your instances. Kafka is hand-rolling durability the cloud gives away, and paying network fees for it.

**The design:** make object storage the source of truth. Brokers become stateless — in-memory write buffers on the way in, read caches on the way out. Scaling becomes instant. Node loss becomes boring. Cross-AZ replication cost largely disappears.

**The tradeoff:** S3 writes take hundreds of milliseconds versus single-digit ms for local disk. Acceptable for log aggregation, analytics ingest, and CDC. Disqualifying for low-latency event-driven systems. This tradeoff is the point of the project, not a flaw to hide.

---

## Architecture

```
Producers
    |  gRPC append
    v
Broker (stateless)  ---- commit batch metadata ---->  Postgres
    |  in-memory buffer, flush every ~250ms                 |
    |  batched PUT                                          | offset lookup
    v                                                       v
Object storage (MinIO)  <---- range GET ----------  Consumers
```

**Broker.** Accepts appends. Buffers records in memory across *all* topics and partitions, interleaved. Flushes when the buffer hits a size threshold or a time threshold. One flush = one PUT of one object. Also serves reads from an LRU cache of recently touched objects. Holds nothing durable — kill and restart loses only unflushed buffer.

**Object storage.** MinIO in Docker locally, S3-compatible API so it can point at real S3 later. Only durable component.

**Metadata store.** Postgres. Maps logical offsets to physical byte ranges, and acts as the serialization point that assigns offsets across concurrent broker commits.

**Consumer API.** Given `(topic, partition, offset)`, query metadata for the objects covering that range, issue range GETs, return records in order.

---

## Data model (starting point — revise as needed)

```sql
CREATE TABLE segments (
  id           BIGSERIAL PRIMARY KEY,
  object_key   TEXT    NOT NULL,
  topic        TEXT    NOT NULL,
  partition    INT     NOT NULL,
  start_offset BIGINT  NOT NULL,
  end_offset   BIGINT  NOT NULL,
  byte_start   INT     NOT NULL,
  byte_end     INT     NOT NULL,
  created_at   TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX ON segments (topic, partition, start_offset);

CREATE TABLE partition_offsets (
  topic       TEXT   NOT NULL,
  partition   INT    NOT NULL,
  next_offset BIGINT NOT NULL,
  PRIMARY KEY (topic, partition)
);
```

One object contains records for many `(topic, partition)` pairs, so one PUT produces multiple `segments` rows — one per partition present in that object, each with its own byte range within the object.

Offset assignment happens inside the transaction that inserts those rows: read `next_offset`, assign, update. That transaction is the serialization point.

---

## Milestones

**M1 — End-to-end skeleton.** Docker Compose with MinIO and Postgres. Single broker, in-memory buffer, time-based flush only. gRPC produce API. Consumer fetch that queries metadata and range-GETs. No cache, no size trigger, one partition. Goal: a record goes in and comes back out.

**M2 — Real batching.** Multiple topics and partitions interleaved into one object. Size-or-time flush trigger. Object serialization format (start with length-prefixed protobuf or NDJSON; revisit later). Correct byte-range bookkeeping per partition.

**M3 — Benchmark harness.** Load generator with configurable throughput and record size. Measure p50/p99 write latency, throughput, PUT count per million records. Sweep the batch interval and produce a latency-vs-request-cost curve.

**M4 — Read path.** LRU object cache in the broker. Measure hit rate on tail reads versus historical reads. Show the read amplification problem with numbers before solving it.

**M5 — Crash correctness.** What happens when a broker PUTs an object and dies before committing metadata? Orphaned object, and possibly a gap. Design and implement reconciliation. This is the most interesting part of the project.

**M6+ — stretch.** Consumer groups. Background compaction into per-partition objects. Multiple brokers with consistent-hashed cache. Cost model versus simulated Kafka cross-AZ egress.

Finish M1–M4 before starting anything past M5. A finished small system with real measurements beats a sprawling half-built one.

---

## Open questions I want to work through (don't just answer — help me reason)

1. **Offset assignment under concurrency.** Two brokers commit overlapping batches simultaneously. What Postgres isolation level do I need, and why? What does the failure mode look like at `READ COMMITTED` versus `SERIALIZABLE`? Where does contention bite as broker count grows?

2. **Orphaned objects.** Broker PUTs successfully, then crashes before committing metadata. The object exists but nothing references it. Options: garbage-collect unreferenced objects, or write an intent record before the PUT and reconcile on recovery. What are the tradeoffs?

3. **Batch interval choice.** Why 250ms and not 50ms or 1s? I want a measured curve, not a guess. What should I actually sweep and plot?

4. **Object layout.** Records for one partition are scattered across many objects. How do I lay out the object internally so a partition's records are contiguous within it, minimizing range-GET count?

5. **Metadata as bottleneck.** Postgres is a single point of contention. At what throughput does it become the limit? What would replacing it look like?

6. **Exactly-once.** What would producer idempotency require here? Sequence numbers per producer? Where does dedup state live if brokers are stateless?

---

## Metrics to collect from the start

- Throughput (MB/s, records/s)
- Write latency p50 / p95 / p99
- PUT requests per million records
- Cache hit rate, tail reads versus historical
- Estimated $/TB versus Kafka's cross-AZ replication at equal volume

Build the measurement harness early (M3). Numbers I measured myself are the whole point of this project — both for learning and for the resume.

---

## Stack

Go, Postgres, MinIO, Docker Compose, gRPC. Prometheus for metrics if it's not a distraction.

---

## How I want help

- Explain design decisions before writing code. I'd rather understand one component deeply than have four generated.
- Push back if I'm scoping something badly or building the wrong thing next.
- When there's a real tradeoff, name both sides rather than picking silently.
- Assume I'll ask "why" about most non-obvious choices.