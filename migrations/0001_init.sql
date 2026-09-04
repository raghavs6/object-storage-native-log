-- Metadata schema for the object-storage-native log.
--
-- Postgres runs this automatically, but ONLY on first start with an empty data
-- directory (see the /docker-entrypoint-initdb.d mount in docker-compose.yml).
-- It never re-runs. Changing this file therefore means:
--
--     docker compose down -v && docker compose up -d
--
-- That wipes MinIO too, which costs nothing: minio-init recreates the bucket
-- and the tests re-PUT their objects.
--
-- Deliberately NOT "CREATE TABLE IF NOT EXISTS". That form silently does
-- nothing when a table of the same name already exists with a different shape,
-- which is indistinguishable from success. A loud "relation already exists"
-- tells you the truth: reset the volume.
--
-- Real migration tooling (goose, golang-migrate) waits until there is data
-- worth keeping. There isn't, in M1.

-- One object holds records for many (topic, partition) pairs, so a single PUT
-- produces one row here per partition present in that object, each with its own
-- byte range within the object.
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

-- The consumer's lookup: "which segments cover partition P of topic T from
-- offset N onward?"
CREATE INDEX ON segments (topic, partition, start_offset);

COMMENT ON COLUMN segments.object_key IS
  'Opaque UUID. Carries no meaning: one object spans many partitions, so there is no topic it could be named after.';
COMMENT ON COLUMN segments.start_offset IS
  'Inclusive. Offset ranges are half-open: [start_offset, end_offset).';
COMMENT ON COLUMN segments.end_offset IS
  'EXCLUSIVE. Equals the next segment''s start_offset, and equals partition_offsets.next_offset after the last segment. end_offset - start_offset is the record count.';
COMMENT ON COLUMN segments.byte_start IS
  'Inclusive byte position within the object. Byte ranges are half-open: [byte_start, byte_end).';
COMMENT ON COLUMN segments.byte_end IS
  'EXCLUSIVE, matching Go slice convention. HTTP Range headers are inclusive; that conversion lives only in rangeHeader() in internal/storage/objectstore.go.';

-- The serialization point for offset assignment. One row per partition.
CREATE TABLE partition_offsets (
  topic       TEXT   NOT NULL,
  partition   INT    NOT NULL,
  next_offset BIGINT NOT NULL,
  PRIMARY KEY (topic, partition)
);

COMMENT ON COLUMN partition_offsets.next_offset IS
  'The offset the NEXT record will receive, not the last one used. An empty partition sits at 0. Equals the highest end_offset in segments for this partition.';
