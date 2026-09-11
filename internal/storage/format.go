package storage

import (
	"encoding/binary"
	"fmt"
	"math"
	"sort"

	objv1 "github.com/raghavs6/object-storage-native-log/proto/obj/v1"
	"google.golang.org/protobuf/proto"
)

// PartitionKey identifies one ordered log within a topic.
type PartitionKey struct {
	Topic     string
	Partition int
}

// PartitionRange describes one partition's framed records within an object.
// Object keys and logical offsets are added later by the commit path, so this
// is separate from Segment. ByteEnd is exclusive, matching Go slices.
type PartitionRange struct {
	Topic       string
	Partition   int
	RecordCount int
	ByteStart   int
	ByteEnd     int
}

// EncodeObject packs groups in topic order, then numeric partition order,
// preserving record order within each group. Every nonempty group occupies one
// contiguous byte range, including its framing bytes. Empty groups are skipped.
// An encoding error returns neither partial object bytes nor partial ranges.
func EncodeObject(groups map[PartitionKey][]*objv1.Record) ([]byte, []PartitionRange, error) {
	keys := make([]PartitionKey, 0, len(groups))
	for key, records := range groups {
		if len(records) > 0 {
			keys = append(keys, key)
		}
	}
	// Map iteration order must not determine the physical object layout.
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Topic != keys[j].Topic {
			return keys[i].Topic < keys[j].Topic
		}
		return keys[i].Partition < keys[j].Partition
	})

	var data []byte
	var ranges []PartitionRange
	for _, key := range keys {
		records := groups[key]
		body, err := EncodeRecords(records)
		if err != nil {
			return nil, nil, fmt.Errorf("encode object partition %s/%d: %w", key.Topic, key.Partition, err)
		}
		start := len(data)
		data = append(data, body...)
		ranges = append(ranges, PartitionRange{
			Topic: key.Topic, Partition: key.Partition, RecordCount: len(records),
			ByteStart: start, ByteEnd: len(data),
		})
	}
	return data, ranges, nil
}

// EncodeRecords packs records in order, each preceded by a 4-byte big-endian
// length. The length counts the protobuf body, excluding the prefix itself.
// Empty input encodes to no bytes; nil record pointers are rejected.
func EncodeRecords(records []*objv1.Record) ([]byte, error) {
	var data []byte
	for i, record := range records {
		if record == nil {
			return nil, fmt.Errorf("encode record %d: nil record", i)
		}
		body, err := proto.Marshal(record)
		if err != nil {
			return nil, fmt.Errorf("encode record %d: %w", i, err)
		}
		if uint64(len(body)) > math.MaxUint32 {
			return nil, fmt.Errorf("encode record %d: body length %d exceeds 4-byte prefix", i, len(body))
		}
		data = binary.BigEndian.AppendUint32(data, uint32(len(body)))
		data = append(data, body...)
	}
	return data, nil
}

// DecodeRecords unpacks complete frames in order. Empty input yields no records;
// a zero-length frame yields one empty record. Any malformed frame returns an
// error and no records, including when earlier frames were valid.
func DecodeRecords(data []byte) ([]*objv1.Record, error) {
	var records []*objv1.Record
	for pos := 0; pos < len(data); {
		frameStart := pos
		if len(data)-pos < 4 {
			return nil, fmt.Errorf("decode frame at byte %d: incomplete length prefix", frameStart)
		}
		n := binary.BigEndian.Uint32(data[pos : pos+4])
		pos += 4
		// Compare before converting to int or slicing, so a bogus length cannot
		// overflow an index or cause an allocation based on untrusted input.
		if uint64(n) > uint64(len(data)-pos) {
			return nil, fmt.Errorf("decode frame at byte %d: body length %d exceeds remaining %d bytes",
				frameStart, n, len(data)-pos)
		}
		record := new(objv1.Record)
		if err := proto.Unmarshal(data[pos:pos+int(n)], record); err != nil {
			return nil, fmt.Errorf("decode frame at byte %d: %w", frameStart, err)
		}
		records = append(records, record)
		pos += int(n)
	}
	return records, nil
}
