package storage

import (
	"encoding/binary"
	"fmt"
	"math"

	objv1 "github.com/raghavs6/object-storage-native-log/proto/obj/v1"
	"google.golang.org/protobuf/proto"
)

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
