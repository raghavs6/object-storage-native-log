package storage

import (
	"bytes"
	"strings"
	"testing"

	objv1 "github.com/raghavs6/object-storage-native-log/proto/obj/v1"
)

func TestEncodeObjectRanges(t *testing.T) {
	groups := map[PartitionKey][]*objv1.Record{
		{Topic: "beta", Partition: 2}:   {{Payload: []byte("z")}},
		{Topic: "alpha", Partition: 10}: {{Payload: []byte{0, 0xff, '\n'}}},
		{Topic: "alpha", Partition: 2}:  {{Payload: []byte("hi")}, {}, {Payload: []byte("bye")}},
		{Topic: "alpha", Partition: 0}:  nil,
		{Topic: "empty", Partition: 0}:  {},
	}
	data, ranges, err := EncodeObject(groups)
	if err != nil {
		t.Fatal(err)
	}
	// "hi" occupies 8 bytes, an empty record 4, and "bye" 9. The next
	// groups occupy 9 and 7 bytes. All lengths include the framing prefixes.
	want := []PartitionRange{
		{Topic: "alpha", Partition: 2, RecordCount: 3, ByteStart: 0, ByteEnd: 21},
		{Topic: "alpha", Partition: 10, RecordCount: 1, ByteStart: 21, ByteEnd: 30},
		{Topic: "beta", Partition: 2, RecordCount: 1, ByteStart: 30, ByteEnd: 37},
	}
	if len(ranges) != len(want) {
		t.Fatalf("got %d ranges, want %d", len(ranges), len(want))
	}
	end := 0
	for i, r := range ranges {
		if r != want[i] {
			t.Errorf("range %d: got %+v, want %+v", i, r, want[i])
		}
		if r.ByteStart != end || r.ByteEnd <= r.ByteStart || r.ByteEnd > len(data) {
			t.Fatalf("range %d does not extend coverage from byte %d within object length %d: %+v",
				i, end, len(data), r)
		}
		end = r.ByteEnd

		// This is the consumer's future read: decode only this partition's slice.
		got, err := DecodeRecords(data[r.ByteStart:r.ByteEnd])
		if err != nil {
			t.Fatalf("decode %s/%d: %v", r.Topic, r.Partition, err)
		}
		records := groups[PartitionKey{Topic: r.Topic, Partition: r.Partition}]
		if len(got) != len(records) || len(got) != r.RecordCount {
			t.Fatalf("range %d: decoded %d records, input has %d, metadata says %d",
				i, len(got), len(records), r.RecordCount)
		}
		for j := range records {
			if got[j] == nil || !bytes.Equal(got[j].Payload, records[j].Payload) {
				t.Errorf("range %d record %d: got %v, want %v", i, j, got[j], records[j])
			}
		}
	}
	if end != len(data) {
		t.Errorf("ranges cover %d bytes, object contains %d", end, len(data))
	}

	// Rebuild the map in reverse layout order: insertion order must not change
	// either the object bytes or the returned metadata.
	reversed := make(map[PartitionKey][]*objv1.Record)
	for i := len(want) - 1; i >= 0; i-- {
		key := PartitionKey{Topic: want[i].Topic, Partition: want[i].Partition}
		reversed[key] = groups[key]
	}
	otherData, otherRanges, err := EncodeObject(reversed)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(otherData, data) {
		t.Error("rebuilding the input map changed object bytes")
	}
	if len(otherRanges) != len(ranges) {
		t.Fatalf("rebuilding the input map changed the range count: %d vs %d", len(otherRanges), len(ranges))
	}
	for i := range ranges {
		if otherRanges[i] != ranges[i] {
			t.Errorf("rebuilding the input map changed range %d: %+v vs %+v", i, otherRanges[i], ranges[i])
		}
	}
}

func TestEncodeObjectEmpty(t *testing.T) {
	for _, groups := range []map[PartitionKey][]*objv1.Record{
		nil,
		{},
		{{Topic: "empty", Partition: 0}: nil, {Topic: "empty", Partition: 1}: {}},
	} {
		data, ranges, err := EncodeObject(groups)
		if err != nil || len(data) != 0 || len(ranges) != 0 {
			t.Errorf("empty input: got %x, %v, %v; want no bytes, ranges, or error", data, ranges, err)
		}
	}
}

func TestEncodeObjectErrorDiscardsPartialOutput(t *testing.T) {
	groups := map[PartitionKey][]*objv1.Record{
		{Topic: "alpha", Partition: 0}: {{Payload: []byte("valid")}},
		{Topic: "beta", Partition: 2}:  {nil},
	}
	data, ranges, err := EncodeObject(groups)
	if err == nil {
		t.Fatal("want an encoding error, got nil")
	}
	if data != nil || ranges != nil {
		t.Errorf("got partial output %x, %v; want nil bytes and ranges", data, ranges)
	}
	if !strings.Contains(err.Error(), "beta/2") {
		t.Errorf("error does not identify the failed partition: %v", err)
	}
}
