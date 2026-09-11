package storage

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	objv1 "github.com/raghavs6/object-storage-native-log/proto/obj/v1"
)

func TestRecordsRoundTrip(t *testing.T) {
	want := []*objv1.Record{
		{Payload: []byte("hi")},
		{},
		{Payload: []byte{0, 0xff, 0x80, '\n'}},
		{Payload: []byte{}},
		{Payload: []byte("bye")},
	}
	data, err := EncodeRecords(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeRecords(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d records, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] == nil || !bytes.Equal(got[i].Payload, want[i].Payload) {
			t.Errorf("record %d: got %v, want %v", i, got[i], want[i])
		}
	}
}

// Independently specified bytes catch an encoder and decoder that agree on the
// wrong format. The larger case also exercises more than the prefix's low byte.
func TestRecordsWireFormat(t *testing.T) {
	cases := []struct {
		name    string
		payload []byte
		wire    []byte
	}{
		{"hi", []byte("hi"), []byte{0, 0, 0, 4, 0x0a, 2, 'h', 'i'}},
		{"empty", nil, []byte{0, 0, 0, 0}},
		{
			"256-byte payload", bytes.Repeat([]byte{'x'}, 256),
			// Protobuf: field tag 0x0a, payload length 256 as 0x80 0x02,
			// then 256 payload bytes. Body length = 259 = 0x0103.
			append([]byte{0, 0, 1, 3, 0x0a, 0x80, 0x02}, bytes.Repeat([]byte{'x'}, 256)...),
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := EncodeRecords([]*objv1.Record{{Payload: c.payload}})
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, c.wire) {
				t.Errorf("encoded bytes = %x, want %x", got, c.wire)
			}
			records, err := DecodeRecords(c.wire)
			if err != nil {
				t.Fatal(err)
			}
			if len(records) != 1 || records[0] == nil || !bytes.Equal(records[0].Payload, c.payload) {
				t.Errorf("decoded records = %v, want one record with payload %x", records, c.payload)
			}
		})
	}
}

func TestRecordsEmptyInput(t *testing.T) {
	for _, input := range [][]*objv1.Record{nil, {}} {
		got, err := EncodeRecords(input)
		if err != nil || len(got) != 0 {
			t.Errorf("encode empty: got %x, %v; want no bytes and no error", got, err)
		}
	}
	for _, input := range [][]byte{nil, {}} {
		got, err := DecodeRecords(input)
		if err != nil || len(got) != 0 {
			t.Errorf("decode empty: got %v, %v; want no records and no error", got, err)
		}
	}
}

func TestRecordsRejectNil(t *testing.T) {
	for _, input := range [][]*objv1.Record{{nil}, {{Payload: []byte("hi")}, nil}} {
		got, err := EncodeRecords(input)
		if err == nil || got != nil {
			t.Errorf("got %x, %v; want nil bytes and an error", got, err)
		}
	}
}

func TestRecordsRejectMalformed(t *testing.T) {
	cases := []struct {
		name string
		data []byte
	}{
		{"one-byte prefix", []byte{0}},
		{"two-byte prefix", []byte{0, 0}},
		{"three-byte prefix", []byte{0, 0, 0}},
		{"missing body", []byte{0, 0, 0, 1}},
		{"truncated body", []byte{0, 0, 0, 4, 0x0a, 2, 'h'}},
		{"maximum length with no body", []byte{0xff, 0xff, 0xff, 0xff}},
		{"invalid protobuf tag", []byte{0, 0, 0, 1, 0}},
		{"truncated protobuf field", []byte{0, 0, 0, 3, 0x0a, 2, 'h'}},
	}
	// Each failure must also discard any preceding, successfully decoded record.
	for _, prefix := range [][]byte{nil, {0, 0, 0, 4, 0x0a, 2, 'h', 'i'}} {
		for _, c := range cases {
			t.Run(fmt.Sprintf("%s at byte %d", c.name, len(prefix)), func(t *testing.T) {
				data := append(append([]byte(nil), prefix...), c.data...)
				got, err := DecodeRecords(data)
				if err == nil {
					t.Fatal("want an error, got nil")
				}
				if got != nil {
					t.Errorf("got partial records %v, want nil", got)
				}
				if want := fmt.Sprintf("frame at byte %d:", len(prefix)); !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not identify %q", err, want)
				}
			})
		}
	}
}
