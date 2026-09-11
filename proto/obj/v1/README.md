# Record definition and Go generation

`record.proto` defines the stored record: one byte payload. Field number `1` is
the payload's identity in stored data; do not renumber or reuse it. Empty payloads
are allowed, and absent and empty payloads mean the same thing. Topic, partition,
and assigned offsets live in Postgres metadata.

`record.pb.go` is generated and committed. Ordinary Go builds need only the
protobuf Go dependency in `go.mod`, not the generation tools. Edit the definition
and regenerate instead of editing the generated file. `EncodeRecords` and
`DecodeRecords` in `internal/storage/format.go` delimit concatenated records with
a four-byte big-endian length before each protobuf body. The length excludes the
prefix. No bytes means no records; a zero-length frame means one empty record.
Decoding malformed input returns an error and no partial records.

## Install the pinned tools

Run from the repository root. These commands install `protoc` 36.1 for macOS
(Apple Silicon or Intel) and `protoc-gen-go` v1.36.12 under the ignored `bin/`
directory. The Go protobuf dependency is also pinned to v1.36.12. Go uses its
normal module and build caches while installing the plugin.

```sh
mkdir -p bin
curl -fsSL --max-time 60 \
  https://github.com/protocolbuffers/protobuf/releases/download/v36.1/protoc-36.1-osx-universal_binary.zip \
  -o bin/protoc-36.1.zip
unzip -o bin/protoc-36.1.zip -d bin/protoc-36.1
GOBIN="$PWD/bin" go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12

bin/protoc-36.1/bin/protoc --version
bin/protoc-gen-go --version
```

Expected versions: `libprotoc 36.1` and `protoc-gen-go v1.36.12`.
For other operating systems, use the matching archive from the same
[compiler release](https://github.com/protocolbuffers/protobuf/releases/tag/v36.1).

## Generate

Run from the repository root:

```sh
bin/protoc-36.1/bin/protoc \
  --plugin=protoc-gen-go=bin/protoc-gen-go \
  --go_out=. \
  --go_opt=paths=source_relative \
  proto/obj/v1/record.proto
```

`protoc` reads the definition and calls the Go plugin to produce Go code.
`paths=source_relative` places the generated file beside its definition.
The explicit plugin path avoids depending on globally installed tools.

To check repeatability, run generation twice and compare
`shasum -a 256 proto/obj/v1/record.pb.go` after each run. The hashes must match.
Then run `go build ./...` and `go test ./... -run '^TestRangeHeader$' -count=1`;
neither check needs MinIO or Postgres.
