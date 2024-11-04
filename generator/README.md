# generator

A tool for generating client libraries.

## Example Run

Run the following command from the generator directory:

```bash
go run ./devtools/cmd/generate -language=rust
```

Alternatively, you can run the protoc command directly:

```bash
go install ./cmd/protoc-gen-gclient

protoc -I testdata/googleapis \
    --gclient_out=. \
    --gclient_opt=capture-input=true,language=rust \
    testdata/googleapis/google/cloud/secretmanager/v1/resources.proto \
    testdata/googleapis/google/cloud/secretmanager/v1/service.proto
```

or to playback an old input without the need for `protoc`:

```bash
go run github.com/googleapis/google-cloud-rust/generator/cmd/protoc-gen-gclient -input-path=cmd/protoc-gen-gclient/testdata/rust/rust.bin
```
