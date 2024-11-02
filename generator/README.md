# generator

A tool for generating client libraries.

## Example Run

Run the following command from the generator directory:

```bash
go run ./devtools/cmd/generate -language=rust
```

Alternatively, you can run the protoc command directly:

```bash
go install github.com/googleapis/google-cloud-rust/generator/cmd/protoc-gen-gclient
protoc -I cmd/protoc-gen-gclient/testdata/smprotos \
    -I /path/to/googleapis \
    --gclient_out=. \
    --gclient_opt=capture-input=true,language=rust \
    cmd/protoc-gen-gclient/testdata/smprotos/resources.proto \
    cmd/protoc-gen-gclient/testdata/smprotos/service.proto
```

or to playback an old input without the need for `protoc`:

```bash
go run github.com/googleapis/google-cloud-rust/generator/cmd/protoc-gen-gclient -input-path=cmd/protoc-gen-gclient/testdata/rust/rust.bin
```
