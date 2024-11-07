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

## Installing `protoc`: the Protobuf Compiler

The unit tests use `protoc` to parse text `.proto` files. You will need this
installed in your `$PATH` to run the tests.

You can find numerous guides on how to install the Protobuf Compiler. Here
we suggest two approaches:

- Follow the instructions in the [gRPC Tutorial].

- The Protobuf team ships easy to install binaries with each release. In the
  [latest Protobuf release][protobuf-latest] you can find a `.zip` file for
  your platform. Download and extract such that the `bin/` subdirectory is in
  your path.  For example:

  ```shell
  curl -fSSL -o /tmp/protoc.zip https://github.com/protocolbuffers/protobuf/releases/download/v28.3/protoc-28.3-linux-x86_64.zip
  cd /usr/local
  sudo unzip -x /tmp/protoc.zip
  ```

[grpc tutorial]: https://grpc.io/docs/protoc-installation/
[protobuf-latest]: https://github.com/protocolbuffers/protobuf/releases/latest
