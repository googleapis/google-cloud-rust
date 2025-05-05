# sidekick: a tool to generate and maintain Google Cloud SDKs

`sidekick` automates (or will soon automate) most activities around generating
and maintaining SDKs for Google Cloud.

## Example Run with Protobuf

You need to have `protoc` installed in your path. You can find useful links
below.

This will generate the client library for [Secret Manager] in the
`generator/testdata/rust/openapi/golden` directory. In future releases most
options should be already configured in a `.sidekick.toml` file.

```bash
cd generator
go run cmd/sidekick/main.go generate -project-root=.. \
  -specification-format protobuf \
  -specification-source generator/testdata/googleapis/google/cloud/secretmanager/v1 \
  -service-config generator/testdata/googleapis/google/cloud/secretmanager/v1/secretmanager_v1.yaml \
  -source-option googleapis-root=generator/testdata/googleapis \
  -language rust \
  -output generator/testdata/rust/protobuf/golden/secretmanager \
  -codec-option package-name-override=secretmanager-golden-protobuf \
  -codec-option package:wkt=package=types,path=types,source=google.protobuf \
  -codec-option package:gax=package=gax,path=gax,feature=unstable-sdk-client \
  -codec-option package:iam=package=iam-v1-golden-protobuf,path=generator/testdata/rust/protobuf/golden/iam/v1,source=google.iam.v1
```

## Example Run with OpenAPI

This will generate the client library for [Secret Manager] in the
`generator/testdata/rust/openapi/golden` directory. In future releases most
options should be already configured in a `.sidekick.toml` file.

```bash
cd generator
go run cmd/sidekick/main.go generate -project-root=.. \
  -specification-format openapi \
  -specification-source generator/testdata/openapi/secretmanager_openapi_v1.json \
  -service-config generator/testdata/googleapis/google/cloud/secretmanager/v1/secretmanager_v1.yaml \
  -language rust \
  -output generator/testdata/rust/openapi/golden \
  -codec-option package-name-override=secretmanager-golden-openapi \
  -codec-option package:wkt=package=types,path=types,source=google.protobuf \
  -codec-option package:gax=package=gax,path=gax,feature=unstable-sdk-client
```

## Testing

From the repo root: `go -C generator/ test ./...`

Or from `generator/`: `go test ./...`

## Prerequisites

### Installing `protoc`: the Protobuf Compiler

The generator and its unit tests use `protoc`, the Protobuf compiler. Ensure you
have `protoc >= v23.0` installed and it is found via your `$PATH`.

```bash
protoc --version
```

If not, follow the steps in [Protocol Buffer Compiler Installation] to download
a suitable version.

### Install goimports

```shell
go install golang.org/x/tools/cmd/goimports@latest
```

[protocol buffer compiler installation]: https://protobuf.dev/installation/
[secret manager]: https://cloud.google.com/secret-manager/
