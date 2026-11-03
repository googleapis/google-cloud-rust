# Go side

`google-cloud-go.patch` is the Go part of the POC. It is a patch against upstream
[google-cloud-go](https://github.com/googleapis/google-cloud-go) commit `867fe707ef`, covering the
`cloud.google.com/go/storage` module. It holds everything except the Rust core, which lives in
[`../gcs_core`](../gcs_core).

## Apply, build, test

```sh
git clone https://github.com/googleapis/google-cloud-go
git -C google-cloud-go checkout 867fe707ef
./apply.sh google-cloud-go                    # applies the patch and copies ../gcs_core into it

cd google-cloud-go/storage
./internal/rustcore/build.sh                  # release build of the core, ~3 min -> internal/rustcore/lib/
go build ./... && CGO_ENABLED=0 go build ./... && go build -tags gcsrust ./...

go test -short -tags gcsrust -run TestRustUnit -v .                         # no network
RUST_DP_TEST_BUCKET=<bucket> go test -short -race -tags gcsrust -run 'TestRustCore|TestRustUnit' -v .
```

Requirements: Go 1.26+, Rust 1.85+ (stable), linux/amd64, cgo. The real-GCS tests use Application
Default Credentials and write only under `rustcore/<run id>/` in the bucket, deleting what they
write.

Using it from code:

```go
client, err := storage.NewClient(ctx, experimental.WithRustCore())     // JSON reads and listing
client, err := storage.NewGRPCClient(ctx, experimental.WithRustCore()) // gRPC reads and listing
```

## What the patch contains

| Path (in google-cloud-go) | What |
|---|---|
| `storage/rust_client.go` | `rustStorageClient`: construction, transport selection, reads, writes, JSON listing |
| `storage/rust_transport.go` | `rustRPC`: one line per v2 RPC, plus the encode/options/error plumbing |
| `storage/storage_rpc.go` | `storageRPC`: the seam between the Go API mapping and the transport |
| `storage/rust_client_stub.go` | builds without cgo or the build tag: `WithRustCore` then makes `NewClient` fail |
| `storage/internal/rustcore/` | generic cgo bridge (`Call`, `Reader`, `Writer`, completions, callbacks), the C header, `build.sh`, README |
| `storage/{storage,client,invoke,option,grpc_client,grpc_writer}.go`, `experimental/` | small hooks: the option, constructor wiring, one-attempt `run()` when the core owns retries |
| `storage/rust_core_test.go`, `rust_core_unit_test.go`, `rust_client_emulator_test.go` | tests against real GCS (incl. parity with Go's own JSON and gRPC clients), hermetic tests, emulator tests |
| `storage/internal/rustcore/cmd/rustbench`, `cmd/rustbench-report`, `bench/` | benchmark harness, report generator (Go), GCE VM scripts (not run yet) |

`storage/internal/rustcore/gcs_core.h` is a copy of [`../gcs_core.h`](../gcs_core.h) (cgo includes
it from there); keep the two in sync.
