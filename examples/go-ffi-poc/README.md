# Go SDK on the shared Rust core (FFI proof of concept)

**Experimental, not a supported API.** This POC serves the Go Cloud Storage SDK
(`cloud.google.com/go/storage`) from one Rust core, `gcs_core`, built on this repository's
`google-cloud-storage` crate. Go calls the core in-process through cgo (FFI). The Go public API is
unchanged; Go programs opt in with `experimental.WithRustCore()` and the `gcsrust` build tag.

```text
Go application ── storage.Client (public API, unchanged)
                    └─ rustStorageClient ─ existing Go <-> v2 proto mapping, Go-only API semantics
                         └─ cgo bridge: generic, no storage logic (bytes in, bytes out)
                              └─ C ABI (gcs_core.h): ~20 functions, proto3-JSON requests and responses
                                   └─ gcs_core (Rust): transport, auth, retries, idempotency, uploads,
                                      checksums, read resumption, errors
                                        └─ google-cloud-storage 1.20 ──► Cloud Storage (JSON / gRPC)
```

## Layout

| Path | What |
|---|---|
| [`gcs_core/`](gcs_core) | The Rust core: a standalone static-library crate (not part of this workspace), pinned to the published `google-cloud-storage` 1.20 crates it was tested with |
| [`gcs_core.h`](gcs_core.h) | The C ABI: the contract any language binds to |
| [`build.sh`](build.sh) | Builds the core; optionally installs it into a google-cloud-go checkout |
| [`go/`](go) | The Go side: a patch against google-cloud-go, an apply script, and instructions |

## What runs in Rust

Every storage operation of the Go SDK runs here, once, so that every language binding would get
the same behavior:

- **Calls.** All 17 unary v2 RPCs (bucket and object metadata, list, compose, rewrite, move,
  restore, IAM) go through one generic entry point, `gcs_call(method, request, options)`. Object
  reads and writes use streaming handles.
- **Retries.** The caller's retry policy, attempt and time limits, backoff and retry predicate
  (evaluated through a callback), plus per-RPC idempotency rules decided from the request.
- **Uploads and reads.** Single-request or resumable uploads that never commit a partial object.
  Reads with CRC32C validation, resumption after broken connections, and decompressive
  transcoding.
- **Auth.** Any Go credential, bridged through a token callback; Rust caches the token.
- **Errors.** Every failure carries both its gRPC code and its HTTP status.
- **JSON listing.** `storage.objects.list`, which the Rust SDK does not offer (its control plane
  is gRPC-only), implemented on the SDK's own retry loop and error types.
- **Embedding safety.** Panics are caught at every entry point, and async operations complete
  exactly once. Handle lifetimes are safe under concurrent close. SIGPIPE is blocked on the core's
  threads: a static library inside a Go program does not inherit Rust's `main()` setup, and an
  unblocked SIGPIPE kills the whole process.

| | `storage.NewClient` + `WithRustCore` | `storage.NewGRPCClient` + `WithRustCore` |
|---|---|---|
| Reads | JSON API | gRPC `BidiReadObject` (CloudPath) |
| Listing | JSON API (implemented in the core) | gRPC v2 `ListObjects` |
| Uploads | JSON API | JSON API: the Rust SDK has no gRPC upload for standard buckets |

## Status

- **Tests.** 29 tests pass under the Go race detector: 21 against real GCS (including parity with
  Go's own JSON and gRPC clients) and 8 hermetic tests. Go's existing unit tests still pass, and
  the module still builds without cgo.
- **Benchmarks.** The harness (7 client setups, 15 read/range/write/list cases) and the GCE VM
  scripts are in the Go patch. **They have not been run yet.**
- **Known gaps (Rust SDK).** No gRPC uploads to standard buckets; no JSON listing; the JSON
  transport is HTTP/1.1-only; no DirectPath; no per-call headers on reads and writes.
- **Platforms.** Linux x86-64 only so far.

## Quick start

```sh
./build.sh                                   # build the core (~3 min, release with LTO)
# Go side: see go/README.md (apply the patch to google-cloud-go, build, test)
```
