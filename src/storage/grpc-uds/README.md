# `storage-grpc-uds` (`gcs-core-server`)

`storage-grpc-uds` exposes the [`google-cloud-storage`](../) Rust core (`Storage` + `StorageControl`) as a high-throughput gRPC server over a **Unix Domain Socket (UDS)** (with optional Linux `memfd` + `SCM_RIGHTS` shared-memory zero-copy reads/writes).

It is the Rust sidecar server implementation for the **GCS SDK Consolidation Architecture (gRPC-over-UDS Approach)**, enabling thin language veneers (Go, Python, Node.js, Java, C++) to delegate retries, resumable/bidi streaming, CRC32C checksum verification, and gRPC subchannel pooling to a single shared Rust core without `cgo` / FFI ABI hazards in the caller process.

---

## Architecture & Supported Topologies

`storage-grpc-uds` supports three deployment topologies from the same codebase:

### 1. Multi-Process Shared Sidecar Daemon (`gcs-core-server`)
A single per-UID daemon process listens on `$XDG_RUNTIME_DIR/gcs-core/gcs-core.sock` (or `/tmp/gcs-core-<uid>/gcs-core.sock`, mode `0700` directory / `0600` socket).
- **Cross-Process Connection Pooling:** All thin-client processes on the host multiplex onto the daemon's warm HTTP/2 / gRPC subchannel pool (`--grpc-subchannels`), eliminating cold TCP+TLS handshakes for short-lived CLI or serverless workers.
- **Startup Synchronization (`flock` + `READY`):** Thin clients acquire an exclusive advisory `flock(LOCK_EX)` on `<socket>.lock`, spawn `gcs-core-server` if the socket is absent or stale, and wait for the single line `READY <socket_path>\n` on `stdout` before releasing the lock.
- **Connection-Aware Idle Eviction:** The daemon tracks both active UDS connections (`active_conns`) and RPC timestamps, automatically shutting down and unlinking its socket after `--idle-secs` (default `300s`) with zero active connections.

### 2. Per-Process 1:1 Child Sidecar (`--pdeathsig=true`)
When a thin client prefers strict 1:1 fate-sharing with its parent process, passing `--pdeathsig=true` configures `prctl(PR_SET_PDEATHSIG, SIGTERM)` on Linux immediately on startup (solving the parent-exit TOCTOU race by checking `getppid() == 1` right after `prctl`).

### 3. In-Process Background Server (`gcs_core_start_inproc`)
For environments that want the gRPC-over-UDS protocol inside a single OS process, `storage_grpc_uds::gcs_core_start_inproc(socket_path)` exports a C-ABI entry point that spawns a background Tokio runtime and binds the UDS listener in-process.

---

## Why a Purpose-Built `gcscore.v1.GcsCore` Contract?

Instead of proxying `google.storage.v2.Storage` directly over UDS, `storage-grpc-uds` defines [`proto/gcscore.proto`](proto/gcscore.proto) (`gcscore.v1.GcsCore`):
1. **Dual-Transport Routing (`Transport::JSON` vs. `Transport::GRPC`):** Thin clients can select JSON/HTTP (`ReadObject` / `WriteObject`) or gRPC (`BidiReadObject` / `BidiWriteObject`) per request while speaking a single uniform UDS contract.
2. **Multi-Range & Zonal Rapid Storage Support (`OpenBidiRead` + `ReadRanges` + `OpenAppendable`):** Stateful `BidiReadObject` handles (`handle_id`) and appendable writers (`writer_id`) live in a lock-free `RwLock` descriptor table inside the Rust server, allowing multiple concurrent `ReadRanges` calls (or `MultiRangeDownloader` ranges) to multiplex over a single upstream `BidiReadObject` stream.
3. **Automatic Regional Fallback:** When `OpenAppendable` is called against a standard regional bucket that returns `UNIMPLEMENTED` for `BidiWriteObject`, `GcsCoreService` automatically falls back to buffered `write_object` semantics on `FlushAppendable` / `CloseAppendable`.
4. **Shared-Memory Zero-Copy Data Plane (`ShmRead` / `ShmWrite`):** On Linux, `proto/gcscore.proto` allows RPC control messages to pass `(region_id, region_offset, length)` descriptors referencing a `memfd` shared via `SCM_RIGHTS` on `<socket>.shm`, eliminating protobuf payload serialization over UDS.

---

## Security & Multi-Process Credential Isolation

When multiple processes share a single `gcs-core-server` daemon, credentials must never leak across processes:

1. **OS-Level Socket Protection (`SO_PEERCRED`):**
   - The UDS socket is bound with `0600` permissions.
   - A Tonic gRPC interceptor inspects `UdsConnectInfo::peer_cred()` (`getsockopt(SO_PEERCRED)`) on every connection and rejects any caller whose effective UID does not match the server's UID (`PERMISSION_DENIED`).
2. **Per-Caller Token Lending (`CreateSession` / `UpdateToken` / `CloseSession`):**
   - Thin clients evaluate their own native credential source (e.g. Go `oauth2.TokenSource`, Workload Identity, impersonated credentials, or custom endpoints) in the host language and lend the resulting short-lived OAuth2 bearer token + expiry + optional `quota_project` to the daemon via `CreateSession`.
   - `Sessions` binds the returned `session_id` to the caller's kernel-verified `(uid, pid)` tuple from `SO_PEERCRED`.
   - Every data-plane RPC carrying `x-session: <session_id>` verifies that the caller's `(uid, pid)` matches the session owner (`PERMISSION_DENIED` on mismatch) and checks that the lent token has not expired (`UNAUTHENTICATED` on expiry).

---

## High-Throughput UDS & HTTP/2 Tuning

To prevent HTTP/2 flow-control stalls and kernel socket buffer contention under high concurrency (e.g. 48–256 concurrent streams on `c4-standard-192` w/ 200 Gbps Tier-1 networking), `serve_with_listener` configures:

| Parameter | Value | Purpose |
| :--- | :--- | :--- |
| `SO_SNDBUF` / `SO_RCVBUF` | `8 MiB` | Prevents Linux `AF_UNIX` stream socket buffer stalls (default `212 KiB`) |
| `initial_stream_window_size` | `64 MiB` | Allows up to `64 MiB` in flight per UDS stream without `WINDOW_UPDATE` stalls |
| `initial_connection_window_size` | `256 MiB` | Aggregates up to `256 MiB` across multiplexed streams on one UDS connection |
| `max_frame_size` | `4 MiB` | Reduces HTTP/2 framing overhead for `2 MiB`–`4 MiB` `ReadChunk` / `WriteChunk` payloads |
| `max_decoding/encoding_message_size` | `64 MiB` | Supports large streaming chunks |

---

## Building & Running

### Build the Sidecar Binary
```bash
cargo build --release -p storage-grpc-uds --bin gcs-core-server
```

### Command-Line Flags
```text
Usage: gcs-core-server [OPTIONS]

Options:
      --socket <SOCKET>                        Unix Domain Socket path (prefix with `@` on Linux for abstract namespace) [default: /tmp/gcs-core.sock]
      --idle-secs <IDLE_SECS>                  Exit after this many seconds with 0 active connections and no RPCs (0 = never) [default: 300]
      --default-transport <DEFAULT_TRANSPORT>  Default upstream GCS transport (`json` or `grpc`) [default: json]
      --grpc-subchannels <GRPC_SUBCHANNELS>    Number of gRPC subchannels in upstream `Storage` pool (0 = vCPUs, min 4) [default: 0]
      --pdeathsig <true|false>                 Linux 1:1 child mode: send SIGTERM when parent process exits [default: false]
      --peer-uid-check <true|false>            Enforce SO_PEERCRED UID verification [default: true]
      --chunk-size <CHUNK_SIZE>                Default target bytes per streamed ReadChunk [default: 2097152]
      --endpoint <ENDPOINT>                    Optional explicit GCS endpoint URL
      --tcp <TCP>                              Optional additional TCP listen address (`host:port`)
      --require-session                        Reject data RPCs without a valid `x-session` header
      --shm-size <SHM_SIZE>                    Per-client memfd size served on `<socket>.shm` via SCM_RIGHTS (0 = disabled) [default: 268435456]
```

### Run Integration Tests
```bash
cargo test -p storage-grpc-uds
```
