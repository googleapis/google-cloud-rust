# `storage-grpc-uds` (`gcs-core-server`)

`storage-grpc-uds` exposes the [`google-cloud-storage`](../) Rust core (`Storage` + `StorageControl`) as a local gRPC server over a **Unix Domain Socket (UDS)** (plus an optional Linux `memfd` + `SCM_RIGHTS` shared-memory zero-copy read data plane).

Thin language veneers (Go, Python, Java, C++, Node.js) connect to `gcs-core-server` over UDS and delegate retries, resumable/bidi streaming, CRC32C checksum verification, and gRPC subchannel pooling to a single shared Rust core—without `cgo` or FFI ABI hazards in the caller process.

```text
+-----------------------+       gRPC over UDS (0600, SO_PEERCRED)       +----------------------------------------+
| Thin Language Clients |  ==========================================>  |            gcs-core-server             |
| (Go / Python / Java)  |  -- Control & Streaming: gcscore.v1.GcsCore   |  - google_cloud_storage::Storage       |
|                       |  -- Zero-Copy Reads: <socket>.shm (memfd)     |  - google_cloud_storage::StorageControl|
| - Native TokenSource  |  -- Auth Lending: x-session: <session_id>     |  - Warm gRPC subchannel pool           |
+-----------------------+                                               +-------------------+--------------------+
                                                                                            |
                                                                      JSON/HTTP (reqwest)   |   gRPC v2 / CloudPath (tonic)
                                                                                            v
                                                                          +-----------------------------------+
                                                                          |     Google Cloud Storage (GCS)    |
                                                                          +-----------------------------------+
```

---

## Crate Layout

| File | Responsibility |
| :--- | :--- |
| [`proto/gcscore.proto`](proto/gcscore.proto) | `gcscore.v1.GcsCore` protobuf contract between thin clients and the Rust server. |
| [`src/main.rs`](src/main.rs) | CLI entry point (`gcs-core-server`) with opt-in Linux `PR_SET_PDEATHSIG(SIGTERM)`. |
| [`src/lib.rs`](src/lib.rs) | UDS listener setup (`0600` mode, stale-socket recovery, `@` abstract sockets), `SO_PEERCRED` UID interceptor, `TrackedUnixStream` connection ref-counting, dead-PID session/handle reaper, and `gcs_core_start_inproc` / `gcs_core_stop_inproc` C-FFI entry points. |
| [`include/gcs_core_uds.h`](include/gcs_core_uds.h) | C/C++ header declaring `gcs_core_start_inproc` and `gcs_core_stop_inproc` for linking `libstorage_grpc_uds.{a,so}`. |
| [`src/service.rs`](src/service.rs) | `GcsCore` RPC implementation bridging UDS streams to `Storage` and `StorageControl`. |
| [`src/session.rs`](src/session.rs) | Per-caller OAuth2 token-lending registry (`Sessions`) bound to `(uid, pid)` with dead-PID reaping. |
| [`src/shm.rs`](src/shm.rs) | Linux `memfd_create` + `F_ADD_SEALS` + `SCM_RIGHTS` FD-passing server on `<socket>.shm`. |
| [`tests/uds_integration.rs`](tests/uds_integration.rs) | End-to-end integration tests over real UDS sockets, `httptest`, and `storage-grpc-mock`. |

---

## Supported Deployment Modes

`storage-grpc-uds` supports three deployment topologies from the same binary/library:

| Mode | How to Launch | Process Lifecycle & Use Case |
| :--- | :--- | :--- |
| **1. `N:1` Shared Host Daemon** *(Default)* | `gcs-core-server --socket /run/user/$UID/gcs-core.sock --idle-secs 300` | Multiple thin-client processes share one daemon and one warm GCS gRPC subchannel pool. `TrackedUnixStream` tracks active UDS connections (`active_conns`), and the daemon exits only after `--idle-secs` with `0` active connections. |
| **2. `1:1` Per-Process Child Sidecar** | `gcs-core-server --socket /tmp/gcs-$PID.sock --pdeathsig true --idle-secs 0` | Dedicated child process per application. On Linux, `--pdeathsig true` sets `prctl(PR_SET_PDEATHSIG, SIGTERM)` (with immediate `getppid() == 1` TOCTOU verification) so the sidecar terminates immediately if its parent crashes (`SIGKILL`). |
| **3. Single-Process (In-Process UDS)** | Call `gcs_core_start_inproc(socket_path)` (default flags) or `gcs_core_start_inproc_with_args(argc, argv)` (same flags as the binary, e.g. `--socket @gcs-core-123 --grpc-subchannels 4`), then `gcs_core_stop_inproc(socket_path)`, via C-FFI ([`include/gcs_core_uds.h`](include/gcs_core_uds.h)) | Runs the Tonic UDS server on a background Tokio thread inside the host process. All data RPCs still flow over standard UDS sockets (avoiding per-RPC FFI/`cgo` transitions). |

---

## Service Contract (`gcscore.v1.GcsCore`)

Instead of proxying `google.storage.v2.Storage` 1:1 over UDS, [`proto/gcscore.proto`](proto/gcscore.proto) provides a compact surface tailored for thin SDK veneers:

| RPC | Type | Description |
| :--- | :--- | :--- |
| `Ping` | Unary | Echoes `payload`; used for readiness checks (`READY <socket>`) and UDS latency microbenchmarks. |
| `ReadObject` | Server-streaming | Streams object contents in `--chunk-size` slices (`2 MiB` default) over `Transport::JSON` or `Transport::GRPC` (`BidiReadObject`). First `ReadChunk` carries `ObjectMeta`. |
| `OpenObject` / `ReadRange` / `CloseObject` | Unary + Server-streaming | Opens a persistent `BidiReadObject` `ObjectDescriptor` stored under a `(uid, pid)`-bound `uint64` handle in a lock-free `RwLock` table, enabling concurrent multi-range reads (`ReadRange`) over one upstream stream. |
| `ReadObjectShm` | Server-streaming | **Linux Zero-Copy Read:** Clients connect to `<socket>.shm` to receive a 16-byte little-endian header `[region_id: u64, region_size: u64]` + sealed `memfd` via `SCM_RIGHTS`. `ReadObjectShm` writes payload directly into the client's `memfd` and returns a `ShmChunk` `(offset, len, meta)` descriptor. |
| `WriteObject` | Client-streaming | First message carries `WriteHeader` (bucket, object, content-type, optional preconditions/known size, transport); subsequent messages stream `bytes data`. Uses `open_appendable_object` (`BidiWriteObject`) when built with `--cfg google_cloud_unstable_storage_bidi` (falling back to buffered `write_object` if a regional bucket returns `UNIMPLEMENTED`) and reports the actual transport used in `WriteObjectResponse.transport_used`. |
| `ListObjects` / `ListBuckets` | Unary | Single-page control-plane listing via `StorageControl`, matching thin-client page iterators. |
| `CreateSession` / `UpdateToken` / `CloseSession` | Unary | Per-caller OAuth2 bearer token lending bound to kernel-verified `(uid, pid)`. |
| `Stats` / `Shutdown` | Unary | Returns process CPU (`getrusage`) and RSS/VmHWM (`/proc/self/status`) telemetry, or triggers graceful shutdown. |

---

## Security & Multi-Process Credential Isolation

When multiple processes on a host share one `gcs-core-server` daemon:

1. **OS-Level Socket Protection (`0600` + `SO_PEERCRED`):**
   - `bind_uds_listener` sets `0600` permissions on both `<socket>` and `<socket>.shm` (and verifies `FileTypeExt::is_socket()` before unlinking stale sockets).
   - Both the Tonic gRPC interceptor and `<socket>.shm` inspect `SO_PEERCRED` on every connection and reject any caller whose effective UID does not match the server's UID (`PERMISSION_DENIED`).
2. **Per-Caller Token, Handle & SHM Isolation (`(uid, pid)` binding):**
   - Thin clients evaluate their native credential source in the host language (e.g. `oauth2.TokenSource`, Workload Identity, impersonation) and lend only the short-lived bearer token + expiry + optional `quota_project` via `CreateSession`.
   - Every `session_id` (`Sessions`), `OpenObject` `handle` (`OpenHandle`), and `ReadObjectShm` `region` (`Region`) is bound to the caller's `(uid, pid)` from `SO_PEERCRED`:
     - Cross-process reuse is rejected with `PERMISSION_DENIED`.
     - Expired tokens are rejected with `UNAUTHENTICATED`.
     - If a client process exits or crashes (`SIGKILL`) without calling `CloseSession` / `CloseObject`, the 1-second background reaper detects `kill(pid, 0) == -1 (ESRCH)` and evicts its sessions and `ObjectDescriptor` handles automatically.

---

## UDS & HTTP/2 Flow-Control Tuning

To prevent HTTP/2 window stalls and kernel socket buffer contention under high concurrency (e.g. 48–256 concurrent streams on `c4-standard-192` with 200 Gbps Tier-1 networking), `serve_with_listener` configures:

| Parameter | Value | Why |
| :--- | :--- | :--- |
| `SO_SNDBUF` / `SO_RCVBUF` | `8 MiB` | Avoids Linux `AF_UNIX` stream buffer backpressure (default is `212 KiB`). |
| `initial_stream_window_size` | `64 MiB` | Allows `64 MiB` in flight per stream without blocking on HTTP/2 `WINDOW_UPDATE`. |
| `initial_connection_window_size` | `256 MiB` | Sustains high aggregate throughput across multiplexed streams on a UDS connection. |
| `max_frame_size` | `4 MiB` | Reduces framing overhead for `2 MiB`–`4 MiB` streaming chunks. |
| `max_decoding/encoding_message_size` | `64 MiB` | Supports large streaming chunks. |

---

## Building, Running & Testing

```bash
# Build release binary (include --cfg google_cloud_unstable_storage_bidi to enable BidiWriteObject appendable uploads)
RUSTFLAGS="--cfg google_cloud_unstable_storage_bidi" \
  cargo build --release -p storage-grpc-uds --bin gcs-core-server

# Run server (prints "READY <socket>" on stdout once listening).
# --worker-threads caps the Tokio runtime (default: one worker per CPU).
./target/release/gcs-core-server \
  --socket /tmp/gcs-core.sock \
  --default-transport json \
  --grpc-subchannels 16 \
  --worker-threads 8 \
  --idle-secs 300

# Run unit & end-to-end integration tests
cargo test -p storage-grpc-uds
```
