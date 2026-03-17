# Changelog — google-cloud-rust (s3dlio fork)

All notable changes to this fork are documented in this file.
This fork is maintained at <https://github.com/russfellows/google-cloud-rust>.

---

## [rf-gcsrapid-put-truncation-fix] — 2026-03-17

Branch: `rf-gcsrapid-put-truncation-fix`  
Base: `release-20260212-rf-gcsrapid-20260314.1` (merged PR #1)

### Bug Fixes

- **fix(storage): BidiWriteObject PUT truncation on RAPID/zonal buckets**  
  Objects uploaded via `BidiWriteObject` to GCS RAPID (zonal) buckets were
  silently truncated (2–14 MiB instead of 16 MiB).  Two root causes:

  1. **Race condition**: The `finish_write=true` finalize message could reach the
     server before all HTTP/2 DATA frames were delivered, causing the server to
     finalize with whatever bytes it had received so far.

  2. **Spec-ack trap**: The server sends an initial `Resource { size: 0 }` as a
     "spec acknowledgment" for appendable writes.  The reader task treated this
     as a final `Resource`, short-circuiting the protocol and preventing the
     flush probe from ever seeing a real `PersistedSize`.

  **Fix** (`src/storage/src/storage/transport.rs`):
  - Two-phase flush probe: after sending all data chunks, send a
    `flush=true, state_lookup=true, finish_write=false` message and wait for
    `PersistedSize >= total_len` before sending `finish_write=true`.
  - Reader task skips the initial `Resource { size: 0 }` spec-ack on appendable
    writes (conditions: `is_appendable && !initial_resource_skipped && !seen_persisted_size`).
  - Keep-alive task prevents server-side idle timeout during the flush-wait phase.

  Validated on cluster `sig65-cntrlr-vm`, bucket `sig65-rapid1` — all 8/8 objects
  at exactly 16,777,216 bytes (PUT + stat + GET confirmed).

### Performance

- **perf(storage): zero-copy fast path in `send_grpc()` for single-chunk payloads**  
  When the caller provides the entire payload as a single `Bytes` buffer (the
  common case from s3dlio), skip the `BytesMut` allocation and `memcpy`.
  Eliminates 16 MiB allocation + 16 MiB copy per PUT.  
  File: `src/storage/src/storage/write_object.rs`

- **perf(gax): page-aligned tonic encode buffer for BidiWriteObject**  
  Added `SizedProstCodec` wrapper that provides a 2 MiB + 4 KiB initial encode
  buffer to `bidi_stream_with_status()`, replacing the default 8 KiB.  Eliminates
  ~8 reallocations on the first encoded message.  
  File: `src/gax-internal/src/grpc.rs`

  Combined effect: reduces per-PUT allocation from ~34 MiB to ~18 MiB and
  copies from ~32 MiB to ~16 MiB (~47% reduction).

### Documentation

- Added `doc/RAPID_PUT_TRUNCATION_FIX_HISTORY.md` — detailed history of all 6
  fix attempts with protocol traces, failure analysis, and root cause findings.
- Added `doc/RAPID_BUFFER_ALLOCATION_ANALYSIS.md` — end-to-end buffer allocation
  chain analysis (6 layers) with optimization recommendations.
- Added `doc/Changelog.md` (this file).

---

## [release-20260212-rf-gcsrapid-20260314.1] — 2026-03-14

Tag: `release-20260212-rf-gcsrapid-20260314.1`  
Merged as PR #1.

### Features

- **feat(storage): GCS RAPID BidiWriteObject + HTTP/2 window tuning**  
  Initial support for GCS RAPID/zonal buckets via `BidiWriteObject` gRPC
  streaming.  Centralized runtime constants in `gcs_constants.rs`.  Added
  explicit reconnect progress telemetry.
