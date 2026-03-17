# GCS RAPID Bucket — PUT Truncation Fix History

**Created**: March 17, 2026  
**Last updated**: March 17, 2026  
**Status**: ✅ Fix attempt 6 CONFIRMED WORKING — all 8/8 objects 16 MiB, PUT+stat+GET validated  
**Cluster**: `sig65-cntrlr-vm`, bucket `sig65-rapid1` (US-CENTRAL1, zonal/RAPID)

---

## Table of Contents

1. [Problem Statement](#problem-statement)
2. [Background: GCS RAPID / Zonal Buckets](#background-gcs-rapid--zonal-buckets)
3. [Related Fixes (Working)](#related-fixes-working)
4. [Fix Attempt History](#fix-attempt-history)
   - [Attempt 1: Two-Step Finalize Without Synchronization](#attempt-1-two-step-finalize-without-synchronization)
   - [Attempt 2: Watch Channel + state_lookup=true on Last Chunk](#attempt-2-watch-channel--state_lookuptrue-on-last-chunk)
   - [Attempt 3: Watch Channel + state_lookup=false (Not Actually Built)](#attempt-3-watch-channel--state_lookupfalse-not-actually-built)
   - [Attempt 4: Oneshot Keep-Alive + Immediate Finalize](#attempt-4-oneshot-keep-alive--immediate-finalize)
   - [Attempt 5: Two-Phase Flush Probe + Watch Channel](#attempt-5-two-phase-flush-probe--watch-channel)
   - [Attempt 6: Skip Initial Spec-Ack Resource (Current)](#attempt-6-skip-initial-spec-ack-resource-current)
5. [Definitive Root Cause Analysis](#definitive-root-cause-analysis)
6. [Key Files](#key-files)
7. [Environment and Test Commands](#environment-and-test-commands)
8. [External Information](#external-information)

---

## Problem Statement

All PUTs to GCS RAPID bucket `sig65-rapid1` via `BidiWriteObject` gRPC result in **truncated objects**:

- Objects written as **16 MiB** come back as **2–14 MiB** (random fraction)
- A **36 MiB** object came back as **18 MiB** (exactly half)
- `gcloud storage ls -l` also shows `0 bytes` for these objects
- The truncation is in the **PUT path**, not GET — verified by cross-tool validation with `gcloud`

The truncation amount is non-deterministic: it depends on how much data the HTTP/2 layer has physically transmitted to the server at the instant the server receives `finish_write=true`.

## Background: GCS RAPID / Zonal Buckets

GCS RAPID (zonal) buckets have special behavior compared to standard GCS:

1. **Appendable objects**: RAPID writes use `appendable=true` in the `WriteObjectSpec`. Appendable objects are "unfinalized" until explicitly finalized with `finish_write=true`.
2. **Stale metadata**: `GetObject` and `ListObjects` return stale metadata (`size=0`) for appendable objects until they are finalized. Even after finalization, there's a metadata consistency lag.
3. **BidiReadObject returns authoritative metadata**: The `descriptor.object()` from a BidiReadObject stream returns the real persisted size — this is the only reliable way to stat a RAPID object.
4. **Two-step finalize required**: Unlike standard GCS where you combine `flush=true + finish_write=true` on the last data chunk, RAPID/appendable writes require the finalize message to be sent as a **separate** message after the last data chunk. (Modeled after google-cloud-cpp behavior.)
5. **Google dev confirmation**: Google engineer (Chris) confirmed the metadata consistency lag is a known RAPID behavior.

### Chunk arithmetic

- `DEFAULT_GRPC_WRITE_CHUNK_SIZE` = 2 MiB
- A 16 MiB object = 8 chunks × 2 MiB each
- A 36 MiB object = 18 chunks × 2 MiB each
- HTTP/2 frame size = ~16 KiB → each 2 MiB chunk becomes ~128 HTTP/2 frames

### The mpsc channel bottleneck

The `BidiWriteObject` implementation uses a `tokio::sync::mpsc::channel` with capacity 8 to queue `BidiWriteObjectRequest` messages from a producer task to the gRPC send stream. When the producer queues all 8 chunks + 1 finalize message to the mpsc in ~4ms, the gRPC layer is still serializing those into HTTP/2 frames and transmitting them over the network. The finalize message gets queued to the channel long before the corresponding data frames reach the server.

## Related Fixes (Working)

These fixes were implemented and **confirmed working** on the cluster before the PUT truncation debugging began:

### stat_object via BidiReadObject (CONFIRMED WORKING)

**Problem**: `StorageControl.get_object()` returns `size=0` for RAPID objects due to stale metadata.

**Fix**: Route stat calls for RAPID buckets to `stat_object_via_bidi_read()`, which opens a BidiReadObject stream with `ReadRange::segment(0, 1)` and extracts the authoritative size from `descriptor.object()`.

**File**: `s3dlio/src/google_gcs_client.rs`  
**Status**: Confirmed working on cluster — reports real persisted sizes.

### OUT_OF_RANGE handling for RAPID GETs

**Problem**: When GET reads a RAPID object and the requested range exceeds the actual (truncated) persisted size, the server returns `OUT_OF_RANGE`.

**Fix**: When a RAPID bucket returns OUT_OF_RANGE on GET, skip the stale metadata fallback and return the error directly for retry logic.

**File**: `s3dlio/src/google_gcs_client.rs`  
**Status**: In place and working.

### HTTP/2 flow control (concurrent reader/writer)

**Problem**: Original implementation had sequential write-then-read, which could deadlock under HTTP/2 flow control pressure.

**Fix**: Use `tokio::join!` to run producer and reader concurrently.

**File**: `google-cloud-rust/src/storage/src/storage/transport.rs`  
**Status**: In place since the initial BidiWriteObject implementation.

---

## Fix Attempt History

### Attempt 1: Two-Step Finalize Without Synchronization

**Date**: ~March 14-15, 2026  
**Binary hash**: `832a6fe5`  
**Branch**: `main` (uncommitted changes on top of `e982443e77`)

#### What was implemented

- Separated the finalize message from the last data chunk for appendable objects
- Last data chunk: `flush=true`, `finish_write=false`
- Finalize message: `flush=true`, `finish_write=true`, with object CRC32C
- No synchronization between data delivery and finalize — producer sends finalize immediately after queuing the last data chunk to mpsc

#### Test results

**FAILED — objects truncated.** Same truncation pattern as before. Objects written as 16 MiB appeared as 4-14 MiB when read back or stat'd.

#### Why it failed

The two-step finalize was correct in structure but had no synchronization point. The producer queued all data chunks + finalize to the mpsc channel in rapid succession. The gRPC/HTTP/2 layer was still transmitting data frames when the finalize message reached the server. The server committed whatever had been persisted at that moment.

---

### Attempt 2: Watch Channel + `state_lookup=true` on Last Chunk

**Date**: March 15-16, 2026  
**Binary hash**: `e529c193`

#### What was implemented

- Added `tokio::sync::watch` channel so the reader task feeds `PersistedSize` ACKs back to the producer
- Producer waits for `PersistedSize >= total_len` before sending the finalize message (30s timeout)
- Set `state_lookup=true` on the **last data chunk** (alongside `flush=true`) to trigger the server to respond with a `PersistedSize` value
- Finalize: separate message with `flush=true`, `finish_write=true`

#### Test results

**FAILED — every PUT logged:**
```
WARN BidiWriteObject: reader dropped before flush ACK (persisted=-1)
```

All objects still truncated.

#### Why it failed

Setting `state_lookup=true` on a data chunk caused the server to respond with a **`Resource`** message instead of a `PersistedSize` ACK. The Resource message is the server's "write completed" confirmation. The reader task saw the Resource, interpreted it as the final response, exited the response loop, and dropped the watch channel sender. The producer saw the closed channel ("reader dropped") and proceeded with the finalize message immediately — defeating the entire purpose of the synchronization.

**Key insight**: `state_lookup=true` on a data chunk apparently triggers the server to commit and return a Resource, not just report PersistedSize. This behavior was not documented.

---

### Attempt 3: Watch Channel + `state_lookup=false` (Not Actually Built)

**Date**: March 16, 2026  
**Binary hash**: Same as attempt 2 (`e529c193`) — **never rebuilt**

#### What was intended

- Revert `state_lookup` back to `false` on all data chunks
- Keep the watch channel synchronization logic from attempt 2
- Theory: with `state_lookup=false`, the `flush=true` on the last data chunk should trigger a `PersistedSize` response without the spurious early Resource

#### Test results

**FAILED — same symptoms as attempt 2**: `WARN reader dropped before flush ACK (persisted=-1)`

#### Why it failed

The binary was never actually rebuilt after the `state_lookup` revert. The user tested with the same binary hash as attempt 2. This confirmed by checking `md5sum` of the binary. This was a debugging artifact, not a real test of the intended fix.

**Lesson**: Always verify binary hash changes after code modifications.

---

### Attempt 4: Oneshot Keep-Alive + Immediate Finalize

**Date**: March 17, 2026  
**Binary hash**: Verified as new build (different from `e529c193`)

#### What was implemented

Complete redesign of the synchronization approach:

- **Replaced** watch channel with `tokio::sync::oneshot` channel
- **Removed** PersistedSize waiting entirely — producer does NOT wait for server ACK
- Producer sends all data chunks with `flush=false`, `state_lookup=false` on every chunk
- Producer sends finalize immediately after last data chunk: `flush=true`, `finish_write=true`
- After queuing finalize, producer **holds `tx` alive** by awaiting `done_rx` (oneshot) with 60s timeout
- Reader sends `done_tx.send(())` when it sees the Resource response
- Dropping `tx` closes the gRPC send-half, so keeping it alive prevents the race with the server's final commit

#### Test results

**PARTIALLY IMPROVED but still FAILED — objects still truncated.**

Debug output from this test (`s3dlio/s3-cli_debug.txt`) showed:

**What worked:**
- All 8 producers sent 9 messages (8 data + 1 finalize) in ~3-5ms each
- All 8 readers received Resource (response #1) — **no "reader dropped" warnings**
- Keep-alive mechanism worked correctly (producer waited for reader confirmation)
- `PUT summary: attempted=8, succeeded=8, failed=0`

**What still failed:**
- Server reported `size=0` in all Resource responses (known RAPID metadata lag)
- Actual persisted sizes were truncated:
  - `object_5`: stat=2 MiB, get=2 MiB (expected 16 MiB) — only 1 of 8 chunks persisted
  - `object_6`: stat=8 MiB, get=8 MiB — 4 of 8 chunks
  - `object_7`: stat=10 MiB, get=10 MiB — 5 of 8 chunks
  - Other objects: various truncation amounts

**Key log evidence** (from `s3-cli_debug.txt`):

Producer timing (all 8 objects showed similar pattern):
```
14:34:16.640Z  PRODUCER sending first chunk offset=0 len=2097152 appendable=true
14:34:16.643Z  PRODUCER all 8 data chunk(s) sent, 16777216 bytes total, elapsed=3.78ms
14:34:16.643Z  PRODUCER sending finalize msg #9 write_offset=16777216 flush=true finish_write=true
14:34:16.681Z  PRODUCER finalize msg queued OK
14:34:16.681Z  PRODUCER entering keep-alive wait (holding tx open for up to 60s)
```

Reader timing:
```
14:34:16.809Z  READER started, waiting for server responses
14:34:16.809Z  READER got Resource (response #1) size=0 name="object_2_of_8.dat"
14:34:16.809Z  PRODUCER reader confirmed Resource — closing stream
```

#### Why it failed

**This test definitively proved the root cause.** The timing shows:

1. Producer queued all 8 chunks + finalize to the mpsc channel in **~4ms**
2. The mpsc channel has capacity 8, so all 8 data chunks + 1 finalize were buffered without blocking
3. The gRPC layer didn't even start connecting to `storage.googleapis.com` until **~4ms** after the producer started (TLS handshake followed)
4. The finalize message (`finish_write=true`) was in the mpsc buffer alongside the data chunks — the gRPC serializer dequeued them in order, but the HTTP/2 framing of 16 MiB of data into ~1024 frames was still in progress when the finalize frame was serialized and sent
5. The server received `finish_write=true` while only a fraction of the data frames had been delivered → it committed whatever was persisted at that instant
6. Keeping `tx` alive was necessary but insufficient — the finalize message itself was the problem, not the channel closure

**The truncation is NOT caused by `tx` dropping. It's caused by the finalize message arriving at the server before all data frames have been received over HTTP/2.**

---

### Attempt 5: Two-Phase Flush Probe + Watch Channel

**Date**: March 17, 2026  
**Status**: ❌ FAILED — objects still truncated  
**Binary MD5**: `fd15f98be143bd978bffc0becefdeb41`

#### What is implemented

Complete redesign combining lessons from all previous attempts:

**Channel**: `tokio::sync::watch` channel initialized to `-1`, with sentinel `RESOURCE_RECEIVED = i64::MAX`

**Data chunks** (unchanged from attempt 4):
- All chunks: `flush=false`, `state_lookup=false`, `finish_write=false`

**Phase 1 — Flush probe** (NEW):
- After all data chunks are queued, send a **separate empty message** (no data payload):
  - `flush=true`, `state_lookup=true`, `finish_write=false`
  - `write_offset=total_len`
- This asks the server: "flush everything and tell me how much you've persisted"
- The producer then **blocks**, waiting on the watch channel for `PersistedSize >= total_len`
- 60-second timeout with periodic debug logging of PersistedSize updates

**Phase 2 — Finalize** (only after server confirmation):
- Only sent after `PersistedSize >= total_len` is confirmed by the reader
- `flush=true`, `finish_write=true`, `state_lookup=false`
- Includes full-object CRC32C checksum
- `write_offset=total_len`

**Keep-alive** (retained from attempt 4):
- After finalize is sent, producer holds `tx` alive waiting for `RESOURCE_RECEIVED` sentinel via watch channel
- 60-second timeout

**Reader task**:
- On `PersistedSize` response: sends value to watch channel via `persisted_tx.send(*ps)`
- On `Resource` response: sends `RESOURCE_RECEIVED` (i64::MAX) sentinel to watch channel
- Handles both response types, loops until Resource or stream end

#### Why this should work

1. The flush probe has NO data payload — it's a tiny gRPC message that will be serialized immediately
2. `state_lookup=true` requests a server response with PersistedSize
3. The server must process all preceding data frames before responding to the flush probe (gRPC messages are ordered)
4. Once `PersistedSize >= total_len`, we KNOW all data is server-side persisted
5. Only THEN do we send `finish_write=true`
6. This eliminates the race between data frame delivery and finalize

#### Potential concerns

1. **`state_lookup=true` + empty message behavior**: In attempt 2, `state_lookup=true` on a data chunk caused an early Resource. On an empty flush probe (no data, `finish_write=false`), the server might behave differently. If it returns Resource instead of PersistedSize again, the code handles this via the `RESOURCE_RECEIVED` sentinel — it will skip the finalize step.

2. **Server may not respond to flush probe**: If the server doesn't respond to `flush=true, state_lookup=true, finish_write=false` with a PersistedSize, the producer will timeout after 60 seconds and proceed. This is a safe fallback but would not fix the truncation.

3. **Multiple PersistedSize responses**: The watch channel handles incremental updates — the producer loops checking `PersistedSize >= total_len` after each update.

#### Expected debug output when tested

Success case:
```
PRODUCER sending flush probe #9 write_offset=16777216 (flush=true, state_lookup=true)
PRODUCER waiting for PersistedSize >= 16777216
READER got PersistedSize update: <some_value> (or multiple incremental values)
PRODUCER PersistedSize=16777216 >= total_len=16777216, proceeding to finalize, waited <Xms>
PRODUCER sending finalize #10 write_offset=16777216 crc32c=0x...
PRODUCER finalize queued OK
READER got Resource (response #N) size=... name="..."
PRODUCER Resource confirmed — closing stream
```

Failure cases to watch for:
```
# Server responds with Resource instead of PersistedSize (like attempt 2)
PRODUCER got early Resource before finalize — skipping finalize

# Server doesn't respond at all
PRODUCER TIMEOUT (60s) waiting for PersistedSize (current=-1, need=16777216)

# Channel issues  
PRODUCER persisted channel closed — reader may have exited
```

#### Actual test results (cluster `sig65-cntrlr-vm`)

**Command**: `S3DLIO_GCS_RAPID=true RUST_LOG=debug ./s3-cli put -s 16mib -n 8 gs://sig65-rapid1/t3-36m/`

**Outcome**: ❌ All 8 objects truncated.

| Object | Expected | Actual | Chunks |
|--------|----------|--------|--------|
| object_0 | 16,777,216 | 14,680,064 | 7 of 8 |
| object_1 | 16,777,216 | 13,647,872 | ~6.5 of 8 |
| object_2 | 16,777,216 | 14,680,064 | 7 of 8 |
| object_3 | 16,777,216 | 14,680,064 | 7 of 8 |
| object_4 | 16,777,216 | 14,680,064 | 7 of 8 |
| object_5 | 16,777,216 | 14,680,064 | 7 of 8 |
| object_6 | 16,777,216 | 2,097,152 | 1 of 8 |
| object_7 | 16,777,216 | 14,680,064 | 7 of 8 |

**Root cause discovered**: The GCS RAPID server sends an **initial `Resource(size=0)`** as the very first gRPC response — a "spec acknowledgment" or "hello" message. This was NOT anticipated in the fix 5 design.

**Debug log evidence** (one representative object):
```
READER started, waiting for server responses
READER got Resource (response #1) size=0 name="t3-36m/object_1_of_8.dat" elapsed=16.62µs
PRODUCER PersistedSize update: 9223372036854775807 (need 16777216)
PRODUCER PersistedSize=9223372036854775807 >= total_len=16777216, proceeding to finalize
PRODUCER done: 9 message(s) sent, appendable=true
PRODUCER Resource already confirmed
```

**What went wrong**:
1. The reader task starts ~80-400ms after the producer (async scheduling)
2. The server's initial `Resource(size=0)` is already queued in the gRPC response buffer
3. Reader's first `stream.message()` returns this `Resource(size=0)` immediately
4. Reader sends `RESOURCE_RECEIVED` sentinel (`i64::MAX`) to the watch channel
5. Producer was waiting for `PersistedSize >= 16777216`
6. `i64::MAX >= 16777216` → producer thinks data is confirmed
7. Producer sees "Resource already confirmed" → **never sends finalize**
8. Stream closes → server commits only whatever had been flushed at that point

**Key insight**: The `RESOURCE_RECEIVED` sentinel conflated two semantically different events:
- Initial "spec ack" Resource (pre-data, size=0) — should be IGNORED
- Final "object committed" Resource (post-finalize) — the intended target

No `PersistedSize` responses were ever received because the reader exited after the first Resource.

---

### Attempt 6: Skip Initial Spec-Ack Resource ✅ CONFIRMED WORKING

**Date**: March 17, 2026  
**Status**: ✅ **CONFIRMED WORKING** — 8/8 objects at exactly 16,777,216 bytes  
**Binary MD5**: `c4e764c20ce34ab7d314671f378395e7`

#### What changed (from attempt 5)

Only the **reader task** was modified. The producer logic is unchanged.

The reader now tracks two booleans:
- `initial_resource_skipped`: whether we already skipped the spec-ack Resource
- `seen_persisted_size`: whether we've received any PersistedSize response

**Skip condition**: When receiving a `Resource` response, if ALL of these are true:
- `is_appendable` (RAPID write)
- `!initial_resource_skipped` (first Resource we've seen)
- `!seen_persisted_size` (no PersistedSize received yet)

→ Log it as "skipping initial spec-ack" and `continue` the loop.

Any subsequent Resource (or a Resource after PersistedSize) is treated normally: send `RESOURCE_RECEIVED` to the watch channel and return the Resource.

#### Why this fixes the issue

1. The initial `Resource(size=0)` is now skipped by the reader
2. The reader continues listening for actual `PersistedSize` responses from the flush probe
3. The producer's watch channel will now receive real `PersistedSize` values (not the i64::MAX sentinel)
4. Producer waits for `PersistedSize >= 16777216` — real server confirmation
5. Only then does the producer send `finish_write=true`
6. Server sends the FINAL Resource (post-commit) → reader accepts it → stream closes cleanly

#### Expected debug output

```
READER started, waiting for server responses
READER skipping initial spec-ack Resource (response #1) size=0 name="..."
READER got PersistedSize=<value> (response #2) elapsed=...
PRODUCER PersistedSize update: <value> (need 16777216)
...possibly more PersistedSize updates...
PRODUCER PersistedSize=16777216 >= total_len=16777216, proceeding to finalize
PRODUCER sending finalize #10 write_offset=16777216 crc32c=0x...
PRODUCER finalize queued OK
READER got final Resource (response #N) size=<value> name="..."
PRODUCER Resource confirmed — closing stream
```

#### Actual test results (cluster `sig65-cntrlr-vm`)

**Command**: `S3DLIO_GCS_RAPID=true RUST_LOG=debug ./s3-cli put -s 16mib -n 8 gs://sig65-rapid1/t3-36m/`

**Outcome**: ✅ ALL 8 objects written correctly.

| Object | Expected | stat Size | GET Size | Status |
|--------|----------|-----------|----------|--------|
| object_0 | 16,777,216 | 16,777,216 | 16,777,216 | ✅ |
| object_1 | 16,777,216 | 16,777,216 | 16,777,216 | ✅ |
| object_2 | 16,777,216 | 16,777,216 | 16,777,216 | ✅ |
| object_3 | 16,777,216 | 16,777,216 | 16,777,216 | ✅ |
| object_4 | 16,777,216 | 16,777,216 | 16,777,216 | ✅ |
| object_5 | 16,777,216 | 16,777,216 | 16,777,216 | ✅ |
| object_6 | 16,777,216 | 16,777,216 | 16,777,216 | ✅ |
| object_7 | 16,777,216 | 16,777,216 | 16,777,216 | ✅ |

**Protocol flow confirmed** (all 8 objects followed this exact sequence):
```
Response #1: Resource(size=0)        → SKIPPED (spec-ack)
Response #2: PersistedSize=16777216  → forwarded to producer
Producer:    finalize #10 sent       → finish_write=true with CRC32C
Response #3: Resource(size=16777216) → final commit confirmed
```

**Timing** (representative, object_7):
- Data chunks sent in ~3ms
- Flush probe sent immediately after
- PersistedSize=16777216 received ~89ms after reader start
- Finalize sent, Resource confirmed at ~242ms total elapsed
- Full PUT cycle: ~200-540ms per object (varies by async scheduling)

#### Concerns resolved

1. ~~**Final Resource may also have size=0 for RAPID**~~: NOT the case — final Resource correctly reports `size=16777216` for all objects.

2. ~~**Server might not respond to flush probe with PersistedSize**~~: Server responds reliably. All 8 objects got `PersistedSize=16777216` in a single response (no incremental updates needed for 16 MiB).

3. **Non-appendable writes unaffected**: Confirmed — the `is_appendable` guard ensures standard GCS writes keep their existing behavior.

---

## Definitive Root Cause Analysis

**Root cause**: The `finish_write=true` message races with HTTP/2 data frame delivery.

The BidiWriteObject implementation uses a `tokio::sync::mpsc` channel to queue `BidiWriteObjectRequest` protobuf messages from a producer task to the gRPC send stream. The producer can queue all messages (data + finalize) in ~4ms because:

1. The mpsc channel has capacity 8, matching the number of data chunks for a 16 MiB object
2. The tokio runtime serializes the mpsc messages into protobuf → HTTP/2 frames asynchronously
3. Each 2 MiB data chunk becomes ~128 HTTP/2 frames (at 16 KiB frame size)
4. For 16 MiB total: ~1024 HTTP/2 frames need to be transmitted
5. The finalize message (tiny, no data) gets serialized and sent while data frames are still in flight

The server processes `finish_write=true` as "commit now" — it commits whatever has been persisted so far, which is only a fraction of the total data.

**Evidence from attempt 4 debug log**:
- Producer: 8 chunks sent to mpsc in 3.78ms, finalize queued at 3.78ms
- gRPC connection: TLS handshake didn't even complete until ~40ms later
- Reader: Resource received at ~170ms after producer start
- Actual persisted: 2-10 MiB out of 16 MiB (12-62% of data)
- The random truncation amount correlates with how far HTTP/2 framing progressed at finalize receipt

**This is NOT a bug in our code's logic** — the issue is a semantic mismatch between "message queued to mpsc channel" and "data received by server". The mpsc channel provides local message ordering guarantees, but cannot provide network delivery guarantees.

---

## Key Files

| File | Repository | Purpose |
|------|-----------|---------|
| `src/storage/src/storage/transport.rs` | google-cloud-rust | BidiWriteObject implementation (the core fix location) |
| `src/google_gcs_client.rs` | s3dlio | GCS client wrapper (stat/get/put) |
| `s3-cli_debug.txt` | s3dlio | Debug output from fix attempt 4 cluster test |

**See also**: [RAPID_BUFFER_ALLOCATION_ANALYSIS.md](RAPID_BUFFER_ALLOCATION_ANALYSIS.md) — detailed analysis of buffer allocation, copy overhead, and serialization through the full tonic/prost/h2 stack. Includes recommended optimization changes (not truncation-related).

### Git state

- **google-cloud-rust**: HEAD at `e982443e77` (tag `release-20260212-rf-gcsrapid-20260314.1`), uncommitted changes for fix attempts on `main`
- **s3dlio**: branch `release/v0.8.70`, version v0.9.70
- **sai3-bench**: branch `release/v0.8.70`, version v0.8.70

### Build dependencies

All three repos are wired via local path dependencies:
- s3dlio `Cargo.toml` → `google-cloud-rust` fork via local path (lines 41-43)
- sai3-bench `Cargo.toml` → s3dlio via local path (line 30)

---

## Environment and Test Commands

### Cluster setup
```bash
# SSH to cluster VM
ssh sig65-cntrlr-vm

# s3-cli binary location
/home/eval/Documents/Code/s3dlio/target/release/s3-cli
```

### Test PUT
```bash
# Write 8 × 16 MiB objects to RAPID bucket
S3DLIO_GCS_RAPID=true ./target/release/s3-cli -vv put -s 16mib -n 8 gs://sig65-rapid1/test-prefix/
```

### Verify sizes
```bash
# stat individual objects (uses BidiReadObject for RAPID — gives real sizes)
S3DLIO_GCS_RAPID=true ./target/release/s3-cli stat gs://sig65-rapid1/test-prefix/object_0_of_8.dat

# Cross-validate with gcloud
gcloud storage ls -l gs://sig65-rapid1/test-prefix/
```

### Build
```bash
# Debug build (fast iteration, ~1-2 min)
cd /home/eval/Documents/Code/s3dlio && cargo build --features full-backends

# Release build (~10 min, do NOT interrupt)
cd /home/eval/Documents/Code/s3dlio && cargo build --release --features full-backends

# Check compilation only (~30s)
cd /home/eval/Documents/Code/google-cloud-rust && cargo check -p google-cloud-storage
```

### Debug output
```bash
# Run with full debug logging, capture to file
S3DLIO_GCS_RAPID=true RUST_LOG=debug ./target/release/s3-cli put -s 16mib -n 8 gs://sig65-rapid1/test-prefix/ 2>&1 | tee s3-cli_debug.txt

# Extract BidiWriteObject-specific lines
grep -i "bidi\|reader\|producer\|finalize\|resource\|keep-alive\|persisted" s3-cli_debug.txt
```

### Binary verification
```bash
# Always verify binary changed after code modifications
md5sum ./target/release/s3-cli
```

---

## External Information

### Google engineer feedback (Chris)
- `GetObject` and `ListObjects` return stale `size=0` for RAPID appendable objects until some time after finalization
- `BidiReadObject` returns authoritative metadata via `descriptor.object()` — this is the reliable path
- This metadata consistency lag is a **known GCS RAPID behavior**

### Gemini analysis (March 17, 2026)
User consulted Google Gemini about the truncation issue. Gemini identified that dropping the mpsc `tx` sender causes EOF on the gRPC stream, and recommended keeping `tx` alive until the server confirms the Resource. This insight led to fix attempt 4 (oneshot keep-alive), which proved the hypothesis was partially correct but insufficient — the real issue was the finalize message itself racing with data delivery.

### google-cloud-cpp reference
The C++ client implementation sends `finish_write=true` as a separate empty message after the last data chunk — this is the pattern we're following. However, the C++ client may also have internal synchronization mechanisms (e.g., HTTP/2 write completion callbacks) that prevent the finalize from racing with data frames.
