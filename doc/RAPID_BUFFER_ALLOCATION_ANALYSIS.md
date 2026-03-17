# GCS RAPID — Buffer Allocation & Serialization Analysis

**Created**: March 17, 2026  
**Context**: Investigation during PUT truncation debugging (see `RAPID_PUT_TRUNCATION_FIX_HISTORY.md`)

---

## Table of Contents

1. [Data Flow: 16 MiB PUT End-to-End](#data-flow-16-mib-put-end-to-end)
2. [Allocation Details Per Layer](#allocation-details-per-layer)
3. [The tonic Encode Buffer Problem](#the-tonic-encode-buffer-problem)
4. [Buffer Reuse: Not a Truncation Cause](#buffer-reuse-not-a-truncation-cause)
5. [Optimization Opportunities](#optimization-opportunities)
6. [Recommended Code Changes](#recommended-code-changes)

---

## Data Flow: 16 MiB PUT End-to-End

For a 16 MiB object uploaded to a RAPID bucket with `DEFAULT_GRPC_WRITE_CHUNK_SIZE` = 2 MiB:

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐     ┌──────────────────┐     ┌────────────────┐
│  s3dlio caller   │────▸│  send_grpc()     │────▸│  producer task  │────▸│  tonic encoder   │────▸│  HTTP/2 (h2)   │
│  Bytes (16 MiB)  │     │  BytesMut→Bytes  │     │  data.slice()   │     │  ProstCodec      │     │  16 KiB frames │
│  [Arc-backed]    │     │  COPY into flat   │     │  zero-copy view │     │  COPY into buf   │     │  ~1024 frames  │
└─────────────────┘     └──────────────────┘     └─────────────────┘     └──────────────────┘     └────────────────┘
                              ▲                         │                        ▲
                              │                    mpsc channel                  │
                         16 MiB alloc              (capacity 8)            2 MiB per msg
                         1 copy                    handle only              1 copy each
```

### Constants (source of truth: `gcs_constants.rs`)

| Constant | Value | Notes |
|----------|-------|-------|
| `DEFAULT_GRPC_WRITE_CHUNK_SIZE` | 2,097,152 (2 MiB) | Data payload per `BidiWriteObjectRequest` |
| `MAX_GRPC_WRITE_CHUNK_SIZE` | 4,128,768 (~3.94 MiB) | Server max minus 64 KiB guard band |
| `GCS_SERVER_MAX_MESSAGE_SIZE` | 4,194,304 (4 MiB) | Server rejects messages larger than this |
| Protobuf overhead per chunk | ~89 bytes | Field tags, varints, CRC32C, WriteOffset |
| First chunk overhead | ~200-500 bytes | Includes `WriteObjectSpec` |
| tonic encode initial buffer | 8,192 (8 KiB) | `DEFAULT_CODEC_BUFFER_SIZE` in tonic 0.14.5 |
| tonic yield threshold | 32,768 (32 KiB) | Messages > this are yielded immediately |
| HTTP/2 frame size | ~16,384 (16 KiB) | Standard HTTP/2 max frame size |
| mpsc channel capacity | 8 | `PRODUCER_CHANNEL_CAPACITY` in transport.rs |

### Chunk arithmetic

- 16 MiB object = 8 chunks × 2 MiB + 1 finalize (appendable)
- Each 2 MiB chunk → ~128 HTTP/2 frames
- Total: ~1024 HTTP/2 frames for 16 MiB of data
- Finalize message: ~30-50 bytes (no data payload)

---

## Allocation Details Per Layer

### Layer 1: Caller (`s3dlio` / `s3-cli`)

**File**: `s3dlio/src/s3_utils.rs` — `put_objects_with_random_data_and_type_with_progress()`

The caller generates a `Bytes` buffer (16 MiB) via `generate_object()`. This is `Arc`-backed — cloning it is O(1) (atomic refcount bump). The same `Bytes` is cloned for each URI when uploading to multiple destinations.

**Allocation**: 1 × 16 MiB  
**Copy**: None (generated in-place)

### Layer 2: `send_grpc()` — Payload Collection

**File**: `google-cloud-rust/src/storage/src/storage/write_object.rs` lines 1040-1062

```rust
let total_len: usize = chunks.iter().map(|c| c.len()).sum();
let mut buf = bytes::BytesMut::with_capacity(total_len);   // 16 MiB alloc
for chunk in chunks {
    buf.extend_from_slice(&chunk);                           // 16 MiB copy
}
let data = buf.freeze();                                     // zero-cost Bytes conversion
```

When called from `s3dlio` with a single `Bytes` payload, `chunks` has exactly one entry: the full 16 MiB. So this allocates a new 16 MiB `BytesMut` and copies the data into it, then freezes it into a new `Bytes`.

**Allocation**: 1 × 16 MiB (`BytesMut::with_capacity`)  
**Copy**: 1 × 16 MiB (`extend_from_slice`)  
**Optimization potential**: HIGH — when there's exactly one chunk equal to the total, skip the copy entirely (just use the Bytes directly)

### Layer 3: Producer Task — `data.slice()`

**File**: `google-cloud-rust/src/storage/src/storage/transport.rs` line 383

```rust
let chunk = data.slice(offset..end);  // zero-copy: new Bytes view, refcount bump
```

`Bytes::slice()` creates a new `Bytes` pointing into the same underlying allocation with an adjusted offset/length. Cost: one atomic increment. Each of the 8 chunk slices shares the 16 MiB allocation from Layer 2.

**Allocation**: None  
**Copy**: None  
**Note**: The `Bytes` handles (24 bytes each on stack) are moved into the `BidiWriteObjectRequest` struct and then into the mpsc channel. No data copies.

### Layer 4: mpsc Channel

Each `BidiWriteObjectRequest` is a protobuf struct containing `Bytes` handles (not inline data). Moving it through the channel is a shallow copy of the struct fields (~200 bytes). The mpsc channel has capacity 8, meaning all 8 data chunks for a 16 MiB object fit without blocking the producer.

**Allocation**: Pre-allocated channel buffer (8 × sizeof(BidiWriteObjectRequest))  
**Copy**: None (struct move only)

### Layer 5: tonic Protobuf Serialization (THE BIG COPY)

**Files**: tonic 0.14.5 `src/codec/encode.rs`, prost 0.14.3 `src/encoding.rs`

This is where the data is serialized from protobuf structs into a wire-format byte stream. The critical path:

1. tonic's `EncodedBytes` stream polls the mpsc `ReceiverStream` for each `BidiWriteObjectRequest`
2. For each message, it calls `prost::Message::encode()` which writes into an `EncodeBuf` (wrapping a `BytesMut`)
3. For the `content: bytes::Bytes` field in `ChecksummedData`, prost calls:
   ```rust
   // prost-0.14.3/src/encoding.rs line 664
   impl sealed::BytesAdapter for Bytes {
       fn append_to(&self, buf: &mut impl BufMut) {
           buf.put(self.clone())    // <— COPIES 2 MiB into BytesMut
       }
   }
   ```
4. `BytesMut::put()` does a `memcpy` of the `Bytes` data into the encode buffer

**The initial encode buffer is only 8 KiB** (`DEFAULT_CODEC_BUFFER_SIZE`). For a 2 MiB message, tonic must grow this buffer through multiple reallocations:

```
Message 1:  8 KiB → 16 KiB → 32 KiB → 64 KiB → 128 KiB → 256 KiB → 512 KiB → 1 MiB → 2 MiB+
           (8 reallocations + copies for the first message)

Message 2+: Buffer already at 2+ MiB from message 1 — no realloc needed
            (tonic reuses the BytesMut after split_to().freeze())
```

After encoding, tonic calls `buf.split_to(buf.len()).freeze()` which creates a new `Bytes` from the encoded data and resets the `BytesMut` position (but keeps the allocated capacity).

**Allocation**: 1 × ~2 MiB (amortized after first message; first message: ~8 intermediate allocations)  
**Copy**: 8 × 2 MiB = 16 MiB total (one full copy of all data through encode)  
**Optimization potential**: MEDIUM — custom codec with `BufferSettings::new(2 * 1024 * 1024 + 256, ...)` would eliminate reallocs

### Layer 6: HTTP/2 Framing (h2 crate)

The `h2` crate takes the encoded `Bytes` from tonic and frames them into HTTP/2 DATA frames (default max 16 KiB each). The h2 crate uses `Bytes::slice()` for framing — zero-copy. Flow control windows govern when frames can be sent.

**Allocation**: Minimal (frame headers only)  
**Copy**: None (h2 uses Bytes slicing)

---

## The tonic Encode Buffer Problem

### Current situation

tonic 0.14.5 uses `ProstCodec::default()` which uses `BufferSettings::default()`:

```rust
// tonic-0.14.5/src/codec/mod.rs
const DEFAULT_CODEC_BUFFER_SIZE: usize = 8 * 1024;       // 8 KiB initial
const DEFAULT_YIELD_THRESHOLD: usize = 32 * 1024;        // 32 KiB yield
```

For a 2 MiB data chunk + ~89 bytes overhead = ~2,097,241 bytes per encoded message.

The 8 KiB initial buffer is **absurdly small** for our use case. The first message triggers ~8 `BytesMut` reallocations, each involving an allocation + memcpy of the growing buffer.

### Why this doesn't cause truncation

The reallocation pattern is wasteful but correct — the data ends up fully serialized. The truncation is caused by the `finish_write=true` finalize message reaching the server before all HTTP/2 data frames are delivered (a synchronization issue, not a buffer issue).

### tonic's yield behavior

With `yield_threshold: 32768`, tonic checks after each message: is the buffer >= 32 KiB? For our 2 MiB messages: always yes. So each message is yielded immediately as a standalone `Bytes` to h2, which is the correct behavior (no message batching issues).

---

## Buffer Reuse: Not a Truncation Cause

**Can buffers be reused too soon?** No, and here's why:

1. **`Bytes` is Arc-backed**: `data.slice()` shares the same underlying allocation. The 16 MiB allocation lives until ALL slices are dropped.

2. **Ownership transfer**: Each `BidiWriteObjectRequest` takes ownership of its `Bytes` slice. Once tonic serializes it (copies into encode buffer), the slice is dropped, decrementing the refcount.

3. **Producer lifetime**: The `data` clone in the producer task keeps one refcount alive until the producer exits (after keep-alive completes).

4. **No buffer pool exists**: Each PUT creates entirely fresh allocations. There is no reuse mechanism that could cause premature recycling.

5. **tonic encode buffer lifecycle**: The encode `BytesMut` is owned by the `EncodedBytes` stream, which lives for the duration of the gRPC call. It cannot be shared or reused across calls.

---

## Optimization Opportunities

### Priority 1: Eliminate the `send_grpc()` copy (Layer 2)

Currently, `send_grpc()` always collects all payload chunks into a new `BytesMut`, even when the payload is a single `Bytes` buffer. For the common case (s3dlio passes a single `Bytes`), this 16 MiB copy is completely unnecessary.

**Impact**: Eliminates 16 MiB allocation + 16 MiB memcpy per PUT  
**Risk**: Low — purely an optimization, no behavioral change

### Priority 2: Custom tonic BufferSettings (Layer 5)

Override `ProstCodec` with pre-sized encode buffers matching our chunk size.

**Impact**: Eliminates ~8 reallocations on the first message  
**Risk**: Low — tonic supports custom buffer settings via `Codec` trait

### Priority 3: Buffer pool for repeated PUTs

For workloads uploading many same-sized objects (benchmarking, DLIO), a pool of pre-allocated `BytesMut` buffers could eliminate per-PUT allocation overhead.

**Impact**: Amortizes allocation cost across many PUTs  
**Risk**: Medium — requires careful lifetime management

---

## Recommended Code Changes

### Change 1: Skip copy in `send_grpc()` for single-chunk payloads

**File**: `google-cloud-rust/src/storage/src/storage/write_object.rs`  
**Function**: `send_grpc()`

```rust
// Current (always copies):
let total_len: usize = chunks.iter().map(|c| c.len()).sum();
let mut buf = bytes::BytesMut::with_capacity(total_len);
for chunk in chunks {
    buf.extend_from_slice(&chunk);
}
let data = buf.freeze();

// Proposed (skip copy for single chunk):
let data = if chunks.len() == 1 {
    chunks.into_iter().next().unwrap()  // zero-copy: use Bytes directly
} else {
    let total_len: usize = chunks.iter().map(|c| c.len()).sum();
    let mut buf = bytes::BytesMut::with_capacity(total_len);
    for chunk in chunks {
        buf.extend_from_slice(&chunk);
    }
    buf.freeze()
};
```

### Change 2: Custom ProstCodec with larger encode buffer

**File**: `google-cloud-rust/src/gax-internal/src/grpc.rs`  
**Function**: `bidi_stream_with_status()`

The current code uses `ProstCodec::default()` with 8 KiB initial buffer. For BidiWriteObject, we should provide a codec with a buffer pre-sized to the chunk size:

```rust
// Current:
let codec = tonic_prost::ProstCodec::<Request, Response>::default();

// Proposed: use a codec with buffer matching the write chunk size
// This requires either:
// (a) tonic_prost::ProstCodec supporting BufferSettings (check API), or
// (b) A wrapper codec that overrides buffer_settings()
```

**Note**: This requires checking whether `tonic_prost::ProstCodec` exposes a way to customize `BufferSettings`. If not, we'd need a thin wrapper implementing `tonic::codec::Codec` that delegates to `ProstCodec` but overrides `buffer_settings()`. This is a bit more involved but straightforward.

The ideal buffer size would be:
```
chunk_size + 1024 = 2,097,152 + 1024 = 2,098,176 bytes
```
(1024 bytes covers protobuf overhead + gRPC frame header for any message)

### Change 3 (Future): Avoid the tonic encode copy entirely

The deepest optimization would be to avoid the `Bytes → BytesMut` copy in prost encoding entirely. This would require either:

1. **A custom `Encoder`** that serializes the protobuf header + checksum fields into a small buffer, then chains the `Bytes` data payload using `h2`'s ability to send multiple `Bytes` per DATA frame — effectively zero-copy encoding.

2. **Upstream prost change**: If prost's `BytesAdapter::append_to` could use `BufMut::put_bytes_owned()` or similar to transfer `Bytes` ownership without copying.

This is complex and best deferred until after the truncation fix is confirmed.

### Non-changes

**Buffer pooling**: Not recommended at this time. The current per-PUT allocation pattern is simple and correct. Pooling adds complexity without fixing any bug. Revisit only if profiling shows allocation as a bottleneck in sustained throughput tests.

**Chunk size tuning**: The 2 MiB default is conservative but reasonable. Increasing to the maximum (~3.94 MiB) would reduce the number of gRPC messages per PUT by ~2x but increase per-message overhead. Not a priority while debugging truncation.

---

## Summary Table

| Layer | File | Alloc | Copy | Optimization |
|-------|------|-------|------|-------------|
| Caller | `s3dlio/src/s3_utils.rs` | 16 MiB | None | N/A |
| `send_grpc()` | `write_object.rs:1040` | 16 MiB | **16 MiB** | Skip for single chunk |
| `data.slice()` | `transport.rs:383` | None | None | Already optimal |
| mpsc channel | `transport.rs:356` | Minimal | None | Already optimal |
| tonic encode | tonic 0.14.5 `encode.rs` | 8 KiB→2 MiB | **2 MiB × 8** | Custom BufferSettings |
| HTTP/2 framing | h2 crate | Minimal | None | Already optimal |
| **Total per PUT** | | **~34 MiB** | **~32 MiB** | Reducible to ~16 MiB alloc, ~16 MiB copy |
