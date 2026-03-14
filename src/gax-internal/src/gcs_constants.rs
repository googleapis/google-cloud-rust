// vendor/google-cloud-gax-internal/src/gcs_constants.rs
//
// ── Single source of truth for all s3dlio-patched gRPC / GCS protocol constants ──
//
// Every numeric default and every environment-variable name lives here so that
// changing a value in one place propagates everywhere automatically.
//
// Consumers:
//   vendor/google-cloud-gax-internal/src/grpc.rs         – HTTP/2 window tuning
//   vendor/google-cloud-storage/src/storage/transport.rs – gRPC write chunk size
//   src/gcs_constants.rs                                 – re-exports for s3dlio application layer

// ── HTTP/2 flow-control window ────────────────────────────────────────────────

/// Default HTTP/2 initial connection and per-stream window size (MiB).
///
/// The HTTP/2 protocol default is 65,535 bytes (RFC 7540 §6.9.2), which caps a
/// single gRPC stream at roughly 64 MB/s on a 1 ms same-region GCS link.  A
/// 128 MiB window eliminates the protocol as a bottleneck for every GCP VM
/// configuration at or below 100 Gbps.
///
/// Set `S3DLIO_GRPC_INITIAL_WINDOW_MIB=0` to fall back to the protocol default.
pub const DEFAULT_WINDOW_MIB: u64 = 128;

/// Environment variable that overrides [`DEFAULT_WINDOW_MIB`] at runtime.
///
/// Value is interpreted as an integer number of MiB.  Set to `0` to use the
/// HTTP/2 protocol default (65 KB).
pub const ENV_GRPC_INITIAL_WINDOW_MIB: &str = "S3DLIO_GRPC_INITIAL_WINDOW_MIB";

// ── gRPC write chunk size ─────────────────────────────────────────────────────

/// Raw server-enforced ceiling for a single `BidiWriteObjectRequest` gRPC message.
///
/// The GCS server rejects any message whose serialized size exceeds this with:
/// ```text
/// RESOURCE_EXHAUSTED: SERVER: Received message larger than max (N vs. 4194304)
/// ```
/// This limit applies to the **entire serialized protobuf message** — including
/// field tags, varint-encoded lengths, `write_offset`, `ChecksummedData` wrapper,
/// per-chunk CRC32C, and (on the first message) `WriteObjectSpec`.  Observed
/// framing overhead is ~88–90 bytes for middle chunks.
///
/// **Do not use this as a data chunk size.**  Use [`MAX_GRPC_WRITE_CHUNK_SIZE`]
/// for the data payload limit; it already accounts for overhead.
pub const GCS_SERVER_MAX_MESSAGE_SIZE: usize = 4 * 1024 * 1024; // 4,194,304 bytes

/// Maximum safe data payload for a single `BidiWriteObjectRequest` message.
///
/// Derived from [`GCS_SERVER_MAX_MESSAGE_SIZE`] minus one 64 KiB guard band:
///
/// ```text
/// GCS_SERVER_MAX_MESSAGE_SIZE   = 4,194,304 bytes  (4 MiB)
/// MAX_GRPC_WRITE_CHUNK_SIZE     = 4,128,768 bytes  (63 × 64 KiB)
/// guard band                    =    65,536 bytes  (64 KiB)
/// observed overhead             =        ~89 bytes
/// ```
///
/// The 64 KiB guard band is >> the observed ~89-byte framing overhead, so
/// RESOURCE_EXHAUSTED is impossible even if overhead grows.  The value is
/// 64 KiB-aligned (63 × 65,536).
pub const MAX_GRPC_WRITE_CHUNK_SIZE: usize = GCS_SERVER_MAX_MESSAGE_SIZE - (64 * 1024); // 4,128,768 bytes

/// Default gRPC write chunk size in bytes (2 MiB = 32 × 64 KiB).
///
/// Conservative default that leaves ~2 MiB of headroom below the server limit.
/// For a 32 MiB object this produces 16 messages — a modest increase over the
/// theoretical 8 at the maximum chunk size, but protection against
/// RESOURCE_EXHAUSTED errors in all configurations.
///
/// Override at runtime via [`ENV_GRPC_WRITE_CHUNK_SIZE`] (bytes).  Values above
/// [`MAX_GRPC_WRITE_CHUNK_SIZE`] are silently clamped.  All values are 64 KiB-
/// aligned; non-aligned values will be rounded down to the nearest 64 KiB
/// boundary by the transport layer.
pub const DEFAULT_GRPC_WRITE_CHUNK_SIZE: usize = 2 * 1024 * 1024; // 2,097,152 bytes (32 × 64 KiB)

/// Environment variable that overrides [`DEFAULT_GRPC_WRITE_CHUNK_SIZE`] at runtime.
///
/// Value is interpreted as an integer number of bytes.  Values exceeding
/// [`MAX_GRPC_WRITE_CHUNK_SIZE`] (4,128,768) are silently clamped to the maximum.
/// Example: `export S3DLIO_GRPC_WRITE_CHUNK_SIZE=4128768` for 4 MiB - 64 KiB chunks.
pub const ENV_GRPC_WRITE_CHUNK_SIZE: &str = "S3DLIO_GRPC_WRITE_CHUNK_SIZE";
