// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Custom errors for the Cloud Storage client.
//!
//! The storage client defines additional error types. These are often returned
//! as the `source()` of an [Error][crate::Error].

use crate::model::{Object, ObjectChecksums};

/// Indicates that a checksum mismatch was detected while reading or writing
/// Cloud Storage object.
///
/// When performing a full read of an object, the client library automatically
/// computes the CRC32C checksum (and optionally the MD5 hash) of the received
/// data. At the end of the read The client library automatically computes this
/// checksum to the values reported by the service. If the values do not match,
/// the read operation completes with an error and the error includes this type
/// showing which checksums did not match.
///
/// Likewise, when performing an object write, the client library automatically
/// compares the CRC32C checksum (and optionally the MD5 hash) of the data sent
/// to the service against the values reported by the service when the object is
/// finalized. If the values do not match, the write operation completes with an
/// error and the error includes this type.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum ChecksumMismatch {
    /// The CRC32C checksum sent by the service does not match the computed (or expected) value.
    Crc32c { got: u32, want: u32 },

    /// The MD5 hash sent by the service does not match the computed (or expected) value.
    Md5 {
        got: bytes::Bytes,
        want: bytes::Bytes,
    },

    /// The CRC32C checksum **and** the MD5 hash sent by the service do not
    /// match the computed (or expected) values.
    Both {
        got: Box<ObjectChecksums>,
        want: Box<ObjectChecksums>,
    },
}

impl std::fmt::Display for ChecksumMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Crc32c { got, want } => write!(
                f,
                "the CRC32C checksums do not match: got=0x{got:08x}, want=0x{want:08x}"
            ),
            Self::Md5 { got, want } => write!(
                f,
                "the MD5 hashes do not match: got={:0x?}, want={:0x?}",
                &got, &want
            ),
            Self::Both { got, want } => {
                write!(
                    f,
                    "both the CRC32C checksums and MD5 hashes do not match: got.crc32c=0x{:08x}, want.crc32c=0x{:08x}, got.md5={:x?}, want.md5={:x?}",
                    got.crc32c.unwrap_or_default(),
                    want.crc32c.unwrap_or_default(),
                    got.md5_hash,
                    want.md5_hash
                )
            }
        }
    }
}

/// Represents errors that can occur when converting to [KeyAes256] instances.
///
/// # Example:
/// ```
/// # use google_cloud_storage::{model::request_helpers::KeyAes256, error::KeyAes256Error};
/// let invalid_key_bytes: &[u8] = b"too_short_key"; // Less than 32 bytes
/// let result = KeyAes256::new(invalid_key_bytes);
///
/// assert!(matches!(result, Err(KeyAes256Error::InvalidLength)));
/// ```
///
/// [KeyAes256]: crate::model::request_helpers::KeyAes256
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum KeyAes256Error {
    /// The provided key's length was not exactly 32 bytes.
    #[error("Key has an invalid length: expected 32 bytes.")]
    InvalidLength,
}

/// Represents an error that can occur when invalid range is specified.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum RangeError {
    /// The provided read limit was negative.
    #[error("read limit was negative, expected non-negative value.")]
    NegativeLimit,
    /// A negative offset was provided with a read limit.
    #[error("negative read offsets cannot be used with read limits.")]
    NegativeOffsetWithLimit,
}

/// Represents an error that can occur when reading response data.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum ReadError {
    /// The calculated crc32c did not match server provided crc32c.
    #[error("checksum mismatch {0}")]
    ChecksumMismatch(ChecksumMismatch),

    /// The read was interrupted before all the expected bytes arrived.
    #[error("missing {0} bytes at the end of the stream")]
    ShortRead(u64),

    /// The read received more bytes than expected.
    #[error("too many bytes received: expected {expected}, stopped read at {got}")]
    LongRead { got: u64, expected: u64 },

    /// Only 200 and 206 status codes are expected in successful responses.
    #[error("unexpected success code {0} in read request, only 200 and 206 are expected")]
    UnexpectedSuccessCode(u16),

    /// Successful HTTP response must include some headers.
    #[error("the response is missing '{0}', a required header")]
    MissingHeader(&'static str),

    /// The received header format is invalid.
    #[error("the format for header '{0}' is incorrect")]
    BadHeaderFormat(
        &'static str,
        #[source] Box<dyn std::error::Error + Send + Sync + 'static>,
    ),
}

/// An unrecoverable problem in the upload protocol.
///
/// # Example
/// ```
/// # use google_cloud_storage::{client::Storage, error::WriteError};
/// # async fn sample(client: &Storage) -> anyhow::Result<()> {
/// use std::error::Error as _;
/// let writer = client
///     .write_object("projects/_/buckets/my-bucket", "my-object", "hello world")
///     .with_if_generation_not_match(0);
/// match writer.send_buffered().await {
///     Ok(object) => println!("Successfully created the object {object:?}"),
///     Err(error) if error.is_serialization() => {
///         println!("Some problem {error:?} sending the data to the service");
///         if let Some(m) = error.source().and_then(|e| e.downcast_ref::<WriteError>()) {
///             println!("{m}");
///         }
///     },
///     Err(e) => return Err(e.into()), // not handled in this example
/// }
/// # Ok(()) }
/// ```
///
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum WriteError {
    /// The service has "uncommitted" previously persisted bytes.
    ///
    /// # Troubleshoot
    ///
    /// In the resumable upload protocol the service reports how many bytes are
    /// persisted. This error indicates that the service previously reported
    /// more bytes as persisted than in the latest report. This could indicate:
    /// - a corrupted message from the service, either the earlier message
    ///   reporting more bytes persisted than actually are, or the current
    ///   message indicating fewer bytes persisted.
    /// - a bug in the service, where it reported bytes as persisted when they
    ///   were not.
    /// - a bug in the client, maybe storing the incorrect byte count, or
    ///   parsing the messages incorrectly.
    ///
    /// All of these conditions indicate a bug, and in Rust it is idiomatic to
    /// `panic!()` when a bug is detected. However, in this case it seems more
    /// appropriate to report the problem, as the client library cannot
    /// determine the location of the bug.
    #[error(
        "the service previously persisted {offset} bytes, but now reports only {persisted} as persisted"
    )]
    UnexpectedRewind { offset: u64, persisted: u64 },

    /// The service reports more bytes persisted than sent.
    ///
    /// # Troubleshoot
    ///
    /// Most likely this indicates that two concurrent uploads are using the
    /// same session. Review your application design to avoid concurrent
    /// uploads.
    ///
    /// It is possible that this indicates a bug in the service, client, or
    /// messages corrupted in transit.
    #[error("the service reports {persisted} bytes as persisted, but we only sent {sent} bytes")]
    TooMuchProgress { sent: u64, persisted: u64 },

    /// The checksums reported by the service do not match the expected checksums.
    ///
    /// # Troubleshoot
    ///
    /// The client library compares the CRC32C checksum and/or MD5 hash of the
    /// uploaded data against the hash reported by the service at the end of
    /// the upload. This error indicates the hashes did not match.
    ///
    /// If you provided known values for these checksums verify those values are
    /// correct.
    ///
    /// Otherwise, this is probably a data corruption problem. These are
    /// notoriously difficult to root cause. They probably indicate faulty
    /// equipment, such as the physical machine hosting your client, the network
    /// elements between your client and the service, or the physical machine
    /// hosting the service.
    ///
    /// If possible, resend the data from a different machine.
    #[error("checksum mismatch {mismatch} when uploading {} to {}", object.name, object.bucket)]
    ChecksumMismatch {
        mismatch: ChecksumMismatch,
        object: Box<Object>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mismatch_crc32c() {
        let value = ChecksumMismatch::Crc32c {
            got: 0x01020304_u32,
            want: 0x02030405_u32,
        };
        let fmt = value.to_string();
        assert!(fmt.contains("got=0x01020304"), "{value:?} => {fmt}");
        assert!(fmt.contains("want=0x02030405"), "{value:?} => {fmt}");
    }

    #[test]
    fn mismatch_md5() {
        let value = ChecksumMismatch::Md5 {
            got: bytes::Bytes::from_owner([0x01_u8, 0x02, 0x03, 0x04]),
            want: bytes::Bytes::from_owner([0x02_u8, 0x03, 0x04, 0x05]),
        };
        let fmt = value.to_string();
        assert!(
            fmt.contains(r#"got=b"\x01\x02\x03\x04""#),
            "{value:?} => {fmt}"
        );
        assert!(
            fmt.contains(r#"want=b"\x02\x03\x04\x05""#),
            "{value:?} => {fmt}"
        );
    }

    #[test]
    fn mismatch_both() {
        let got = ObjectChecksums::new()
            .set_crc32c(0x01020304_u32)
            .set_md5_hash(bytes::Bytes::from_owner([0x01_u8, 0x02, 0x03, 0x04]));
        let want = ObjectChecksums::new()
            .set_crc32c(0x02030405_u32)
            .set_md5_hash(bytes::Bytes::from_owner([0x02_u8, 0x03, 0x04, 0x05]));
        let value = ChecksumMismatch::Both {
            got: Box::new(got),
            want: Box::new(want),
        };
        let fmt = value.to_string();
        assert!(fmt.contains("got.crc32c=0x01020304"), "{value:?} => {fmt}");
        assert!(fmt.contains("want.crc32c=0x02030405"), "{value:?} => {fmt}");
        assert!(
            fmt.contains(r#"got.md5=b"\x01\x02\x03\x04""#),
            "{value:?} => {fmt}"
        );
        assert!(
            fmt.contains(r#"want.md5=b"\x02\x03\x04\x05""#),
            "{value:?} => {fmt}"
        );
    }
}
