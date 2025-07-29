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

#[allow(dead_code)]
pub(crate) mod checksum;
pub(crate) mod client;
pub(crate) mod perform_upload;
pub(crate) mod read_object;
pub(crate) mod request_options;
pub(crate) mod upload_object;
pub mod upload_source;
pub(crate) mod v1;

use crate::model::Object;
use crate::upload_source::InsertPayload;
use crate::{Error, Result};

/// An unrecoverable problem in the upload protocol.
///
/// # Example
/// ```
/// # use google_cloud_storage::client::Storage;
/// # use google_cloud_storage::UploadError;
/// # async fn sample(client: &Storage) -> anyhow::Result<()> {
/// use std::error::Error as _;
/// let upload = client
///     .upload_object("projects/_/buckets/my-bucket", "my-object", "hello world")
///     .with_if_generation_not_match(0);
/// match upload.send().await {
///     Ok(object) => println!("Successfully uploaded the object"),
///     Err(error) if error.is_serialization() => {
///         println!("Some problem {error:?} sending the data to the service");
///         if let Some(m) = error.source().and_then(|e| e.downcast_ref::<UploadError>()) {
///             println!("{m}");
///         }
///     },
///     Err(e) => return Err(e.into()), // not handled in this example
/// }
/// # Ok(()) }
/// ```
///
/// # Troubleshoot
///
/// These errors indicate a bug in the resumable upload protocol implementation,
/// either in the service or the client library. Neither are expected to be
/// common, but neither are impossible. We recommend you [open a bug], there is
/// little you could do to recover from this problem.
///
/// While it is customary to `panic!()` when a bug triggers a problem, we do not
/// believe it is appropriate to do so in this case, as the invariants involve
/// different machines and the upload protocol.
///
/// [open a bug]: https://github.com/googleapis/google-cloud-rust/issues/new/choose
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum UploadError {
    #[error(
        "the service previously persisted {offset} bytes, but now reports only {persisted} as persisted"
    )]
    UnexpectedRewind { offset: u64, persisted: u64 },

    #[error("the service reports {persisted} bytes as persisted, but we only sent {sent} bytes")]
    TooMuchProgress { sent: u64, persisted: u64 },
}

/// The error type for checksum comparisons.
///
/// By default the client library computes a checksum of the uploaded data, and
/// compares this checksums against the value returned by the service.
///
/// # Example
/// ```
/// # use google_cloud_storage::client::Storage;
/// # use google_cloud_storage::ChecksumMismatch;
/// # async fn sample(client: &Storage) -> anyhow::Result<()> {
/// use std::error::Error as _;
/// let upload = client
///     .upload_object("projects/_/buckets/my-bucket", "my-object", "hello world")
///     .with_if_generation_not_match(0);
/// match upload.send().await {
///     Ok(object) => println!("Successfully uploaded the object"),
///     Err(error) if error.is_serialization() => {
///         println!("Some problem {error:?} sending the data to the service");
///         if let Some(m) = error.source().and_then(|e| e.downcast_ref::<ChecksumMismatch>()) {
///             println!("The checksums did not match: {m}");
///         }
///     },
///     Err(e) => return Err(e.into()), // not handled in this example
/// }
/// # Ok(()) }
/// ```
///  
/// # Troubleshooting
///
/// Data integrity problems are notoriously difficult to root cause. If you are
/// using pre-existing, or pre-computed checksum values, you may want to verify
/// the source data.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum ChecksumMismatch {
    #[error("mismatched CRC32C values {0}")]
    Crc32c(String),
    #[error("mismatched MD5 values: {0}")]
    MD5(String),
    #[error("mismatched CRC32C and MD5 values {0}")]
    Both(String),
}
