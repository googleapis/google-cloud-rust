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

pub(crate) mod client;
pub(crate) mod read_object;
pub(crate) mod request_options;
pub(crate) mod upload_object;
pub mod upload_source;
pub(crate) mod v1;

use crate::model::Object;
use crate::upload_source::{InsertPayload, StreamingSource};
use crate::{Error, Result};

/// An unrecoverable problem in the upload protocol.
///
/// These errors indicate a bug in the resumable upload protocol implementation,
/// either in the service or the client library. Neither are expected to be
/// common, but neither are impossible. It seems safer to return an error rather
/// than panic, as the invariants involve different machines and the write
/// protocol.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
enum UploadError {
    #[error(
        "the service previously persisted {offset} bytes, but now reports only {persisted} as persisted"
    )]
    UnexpectedRewind { offset: u64, persisted: u64 },

    #[error("the service reports {persisted} bytes as persisted, but we only sent {sent} bytes")]
    TooMuchProgress { sent: u64, persisted: u64 },
}
