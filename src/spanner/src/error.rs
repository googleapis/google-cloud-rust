// Copyright 2026 Google LLC
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

/// An unexpected error that occurs when the client receives data from Spanner
/// that it cannot properly parse or handle. This typically indicates a bug in
/// the client library or the Spanner service itself, though other causes are possible.
///
/// # Troubleshooting
///
/// This indicates a bug in the client, the service, or a message corrupted
/// while in transit. Please [open an issue] with as much detail as possible.
///
/// [open an issue]: https://github.com/googleapis/google-cloud-rust/issues/new/choose
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SpannerInternalError {
    #[error("unexpected data received from Spanner: {0}")]
    UnexpectedData(String),
}

impl SpannerInternalError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self::UnexpectedData(message.into())
    }
}

pub(crate) fn internal_error(message: impl Into<String>) -> crate::Error {
    crate::Error::deser(SpannerInternalError::new(message))
}
