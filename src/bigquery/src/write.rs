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

pub use append_future::AppendFuture;

pub use buffered::BufferedWriter;
pub use committed::CommittedWriter;
pub use default::DefaultWriter;
pub use pending::PendingWriter;

pub use google_cloud_bigquery_derive::ToRow;
pub use to_row::ToRow;

/// Defines the data formats accepted by a writer.
pub mod format;

/// Defines the retry policy for the BigQuery Storage Write API.
pub mod retry_policy;

pub mod stream_type;

/// Helpers for the code that `#[derive(ToRow)]` generates.
///
/// This module is not part of the public API. Its contents may change, or be
/// removed, in any release.
#[doc(hidden)]
pub mod __private {
    pub use super::to_row::{ProtoValue, message_schema};
    pub use bytes::Bytes;
}

pub(super) mod append_future;
pub(super) mod append_response;
pub(super) mod builder;
pub(super) mod client;
pub(super) mod client_builder;
pub(super) mod error;
pub(super) mod writer_builder;

mod base;
mod buffered;
mod committed;
mod default;
mod dispatcher;
mod entry;
mod optimizer;
mod pending;
mod pool;
mod proto_schema;
mod runner;
mod stream;
mod to_row;
mod transport;
mod validate;
mod wire_format;

// TODO(#4832) - remove handwritten code.
mod status;

#[allow(dead_code)]
pub(crate) mod generated;

#[cfg(test)]
mod test;
