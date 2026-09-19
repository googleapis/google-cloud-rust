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

// TODO(#6855) - rename as `stream_type::Stream` and generalize.
pub use writer::Writer;

// TODO(#6855) - expose in `crate::builder::write` only.
pub use writer_builder::WriterBuilder;

/// Defines the data formats accepted by a writer.
pub mod format;

/// Defines the retry policy for the BigQuery Storage Write API.
pub mod retry_policy;

// TODO(#6855) - use markers, traits and expose this mod.
#[cfg_attr(not(test), expect(unused_imports))]
#[cfg_attr(not(test), expect(dead_code))]
mod stream_type;

pub(super) mod append_future;
pub(super) mod append_response;
pub(super) mod builder;
pub(super) mod client;
pub(super) mod client_builder;
pub(super) mod error;

mod base;
mod buffered;
mod committed;
mod default;
mod dispatcher;
mod entry;
mod pending;
mod pool;
mod proto_schema;
mod runner;
mod stream;
mod transport;
mod validate;
// TODO(#6855) - replace with the marker types in `stream_type`
mod writer;
mod writer_builder;

// TODO(#4832) - remove handwritten code.
mod status;

#[allow(dead_code)]
pub(crate) mod generated;

#[cfg(test)]
mod test;
