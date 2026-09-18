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

// TODO(#6855) - delete this module
/// Types to write data in [Arrow] format.
///
/// [arrow]: https://arrow.apache.org/
pub mod arrow;
// TODO(#6855) - delete this module
#[allow(dead_code)]
pub(crate) mod proto;

pub use append_future::AppendFuture;

pub use default::DefaultWriter;

/// Defines the data formats accepted by a writer.
pub mod format;

/// Defines the retry policy for the BigQuery Storage Write API.
pub mod retry_policy;

pub(super) mod append_future;
pub(super) mod append_response;
pub(super) mod builder;
pub(super) mod client;
pub(super) mod client_builder;
pub(super) mod error;

mod default;
mod dispatcher;
mod entry;
mod pool;
mod proto_schema;
mod runner;
mod stream;
mod transport;
mod validate;

// TODO(#4832) - remove handwritten code.
mod status;

#[allow(dead_code)]
pub(crate) mod generated;

#[cfg(test)]
mod test;
