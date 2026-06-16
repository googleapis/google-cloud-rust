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

/// Connection statement batch abstraction.
pub(crate) mod batch;
pub(crate) mod checksum;
/// Client-side command executor.
pub(crate) mod command;
pub(crate) mod commands;
/// Stateful orchestrator representing a database connection.
#[allow(clippy::module_inception)]
pub mod connection;
/// Connection properties definitions.
pub mod connectionproperties;
/// Connection state engine.
pub mod connectionstate;
/// Token-based SQL statement classifier and parser.
pub mod parser;
/// Shared Spanner client pooling cache.
pub(crate) mod pool;
pub(crate) mod statements;
/// Connection transaction run strategies.
pub(crate) mod transaction;
/// Core types and error definitions.
pub mod types;

pub use connection::{Connection, ExecutionResult};
pub use types::{ConnectionError, Dialect, SavepointSupport};
