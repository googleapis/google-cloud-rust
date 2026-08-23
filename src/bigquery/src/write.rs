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

/// Types to write data in [Arrow] format
///
/// [arrow]: https://arrow.apache.org/
pub mod arrow;

// TODO(#6443) - relocate this.
/// The messages and enums that are part of this client library
pub mod model {
    pub(crate) use crate::write::generated::gapic_storage::model::*;
    pub use crate::write::generated::gapic_storage::model::{
        ArrowRecordBatch, ArrowSchema, BatchCommitWriteStreamsResponse,
        FinalizeWriteStreamResponse, FlushRowsResponse, RowError, StorageError, TableFieldSchema,
        TableSchema, row_error, storage_error, table_field_schema,
    };
}

pub use append_future::AppendFuture;

pub(super) mod append_builder;
pub(super) mod append_future;
pub(super) mod append_response;
pub(super) mod client;
pub(super) mod client_builder;
pub(super) mod error;
mod proto_schema;
mod runner;
mod stream;
mod transport;

// TODO(#4832) - remove handwritten code.
mod status;

#[allow(dead_code)]
pub(crate) mod generated;
