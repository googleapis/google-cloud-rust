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

use super::runner::WriteRequest;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tokio::sync::mpsc;

/// An entry in the stream pool serviced by a `Runner`.
#[derive(Clone, Debug)]
pub(crate) struct StreamEntry {
    /// Unique identifier for this stream connection.
    pub(crate) id: u64,

    /// Channel to send requests to the stream's background runner task.
    pub(crate) req_tx: mpsc::UnboundedSender<WriteRequest>,

    /// The number of outstanding requests on this stream.
    pub(crate) outstanding_requests: Arc<AtomicU64>,

    /// The total outstanding bytes on this stream.
    pub(crate) outstanding_bytes: Arc<AtomicU64>,
}
