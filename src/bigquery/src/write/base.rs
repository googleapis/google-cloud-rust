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

use super::format::DataFormat;
use crate::Result;
use crate::model::{AppendRowsRequest, FinalizeWriteStreamResponse};
use crate::write::generated::gapic_storage::client::BigQueryWrite;
use crate::write::runner::Runner;
use crate::write::transport::Transport;
use std::sync::Arc;

/// A shared internal structure for holding common state across different stream types,
/// providing shared implementations of operations core to most write streams.
///
/// Specific stream behaviors should be handled individually by their respective wrapper
/// structs (e.g. `BufferedWriter`, `CommittedWriter`, `PendingWriter`).
#[derive(Debug)]
pub(crate) struct BaseWriter<F> {
    pub(crate) runner: Runner,
    pub(crate) write_stream: String,
    pub(crate) format: F,
    pub(crate) client: BigQueryWrite,
}

impl<F> BaseWriter<F>
where
    F: DataFormat,
{
    pub(crate) fn new(inner: Arc<Transport>, write_stream: String, format: F) -> Self {
        let runner = Runner::new(inner.clone());
        let client = BigQueryWrite::from_stub::<Transport>(inner);
        Self {
            runner,
            write_stream,
            format,
            client,
        }
    }

    pub(crate) fn append_request(&self, rows: F::Rows) -> AppendRowsRequest {
        self.format.make_request(&self.write_stream, rows)
    }

    pub(crate) async fn finalize(&self) -> Result<FinalizeWriteStreamResponse> {
        self.client
            .finalize_write_stream()
            .set_name(&self.write_stream)
            .send()
            .await
    }
}
