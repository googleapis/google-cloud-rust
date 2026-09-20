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

use super::base::BaseWriter;
use super::format::DataFormat;
use crate::Result;
use crate::model::{BatchCommitWriteStreamsResponse, FinalizeWriteStreamResponse};
use crate::write::builder::AppendWithOffset;
use crate::write::transport::Transport;
use std::sync::Arc;

/// A writer for a [pending stream].
///
/// [pending stream]: https://docs.cloud.google.com/bigquery/docs/write-api-grpc#pending_type
#[derive(Debug)]
pub struct PendingWriter<F> {
    pub(crate) inner: BaseWriter<F>,
}

impl<F> PendingWriter<F>
where
    F: DataFormat,
{
    pub(crate) fn new(inner: Arc<Transport>, write_stream: String, format: F) -> Self {
        Self {
            inner: BaseWriter::new(inner, write_stream, format),
        }
    }

    /// Returns the full resource name of the underlying write stream.
    pub fn write_stream(&self) -> &str {
        &self.inner.write_stream
    }

    /// Appends rows to the pending stream.
    pub fn append(&self, rows: F::Rows) -> AppendWithOffset {
        AppendWithOffset::new(
            self.inner.runner.req_tx.clone(),
            self.inner.append_request(rows),
        )
    }

    /// Finalizes the pending stream, preventing further writes.
    pub async fn finalize(&self) -> Result<FinalizeWriteStreamResponse> {
        self.inner.finalize().await
    }

    /// Commits the pending stream to the table.
    pub async fn commit(&self) -> Result<BatchCommitWriteStreamsResponse> {
        // Extract the parent table path from the stream name:
        // "projects/p/datasets/d/tables/t/streams/s" -> "projects/p/datasets/d/tables/t"
        let parent = self
            .inner
            .write_stream
            .split_once("/streams/")
            .map_or(self.inner.write_stream.as_str(), |(p, _)| p)
            .to_string();

        self.inner
            .client
            .batch_commit_write_streams()
            .set_parent(parent)
            .set_write_streams(vec![self.inner.write_stream.clone()])
            .send()
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::write::test::*;
    use bigquery_grpc_mock::{MockBigQueryWrite, start};
    use gaxi::grpc::tonic::Response as TonicResponse;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn basic_success() -> anyhow::Result<()> {
        let (response_tx, response_rx) = mpsc::channel(10);

        let mut mock = MockBigQueryWrite::new();
        mock.expect_append_rows()
            .return_once(|_| Ok(TonicResponse::from(response_rx)));

        mock.expect_finalize_write_stream()
            .return_once(|_| Ok(TonicResponse::new(
                bigquery_grpc_mock::google::cloud::bigquery::storage::v1::FinalizeWriteStreamResponse::default()
            )));

        mock.expect_batch_commit_write_streams()
            .return_once(|_| Ok(TonicResponse::new(
                bigquery_grpc_mock::google::cloud::bigquery::storage::v1::BatchCommitWriteStreamsResponse::default()
            )));

        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = Arc::new(test_transport(endpoint).await?);

        let writer = PendingWriter::new(transport, write_stream(), format());
        assert_eq!(writer.write_stream(), write_stream());

        response_tx.send(Ok(convert(&test_response(1)))).await?;
        let resp = writer.append(rows(1)).send().await?;
        assert_eq!(resp.offset, Some(1));

        writer.finalize().await?;
        writer.commit().await?;

        Ok(())
    }
}
