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

use super::super::append_builder::AppendWithOffset;
use super::super::generated::gapic_storage::client::BigQueryWrite;
use super::super::runner::Runner;
use super::super::transport::Transport;
use crate::Result;
use crate::model::append_rows_request::ArrowData;
use crate::model::{
    AppendRowsRequest, ArrowRecordBatch, ArrowSchema, BatchCommitWriteStreamsResponse,
    FinalizeWriteStreamResponse,
};
use std::sync::Arc;

/// A writer for a pending stream.
#[derive(Debug)]
pub struct PendingWriter {
    runner: Runner,
    pub(crate) write_stream: String,
    pub(crate) schema: ArrowSchema,
    client: BigQueryWrite,
}

impl PendingWriter {
    pub(crate) fn new(inner: Arc<Transport>, write_stream: String, schema: ArrowSchema) -> Self {
        let runner = Runner::new(inner.clone());
        let client = BigQueryWrite::from_stub::<Transport>(inner);
        Self {
            runner,
            write_stream,
            schema,
            client,
        }
    }

    /// Appends rows to the pending stream.
    pub fn append(&self, rows: ArrowRecordBatch) -> AppendWithOffset {
        let req = AppendRowsRequest::new()
            .set_write_stream(&self.write_stream)
            .set_arrow_rows(
                ArrowData::new()
                    .set_writer_schema(self.schema.clone())
                    .set_rows(rows),
            );
        AppendWithOffset::new(self.runner.req_tx.clone(), req)
    }

    /// Finalizes the pending stream, preventing further writes.
    pub async fn finalize(&self) -> Result<FinalizeWriteStreamResponse> {
        self.client
            .finalize_write_stream()
            .set_name(&self.write_stream)
            .send()
            .await
    }

    /// Commits the pending stream to the table.
    pub async fn commit(&self) -> Result<BatchCommitWriteStreamsResponse> {
        // Extract the parent table path from the stream name:
        // "projects/p/datasets/d/tables/t/streams/s" -> "projects/p/datasets/d/tables/t"
        let parent = self
            .write_stream
            .split_once("/streams/")
            .map_or(self.write_stream.as_str(), |(p, _)| p)
            .to_string();

        self.client
            .batch_commit_write_streams()
            .set_parent(parent)
            .set_write_streams(vec![self.write_stream.clone()])
            .send()
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::runner::tests::*;
    use super::super::super::transport::tests::*;
    use super::*;
    use bigquery_grpc_mock::{MockBigQueryWrite, start};
    use gaxi::grpc::tonic::Response as TonicResponse;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn request_fields() -> anyhow::Result<()> {
        let transport = Arc::new(test_transport("http://ignored:1".to_string()).await?);
        let writer = PendingWriter::new(transport, write_stream(), schema());

        let b = writer.append(rows(1));
        assert_eq!(b.req.write_stream, write_stream());
        let data = b.req.arrow_rows().expect("arrow rows should be set");
        let s = data.writer_schema.as_ref().expect("schema should be set");
        assert_eq!(s.serialized_schema, "test");
        let r = data.rows.as_ref().expect("rows should be set");
        assert_eq!(r.serialized_record_batch, "1");

        Ok(())
    }

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

        let writer = PendingWriter::new(transport, write_stream(), schema());

        response_tx.send(Ok(convert(&test_response(1)))).await?;
        let resp = writer.append(rows(1)).send().await?;
        assert_eq!(resp.offset, Some(1));

        writer.finalize().await?;
        writer.commit().await?;

        Ok(())
    }

    fn write_stream() -> String {
        "projects/p/datasets/d/tables/t/streams/s".to_string()
    }

    fn schema() -> ArrowSchema {
        ArrowSchema::new().set_serialized_schema("test")
    }

    fn rows(id: i64) -> ArrowRecordBatch {
        ArrowRecordBatch::new().set_serialized_record_batch(id.to_string())
    }
}
