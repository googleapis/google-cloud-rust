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
    AppendRowsRequest, ArrowRecordBatch, ArrowSchema, FinalizeWriteStreamResponse,
    FlushRowsResponse,
};
use std::sync::Arc;

/// A writer for a [buffered stream].
///
/// [buffered stream]: https://docs.cloud.google.com/bigquery/docs/write-api-grpc#buffered_type
#[derive(Debug)]
pub struct BufferedWriter {
    runner: Runner,
    pub(crate) write_stream: String,
    pub(crate) schema: ArrowSchema,
    client: BigQueryWrite,
}

impl BufferedWriter {
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

    /// Append rows to the buffered stream.
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

    /// Flush the buffered stream, making rows up to the specified offset available for reading.
    pub async fn flush(&self, offset: i64) -> Result<FlushRowsResponse> {
        self.client
            .flush_rows()
            .set_write_stream(&self.write_stream)
            .set_offset(offset)
            .send()
            .await
    }

    /// Finalize the buffered stream, preventing further writes.
    pub async fn finalize(&self) -> Result<FinalizeWriteStreamResponse> {
        self.client
            .finalize_write_stream()
            .set_name(&self.write_stream)
            .send()
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::runner::tests::*;
    use super::super::super::transport::tests::*;
    use super::*;
    use crate::error::AppendError;
    use bigquery_grpc_mock::{MockBigQueryWrite, start};
    use gaxi::grpc::tonic::Response as TonicResponse;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn request_fields() -> anyhow::Result<()> {
        let transport = Arc::new(test_transport("http://ignored:1".to_string()).await?);
        let writer = BufferedWriter::new(transport, write_stream(), schema());

        let b = writer.append(rows(1));
        assert_eq!(b.req.write_stream, write_stream());
        let data = b.req.arrow_rows().expect("arrow rows should be set");
        let s = data.writer_schema.as_ref().expect("schema should be set");
        assert_eq!(s.serialized_schema, "test");
        let r = data.rows.as_ref().expect("rows should be set");
        assert_eq!(r.serialized_record_batch, "1");

        let b = writer.append(rows(2));
        assert_eq!(b.req.write_stream, write_stream());
        let data = b.req.arrow_rows().expect("arrow rows should be set");
        let s = data.writer_schema.as_ref().expect("schema should be set");
        assert_eq!(s.serialized_schema, "test");
        let r = data.rows.as_ref().expect("rows should be set");
        assert_eq!(r.serialized_record_batch, "2");

        Ok(())
    }

    #[tokio::test]
    async fn basic_success() -> anyhow::Result<()> {
        let (response_tx, response_rx) = mpsc::channel(10);

        let mut mock = MockBigQueryWrite::new();
        mock.expect_append_rows()
            .return_once(|_| Ok(TonicResponse::from(response_rx)));

        mock.expect_flush_rows()
            .return_once(|req| {
                assert_eq!(req.get_ref().offset, Some(3));
                assert_eq!(req.get_ref().write_stream, write_stream());
                Ok(TonicResponse::new(
                    bigquery_grpc_mock::google::cloud::bigquery::storage::v1::FlushRowsResponse::default()
                ))
            });

        mock.expect_finalize_write_stream()
            .return_once(|req| {
                assert_eq!(req.get_ref().name, write_stream());
                Ok(TonicResponse::new(
                    bigquery_grpc_mock::google::cloud::bigquery::storage::v1::FinalizeWriteStreamResponse::default()
                ))
            });

        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = Arc::new(test_transport(endpoint).await?);

        let writer = BufferedWriter::new(transport, write_stream(), schema());

        response_tx.send(Ok(convert(&test_response(1)))).await?;
        let resp = writer.append(rows(1)).send().await?;
        assert_eq!(resp.offset, Some(1));

        response_tx.send(Ok(convert(&test_response(2)))).await?;
        let resp = writer.append(rows(2)).send().await?;
        assert_eq!(resp.offset, Some(2));

        response_tx.send(Ok(convert(&test_response(3)))).await?;
        let resp = writer.append(rows(3)).send().await?;
        assert_eq!(resp.offset, Some(3));

        drop(response_tx);
        let err = writer.append(rows(4)).send().await.expect_err("channel");
        assert!(matches!(err, AppendError::UnexpectedEndOfStream));

        writer.flush(3).await?;
        writer.finalize().await?;

        Ok(())
    }

    #[tokio::test]
    async fn multiple_flushes() -> anyhow::Result<()> {
        let (response_tx, response_rx) = mpsc::channel(10);
        let mut mock = MockBigQueryWrite::new();
        mock.expect_append_rows()
            .return_once(|_| Ok(TonicResponse::from(response_rx)));

        mock.expect_flush_rows().times(2).returning(|req| {
            Ok(TonicResponse::new(
                bigquery_grpc_mock::google::cloud::bigquery::storage::v1::FlushRowsResponse {
                    offset: req.get_ref().offset.unwrap_or(0),
                },
            ))
        });

        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = Arc::new(test_transport(endpoint).await?);
        let writer = BufferedWriter::new(transport, write_stream(), schema());

        response_tx.send(Ok(convert(&test_response(1)))).await?;
        let _ = writer.append(rows(1)).send().await?;
        let flush1 = writer.flush(1).await?;
        assert_eq!(flush1.offset, 1);

        response_tx.send(Ok(convert(&test_response(2)))).await?;
        let _ = writer.append(rows(2)).send().await?;
        let flush2 = writer.flush(2).await?;
        assert_eq!(flush2.offset, 2);

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
