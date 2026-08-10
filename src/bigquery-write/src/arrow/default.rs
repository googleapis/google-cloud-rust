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

use crate::builder::write::Append;
use crate::model::append_rows_request::ArrowData;
use crate::model::{AppendRowsRequest, ArrowRecordBatch, ArrowSchema};
use crate::runner::Runner;
use crate::transport::Transport;
use std::sync::Arc;

/// A writer for the [default stream]
///
/// [default stream]: https://docs.cloud.google.com/bigquery/docs/write-api#default_stream
#[derive(Debug)]
pub struct DefaultWriter {
    // TODO(#5744) - support multiplexed connections
    runner: Runner,
    pub(crate) write_stream: String,
    pub(crate) schema: ArrowSchema,
}

impl DefaultWriter {
    pub(crate) fn new(inner: Arc<Transport>, write_stream: String, schema: ArrowSchema) -> Self {
        let runner = Runner::new(inner);
        Self {
            runner,
            write_stream,
            schema,
        }
    }

    /// Append rows to the stream.
    pub fn append(&self, rows: ArrowRecordBatch) -> Append {
        // TODO(#5744) - send optimization
        let req = AppendRowsRequest::new()
            .set_write_stream(&self.write_stream)
            .set_arrow_rows(
                ArrowData::new()
                    .set_writer_schema(self.schema.clone())
                    .set_rows(rows),
            );
        Append::new(self.runner.req_tx.clone(), req)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::AppendError;
    use crate::runner::tests::*;
    use crate::transport::tests::*;
    use bigquery_write_grpc_mock::{MockBigQueryWrite, start};
    use gaxi::grpc::tonic::Response as TonicResponse;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn request_fields() -> anyhow::Result<()> {
        let transport = Arc::new(test_transport("http://ignored:1".to_string()).await?);
        let writer = DefaultWriter::new(transport, write_stream(), schema());

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
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = Arc::new(test_transport(endpoint).await?);

        let writer = DefaultWriter::new(transport, write_stream(), schema());

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

        Ok(())
    }

    fn write_stream() -> String {
        "projects/p/datasets/d/tables/t/streams/_default".to_string()
    }

    fn schema() -> ArrowSchema {
        ArrowSchema::new().set_serialized_schema("test")
    }

    fn rows(id: i64) -> ArrowRecordBatch {
        ArrowRecordBatch::new().set_serialized_record_batch(id.to_string())
    }
}
