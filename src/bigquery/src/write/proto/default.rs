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

use super::super::builder::Append;
use super::super::dispatcher::Dispatcher;
use super::super::pool::StreamPool;
use crate::model::append_rows_request::ProtoData;
use crate::model::{AppendRowsRequest, ProtoRows, ProtoSchema};
use std::sync::Arc;

/// A writer for the [default stream] using Protobuf as the data format.
///
/// [default stream]: https://docs.cloud.google.com/bigquery/docs/write-api#default_stream
#[derive(Debug)]
pub struct DefaultWriter {
    inner: Arc<Dispatcher>,
    pub(crate) write_stream: String,
    pub(crate) schema: ProtoSchema,
}

impl DefaultWriter {
    pub(crate) fn new(pool: Arc<StreamPool>, write_stream: String, schema: ProtoSchema) -> Self {
        let inner = Arc::new(Dispatcher::new(pool));
        Self {
            inner,
            write_stream,
            schema,
        }
    }

    /// Append rows to the stream.
    pub fn append(&self, rows: ProtoRows) -> Append {
        // TODO(#5744) - send optimization
        let req = AppendRowsRequest::new()
            .set_write_stream(&self.write_stream)
            .set_proto_rows(
                ProtoData::new()
                    .set_writer_schema(self.schema.clone())
                    .set_rows(rows),
            );
        Append::new(self.inner.clone(), req)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::AppendError;
    use crate::write::test::*;
    use bigquery_grpc_mock::{MockBigQueryWrite, start};
    use gaxi::grpc::tonic::Response as TonicResponse;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn request_fields() -> anyhow::Result<()> {
        let transport = Arc::new(test_transport("http://ignored:1").await?);
        let pool = Arc::new(StreamPool::new(transport, 1));
        let writer = DefaultWriter::new(pool, write_stream(), proto_schema());

        let b = writer.append(rows(1));
        assert_eq!(b.req.write_stream, write_stream());
        let data = b.req.proto_rows().expect("proto rows should be set");
        let s = data.writer_schema.as_ref().expect("schema should be set");
        assert_eq!(s.proto_descriptor.as_ref().unwrap().name, "TestMessage");
        let r = data.rows.as_ref().expect("rows should be set");
        assert_eq!(r.serialized_rows, vec![bytes::Bytes::from("1")]);

        let b = writer.append(rows(2));
        assert_eq!(b.req.write_stream, write_stream());
        let data = b.req.proto_rows().expect("proto rows should be set");
        let s = data.writer_schema.as_ref().expect("schema should be set");
        assert_eq!(s.proto_descriptor.as_ref().unwrap().name, "TestMessage");
        let r = data.rows.as_ref().expect("rows should be set");
        assert_eq!(r.serialized_rows, vec![bytes::Bytes::from("2")]);

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
        let pool = Arc::new(StreamPool::new(transport, 1));

        let writer = DefaultWriter::new(pool, write_stream(), proto_schema());

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

    fn rows(id: i64) -> ProtoRows {
        ProtoRows::new().set_serialized_rows(vec![bytes::Bytes::from(id.to_string())])
    }
}
