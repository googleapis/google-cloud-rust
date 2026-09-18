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

use super::builder::Append;
use super::dispatcher::Dispatcher;
use super::format::DataFormat;
use super::pool::StreamPool;
use super::retry_policy::RetryOptions;
use std::sync::Arc;

/// A writer for the [default stream].
///
/// [default stream]: https://docs.cloud.google.com/bigquery/docs/write-api#default_stream
#[derive(Debug)]
pub struct DefaultWriter<F> {
    pub(crate) inner: Arc<Dispatcher>,
    pub(crate) write_stream: String,
    pub(crate) format: F,
}

impl<F> DefaultWriter<F>
where
    F: DataFormat,
{
    pub(crate) fn new(
        pool: Arc<StreamPool>,
        retry_options: RetryOptions,
        write_stream: String,
        format: F,
    ) -> Self {
        let inner = Arc::new(Dispatcher::new(pool, retry_options));
        Self {
            inner,
            write_stream,
            format,
        }
    }

    /// Append rows to the stream.
    pub fn append(&self, rows: F::Rows) -> Append {
        let req = self.format.make_request(&self.write_stream, rows);
        Append::new(self.inner.clone(), req)
    }
}

#[cfg(test)]
mod tests {
    use super::super::format::Arrow;
    use super::super::pool::StreamPoolOptions;
    use super::*;
    use crate::error::AppendError;
    use crate::model::ArrowRecordBatch;
    use crate::write::test::*;
    use bigquery_grpc_mock::{MockBigQueryWrite, start};
    use gaxi::grpc::tonic::{Response as TonicResponse, Status as TonicStatus};
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn basic_success() -> anyhow::Result<()> {
        let (response_tx, response_rx) = mpsc::channel(10);

        let mut mock = MockBigQueryWrite::new();
        mock.expect_append_rows()
            .return_once(|_| Ok(TonicResponse::from(response_rx)));
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = Arc::new(test_transport(endpoint).await?);
        let pool = Arc::new(StreamPool::new(transport, StreamPoolOptions::default()));

        let format = Arrow { schema: schema() };
        let writer = DefaultWriter::new(pool, test_retry_options(), write_stream(), format);

        response_tx.send(Ok(convert(&test_response(1)))).await?;
        let resp = writer.append(rows(1)).send().await?;
        assert_eq!(resp.offset, Some(1));

        response_tx.send(Ok(convert(&test_response(2)))).await?;
        let resp = writer.append(rows(2)).send().await?;
        assert_eq!(resp.offset, Some(2));

        response_tx.send(Ok(convert(&test_response(3)))).await?;
        let resp = writer.append(rows(3)).send().await?;
        assert_eq!(resp.offset, Some(3));

        response_tx
            .send(Err(TonicStatus::failed_precondition("fail")))
            .await?;
        let err = writer.append(rows(4)).send().await.expect_err("fail");
        assert!(matches!(err, AppendError::Rpc { source: _ }), "{err:?}");

        Ok(())
    }

    fn rows(id: i64) -> ArrowRecordBatch {
        ArrowRecordBatch::new().set_serialized_record_batch(id.to_string())
    }
}
