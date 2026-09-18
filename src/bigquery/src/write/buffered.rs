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
use crate::Result;
use super::format::DataFormat;
use crate::model::{FinalizeWriteStreamResponse, FlushRowsResponse};
use crate::write::builder::AppendWithOffset;
use crate::write::transport::Transport;
use std::sync::Arc;

/// A writer for a [buffered stream].
///
/// [buffered stream]: https://docs.cloud.google.com/bigquery/docs/write-api-grpc#buffered_type
#[derive(Debug)]
pub struct BufferedWriter<F> {
    pub(crate) inner: BaseWriter<F>,
}

impl<F> BufferedWriter<F>
where F: DataFormat {
    pub(crate) fn new(inner: Arc<Transport>, write_stream: String, format: F) -> Self {
        Self {
            inner: BaseWriter::new(inner, write_stream, format),
        }
    }

    /// Return the full resource name of the underlying write stream.
    pub fn write_stream(&self) -> &str {
        &self.inner.write_stream
    }

    /// Append rows to the buffered stream.
    pub fn append(&self, rows: F::Rows) -> AppendWithOffset {
        AppendWithOffset::new(
            self.inner.runner.req_tx.clone(),
            self.inner.append_request(rows),
        )
    }

    /// Flush the buffered stream, making rows up to the specified offset available for reading.
    pub async fn flush(&self, offset: i64) -> Result<FlushRowsResponse> {
        self.inner
            .client
            .flush_rows()
            .set_write_stream(&self.inner.write_stream)
            .set_offset(offset)
            .send()
            .await
    }

    /// Finalize the buffered stream, preventing further writes.
    pub async fn finalize(&self) -> Result<FinalizeWriteStreamResponse> {
        self.inner.finalize().await
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

        let writer = BufferedWriter::new(transport, write_stream(), format());
        assert_eq!(writer.write_stream(), write_stream());

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
        let writer = BufferedWriter::new(transport, write_stream(), format());
        assert_eq!(writer.write_stream(), write_stream());

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
}
