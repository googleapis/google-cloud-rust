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
use super::error::CommitError;
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
    pub async fn commit(
        &self,
    ) -> std::result::Result<BatchCommitWriteStreamsResponse, CommitError> {
        // Extract the parent table path from the stream name:
        // "projects/p/datasets/d/tables/t/streams/s" -> "projects/p/datasets/d/tables/t"
        let parent = self
            .inner
            .write_stream
            .split_once("/streams/")
            .map_or(self.inner.write_stream.as_str(), |(p, _)| p)
            .to_string();

        let resp = self
            .inner
            .client
            .batch_commit_write_streams()
            .set_parent(parent)
            .set_write_streams(vec![self.inner.write_stream.clone()])
            .send()
            .await?;

        if !resp.stream_errors.is_empty() || resp.commit_time.is_none() {
            return Err(CommitError::FailedTransaction {
                stream_errors: resp.stream_errors,
            });
        }

        Ok(resp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::storage_error::StorageErrorCode;
    use crate::write::test::*;
    use bigquery_grpc_mock::google::cloud::bigquery::storage::v1;
    use bigquery_grpc_mock::{MockBigQueryWrite, start};
    use gaxi::grpc::tonic::{Response as TonicResponse, Status as TonicStatus};
    use google_cloud_gax::error::rpc::Code;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn basic_success() -> anyhow::Result<()> {
        let (response_tx, response_rx) = mpsc::channel(10);

        let mut mock = MockBigQueryWrite::new();
        mock.expect_append_rows()
            .return_once(|_| Ok(TonicResponse::from(response_rx)));

        mock.expect_finalize_write_stream().return_once(|_| {
            Ok(TonicResponse::new(
                v1::FinalizeWriteStreamResponse::default(),
            ))
        });

        mock.expect_batch_commit_write_streams().return_once(|_| {
            Ok(TonicResponse::new(v1::BatchCommitWriteStreamsResponse {
                commit_time: Some(prost_types::Timestamp {
                    seconds: 1_700_000_000,
                    nanos: 0,
                }),
                stream_errors: Vec::new(),
            }))
        });

        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = Arc::new(test_transport(endpoint).await?);

        let writer = PendingWriter::new(transport, write_stream(), format());
        assert_eq!(writer.write_stream(), write_stream());

        response_tx.send(Ok(convert(&test_response(1)))).await?;
        let resp = writer.append(rows(1)).send().await?;
        assert_eq!(resp.offset, Some(1));

        writer.finalize().await?;
        let commit_resp = writer.commit().await?;
        assert!(commit_resp.commit_time.is_some());

        Ok(())
    }

    #[tokio::test]
    async fn commit_stream_errors() -> anyhow::Result<()> {
        let mut mock = MockBigQueryWrite::new();
        mock.expect_batch_commit_write_streams().return_once(|_| {
            Ok(TonicResponse::new(v1::BatchCommitWriteStreamsResponse {
                commit_time: None,
                stream_errors: vec![v1::StorageError {
                    code: v1::storage_error::StorageErrorCode::InvalidStreamState as i32,
                    entity: write_stream(),
                    error_message: "stream is not finalized".to_string(),
                }],
            }))
        });

        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = Arc::new(test_transport(endpoint).await?);
        let writer = PendingWriter::new(transport, write_stream(), format());

        let err = writer.commit().await.expect_err("commit should fail");
        let CommitError::FailedTransaction { stream_errors } = err else {
            anyhow::bail!("expected FailedTransaction, got: {err:?}");
        };
        assert_eq!(stream_errors.len(), 1);
        assert_eq!(stream_errors[0].code, StorageErrorCode::InvalidStreamState);
        assert_eq!(stream_errors[0].entity, write_stream());
        assert_eq!(stream_errors[0].error_message, "stream is not finalized");

        Ok(())
    }

    #[tokio::test]
    async fn commit_missing_commit_time() -> anyhow::Result<()> {
        let mut mock = MockBigQueryWrite::new();
        mock.expect_batch_commit_write_streams().return_once(|_| {
            Ok(TonicResponse::new(
                v1::BatchCommitWriteStreamsResponse::default(),
            ))
        });

        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = Arc::new(test_transport(endpoint).await?);
        let writer = PendingWriter::new(transport, write_stream(), format());

        let err = writer.commit().await.expect_err("commit should fail");
        let CommitError::FailedTransaction { stream_errors } = err else {
            anyhow::bail!("expected FailedTransaction, got: {err:?}");
        };
        assert!(stream_errors.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn commit_rpc_error() -> anyhow::Result<()> {
        let mut mock = MockBigQueryWrite::new();
        mock.expect_batch_commit_write_streams()
            .return_once(|_| Err(TonicStatus::failed_precondition("fail")));

        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = Arc::new(test_transport(endpoint).await?);
        let writer = PendingWriter::new(transport, write_stream(), format());

        let err = writer.commit().await.expect_err("commit should fail");
        let CommitError::Rpc { source } = err else {
            anyhow::bail!("expected Rpc error, got: {err:?}");
        };
        let status = source.status().expect("error should have status");
        assert_eq!(status.code, Code::FailedPrecondition);
        assert_eq!(status.message, "fail");

        Ok(())
    }
}
