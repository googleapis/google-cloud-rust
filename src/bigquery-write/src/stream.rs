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

use crate::google::cloud::bigquery::storage::v1::{AppendRowsRequest, AppendRowsResponse};
use crate::transport::Transport;
use crate::{Error, Result};
use gaxi::grpc::tonic::Streaming;
use google_cloud_gax::options::RequestOptions;
use std::sync::Arc;
use tokio::sync::mpsc::{Sender, channel};

#[derive(Debug)]
pub(crate) struct Stream {
    pub(crate) stream: Streaming<AppendRowsResponse>,
    pub(crate) request_tx: Sender<AppendRowsRequest>,
}

impl Stream {
    /// Open a stream for the `AppendRows` RPC.
    pub(crate) async fn new(inner: Arc<Transport>, initial_req: AppendRowsRequest) -> Result<Self> {
        // TODO(#5743) - retry on transient errors
        open_stream(inner, initial_req).await
    }
}

/// One attempt to open a stream for the `AppendRows` RPC.
async fn open_stream(inner: Arc<Transport>, initial_req: AppendRowsRequest) -> Result<Stream> {
    let (request_tx, request_rx) = channel(100);
    let request_params = format!("write_stream={}", initial_req.write_stream);

    request_tx.send(initial_req).await.map_err(Error::io)?;

    let stream = inner
        .append_rows(&request_params, request_rx, RequestOptions::default())
        .await?
        .into_inner();

    Ok(Stream { stream, request_tx })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::google::cloud::bigquery::storage::v1::append_rows_response::{
        AppendResult, Response,
    };
    use crate::transport::tests::*;
    use bigquery_write_grpc_mock::{MockBigQueryWrite, start};
    use gaxi::grpc::tonic::{Response as TonicResponse, Status as TonicStatus};
    use google_cloud_gax::error::rpc::Code;

    #[tokio::test]
    async fn routing_header() -> anyhow::Result<()> {
        let mut mock = MockBigQueryWrite::new();
        mock.expect_append_rows().return_once(|request| {
            let metadata = request.metadata();
            assert_eq!(
                metadata
                    .get("x-goog-request-params")
                    .expect("routing header missing"),
                "write_stream=projects/p/datasets/d/tables/t/streams/s"
            );
            Err(TonicStatus::failed_precondition("fail"))
        });
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = Arc::new(test_transport(endpoint).await?);
        let initial = AppendRowsRequest {
            write_stream: "projects/p/datasets/d/tables/t/streams/s".to_string(),
            ..Default::default()
        };
        let _ = Stream::new(transport, initial).await;

        Ok(())
    }

    #[tokio::test]
    async fn basic_success() -> anyhow::Result<()> {
        let (response_tx, response_rx) = channel(1);
        let expected = AppendRowsResponse {
            response: Some(Response::AppendResult(AppendResult { offset: Some(1024) })),
            write_stream: "projects/p/datasets/d/tables/t/streams/s".to_string(),
            ..Default::default()
        };
        response_tx.send(Ok(convert(&expected))).await?;

        // We use this channel to surface writes (requests) from outside our
        // mock expectation.
        let (recover_writes_tx, mut recover_writes_rx) = channel(1);

        let mut mock = MockBigQueryWrite::new();
        mock.expect_append_rows().return_once(|request| {
            tokio::spawn(async move {
                let mut request_rx = request.into_inner();
                while let Some(request) = request_rx.recv().await {
                    recover_writes_tx
                        .send(request)
                        .await
                        .expect("forwarding writes always succeeds");
                }
            });
            Ok(TonicResponse::from(response_rx))
        });
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = Arc::new(test_transport(endpoint).await?);
        let initial = AppendRowsRequest {
            write_stream: "projects/p/datasets/d/tables/t/streams/s".to_string(),
            offset: Some(0),
            ..Default::default()
        };
        let Stream {
            mut stream,
            request_tx,
        } = Stream::new(transport, initial).await?;

        // Send a write.
        let write = AppendRowsRequest {
            write_stream: "projects/p/datasets/d/tables/t/streams/s".to_string(),
            offset: Some(1),
            ..Default::default()
        };
        request_tx.send(write).await?;

        // Read from the stream.
        assert_eq!(stream.message().await?, Some(expected));

        // Close the stream.
        drop(response_tx);
        assert_eq!(stream.message().await?, None);

        // Verify the initial request.
        let initial_req = recover_writes_rx
            .recv()
            .await
            .expect("should receive an initial request")?;
        assert_eq!(
            initial_req.write_stream,
            "projects/p/datasets/d/tables/t/streams/s"
        );
        assert_eq!(initial_req.offset, Some(0));

        // Verify the write.
        let write_req = recover_writes_rx
            .recv()
            .await
            .expect("should receive a write")?;
        assert_eq!(
            write_req.write_stream,
            "projects/p/datasets/d/tables/t/streams/s"
        );
        assert_eq!(write_req.offset, Some(1));

        Ok(())
    }

    #[tokio::test]
    async fn permanent_error_opening_stream() -> anyhow::Result<()> {
        let mut mock = MockBigQueryWrite::new();
        mock.expect_append_rows()
            .return_once(|_| Err(TonicStatus::failed_precondition("fail")));
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = Arc::new(test_transport(endpoint).await?);
        let initial = AppendRowsRequest {
            write_stream: "projects/p/datasets/d/tables/t/streams/s".to_string(),
            ..Default::default()
        };
        let err = Stream::new(transport, initial)
            .await
            .expect_err("open_stream should fail");
        let Some(status) = err.status() else {
            anyhow::bail!("expected a status, got: {err:?}");
        };
        assert_eq!(status.code, Code::FailedPrecondition);
        assert_eq!(status.message, "fail");

        Ok(())
    }
}
