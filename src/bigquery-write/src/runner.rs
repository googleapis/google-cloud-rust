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

use crate::Result;
use crate::error::AppendResult;
use crate::google::cloud::bigquery::storage::v1::{AppendRowsRequest, AppendRowsResponse};
use crate::stream::Stream;
use crate::transport::Transport;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

#[derive(Debug)]
pub(crate) struct WriteRequest {
    pub(crate) req: AppendRowsRequest,
    pub(crate) resp_tx: oneshot::Sender<AppendResult<AppendRowsResponse>>,
}

/// A helper that runs the event loop for an `AppendRows` stream.
///
/// This type spawns a background task to manage the stream's lifecycle. It
/// listens for incoming requests on the `req_tx` channel, forwards them to the
/// stream, and correlates the returning responses back to the original request.
///
/// Because the service guarantees responses are returned in the exact order
/// they were received, the client can queue multiple requests concurrently
/// before receiving a response.
///
/// If the stream terminates for any reason, the background task exits. Any
/// unsatisfied requests are dropped, which surfaces to the client as a
/// `oneshot::error::RecvError` on their response channel.
pub(crate) struct Runner {
    pub(crate) req_tx: mpsc::Sender<WriteRequest>,
    pub(crate) handle: JoinHandle<()>,
}

impl Runner {
    pub(crate) fn new(inner: Arc<Transport>) -> Self {
        // TODO(#6122) - configure flow control settings
        let (req_tx, req_rx) = mpsc::channel(100);
        let handle = tokio::spawn(async move {
            run_stream_task(inner, req_rx).await;
        });
        Runner { req_tx, handle }
    }
}

async fn run_stream_task(inner: Arc<Transport>, mut req_rx: mpsc::Receiver<WriteRequest>) {
    // Wait for the first write before opening the stream. Tonic will not yield
    // us a stream until we have performed the first write.
    let Some(initial_req) = req_rx.recv().await else {
        return;
    };

    // A queue of responses we need to satisfy
    let mut resp_txs = VecDeque::new();
    resp_txs.push_back(initial_req.resp_tx);

    // Open the stream.
    let Stream { stream, request_tx } = match Stream::new(inner, initial_req.req).await {
        Ok(s) => s,
        Err(e) => {
            process_gax_response(&mut resp_txs, Err(e));
            return;
        }
    };
    // TODO(#5831) - implement stream event loop
    let _ = (stream, request_tx);
}

fn process_gax_response(
    resp_txs: &mut VecDeque<oneshot::Sender<AppendResult<AppendRowsResponse>>>,
    result: Result<AppendRowsResponse>,
) {
    // Pop the response channel associated with this response.
    let resp_tx = resp_txs
        .pop_front()
        .expect("the service sends one response per request");

    // Forward the result.
    let resp = result.map_err(|e| e.into());
    let _ = resp_tx.send(resp);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::AppendError;
    use crate::transport::tests::*;
    use bigquery_write_grpc_mock::{MockBigQueryWrite, start};
    use gaxi::grpc::tonic::{Response as TonicResponse, Status as TonicStatus};
    use google_cloud_gax::error::rpc::Code;

    #[tokio::test]
    async fn no_requests() -> anyhow::Result<()> {
        let (_, response_rx) = mpsc::channel(1);
        let mut mock = MockBigQueryWrite::new();
        mock.expect_append_rows()
            .return_once(|_| Ok(TonicResponse::from(response_rx)));
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = Arc::new(test_transport(endpoint).await?);

        let Runner { req_tx, handle } = Runner::new(transport);

        // Drop the request sender before making any requests.
        drop(req_tx);
        handle.await?;

        Ok(())
    }

    #[tokio::test]
    async fn success() -> anyhow::Result<()> {
        let (response_tx, response_rx) = mpsc::channel(10);
        let mut mock = MockBigQueryWrite::new();
        mock.expect_append_rows()
            .return_once(|_| Ok(TonicResponse::from(response_rx)));
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = Arc::new(test_transport(endpoint).await?);

        let Runner { req_tx, handle } = Runner::new(transport);

        // write 1
        let (resp_tx1, resp_rx1) = oneshot::channel();
        let write1 = WriteRequest {
            req: test_request(1),
            resp_tx: resp_tx1,
        };
        req_tx.send(write1).await?;

        // TODO(#5831) - write to and read from the stream.
        drop(response_tx);
        drop(req_tx);
        let _ = resp_rx1.await;
        handle.await?;

        Ok(())
    }

    #[tokio::test]
    async fn error_starting_stream() -> anyhow::Result<()> {
        let mut mock = MockBigQueryWrite::new();
        mock.expect_append_rows()
            .return_once(|_| Err(TonicStatus::failed_precondition("fail")));
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = Arc::new(test_transport(endpoint).await?);

        let Runner { req_tx, handle } = Runner::new(transport);

        let (resp_tx, resp_rx) = oneshot::channel();
        let write = WriteRequest {
            req: test_request(1),
            resp_tx,
        };
        req_tx.send(write).await?;

        let resp = resp_rx.await?;
        let Err(AppendError::Rpc { source: err }) = resp else {
            anyhow::bail!("expected an RPC error, got: {resp:?}");
        };
        let Some(status) = err.status() else {
            anyhow::bail!("expected a status, got: {err:?}");
        };
        assert_eq!(status.code, Code::FailedPrecondition);
        assert_eq!(status.message, "fail");

        drop(req_tx);
        handle.await?;

        Ok(())
    }

    fn test_request(index: i64) -> AppendRowsRequest {
        AppendRowsRequest {
            write_stream: "projects/p/datasets/d/tables/t/streams/s".to_string(),
            offset: Some(index),
            ..Default::default()
        }
    }
}
