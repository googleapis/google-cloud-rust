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

use super::error::{AppendError, AppendResult};
use super::optimizer::SendOptimizer;
use super::stream::Stream;
use super::transport::{Transport, info::VERSION};
use crate::Result;
use crate::google::cloud::bigquery::storage::v1::{AppendRowsRequest, AppendRowsResponse};
use gaxi::grpc::from_status::to_gax_error;
use gaxi::grpc::tonic::{Status as TonicStatus, Streaming};
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

type TonicResult<T> = std::result::Result<T, TonicStatus>;

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
#[derive(Debug)]
pub(crate) struct Runner {
    pub(crate) req_tx: mpsc::UnboundedSender<WriteRequest>,
    #[allow(dead_code)]
    pub(crate) handle: JoinHandle<()>,
}

impl Runner {
    pub(crate) fn new(inner: Arc<Transport>) -> Self {
        // TODO(#6122) - configure flow control settings
        let (req_tx, req_rx) = mpsc::unbounded_channel();
        let handle = tokio::spawn(async move {
            run_stream_task(inner, req_rx).await;
        });
        Runner { req_tx, handle }
    }
}

async fn run_stream_task(inner: Arc<Transport>, mut req_rx: mpsc::UnboundedReceiver<WriteRequest>) {
    // Wait for the first write before opening the stream. Tonic will not yield
    // us a stream until we have performed the first write.
    let Some(initial_req) = req_rx.recv().await else {
        return;
    };

    // Identify the client library to the server on the initial request.
    let mut req = initial_req.req;
    req.trace_id = format!("rust-writer:{VERSION}");

    // Initialize the send optimizer.
    let mut optimizer = SendOptimizer::new(&req);

    // A queue of responses we need to satisfy
    let mut resp_txs = VecDeque::new();
    resp_txs.push_back(initial_req.resp_tx);

    // Open the stream.
    let Stream {
        mut stream,
        request_tx,
    } = match Stream::new(inner, req).await {
        Ok(s) => s,
        Err(e) => {
            process_gax_response(&mut resp_txs, Err(e));
            return;
        }
    };

    loop {
        tokio::select! {
            req = req_rx.recv() => {
                match req {
                    Some(r) => {
                        let mut req = r.req;

                        // Drop redundant fields from the request.
                        optimizer.optimize(&mut req);

                        // Keep track of the response channel.
                        resp_txs.push_back(r.resp_tx);

                        // Forward the request to the stream.
                        let _ = request_tx.send(req).await;
                    }
                    None => {
                        drop(request_tx);
                        break drain_stream(stream, resp_txs).await;
                    }
                }
            }
            resp = stream.message() => {
                match resp.transpose() {
                    Some(r) => process_response(&mut resp_txs, r),
                    // Note that tonic yields `None` after an `Err(e)`.
                    None => break,
                }
            }
        }
    }
}

async fn drain_stream(
    mut stream: Streaming<AppendRowsResponse>,
    mut resp_txs: VecDeque<oneshot::Sender<AppendResult<AppendRowsResponse>>>,
) {
    while let Some(r) = stream.message().await.transpose() {
        process_response(&mut resp_txs, r);
    }
}

fn process_response(
    resp_txs: &mut VecDeque<oneshot::Sender<AppendResult<AppendRowsResponse>>>,
    resp: TonicResult<AppendRowsResponse>,
) {
    process_gax_response(resp_txs, resp.map_err(to_gax_error))
}

fn process_gax_response(
    resp_txs: &mut VecDeque<oneshot::Sender<AppendResult<AppendRowsResponse>>>,
    resp: Result<AppendRowsResponse>,
) {
    // Pop the response channel associated with this response.
    let Some(resp_tx) = resp_txs.pop_front() else {
        // Note that the server may close an idle stream that has no requests
        // queued up. If so, the runner task will terminate gracefully.
        return;
    };

    // Forward the result.
    let _ = resp_tx.send(resp.map_err(AppendError::from));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::write::test::*;
    use bigquery_grpc_mock::{MockBigQueryWrite, start};
    use gaxi::grpc::tonic::Response as TonicResponse;
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
        req_tx.send(write1)?;

        // write 2
        let (resp_tx2, resp_rx2) = oneshot::channel();
        let write2 = WriteRequest {
            req: test_request(2),
            resp_tx: resp_tx2,
        };
        req_tx.send(write2)?;

        // resp 1
        response_tx.send(Ok(convert(&test_response(1)))).await?;
        let resp1 = resp_rx1.await??;
        assert_eq!(resp1, test_response(1));

        // write 3
        let (resp_tx3, resp_rx3) = oneshot::channel();
        let write3 = WriteRequest {
            req: test_request(3),
            resp_tx: resp_tx3,
        };
        req_tx.send(write3)?;

        // resp 2
        response_tx.send(Ok(convert(&test_response(2)))).await?;
        let resp2 = resp_rx2.await??;
        assert_eq!(resp2, test_response(2));

        // resp 3
        response_tx.send(Ok(convert(&test_response(3)))).await?;
        let resp3 = resp_rx3.await??;
        assert_eq!(resp3, test_response(3));

        drop(req_tx);
        drop(response_tx);
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
        req_tx.send(write)?;

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

    #[tokio::test]
    async fn error_mid_stream() -> anyhow::Result<()> {
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
        req_tx.send(write1)?;

        // write 2
        let (resp_tx2, resp_rx2) = oneshot::channel();
        let write2 = WriteRequest {
            req: test_request(2),
            resp_tx: resp_tx2,
        };
        req_tx.send(write2)?;

        // write 3
        let (resp_tx3, resp_rx3) = oneshot::channel();
        let write3 = WriteRequest {
            req: test_request(3),
            resp_tx: resp_tx3,
        };
        req_tx.send(write3)?;

        // resp 1
        response_tx.send(Ok(convert(&test_response(1)))).await?;
        let resp1 = resp_rx1.await??;
        assert_eq!(resp1, test_response(1));

        // resp 2 - error
        response_tx
            .send(Err(TonicStatus::failed_precondition("fail")))
            .await?;
        let resp2 = resp_rx2.await?;
        let Err(AppendError::Rpc { source: err }) = resp2 else {
            anyhow::bail!("expected an RPC error, got: {resp2:?}");
        };
        let Some(status) = err.status() else {
            anyhow::bail!("expected a status, got: {err:?}");
        };
        assert_eq!(status.code, Code::FailedPrecondition);
        assert_eq!(status.message, "fail");

        // resp 3 - channel closed error
        let _resp3 = resp_rx3.await.expect_err("channel should be closed");

        drop(req_tx);
        drop(response_tx);
        handle.await?;

        Ok(())
    }

    #[tokio::test]
    async fn sender_dropped_mid_stream() -> anyhow::Result<()> {
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
        req_tx.send(write1)?;

        // write 2
        let (resp_tx2, resp_rx2) = oneshot::channel();
        let write2 = WriteRequest {
            req: test_request(2),
            resp_tx: resp_tx2,
        };
        req_tx.send(write2)?;

        // write 3
        let (resp_tx3, resp_rx3) = oneshot::channel();
        let write3 = WriteRequest {
            req: test_request(3),
            resp_tx: resp_tx3,
        };
        req_tx.send(write3)?;

        // resp 1
        response_tx.send(Ok(convert(&test_response(1)))).await?;
        let resp1 = resp_rx1.await??;
        assert_eq!(resp1, test_response(1));

        // Drop the request sender before all the writes are finished.
        drop(req_tx);

        // resp 2
        response_tx.send(Ok(convert(&test_response(2)))).await?;
        let resp2 = resp_rx2.await??;
        assert_eq!(resp2, test_response(2));

        // resp 3
        response_tx.send(Ok(convert(&test_response(3)))).await?;
        let resp3 = resp_rx3.await??;
        assert_eq!(resp3, test_response(3));

        drop(response_tx);
        handle.await?;

        Ok(())
    }

    #[tokio::test]
    async fn unexpected_end_of_stream() -> anyhow::Result<()> {
        // If the stream ends without responding to us, the service broke its contract. It is easy
        // enough to be defensive.
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
        req_tx.send(write1)?;

        // write 2
        let (resp_tx2, resp_rx2) = oneshot::channel();
        let write2 = WriteRequest {
            req: test_request(2),
            resp_tx: resp_tx2,
        };
        req_tx.send(write2)?;

        // write 3
        let (resp_tx3, resp_rx3) = oneshot::channel();
        let write3 = WriteRequest {
            req: test_request(3),
            resp_tx: resp_tx3,
        };
        req_tx.send(write3)?;

        // resp 1
        response_tx.send(Ok(convert(&test_response(1)))).await?;
        let resp1 = resp_rx1.await??;
        assert_eq!(resp1, test_response(1));

        // Close the stream
        drop(response_tx);

        // resp 2 - channel closed error
        let _resp2 = resp_rx2.await.expect_err("channel should be closed");

        // resp 3 - channel closed error
        let _resp3 = resp_rx3.await.expect_err("channel should be closed");
        handle.await?;

        Ok(())
    }

    #[tokio::test]
    async fn stream_closes_when_client_drops_sender() -> anyhow::Result<()> {
        let (response_tx, response_rx) = mpsc::channel(10);
        let mut mock = MockBigQueryWrite::new();

        mock.expect_append_rows().return_once(|request| {
            let mut request_rx = request.into_inner();
            tokio::spawn(async move {
                while request_rx.recv().await.is_some() {}
                drop(response_tx);
            });
            Ok(TonicResponse::from(response_rx))
        });

        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = Arc::new(test_transport(endpoint).await?);

        let Runner { req_tx, handle } = Runner::new(transport);

        let (resp_tx, _resp_rx) = oneshot::channel();
        let write = WriteRequest {
            req: test_request(1),
            resp_tx,
        };
        req_tx.send(write)?;
        drop(req_tx);

        handle.await?;

        Ok(())
    }

    #[tokio::test]
    async fn stream_error_without_pending_requests() -> anyhow::Result<()> {
        // This is a regression test for
        // https://github.com/googleapis/google-cloud-rust/issues/6815

        let (response_tx, response_rx) = mpsc::channel(10);
        let mut mock = MockBigQueryWrite::new();
        mock.expect_append_rows()
            .return_once(|_| Ok(TonicResponse::from(response_rx)));
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = Arc::new(test_transport(endpoint).await?);

        let Runner { req_tx, handle } = Runner::new(transport);

        // Perform a write, opening the stream.
        let (resp_tx, resp_rx) = oneshot::channel();
        let write = WriteRequest {
            req: test_request(1),
            resp_tx,
        };
        req_tx.send(write)?;

        // Respond to the write, draining the request queue.
        response_tx.send(Ok(convert(&test_response(1)))).await?;
        let _ = resp_rx.await??;

        // Close the stream with an error
        response_tx
            .send(Err(TonicStatus::failed_precondition("fail")))
            .await?;

        handle.await?;

        Ok(())
    }

    #[tokio::test]
    async fn trace_id() -> anyhow::Result<()> {
        // We use this channel to surface writes (requests) from outside our
        // mock expectation.
        let (recover_writes_tx, mut recover_writes_rx) = mpsc::channel(10);
        let (response_tx, response_rx) = mpsc::channel(10);
        let mut mock = MockBigQueryWrite::new();
        mock.expect_append_rows().return_once(move |request| {
            tokio::spawn(async move {
                // Note that this task stays alive as long as we hold
                // `recover_writes_rx`.
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

        let Runner { req_tx, handle } = Runner::new(transport);

        // write 1
        let (resp_tx1, _) = oneshot::channel();
        let write1 = WriteRequest {
            req: test_request(1),
            resp_tx: resp_tx1,
        };
        req_tx.send(write1)?;

        // write 2
        let (resp_tx2, _) = oneshot::channel();
        let write2 = WriteRequest {
            req: test_request(2),
            resp_tx: resp_tx2,
        };
        req_tx.send(write2)?;

        let initial_req = recover_writes_rx
            .recv()
            .await
            .expect("should receive a request")?;
        assert!(
            initial_req.trace_id.starts_with("rust-writer:"),
            "got trace_id: {}",
            initial_req.trace_id
        );

        let second_req = recover_writes_rx
            .recv()
            .await
            .expect("should receive a second request")?;
        assert_eq!(second_req.trace_id, "");

        drop(response_tx);
        handle.await?;

        Ok(())
    }

    #[tokio::test]
    async fn send_optimization() -> anyhow::Result<()> {
        // We use this channel to surface writes (requests) from outside our
        // mock expectation.
        let (recover_writes_tx, mut recover_writes_rx) = mpsc::channel(10);
        let (response_tx, response_rx) = mpsc::channel(10);
        let mut mock = MockBigQueryWrite::new();
        mock.expect_append_rows().return_once(move |request| {
            tokio::spawn(async move {
                // Note that this task stays alive as long as we hold
                // `recover_writes_rx`.
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

        let Runner { req_tx, handle } = Runner::new(transport);

        // write 1
        let (resp_tx1, _) = oneshot::channel();
        let write1 = WriteRequest {
            req: test_request(1),
            resp_tx: resp_tx1,
        };
        req_tx.send(write1)?;

        // write 2
        let (resp_tx2, _) = oneshot::channel();
        let write2 = WriteRequest {
            req: test_request(2),
            resp_tx: resp_tx2,
        };
        req_tx.send(write2)?;

        let initial_req = recover_writes_rx
            .recv()
            .await
            .expect("should receive a request")?;
        assert_eq!(initial_req.write_stream, write_stream());

        let second_req = recover_writes_rx
            .recv()
            .await
            .expect("should receive a second request")?;
        assert_eq!(second_req.write_stream, "");

        drop(response_tx);
        handle.await?;

        Ok(())
    }
}
