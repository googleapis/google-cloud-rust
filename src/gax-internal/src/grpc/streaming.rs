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

//! Helper functions for gRPC streaming requests and responses.

use crate::grpc::from_status::to_gax_error;
use crate::prost::{FromProto, ToProto};
use futures::FutureExt as _;
use futures::stream::StreamExt as _;
use google_cloud_gax::error::Error;
use google_cloud_gax::options::RequestOptions;
use google_cloud_gax::streaming::{RequestSender, ResponseStream, SendError};

/// Default buffer capacity for request streaming channels.
pub(crate) const DEFAULT_REQUEST_CHANNEL_CAPACITY: usize = 16;

/// Creates the inbound request mpsc channel with capacity resolved from options.
pub(crate) fn create_request_channel<ProstReq>(
    options: &RequestOptions,
) -> (
    tokio::sync::mpsc::Sender<ProstReq>,
    tokio_stream::wrappers::ReceiverStream<ProstReq>,
) {
    let capacity = options
        .request_stream_channel_capacity()
        .unwrap_or(DEFAULT_REQUEST_CHANNEL_CAPACITY);
    let (req_tx, req_rx) = tokio::sync::mpsc::channel(capacity);
    (req_tx, tokio_stream::wrappers::ReceiverStream::new(req_rx))
}

/// Creates a [`RequestSender<DomainReq>`] that converts domain requests to proto messages
/// and forwards them over the given `mpsc::Sender<ProstReq>`.
pub(crate) fn create_request_sender<DomainReq, ProstReq>(
    req_tx: tokio::sync::mpsc::Sender<ProstReq>,
) -> RequestSender<DomainReq>
where
    DomainReq: ToProto<ProstReq, Output = ProstReq> + Send + 'static,
    ProstReq: Send + 'static,
{
    RequestSender::from_fn(move |item: DomainReq| {
        let req_tx = req_tx.clone();
        async move {
            let prost_item = item
                .to_proto()
                .map_err(|e| SendError::Serialization(Box::new(e)))?;
            req_tx
                .send(prost_item)
                .await
                .map_err(|_| SendError::StreamClosed)
        }
    })
}

/// Decodes a stream of raw Prost results (from Tonic) into a stream of domain model results.
pub(crate) fn decode_response_stream<DomainResp, ProstResp, S>(
    stream: S,
) -> impl futures::Stream<Item = google_cloud_gax::Result<DomainResp>> + Send + 'static
where
    S: futures::Stream<Item = std::result::Result<ProstResp, tonic::Status>> + Send + 'static,
    DomainResp: Send + 'static,
    ProstResp: FromProto<DomainResp> + Send + 'static,
{
    stream.map(|res| {
        res.map_err(to_gax_error)
            .and_then(|m| m.cnv().map_err(Error::deser))
    })
}

/// Constructs a [`ResponseStream<DomainResp>`] from a oneshot receiver waiting for the tonic response.
pub(crate) fn create_response_stream<DomainResp, ProstResp>(
    resp_rx: tokio::sync::oneshot::Receiver<
        google_cloud_gax::Result<tonic::Response<tonic::Streaming<ProstResp>>>,
    >,
) -> ResponseStream<DomainResp>
where
    DomainResp: Send + 'static,
    ProstResp: FromProto<DomainResp> + Send + 'static,
{
    let future = resp_rx.map(|res| match res {
        Ok(Ok(response)) => Ok(decode_response_stream(response.into_inner())),
        Ok(Err(err)) => Err(err),
        Err(_) => Err(Error::io(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "stream initialization task cancelled",
        ))),
    });
    ResponseStream::from_future(future)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prost::ConvertError;
    use google_cloud_gax::error::rpc::Code;
    use google_cloud_gax::options::RequestOptions;
    use test_case::test_case;

    #[derive(Clone, Debug, PartialEq)]
    struct MockDomainReq {
        msg: String,
        should_fail: bool,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct MockProstReq {
        #[prost(string, tag = "1")]
        msg: String,
    }

    impl ToProto<MockProstReq> for MockDomainReq {
        type Output = MockProstReq;
        fn to_proto(self) -> std::result::Result<MockProstReq, ConvertError> {
            if self.should_fail {
                return Err(ConvertError::other("mock serialization error"));
            }
            Ok(MockProstReq { msg: self.msg })
        }
    }

    #[derive(Clone, Debug, PartialEq)]
    struct MockDomainResp {
        msg: String,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct MockProstResp {
        #[prost(string, tag = "1")]
        msg: String,
        #[prost(bool, tag = "2")]
        should_fail: bool,
    }

    impl FromProto<MockDomainResp> for MockProstResp {
        fn cnv(self) -> std::result::Result<MockDomainResp, ConvertError> {
            if self.should_fail {
                return Err(ConvertError::other("mock deserialization error"));
            }
            Ok(MockDomainResp { msg: self.msg })
        }
    }

    #[test]
    fn create_request_channel_default_capacity() {
        let options = RequestOptions::default();
        let (tx, _rx) = create_request_channel::<MockProstReq>(&options);
        assert_eq!(tx.capacity(), DEFAULT_REQUEST_CHANNEL_CAPACITY);
    }

    #[test_case(1; "minimum capacity")]
    #[test_case(16; "default capacity")]
    #[test_case(42; "arbitrary capacity")]
    #[test_case(1024; "large capacity")]
    fn create_request_channel_custom_capacity(cap: usize) {
        let mut options = RequestOptions::default();
        options.set_request_stream_channel_capacity(cap);
        let (tx, _rx) = create_request_channel::<MockProstReq>(&options);
        assert_eq!(tx.capacity(), cap);
    }

    #[tokio::test]
    async fn create_request_sender_success() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        let sender = create_request_sender::<MockDomainReq, MockProstReq>(tx);

        sender
            .send(MockDomainReq {
                msg: "hello".to_string(),
                should_fail: false,
            })
            .await
            .expect("send should succeed");

        let received = rx.recv().await.expect("should receive item");
        assert_eq!(received.msg, "hello");
    }

    #[tokio::test]
    async fn create_request_sender_serialization_error() {
        let (tx, _rx) = tokio::sync::mpsc::channel(16);
        let sender = create_request_sender::<MockDomainReq, MockProstReq>(tx);

        let err = sender
            .send(MockDomainReq {
                msg: "fail".to_string(),
                should_fail: true,
            })
            .await
            .expect_err("send should fail on invalid item");

        assert!(matches!(err, SendError::Serialization(_)));
    }

    #[tokio::test]
    async fn create_request_sender_channel_closed() {
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        let sender = create_request_sender::<MockDomainReq, MockProstReq>(tx);

        drop(rx);
        let err = sender
            .send(MockDomainReq {
                msg: "hello".to_string(),
                should_fail: false,
            })
            .await
            .expect_err("send should fail when channel closed");

        assert!(matches!(err, SendError::StreamClosed));
    }

    #[test_case(1; "single message")]
    #[test_case(3; "multiple messages")]
    #[test_case(10; "ten messages")]
    #[tokio::test]
    async fn create_request_sender_sequential_sends(count: usize) {
        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        let sender = create_request_sender::<MockDomainReq, MockProstReq>(tx);

        for i in 0..count {
            sender
                .send(MockDomainReq {
                    msg: format!("msg{i}"),
                    should_fail: false,
                })
                .await
                .expect("send should succeed");
        }

        for i in 0..count {
            let received = rx.recv().await.expect("should receive item");
            assert_eq!(received.msg, format!("msg{i}"));
        }
    }

    #[tokio::test]
    async fn create_request_sender_cloned_handles() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        let sender1 = create_request_sender::<MockDomainReq, MockProstReq>(tx);
        let sender2 = sender1.clone();

        let t1 = tokio::spawn(async move {
            sender1
                .send(MockDomainReq {
                    msg: "from_t1".to_string(),
                    should_fail: false,
                })
                .await
        });

        let t2 = tokio::spawn(async move {
            sender2
                .send(MockDomainReq {
                    msg: "from_t2".to_string(),
                    should_fail: false,
                })
                .await
        });

        t1.await.unwrap().expect("t1 send ok");
        t2.await.unwrap().expect("t2 send ok");

        let mut received = Vec::new();
        received.push(rx.recv().await.expect("recv 1").msg);
        received.push(rx.recv().await.expect("recv 2").msg);
        received.sort();

        assert_eq!(received, vec!["from_t1", "from_t2"]);
    }

    #[tokio::test]
    async fn create_request_sender_closed_after_send() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        let sender = create_request_sender::<MockDomainReq, MockProstReq>(tx);

        sender
            .send(MockDomainReq {
                msg: "first".to_string(),
                should_fail: false,
            })
            .await
            .expect("first send succeeds");

        let received = rx.recv().await.expect("recv first");
        assert_eq!(received.msg, "first");

        drop(rx);

        let err = sender
            .send(MockDomainReq {
                msg: "second".to_string(),
                should_fail: false,
            })
            .await
            .expect_err("second send should fail");

        assert!(matches!(err, SendError::StreamClosed));
    }

    #[tokio::test]
    async fn decode_response_stream_success() {
        let raw_stream = futures::stream::iter(vec![
            Ok(MockProstResp {
                msg: "item1".to_string(),
                should_fail: false,
            }),
            Ok(MockProstResp {
                msg: "item2".to_string(),
                should_fail: false,
            }),
        ]);

        let mut decoded = decode_response_stream::<MockDomainResp, _, _>(raw_stream);

        let first = decoded
            .next()
            .await
            .expect("first item")
            .expect("should be Ok");
        assert_eq!(
            first,
            MockDomainResp {
                msg: "item1".to_string()
            }
        );

        let second = decoded
            .next()
            .await
            .expect("second item")
            .expect("should be Ok");
        assert_eq!(
            second,
            MockDomainResp {
                msg: "item2".to_string()
            }
        );

        assert!(decoded.next().await.is_none());
    }

    #[tokio::test]
    async fn decode_response_stream_empty() {
        let raw_stream = futures::stream::empty::<Result<MockProstResp, tonic::Status>>();
        let mut decoded = decode_response_stream::<MockDomainResp, MockProstResp, _>(raw_stream);
        assert!(decoded.next().await.is_none());
    }

    #[test_case(tonic::Code::InvalidArgument, Code::InvalidArgument; "invalid argument")]
    #[test_case(tonic::Code::NotFound, Code::NotFound; "not found")]
    #[test_case(tonic::Code::PermissionDenied, Code::PermissionDenied; "permission denied")]
    #[test_case(tonic::Code::Unavailable, Code::Unavailable; "unavailable")]
    #[tokio::test]
    async fn decode_response_stream_status_error(tonic_code: tonic::Code, want_code: Code) {
        let raw_stream =
            futures::stream::iter(vec![Err(tonic::Status::new(tonic_code, "status error"))]);

        let mut decoded = decode_response_stream::<MockDomainResp, MockProstResp, _>(raw_stream);

        let err = decoded
            .next()
            .await
            .expect("item")
            .expect_err("should be Err");
        assert_eq!(err.status().map(|s| s.code), Some(want_code));
        assert!(decoded.next().await.is_none());
    }

    #[test_case(tonic::Code::InvalidArgument, Code::InvalidArgument; "invalid argument")]
    #[test_case(tonic::Code::Unavailable, Code::Unavailable; "unavailable")]
    #[test_case(tonic::Code::PermissionDenied, Code::PermissionDenied; "permission denied")]
    #[test_case(tonic::Code::Internal, Code::Internal; "internal error")]
    #[tokio::test]
    async fn decode_response_stream_items_then_status(tonic_code: tonic::Code, want_code: Code) {
        let raw_stream = futures::stream::iter(vec![
            Ok(MockProstResp {
                msg: "item1".to_string(),
                should_fail: false,
            }),
            Ok(MockProstResp {
                msg: "item2".to_string(),
                should_fail: false,
            }),
            Err(tonic::Status::new(tonic_code, "mid-stream error")),
        ]);

        let mut decoded = decode_response_stream::<MockDomainResp, MockProstResp, _>(raw_stream);

        let first = decoded.next().await.expect("first item").expect("Ok");
        assert_eq!(first.msg, "item1");

        let second = decoded.next().await.expect("second item").expect("Ok");
        assert_eq!(second.msg, "item2");

        let err = decoded.next().await.expect("error item").expect_err("Err");
        assert_eq!(err.status().map(|s| s.code), Some(want_code));

        assert!(decoded.next().await.is_none());
    }

    #[test_case(0; "fail on first item")]
    #[test_case(1; "fail on second item")]
    #[test_case(2; "fail on third item")]
    #[tokio::test]
    async fn decode_response_stream_deserialization_at_index(fail_idx: usize) {
        let items: Vec<Result<MockProstResp, tonic::Status>> = (0..3)
            .map(|i| {
                Ok(MockProstResp {
                    msg: format!("item{i}"),
                    should_fail: i == fail_idx,
                })
            })
            .collect();

        let raw_stream = futures::stream::iter(items);
        let mut decoded = decode_response_stream::<MockDomainResp, _, _>(raw_stream);

        for i in 0..3 {
            let res = decoded.next().await.expect("item");
            if i == fail_idx {
                assert!(res.unwrap_err().is_deserialization());
            } else {
                assert_eq!(res.unwrap().msg, format!("item{i}"));
            }
        }
        assert!(decoded.next().await.is_none());
    }

    #[tokio::test]
    async fn create_response_stream_success_empty() {
        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        let mut resp_stream = create_response_stream::<MockDomainResp, MockProstResp>(resp_rx);

        let streaming = tonic::codec::Streaming::new_empty(
            tonic_prost::ProstDecoder::<MockProstResp>::default(),
            http_body_util::Empty::<bytes::Bytes>::new(),
        );
        let response = tonic::Response::new(streaming);
        resp_tx.send(Ok(response)).unwrap();

        assert!(resp_stream.next().await.is_none());
    }

    #[test_case(Code::PermissionDenied, "permission denied"; "permission denied")]
    #[test_case(Code::NotFound, "not found"; "not found")]
    #[test_case(Code::Unavailable, "unavailable"; "unavailable")]
    #[tokio::test]
    async fn create_response_stream_connection_error(code: Code, msg: &'static str) {
        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        let mut resp_stream = create_response_stream::<MockDomainResp, MockProstResp>(resp_rx);

        let status = google_cloud_gax::error::rpc::Status::default()
            .set_code(code)
            .set_message(msg);
        let _ = resp_tx.send(Err(google_cloud_gax::error::Error::service(status)));

        let err = resp_stream
            .next()
            .await
            .expect("item")
            .expect_err("should be Err");
        assert_eq!(err.status().map(|s| s.code), Some(code));
        assert!(resp_stream.next().await.is_none());
    }

    #[tokio::test]
    async fn create_response_stream_task_cancelled() {
        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel::<
            google_cloud_gax::Result<tonic::Response<tonic::Streaming<MockProstResp>>>,
        >();
        let mut resp_stream = create_response_stream::<MockDomainResp, MockProstResp>(resp_rx);

        drop(resp_tx); // task cancelled

        let err = resp_stream
            .next()
            .await
            .expect("item")
            .expect_err("should be Err");
        assert!(err.is_io());
        assert!(
            err.to_string()
                .contains("stream initialization task cancelled")
        );
        assert!(resp_stream.next().await.is_none());
    }
}
