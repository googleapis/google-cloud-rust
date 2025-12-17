// Copyright 2025 Google LLC
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

//! End-to-end mocks for the `google.pubsub.v1.Subscriber` gRPC service.
//!
//! Use this crate for end-to-end client library tests. Start a local server
//! implementing the `google.pubsub.v1.Subscriber` API, with the implementation
//! defined by a mock. Then test the client library against this mock.
//!
//! # Example
//! ```no_rust
//! use pubsub_grpc_mock::{start, MockSubscriber};
//! use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
//!
//! # async fn test() -> anyhow::Result<()> {
//! let mut mock = MockSubscriber::new();
//! mock.expect_streaming_pull()
//!     .return_once(|_| Err(tonic::Status::invalid_argument("test message")));
//! // Starts a service using `mock` and a random port.
//! let (endpoint, server) = start("0.0.0.0:0", mock).await?;
//! // Use the service in a test.
//! let client = Subscriber::builder()
//!     .with_endpoint(endpoint)
//!     .with_credentials(Anonymous::default().build())
//!     .build()
//!     .await?;
//! let err = client.streaming_pull("projects/my-project/subscriptions/my-subscription")
//!     .send()
//!     .unwrap_err("mock returns an error");
//! assert_eq!(err.status().is_some(), "{err:?}");
//! # Ok(()) }
//! ```

mod mocks;
use std::net::SocketAddr;
use tokio::task::JoinHandle;

/// A mock for the `google.pubsub.v1.Pubsub` gRPC service.
///
/// # Example
/// ```
/// use pubsub_grpc_mock::MockSubscriber;
/// let mut mock = MockSubscriber::new();
/// let (tx, rx) = tokio::sync::mpsc::channel(128);
/// mock.expect_streaming_pull()
///     .return_once(|_request| Ok(tonic::Response::from(rx)));
/// // use `tx` to mock streaming responses.
/// ```
pub use mocks::MockSubscriber;

/// Starts a mock `google.pubsub.v1.Pubsub` gRPC service.
///
/// # Example
/// ```
/// use pubsub_grpc_mock::{start, MockSubscriber};
/// # async fn test() -> anyhow::Result<()> {
/// let mut mock = MockSubscriber::new();
/// mock.expect_streaming_pull()
///     .return_once(|_| Err(tonic::Status::invalid_argument("test message")));
/// // starts a service using `mock` and a random port.
/// let (address, server) = start("0.0.0.0:0", mock).await?;
/// // ... ... test goes here ... ...
/// # Ok(()) }
/// ```
pub async fn start<T>(address: &str, service: T) -> anyhow::Result<(String, JoinHandle<()>)>
where
    T: google::pubsub::v1::subscriber_server::Subscriber,
{
    let listener = tokio::net::TcpListener::bind(address).await?;
    let addr = listener.local_addr()?;

    let server = tokio::spawn(async {
        let stream = tokio_stream::wrappers::TcpListenerStream::new(listener);

        let _ = tonic::transport::Server::builder()
            .add_service(google::pubsub::v1::subscriber_server::SubscriberServer::new(service))
            .serve_with_incoming(stream)
            .await;
    });

    Ok((to_uri(addr), server))
}

fn to_uri(addr: SocketAddr) -> String {
    if addr.is_ipv6() {
        format!("http://[{}]:{}", addr.ip(), addr.port())
    } else {
        format!("http://{}:{}", addr.ip(), addr.port())
    }
}

#[allow(clippy::large_enum_variant)]
pub mod google {
    pub mod pubsub {
        pub mod v1 {
            include!("generated/protos/google.pubsub.v1.rs");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use google::pubsub::v1;
    use google::pubsub::v1::subscriber_client::SubscriberClient;
    use pastey::paste;
    use std::str::FromStr;
    use std::sync::{Arc, Mutex};
    use test_case::test_case;
    use tonic::transport::Channel;

    #[tokio::test]
    async fn streaming_pull() -> anyhow::Result<()> {
        let (response_tx, response_rx) = tokio::sync::mpsc::channel(1);
        let capture = Arc::new(Mutex::new(None));
        let received = capture.clone();
        let mut mock = MockSubscriber::new();
        mock.expect_streaming_pull().once().return_once(move |r| {
            received.lock().expect("never poisoned").get_or_insert(r);
            Ok(tonic::Response::from(response_rx))
        });

        let (address, _server) = start("0.0.0.0:0", mock).await?;
        let endpoint = Channel::from_shared(address.clone())?.connect().await?;
        let mut client = SubscriberClient::new(endpoint);
        // Prepare the request, send one element on the stream and close it.
        let (request_tx, request_rx) = tokio::sync::mpsc::channel(1);

        // Send the request.
        let response = client
            .streaming_pull(tokio_stream::wrappers::ReceiverStream::new(request_rx))
            .await?;
        // Verify the mock can receive messages.
        let request = capture
            .lock()
            .expect("never poisoned")
            .take()
            .expect("request captured");
        let (_metadata, _extensions, mut stream) = request.into_parts();
        let _ = request_tx.send(v1::StreamingPullRequest::default()).await;
        drop(request_tx);
        let message = stream.recv().await;
        assert!(message.is_some(), "{message:?}");
        let message = stream.recv().await;
        assert!(message.is_none(), "{message:?}");

        // Verify we can use the mock to send back messages. Use an
        // error response to keep this code simpler.
        let (_metadata, mut stream, _extensions) = response.into_parts();
        response_tx
            .send(Err(tonic::Status::invalid_argument(
                "missing initial request",
            )))
            .await?;
        drop(response_tx);
        // Read the simulated error message.
        let status = stream
            .message()
            .await
            .transpose()
            .expect("at least one response")
            .expect_err("response should be an error");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert_eq!(status.message(), "missing initial request");
        // Expect the stream to be closed.
        let message = stream.message().await.transpose();
        assert!(message.is_none(), "{message:?}");
        Ok(())
    }

    macro_rules! stub_tests {
        ($(($method:ident, $request:path)),*) => {
            $( paste! {
                #[tokio::test]
                async fn [<mock_stub_$method>]() -> anyhow::Result<()> {
                    let mut mock = MockSubscriber::new();
                    mock.[<expect_$method>]()
                        .once()
                        .returning(|_| Err(tonic::Status::unimplemented("test-only")));

                    let (address, _server) = start("0.0.0.0:0", mock).await?;
                    let endpoint = Channel::from_shared(address.clone())?.connect().await?;
                    let mut client = SubscriberClient::new(endpoint);
                    let status = client
                        .$method($request::default())
                        .await
                        .unwrap_err();
                    assert_eq!(status.code(), tonic::Code::Unimplemented);
                    assert_eq!(status.message(), "test-only");
                    Ok(())
                }
            })*
        };
    }

    stub_tests!(
        (acknowledge, v1::AcknowledgeRequest),
        (modify_ack_deadline, v1::ModifyAckDeadlineRequest),
        (create_subscription, v1::Subscription),
        (get_subscription, v1::GetSubscriptionRequest),
        (update_subscription, v1::UpdateSubscriptionRequest),
        (list_subscriptions, v1::ListSubscriptionsRequest),
        (delete_subscription, v1::DeleteSubscriptionRequest),
        (pull, v1::PullRequest),
        (modify_push_config, v1::ModifyPushConfigRequest),
        (get_snapshot, v1::GetSnapshotRequest),
        (list_snapshots, v1::ListSnapshotsRequest),
        (create_snapshot, v1::CreateSnapshotRequest),
        (update_snapshot, v1::UpdateSnapshotRequest),
        (delete_snapshot, v1::DeleteSnapshotRequest),
        (seek, v1::SeekRequest)
    );

    #[test_case("127.0.0.1:12345", "http://127.0.0.1:12345")]
    #[test_case("[::1]:12345", "http://[::1]:12345")]
    fn format(input: &str, want: &str) -> anyhow::Result<()> {
        let got = to_uri(SocketAddr::from_str(input)?);
        assert_eq!(got, want);
        Ok(())
    }
}
