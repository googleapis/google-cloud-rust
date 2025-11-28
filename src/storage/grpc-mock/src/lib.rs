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

//! End-to-end mocks for the `google.storage.v2.Storage` gRPC service.
//!
//! Use this crate for end-to-end client library tests. Start a local server
//! implementing the `google.storage.v2.Storage` API, with the implementation
//! defined by a mock. Then test the client library against this mock.
//!
//! # Example
//! ```no_rust
//! use storage_grpc_mock::{start, MockStorage};
//! use google_cloud_storage::client::StorageControl;
//! use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
//!
//! # async fn test() -> anyhow::Result<()> {
//! let mut mock = MockStorage::new();
//! mock.expect_delete_bucket()
//!     .return_once(|_| Err(tonic::Status::invalid_argument("test message")));
//! // Starts a service using `mock` and a random port.
//! let (endpoint, server) = start("0.0.0.0:0", mock).await?;
//! // Use the service in a test.
//! let client = StorageControl::builder()
//!     .with_endpoint(endpoint)
//!     .with_credentials(Anonymous::default().build())
//!     .build()
//!     .await?;
//! let err = client.delete_bucket().set_name("projects/_/buckets/test-bucket")
//!     .send()
//!     .unwrap_err("mock returns an error");
//! assert_eq!(err.status().is_some(), "{err:?}");
//! # Ok(()) }
//! ```

mod mocks;
use std::net::SocketAddr;
use tokio::task::JoinHandle;

/// A mock for the `google.storage.v2.Storage` gRPC service.
///
/// # Example
/// ```
/// use storage_grpc_mock::MockStorage;
/// let mut mock = MockStorage::new();
/// let (tx, rx) = tokio::sync::mpsc::channel(128);
/// mock.expect_bidi_write_object()
///     .return_once(|_request| Ok(tonic::Response::from(rx)));
/// // use `tx` to mock streaming responses.
/// ```
pub use mocks::MockStorage;

/// Starts a mock `google.storage.v2.Storage` gRPC service.
///
/// # Example
/// ```
/// use storage_grpc_mock::{start, MockStorage};
/// # async fn test() -> anyhow::Result<()> {
/// let mut mock = MockStorage::new();
/// mock.expect_delete_bucket()
///     .return_once(|_| Err(tonic::Status::invalid_argument("test message")));
/// // starts a service using `mock` and a random port.
/// let (address, server) = start("0.0.0.0:0", mock).await?;
/// // ... ... test goes here ... ...
/// # Ok(()) }
/// ```
pub async fn start<T>(address: &str, service: T) -> anyhow::Result<(String, JoinHandle<()>)>
where
    T: google::storage::v2::storage_server::Storage,
{
    let listener = tokio::net::TcpListener::bind(address).await?;
    let addr = listener.local_addr()?;

    let server = tokio::spawn(async {
        let stream = tokio_stream::wrappers::TcpListenerStream::new(listener);

        let _ = tonic::transport::Server::builder()
            .add_service(google::storage::v2::storage_server::StorageServer::new(
                service,
            ))
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
    pub mod iam {
        pub mod v1 {
            include!("generated/protos/google.iam.v1.rs");
        }
    }
    pub mod rpc {
        include!("generated/protos/google.rpc.rs");
    }
    pub mod storage {
        pub mod v2 {
            include!("generated/protos/google.storage.v2.rs");
        }
    }
    pub mod r#type {
        include!("generated/protos/google.r#type.rs");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use google::storage::v2::storage_client::StorageClient;
    use paste::paste;
    use std::str::FromStr;
    use test_case::test_case;
    use tonic::transport::Channel;

    #[tokio::test]
    async fn mock_stub_server_streaming_success() -> anyhow::Result<()> {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let mut mock = MockStorage::new();
        mock.expect_read_object()
            .once()
            .return_once(move |_| Ok(tonic::Response::from(rx)));

        let (address, _server) = start("0.0.0.0:0", mock).await?;
        let endpoint = Channel::from_shared(address.clone())?.connect().await?;
        let mut client = StorageClient::new(endpoint);
        let response = client
            .read_object(google::storage::v2::ReadObjectRequest::default())
            .await?;
        let (_metadata, mut stream, _extensions) = response.into_parts();
        tx.send(Err(tonic::Status::invalid_argument("missing bucket")))
            .await?;
        drop(tx);
        let status = stream
            .message()
            .await
            .transpose()
            .expect("at least one response")
            .expect_err("response should be an error");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert_eq!(status.message(), "missing bucket");
        Ok(())
    }

    #[tokio::test]
    async fn mock_stub_client_streaming_success() -> anyhow::Result<()> {
        use google::storage::v2::{BidiReadObjectRequest, BidiReadObjectSpec};
        use std::sync::{Arc, Mutex};

        let (response_tx, response_rx) = tokio::sync::mpsc::channel(1);
        let request = Arc::new(Mutex::new(None));
        let capture = request.clone();
        let mut mock = MockStorage::new();
        mock.expect_bidi_read_object().once().return_once(move |r| {
            capture.lock().expect("never poisoned").get_or_insert(r);
            Ok(tonic::Response::from(response_rx))
        });

        let (address, _server) = start("0.0.0.0:0", mock).await?;
        let endpoint = Channel::from_shared(address.clone())?.connect().await?;
        let mut client = StorageClient::new(endpoint);

        let (request_tx, request_rx) = tokio::sync::mpsc::channel(8);
        for i in 0..3 {
            let result = request_tx
                .send(BidiReadObjectRequest {
                    read_object_spec: Some(BidiReadObjectSpec {
                        generation: i as i64,
                        ..BidiReadObjectSpec::default()
                    }),
                    ..BidiReadObjectRequest::default()
                })
                .await;
            assert!(result.is_ok(), "i = {i}, result = {result:?}");
        }

        let response = client
            .bidi_read_object(tokio_stream::wrappers::ReceiverStream::from(request_rx))
            .await?;

        // Verify the mock gets the messages.
        let (_metadata, _extensions, mut stream) = request
            .lock()
            .expect("never poisoned")
            .take()
            .expect("has captured_value")
            .into_parts();
        for _ in 0..2 {
            let message = stream.recv().await;
            assert!(matches!(message, Some(Ok(_))), "{message:?}");
        }
        stream.close();

        // Verify the mock response can be used to read messages.
        let (_metadata, mut stream, _extensions) = response.into_parts();
        response_tx
            .send(Err(tonic::Status::invalid_argument("missing bucket")))
            .await?;
        drop(response_tx);
        let status = stream
            .message()
            .await
            .transpose()
            .expect("at least one response")
            .expect_err("response should be an error");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert_eq!(status.message(), "missing bucket");
        Ok(())
    }

    macro_rules! bidi_streaming_stub_tests {
        ($($method:ident),*) => {
            $( paste! {
                #[tokio::test]
                async fn [<mock_stub_success_$method>]() -> anyhow::Result<()> {
                    let (response_tx, response_rx) = tokio::sync::mpsc::channel(1);
                    let mut mock = MockStorage::new();
                    mock.[<expect_$method>]()
                        .once()
                        .return_once(move |_| Ok(tonic::Response::from(response_rx)));

                    let (address, _server) = start("0.0.0.0:0", mock).await?;
                    let endpoint = Channel::from_shared(address.clone())?.connect().await?;
                    let mut client = StorageClient::new(endpoint);
                    let (request_tx, request_rx) = tokio::sync::mpsc::channel(1);
                    drop(request_tx);
                    let response = client
                        .$method(tokio_stream::wrappers::ReceiverStream::new(request_rx))
                        .await?;
                    let (_metadata, mut stream, _extensions) = response.into_parts();
                    response_tx
                        .send(Err(tonic::Status::invalid_argument(
                            "missing initial request",
                        )))
                        .await?;
                    drop(response_tx);
                    let status = stream
                        .message()
                        .await
                        .transpose()
                        .expect("at least one response")
                        .expect_err("response should be an error");
                    assert_eq!(status.code(), tonic::Code::InvalidArgument);
                    assert_eq!(status.message(), "missing initial request");
                    Ok(())
                }
            })*
        };
    }

    bidi_streaming_stub_tests!(bidi_read_object, bidi_write_object);

    macro_rules! client_streaming_stub_tests {
        ($($method:ident),*) => {
            $( paste! {
                #[tokio::test]
                async fn [<mock_stub_$method>]() -> anyhow::Result<()> {
                    let mut mock = MockStorage::new();
                    mock.[<expect_$method>]()
                        .once()
                        .return_once(move |_| Err(tonic::Status::unimplemented("test only")));

                    let (address, _server) = start("0.0.0.0:0", mock).await?;
                    let endpoint = Channel::from_shared(address.clone())?.connect().await?;
                    let mut client = StorageClient::new(endpoint);
                    let (tx, rx) = tokio::sync::mpsc::channel(1);
                    drop(tx);
                    let status = client
                        .$method(tokio_stream::wrappers::ReceiverStream::new(rx))
                        .await
                        .unwrap_err();
                    assert_eq!(status.code(), tonic::Code::Unimplemented);
                    assert_eq!(status.message(), "test only");
                    Ok(())
                }
            })*
        };
    }

    client_streaming_stub_tests!(write_object, bidi_read_object, bidi_write_object);

    macro_rules! stub_tests {
        ($(($method:ident, $request:path)),*) => {
            $( paste! {
                #[tokio::test]
                async fn [<mock_stub_$method>]() -> anyhow::Result<()> {
                    let mut mock = MockStorage::new();
                    mock.[<expect_$method>]()
                        .once()
                        .returning(|_| Err(tonic::Status::unimplemented("test-only")));

                    let (address, _server) = start("0.0.0.0:0", mock).await?;
                    let endpoint = Channel::from_shared(address.clone())?.connect().await?;
                    let mut client = StorageClient::new(endpoint);
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
        (read_object, google::storage::v2::ReadObjectRequest),
        (delete_bucket, google::storage::v2::DeleteBucketRequest),
        (get_bucket, google::storage::v2::GetBucketRequest),
        (create_bucket, google::storage::v2::CreateBucketRequest),
        (list_buckets, google::storage::v2::ListBucketsRequest),
        (
            lock_bucket_retention_policy,
            google::storage::v2::LockBucketRetentionPolicyRequest
        ),
        (get_iam_policy, google::iam::v1::GetIamPolicyRequest),
        (set_iam_policy, google::iam::v1::SetIamPolicyRequest),
        (
            test_iam_permissions,
            google::iam::v1::TestIamPermissionsRequest
        ),
        (update_bucket, google::storage::v2::UpdateBucketRequest),
        (compose_object, google::storage::v2::ComposeObjectRequest),
        (delete_object, google::storage::v2::DeleteObjectRequest),
        (restore_object, google::storage::v2::RestoreObjectRequest),
        (
            cancel_resumable_write,
            google::storage::v2::CancelResumableWriteRequest
        ),
        (get_object, google::storage::v2::GetObjectRequest),
        (update_object, google::storage::v2::UpdateObjectRequest),
        (list_objects, google::storage::v2::ListObjectsRequest),
        (rewrite_object, google::storage::v2::RewriteObjectRequest),
        (
            start_resumable_write,
            google::storage::v2::StartResumableWriteRequest
        ),
        (
            query_write_status,
            google::storage::v2::QueryWriteStatusRequest
        ),
        (move_object, google::storage::v2::MoveObjectRequest)
    );

    #[test_case("127.0.0.1:12345", "http://127.0.0.1:12345")]
    #[test_case("[::1]:12345", "http://[::1]:12345")]
    fn format(input: &str, want: &str) -> anyhow::Result<()> {
        let got = to_uri(SocketAddr::from_str(input)?);
        assert_eq!(got, want);
        Ok(())
    }
}
