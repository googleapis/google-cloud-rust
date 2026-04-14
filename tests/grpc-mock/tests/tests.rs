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

#[cfg(test)]
mod tests {
    use storage_grpc_mock::*;
    use google::storage::v2::storage_client::StorageClient;
    use pastey::paste;
    use std::net::SocketAddr;
    use std::str::FromStr;
    use std::sync::{Arc, Mutex};
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
        ($(($method:ident,$request:path)),*) => {
            $( paste! {
                #[tokio::test]
                async fn [<mock_stub_success_$method>]() -> anyhow::Result<()> {
                    let (response_tx, response_rx) = tokio::sync::mpsc::channel(1);
                    let capture = Arc::new(Mutex::new(None));
                    let received = capture.clone();
                    let mut mock = MockStorage::new();
                    mock.[<expect_$method>]()
                        .once()
                        .return_once(move |r| {
                            received.lock().expect("never poisoned").get_or_insert(r);
                            Ok(tonic::Response::from(response_rx))
                        });

                    let (address, _server) = start("0.0.0.0:0", mock).await?;
                    let endpoint = Channel::from_shared(address.clone())?.connect().await?;
                    let mut client = StorageClient::new(endpoint);
                    // Prepare the request, send one element on the stream and close it.
                    let (request_tx, request_rx) = tokio::sync::mpsc::channel(1);

                    // Send the request.
                    let response = client
                        .$method(tokio_stream::wrappers::ReceiverStream::new(request_rx))
                        .await?;
                    // Verify the mock can receive messages.
                    let request = capture.lock().expect("never poisoned").take().expect("request captured");
                    let (_metadata, _extensions, mut stream) = request.into_parts();
                    let _ = request_tx.send($request::default()).await;
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
            })*
        };
    }

    bidi_streaming_stub_tests!(
        (bidi_read_object, google::storage::v2::BidiReadObjectRequest),
        (
            bidi_write_object,
            google::storage::v2::BidiWriteObjectRequest
        )
    );

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
