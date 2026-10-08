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

use anyhow::Context as _;
use gaxi::grpc::tonic::{Response as TonicResponse, Result as TonicResult, Status as TonicStatus};
use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
use google_cloud_storage::client::Storage;
use google_cloud_storage::model::Object;
use google_cloud_storage::model_ext::ReadRange;
use google_cloud_storage::read_object::ReadObjectResponse;
use pretty_assertions::assert_eq;
use storage_grpc_mock::google::storage::v2::{
    BidiReadObjectRequest, BidiReadObjectResponse, ChecksummedData, Object as ProtoObject,
    ObjectRangeData, ReadRange as ProtoRange,
};
use storage_grpc_mock::{MockStorage, start};

const BIND_ADDRESS: &str = "127.0.0.1:0";
const BUCKET_NAME: &str = "projects/_/buckets/test-bucket";
const OBJECT_NAME: &str = "test-object";
const OBJECT_GENERATION: i64 = 123456;
const OBJECT_CONTENT: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

const ERR_STREAM_CLOSED_PREMATURELY: &str = "gRPC stream closed before the request was received";
const ERR_RECV_ERROR: &str = "error while reading the request";

#[tokio::test]
async fn send_and_read_single_response_success() -> anyhow::Result<()> {
    // Arrange
    const USER_AGENT: &str = "open_object_grpc/1.0";
    const QUOTA_PROJECT: &str = "open-object-quota-project";
    const RESPONSE_HEADER_KEY: &str = "x-test-response";
    const RESPONSE_HEADER_VALUE: &str = "response-value";
    const READ_ID: i64 = 0;

    let (observed_tx, observed_rx) = tokio::sync::oneshot::channel::<BidiReadObjectRequest>();

    let mut mock = MockStorage::new();
    mock.expect_bidi_read_object().return_once(move |request| {
        assert_request_metadata(request.metadata(), USER_AGENT, QUOTA_PROJECT);
        let (_, _, mut requests) = request.into_parts();
        tokio::spawn(async move {
            let first = recv_request(&mut requests).await;
            observed_tx
                .send(first)
                .expect("failed to send recorded request");
        });

        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tx.try_send(Ok(initial_response_with_data(
            ProtoRange {
                read_id: READ_ID,
                ..ProtoRange::default()
            },
            OBJECT_CONTENT.to_vec(),
            true,
        )))
        .expect("failed to send response");

        let mut response = TonicResponse::from(rx);
        response.metadata_mut().insert(
            RESPONSE_HEADER_KEY,
            RESPONSE_HEADER_VALUE.parse().expect("valid header value"),
        );
        Ok(response)
    });
    let (client, _server) = start_test_server(mock).await?;

    // Act
    let (descriptor, reader) = client
        .open_object(BUCKET_NAME, OBJECT_NAME)
        .with_user_agent(USER_AGENT)
        .with_quota_project(QUOTA_PROJECT)
        .send_and_read(ReadRange::all())
        .await?;

    // Assert
    // Verify requested details sent to the server
    let first_request = observed_rx.await?;
    let spec = first_request
        .read_object_spec
        .expect("first request should contain read_object_spec");
    assert_eq!(spec.bucket, BUCKET_NAME);
    assert_eq!(spec.object, OBJECT_NAME);
    assert_eq!(
        first_request.read_ranges,
        [ProtoRange {
            read_id: READ_ID,
            ..ProtoRange::default()
        }]
    );

    // Verify object metadata and response headers
    let want_object = Object::new()
        .set_bucket(BUCKET_NAME)
        .set_name(OBJECT_NAME)
        .set_generation(OBJECT_GENERATION);
    assert_eq!(descriptor.object(), want_object, "{descriptor:?}");
    assert_eq!(
        descriptor.headers()[RESPONSE_HEADER_KEY],
        RESPONSE_HEADER_VALUE
    );

    // Verify payload
    let got_payload = read_all_bytes(reader).await?;
    assert_eq!(got_payload, OBJECT_CONTENT);

    Ok(())
}

#[tokio::test]
async fn send_and_read_reads_range_split_across_multiple_responses() -> anyhow::Result<()> {
    const PARTIAL_PAYLOAD_LEN: u64 = 4;

    // Arrange
    let (tx, rx) = tokio::sync::mpsc::channel::<TonicResult<BidiReadObjectResponse>>(2);

    let mut mock = MockStorage::new();
    mock.expect_bidi_read_object().return_once(|request| {
        // Extract the gRPC request stream
        let (_, _, mut requests) = request.into_parts();

        // Setup the Storage service
        tokio::spawn(async move {
            let first = recv_request(&mut requests).await;

            // Initial message should contain the object spec and range request
            assert!(first.read_object_spec.is_some(), "{first:?}");

            let range = single_range(&first);

            // Split the requested range payload across two separate response messages
            let first_payload =
                slice_range_for_len(OBJECT_CONTENT, &range, PARTIAL_PAYLOAD_LEN as usize).to_vec();

            let second_range = ProtoRange {
                read_offset: range.read_offset + PARTIAL_PAYLOAD_LEN as i64,
                read_length: range.read_length - PARTIAL_PAYLOAD_LEN as i64,
                read_id: range.read_id,
            };
            let remaining_payload = slice_range(OBJECT_CONTENT, &second_range).to_vec();

            // Send initial response message with object metadata and partial data
            tx.send(Ok(initial_response_with_data(
                ProtoRange {
                    read_length: PARTIAL_PAYLOAD_LEN as i64,
                    ..range
                },
                first_payload,
                false, // range_end
            )))
            .await
            .expect("failed to send initial data response");

            // Send follow-up data-only response message with remaining data
            tx.send(Ok(data_only_response(
                second_range,
                remaining_payload,
                true, // range_end
            )))
            .await
            .expect("failed to send follow-up data response");
        });
        Ok(TonicResponse::from(rx))
    });
    let (client, _server) = start_test_server(mock).await?;

    // Act
    let (_, reader) = client
        .open_object(BUCKET_NAME, OBJECT_NAME)
        .send_and_read(ReadRange::segment(10, 8))
        .await?;

    // Assert
    let payload = read_all_bytes(reader).await?;
    assert_eq!(payload, &OBJECT_CONTENT[10..18]);
    Ok(())
}

#[tokio::test]
async fn descriptor_sends_ranges_after_open_and_reads_multiple_messages() -> anyhow::Result<()> {
    // Arrange
    let (tx, rx) = tokio::sync::mpsc::channel::<TonicResult<BidiReadObjectResponse>>(4);

    let mut mock = MockStorage::new();
    mock.expect_bidi_read_object().return_once(|request| {
        // Extract the gRPC request stream
        let (_, _, mut requests) = request.into_parts();

        // Setup the Storage service
        tokio::spawn(async move {
            let open = recv_request(&mut requests).await;

            // Initial message should contain the object spec and no range requests
            assert!(open.read_object_spec.is_some(), "{open:?}");
            assert!(open.read_ranges.is_empty(), "{open:?}");

            // Initial response contains only the object metadata
            tx.send(Ok(initial_response()))
                .await
                .expect("failed to send initial response");

            // Simulate the client requesting two distinct ranges sequentially
            for _ in 0..2 {
                let request = recv_request(&mut requests).await;

                // Subsequent requests on the open stream must NOT send the object spec
                assert!(request.read_object_spec.is_none(), "{request:?}");

                let range = single_range(&request);
                let payload = slice_range(OBJECT_CONTENT, &range).to_vec();

                // Return the requested payload slice to the client
                tx.send(Ok(data_only_response(range, payload, true)))
                    .await
                    .expect("failed to send data response");
            }
        });
        Ok(TonicResponse::from(rx))
    });
    let (client, _server) = start_test_server(mock).await?;

    // Act
    let descriptor = client
        .open_object(BUCKET_NAME, OBJECT_NAME)
        // Disable stream auto-resumption because this test verifies sequential range
        // reads over a single continuous gRPC stream connection without retry/reconnect
        .with_read_resume_policy(google_cloud_storage::read_resume_policy::NeverResume)
        .send()
        .await?;

    // Perform the range reads
    let first_payload =
        read_all_bytes(descriptor.read_range(ReadRange::segment(10, 5)).await).await?;
    let second_payload =
        read_all_bytes(descriptor.read_range(ReadRange::segment(20, 6)).await).await?;

    // Assert
    assert_eq!(first_payload, &OBJECT_CONTENT[10..15]);
    assert_eq!(second_payload, &OBJECT_CONTENT[20..26]);
    Ok(())
}

#[tokio::test]
async fn transient_stream_error_resumes_partial_read() -> anyhow::Result<()> {
    // Arrange
    // Channel used to record the client's requests
    let (observed_tx, mut observed_rx) = tokio::sync::mpsc::channel::<BidiReadObjectRequest>(1);

    let mut mock = MockStorage::new();
    let mut seq = mockall::Sequence::new();

    // Initial stream attempt
    mock.expect_bidi_read_object()
        .once()
        .in_sequence(&mut seq)
        .returning(move |request| {
            // Extract the gRPC request stream
            let (_, _, mut requests) = request.into_parts();
            let (tx, rx) = tokio::sync::mpsc::channel(2);

            // Setup the Storage service
            tokio::spawn(async move {
                let first = recv_request(&mut requests).await;
                let range = single_range(&first);

                // Verify original range request
                assert!(first.read_object_spec.is_some(), "{first:?}");
                assert_eq!(range.read_offset, 10, "{first:?}");
                assert_eq!(range.read_length, 8, "{first:?}");

                // Return initial metadata with partial range payload
                tx.send(Ok(initial_response_with_data(
                    range,
                    slice_range_for_len(OBJECT_CONTENT, &range, 4).to_vec(),
                    false,
                )))
                .await
                .expect("failed to send initial partial data response");

                // Inject an error mid-read
                tx.send(Err(TonicStatus::unavailable("try another stream")))
                    .await
                    .expect("failed to send transient stream error");
            });
            Ok(TonicResponse::from(rx))
        });

    // Resumed stream attempt
    mock.expect_bidi_read_object()
        .once()
        .in_sequence(&mut seq)
        .returning(move |request| {
            let (_, _, mut requests) = request.into_parts();
            let (tx, rx) = tokio::sync::mpsc::channel(2);
            let observed_tx = observed_tx.clone();

            tokio::spawn(async move {
                let first = recv_request(&mut requests).await;
                let range = single_range(&first);

                // Capture the resumed request for assertion
                observed_tx
                    .send(first)
                    .await
                    .expect("failed to send observed request");

                // Return remaining payload
                tx.send(Ok(initial_response_with_data(
                    range,
                    slice_range(OBJECT_CONTENT, &range).to_vec(),
                    true,
                )))
                .await
                .expect("failed to send resumed data response");
            });
            Ok(TonicResponse::from(rx))
        });
    let (client, _server) = start_test_server(mock).await?;

    // Act
    let (_, reader) = client
        .open_object(BUCKET_NAME, OBJECT_NAME)
        .send_and_read(ReadRange::segment(10, 8))
        .await?;
    let payload = read_all_bytes(reader).await?;

    // Assert
    // Verify total accumulated payload
    assert_eq!(payload, &OBJECT_CONTENT[10..18]);

    // Inspect the resumed stream request sent by the client after the transient error
    let resumed = observed_rx
        .recv()
        .await
        .expect("expected resumed stream request");
    let spec = resumed
        .read_object_spec
        .expect("resumed request should contain an object spec");
    assert_eq!(spec.generation, OBJECT_GENERATION);

    // Verify client automatically adjusted the read_offset for the remaining bytes
    assert_eq!(
        resumed.read_ranges,
        [ProtoRange {
            read_offset: 14,
            read_length: 4,
            read_id: 0,
        }]
    );
    Ok(())
}

async fn make_client(endpoint: impl Into<String>) -> anyhow::Result<Storage> {
    let client = Storage::builder()
        .with_credentials(Anonymous::new().build())
        .with_endpoint(endpoint)
        .build()
        .await?;
    Ok(client)
}

/// Drains and collects all byte chunks from a `ReadObjectResponse` stream.
async fn read_all_bytes(mut stream: ReadObjectResponse) -> anyhow::Result<Vec<u8>> {
    let mut payload = Vec::new();
    while let Some(chunk) = stream.next().await {
        payload.extend_from_slice(&chunk.context("range read failed")?);
    }
    Ok(payload)
}

fn assert_request_metadata(
    metadata: &gaxi::grpc::tonic::MetadataMap,
    expected_user_agent: &str,
    expected_quota_project: &str,
) {
    let user_agent = metadata
        .get(http::header::USER_AGENT.as_str())
        .and_then(|value| value.to_str().ok())
        .expect("user-agent should be set");
    assert!(
        user_agent
            .split(' ')
            .any(|value| value == expected_user_agent),
        "{user_agent}"
    );
    assert_eq!(
        metadata
            .get("x-goog-user-project")
            .and_then(|value| value.to_str().ok()),
        Some(expected_quota_project)
    );
    assert!(
        metadata
            .get("x-goog-api-client")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.contains("gccl/")),
        "{metadata:?}"
    );
    assert_eq!(
        metadata
            .get("x-goog-request-params")
            .and_then(|value| value.to_str().ok()),
        Some(format!("bucket={BUCKET_NAME}").as_str())
    );
}

fn test_metadata() -> Option<ProtoObject> {
    Some(ProtoObject {
        bucket: BUCKET_NAME.to_string(),
        name: OBJECT_NAME.to_string(),
        generation: OBJECT_GENERATION,
        ..ProtoObject::default()
    })
}

/// Constructs an initial response containing only object metadata.
fn initial_response() -> BidiReadObjectResponse {
    BidiReadObjectResponse {
        metadata: test_metadata(),
        ..BidiReadObjectResponse::default()
    }
}

/// Constructs an initial response containing both object metadata and a specific range data payload.
fn initial_response_with_data(
    range: ProtoRange,
    payload: Vec<u8>,
    range_end: bool,
) -> BidiReadObjectResponse {
    BidiReadObjectResponse {
        metadata: test_metadata(),
        ..data_only_response(range, payload, range_end)
    }
}

/// Constructs a data-only response (without object metadata).
fn data_only_response(
    range: ProtoRange,
    payload: Vec<u8>,
    range_end: bool,
) -> BidiReadObjectResponse {
    let read_range = ProtoRange {
        read_length: payload.len() as i64,
        ..range
    };
    BidiReadObjectResponse {
        object_data_ranges: vec![ObjectRangeData {
            read_range: Some(read_range),
            range_end,
            checksummed_data: Some(ChecksummedData {
                content: payload,
                crc32c: None,
            }),
        }],
        ..BidiReadObjectResponse::default()
    }
}

/// Slices a buffer according to `range.read_offset` and `range.read_length`.
fn slice_range<'a>(buffer: &'a [u8], range: &ProtoRange) -> &'a [u8] {
    let start = range.read_offset as usize;
    let end = start + range.read_length as usize;
    &buffer[start..end]
}

/// Slices a buffer starting from `range.read_offset` for `len` bytes, ignoring `range.read_length`.
fn slice_range_for_len<'a>(buffer: &'a [u8], range: &ProtoRange, len: usize) -> &'a [u8] {
    let start = range.read_offset as usize;
    &buffer[start..start + len]
}

/// Starts an in-process mock storage gRPC server and returns an initialized client and server guard.
async fn start_test_server(
    mock: MockStorage,
) -> anyhow::Result<(Storage, tokio::task::JoinHandle<()>)> {
    let (endpoint, server) = start(BIND_ADDRESS, mock).await?;
    let client = make_client(endpoint).await?;
    Ok((client, server))
}

/// Receives the next request message from the client's streaming channel.
async fn recv_request(
    requests: &mut tokio::sync::mpsc::Receiver<TonicResult<BidiReadObjectRequest>>,
) -> BidiReadObjectRequest {
    requests
        .recv()
        .await
        .expect(ERR_STREAM_CLOSED_PREMATURELY)
        .expect(ERR_RECV_ERROR)
}

/// Extracts the single `ReadRange` from a `BidiReadObjectRequest`.
fn single_range(request: &BidiReadObjectRequest) -> ProtoRange {
    match request.read_ranges.as_slice() {
        [range] => range.clone(),
        _ => panic!("expected exactly one range"),
    }
}

mod conformance {
    use super::*;
    use google_cloud_gax::retry_policy::{AlwaysRetry, NeverRetry, RetryPolicyExt as _};
    use google_cloud_storage::model_ext::KeyAes256;
    use google_cloud_storage::read_resume_policy::{
        NeverResume, ReadResumePolicyExt as _, Recommended,
    };
    use pretty_assertions::assert_eq;
    use prost::Message as _;
    use sha2::{Digest as _, Sha256};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use storage_grpc_mock::google::rpc::Status as RpcStatus;
    use storage_grpc_mock::google::storage::v2::{BidiReadHandle, BidiReadObjectRedirectedError};

    /// Constructs a tonic `Status` representing a GCS `BidiReadObjectRedirectedError`.
    fn redirect_status(routing: &str) -> TonicStatus {
        let redirect = BidiReadObjectRedirectedError {
            routing_token: Some(routing.to_string()),
            read_handle: Some(BidiReadHandle {
                handle: b"test-handle-redirect".to_vec(),
            }),
        };
        let redirect = prost_types::Any::from_msg(&redirect).expect("serialize any");
        let status = RpcStatus {
            code: gaxi::grpc::tonic::Code::Aborted as i32,
            message: "redirect".to_string(),
            details: vec![redirect],
        };
        let details = bytes::Bytes::from_owner(status.encode_to_vec());
        TonicStatus::with_details(gaxi::grpc::tonic::Code::Aborted, "redirect", details)
    }

    /// Verifies handling mid-read `BidiReadObjectRedirectedError` gRPC responses,
    /// ensuring the client transparently reconnects using redirected `read_handle`
    /// and `routing_token`.
    #[tokio::test]
    async fn handle_redirect_error() -> anyhow::Result<()> {
        // Arrange
        const ROUTING_TOKEN: &str = "test-redirect-routing-token";
        let (resumed_tx, mut resumed_rx) = tokio::sync::mpsc::channel::<BidiReadObjectRequest>(1);

        let mut mock = MockStorage::new();
        let mut seq = mockall::Sequence::new();

        // Initial stream: sends partial data then a redirect error
        mock.expect_bidi_read_object()
            .once()
            .in_sequence(&mut seq)
            .returning(move |request| {
                let (_, _, mut requests) = request.into_parts();
                let (tx, rx) = tokio::sync::mpsc::channel(2);
                tokio::spawn(async move {
                    let first = recv_request(&mut requests).await;
                    let range = single_range(&first);

                    // Send initial partial data (4 bytes)
                    tx.send(Ok(initial_response_with_data(
                        range,
                        slice_range_for_len(OBJECT_CONTENT, &range, 4).to_vec(),
                        false,
                    )))
                    .await
                    .expect("failed to send partial data");

                    // Send redirect error mid-stream
                    tx.send(Err(redirect_status(ROUTING_TOKEN)))
                        .await
                        .expect("failed to send redirect status");
                });
                Ok(TonicResponse::from(rx))
            });

        // Resumed stream: sends remaining data after redirect
        mock.expect_bidi_read_object()
            .once()
            .in_sequence(&mut seq)
            .returning(move |request| {
                let (_, _, mut requests) = request.into_parts();
                let (tx, rx) = tokio::sync::mpsc::channel(2);
                let resumed_tx = resumed_tx.clone();

                tokio::spawn(async move {
                    let first = recv_request(&mut requests).await;
                    let range = single_range(&first);
                    let _ = resumed_tx.send(first).await;

                    tx.send(Ok(initial_response_with_data(
                        range,
                        slice_range(OBJECT_CONTENT, &range).to_vec(),
                        true,
                    )))
                    .await
                    .expect("failed to send resumed data");
                });
                Ok(TonicResponse::from(rx))
            });

        let (client, _server) = start_test_server(mock).await?;

        // Act: Read bytes 10..18 (8 bytes)
        let (_, reader) = client
            .open_object(BUCKET_NAME, OBJECT_NAME)
            .send_and_read(ReadRange::segment(10, 8))
            .await?;
        let payload = read_all_bytes(reader).await?;

        // Assert: Payload matches full 8 bytes
        assert_eq!(payload, &OBJECT_CONTENT[10..18]);

        // Assert: Resumed request has redirect routing token and read_handle
        let resumed_req = resumed_rx
            .recv()
            .await
            .context("expected resumed stream request")?;
        let spec = resumed_req
            .read_object_spec
            .expect("resumed request must contain read_object_spec");
        assert_eq!(spec.routing_token.as_deref(), Some(ROUTING_TOKEN));
        assert_eq!(
            spec.read_handle.as_ref().map(|h| h.handle.as_slice()),
            Some(b"test-handle-redirect".as_slice())
        );
        assert_eq!(
            resumed_req.read_ranges,
            [ProtoRange {
                read_offset: 14,
                read_length: 4,
                read_id: 0,
            }]
        );

        Ok(())
    }

    /// Verifies handling `BidiReadObjectRedirectedError` during initial session open,
    /// reconnecting with the updated routing token and completing session initialization.
    #[tokio::test]
    async fn handle_redirect_error_on_open() -> anyhow::Result<()> {
        // Arrange
        const ROUTING_TOKEN: &str = "test-open-redirect-token";
        let (resumed_tx, mut resumed_rx) = tokio::sync::mpsc::channel::<BidiReadObjectRequest>(1);

        let mut mock = MockStorage::new();
        let mut seq = mockall::Sequence::new();

        // Initial stream attempt: server immediately aborts with redirect status before sending metadata
        mock.expect_bidi_read_object()
            .once()
            .in_sequence(&mut seq)
            .returning(|_| Err(redirect_status(ROUTING_TOKEN)));

        // Second stream attempt: server succeeds with initial metadata
        mock.expect_bidi_read_object()
            .once()
            .in_sequence(&mut seq)
            .returning(move |request| {
                let (_, _, mut requests) = request.into_parts();
                let (tx, rx) = tokio::sync::mpsc::channel(1);
                let resumed_tx = resumed_tx.clone();

                tokio::spawn(async move {
                    let first = recv_request(&mut requests).await;
                    let _ = resumed_tx.send(first).await;

                    tx.send(Ok(initial_response()))
                        .await
                        .expect("failed to send initial response");
                });
                Ok(TonicResponse::from(rx))
            });

        let (client, _server) = start_test_server(mock).await?;

        // Act: open object
        let descriptor = client.open_object(BUCKET_NAME, OBJECT_NAME).send().await?;

        // Assert: open succeeded and descriptor is valid
        assert_eq!(descriptor.object().name, OBJECT_NAME);

        // Assert: second attempt contained the updated routing token and handle
        let second_req = resumed_rx.recv().await.context("expected retry request")?;
        let spec = second_req
            .read_object_spec
            .expect("retry request must contain read_object_spec");
        assert_eq!(spec.routing_token.as_deref(), Some(ROUTING_TOKEN));
        assert_eq!(
            spec.read_handle.as_ref().map(|h| h.handle.as_slice()),
            Some(b"test-handle-redirect".as_slice())
        );

        Ok(())
    }

    /// Verifies redirect limit enforcement when the server repeatedly returns redirects,
    /// confirming the client stops retrying once the attempt budget is exhausted.
    #[tokio::test]
    async fn handle_redirect_error_max_attempts() -> anyhow::Result<()> {
        // Arrange: Mock fails every attempt with a redirect
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_clone = attempts.clone();

        let mut mock = MockStorage::new();
        mock.expect_bidi_read_object().returning(move |_| {
            let count = attempts_clone.fetch_add(1, Ordering::SeqCst);
            Err(redirect_status(&format!("redirect-token-{count}")))
        });

        let (client, _server) = start_test_server(mock).await?;

        // Act: Set retry policy with attempt limit of 2
        let result = client
            .open_object(BUCKET_NAME, OBJECT_NAME)
            .with_retry_policy(AlwaysRetry.with_attempt_limit(2))
            .send()
            .await;

        // Assert: Call fails with error after reaching max attempts
        assert!(
            result.is_err(),
            "expected error after max redirect attempts"
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 2);

        Ok(())
    }

    /// Verifies attempt budget limits under retry settings, confirming that repeated
    /// stream failures exhaust resume attempts and return an error.
    #[tokio::test]
    async fn retry_settings_max_attempt() -> anyhow::Result<()> {
        // Arrange
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_clone = attempts.clone();

        let mut mock = MockStorage::new();
        mock.expect_bidi_read_object().returning(move |request| {
            let attempt = attempts_clone.fetch_add(1, Ordering::SeqCst);
            let (_, _, mut requests) = request.into_parts();
            let (tx, rx) = tokio::sync::mpsc::channel(2);

            tokio::spawn(async move {
                let first = recv_request(&mut requests).await;
                let range = single_range(&first);

                if attempt == 0 {
                    // First attempt sends partial data then fails
                    tx.send(Ok(initial_response_with_data(
                        range,
                        slice_range_for_len(OBJECT_CONTENT, &range, 2).to_vec(),
                        false,
                    )))
                    .await
                    .expect("failed to send initial partial data");
                }

                // Stream is terminated by transient error
                tx.send(Err(TonicStatus::unavailable(format!(
                    "transient error attempt {attempt}"
                ))))
                .await
                .expect("failed to send transient error");
            });
            Ok(TonicResponse::from(rx))
        });

        let (client, _server) = start_test_server(mock).await?;

        // Act: read with read_resume_policy limited to 2 attempts, and NeverRetry for connection attempts
        let (_, reader) = client
            .open_object(BUCKET_NAME, OBJECT_NAME)
            .with_retry_policy(NeverRetry)
            .with_read_resume_policy(Recommended.with_attempt_limit(2))
            .send_and_read(ReadRange::segment(10, 8))
            .await?;

        let result = read_all_bytes(reader).await;

        // Assert: read failed due to resume attempt limit exhaustion
        assert!(
            result.is_err(),
            "expected read error after max resume attempts"
        );
        // Attempt 0 (initial) + Attempt 1 (first resume) = 2 attempts total
        assert_eq!(attempts.load(Ordering::SeqCst), 2);

        Ok(())
    }

    /// Verifies propagation of user project, encryption keys, and preconditions into
    /// gRPC request headers and metadata (`CommonObjectRequestParams`).
    #[tokio::test]
    async fn request_option_verification() -> anyhow::Result<()> {
        // Arrange
        const USER_AGENT: &str = "custom-agent/3.14";
        const QUOTA_PROJECT: &str = "custom-quota-project";
        const OBJECT_GEN: i64 = 777;
        const IF_GEN_MATCH: i64 = 888;
        const IF_METAGEN_MATCH: i64 = 999;
        let raw_key = [42u8; 32];
        let key_sha256 = Sha256::digest(raw_key).to_vec();

        let (observed_req_tx, observed_req_rx) =
            tokio::sync::oneshot::channel::<BidiReadObjectRequest>();
        let (observed_meta_tx, observed_meta_rx) =
            tokio::sync::oneshot::channel::<gaxi::grpc::tonic::MetadataMap>();

        let mut mock = MockStorage::new();
        mock.expect_bidi_read_object().return_once(move |request| {
            let (metadata, _, mut requests) = request.into_parts();
            let _ = observed_meta_tx.send(metadata);

            let (tx, rx) = tokio::sync::mpsc::channel(1);
            tokio::spawn(async move {
                let first = recv_request(&mut requests).await;
                let _ = observed_req_tx.send(first);

                tx.send(Ok(initial_response_with_data(
                    ProtoRange {
                        read_id: 0,
                        read_offset: 0,
                        read_length: 5,
                    },
                    OBJECT_CONTENT[..5].to_vec(),
                    true,
                )))
                .await
                .expect("failed to send response");
            });
            Ok(TonicResponse::from(rx))
        });

        let (client, _server) = start_test_server(mock).await?;

        // Act
        let (descriptor, reader) = client
            .open_object(BUCKET_NAME, OBJECT_NAME)
            .with_user_agent(USER_AGENT)
            .with_quota_project(QUOTA_PROJECT)
            .set_generation(OBJECT_GEN)
            .set_if_generation_match(IF_GEN_MATCH)
            .set_if_metageneration_match(IF_METAGEN_MATCH)
            .set_key(KeyAes256::new(&raw_key)?)
            .send_and_read(ReadRange::segment(0, 5))
            .await?;

        let payload = read_all_bytes(reader).await?;
        assert_eq!(payload, &OBJECT_CONTENT[..5]);

        // Assert: Metadata headers
        let metadata = observed_meta_rx.await?;
        let user_agent = metadata
            .get(http::header::USER_AGENT.as_str())
            .and_then(|v| v.to_str().ok())
            .expect("user-agent header must be present");
        assert!(user_agent.contains(USER_AGENT), "{user_agent}");
        assert_eq!(
            metadata
                .get("x-goog-user-project")
                .and_then(|v| v.to_str().ok()),
            Some(QUOTA_PROJECT)
        );
        assert_eq!(
            metadata
                .get("x-goog-request-params")
                .and_then(|v| v.to_str().ok()),
            Some(format!("bucket={BUCKET_NAME}").as_str())
        );

        // Assert: Protobuf request spec
        let req = observed_req_rx.await?;
        let spec = req
            .read_object_spec
            .expect("read_object_spec must be present");
        assert_eq!(spec.bucket, BUCKET_NAME);
        assert_eq!(spec.object, OBJECT_NAME);
        assert_eq!(spec.generation, OBJECT_GEN);
        assert_eq!(spec.if_generation_match, Some(IF_GEN_MATCH));
        assert_eq!(spec.if_metageneration_match, Some(IF_METAGEN_MATCH));

        // Assert: CSEK encryption params
        let csek = spec
            .common_object_request_params
            .expect("common_object_request_params must be present");
        assert_eq!(csek.encryption_algorithm, "AES256");
        assert_eq!(csek.encryption_key_bytes.as_slice(), raw_key.as_slice());
        assert_eq!(
            csek.encryption_key_sha256_bytes.as_slice(),
            key_sha256.as_slice()
        );

        assert_eq!(descriptor.object().name, OBJECT_NAME);
        Ok(())
    }

    /// Verifies that if an underlying gRPC stream breaks and cannot restart, all
    /// active pending reads and subsequent reads on the descriptor fail immediately.
    #[tokio::test]
    async fn failed_stream_restart_should_fail_all_pending_reads() -> anyhow::Result<()> {
        // Arrange
        let mut mock = MockStorage::new();
        mock.expect_bidi_read_object().return_once(|request| {
            let (_, _, mut requests) = request.into_parts();
            let (tx, rx) = tokio::sync::mpsc::channel(2);

            tokio::spawn(async move {
                let open = recv_request(&mut requests).await;
                assert!(open.read_object_spec.is_some());

                // Initial open handshake succeeds
                tx.send(Ok(initial_response()))
                    .await
                    .expect("failed to send initial response");

                // Client requests a range
                let _ = requests.recv().await;

                // Stream fails with unrecoverable / non-resumed error
                tx.send(Err(TonicStatus::unavailable("stream crash")))
                    .await
                    .expect("failed to send stream error");
            });
            Ok(TonicResponse::from(rx))
        });

        let (client, _server) = start_test_server(mock).await?;

        // Open object with NeverResume so that stream restart fails immediately
        let descriptor = client
            .open_object(BUCKET_NAME, OBJECT_NAME)
            .with_read_resume_policy(NeverResume)
            .send()
            .await?;

        // Act: Request a range read
        let reader = descriptor.read_range(ReadRange::segment(0, 10)).await;
        let result = read_all_bytes(reader).await;

        // Assert: The active read fails with an error
        assert!(
            result.is_err(),
            "expected pending read to fail after stream error"
        );

        // Assert: Subsequent read on the broken descriptor also fails
        let next_reader = descriptor.read_range(ReadRange::segment(10, 5)).await;
        let next_result = read_all_bytes(next_reader).await;
        assert!(next_result.is_err(), "expected subsequent read to fail");

        Ok(())
    }

    /// Verifies that opening an object session retries automatically when the initial
    /// open fails with a transient gRPC error (e.g. `UNAVAILABLE`).
    #[tokio::test]
    async fn retryable_error_while_open() -> anyhow::Result<()> {
        // Arrange
        let mut mock = MockStorage::new();
        let mut seq = mockall::Sequence::new();

        // First attempt fails immediately with transient retryable error (Unavailable)
        mock.expect_bidi_read_object()
            .once()
            .in_sequence(&mut seq)
            .returning(|_| Err(TonicStatus::unavailable("transient error on open")));

        // Second attempt succeeds with initial response
        mock.expect_bidi_read_object()
            .once()
            .in_sequence(&mut seq)
            .returning(|request| {
                let (_, _, mut requests) = request.into_parts();
                let (tx, rx) = tokio::sync::mpsc::channel(1);

                tokio::spawn(async move {
                    let first = recv_request(&mut requests).await;
                    assert!(first.read_object_spec.is_some());

                    tx.send(Ok(initial_response()))
                        .await
                        .expect("failed to send initial response");
                });
                Ok(TonicResponse::from(rx))
            });

        let (client, _server) = start_test_server(mock).await?;

        // Act: open_object.send() should retry transparently
        let descriptor = client.open_object(BUCKET_NAME, OBJECT_NAME).send().await?;

        // Assert: successfully opened
        assert_eq!(descriptor.object().name, OBJECT_NAME);
        assert_eq!(descriptor.object().generation, OBJECT_GENERATION);

        Ok(())
    }

    /// Verifies handling when the server completes a range without delivering the
    /// expected data, ensuring the short read returns an error.
    #[tokio::test]
    async fn on_complete_without_data() -> anyhow::Result<()> {
        // Arrange: Server completes range with range_end: true without returning the requested data
        let mut mock = MockStorage::new();
        mock.expect_bidi_read_object().return_once(|request| {
            let (_, _, mut requests) = request.into_parts();
            let (tx, rx) = tokio::sync::mpsc::channel(1);

            tokio::spawn(async move {
                let first = recv_request(&mut requests).await;
                let range = single_range(&first);

                // Send initial metadata response with range_end: true but NO checksummed data
                let response = BidiReadObjectResponse {
                    metadata: test_metadata(),
                    object_data_ranges: vec![ObjectRangeData {
                        read_range: Some(ProtoRange {
                            read_id: range.read_id,
                            read_offset: range.read_offset,
                            read_length: 0,
                        }),
                        range_end: true,
                        checksummed_data: None,
                    }],
                    ..BidiReadObjectResponse::default()
                };
                tx.send(Ok(response))
                    .await
                    .expect("failed to send response");
            });
            Ok(TonicResponse::from(rx))
        });

        let (client, _server) = start_test_server(mock).await?;

        // Act: Request 10 bytes, but server completes with range_end: true and 0 bytes
        let result = client
            .open_object(BUCKET_NAME, OBJECT_NAME)
            .with_read_resume_policy(NeverResume)
            .send_and_read(ReadRange::segment(0, 10))
            .await;

        // Assert: Client detects short read and returns an error
        let err = result.expect_err("expected error when server completes without requested data");
        let err_str = err.to_string();
        assert!(
            err_str.contains("missing 10 bytes"),
            "expected missing bytes error, got: {err_str}"
        );

        Ok(())
    }

    /// Verifies fast open by bundling read ranges along with the initial `ReadObjectSpec` request.
    #[tokio::test]
    async fn fast_open_read_session() -> anyhow::Result<()> {
        // Arrange
        const READ_LEN: usize = 16;
        let (observed_tx, observed_rx) = tokio::sync::oneshot::channel::<BidiReadObjectRequest>();

        let mut mock = MockStorage::new();
        mock.expect_bidi_read_object().return_once(move |request| {
            let (_, _, mut requests) = request.into_parts();
            let (tx, rx) = tokio::sync::mpsc::channel(1);

            tokio::spawn(async move {
                let first = recv_request(&mut requests).await;

                // Fast open: verify client bundled spec and read_ranges together in the initial message
                assert!(
                    first.read_object_spec.is_some(),
                    "fast open request must contain read_object_spec"
                );
                assert!(
                    !first.read_ranges.is_empty(),
                    "fast open request must bundle read_ranges in the first message"
                );

                let range = single_range(&first);
                let _ = observed_tx.send(first);

                // Fast open: server responds with metadata AND range data in the single initial message
                let payload = slice_range(OBJECT_CONTENT, &range).to_vec();
                tx.send(Ok(initial_response_with_data(range, payload, true)))
                    .await
                    .expect("failed to send combined response");
            });
            Ok(TonicResponse::from(rx))
        });

        let (client, _server) = start_test_server(mock).await?;

        // Act: send_and_read initiates fast open
        let (descriptor, reader) = client
            .open_object(BUCKET_NAME, OBJECT_NAME)
            .send_and_read(ReadRange::segment(0, READ_LEN as u64))
            .await?;

        let payload = read_all_bytes(reader).await?;

        // Assert
        assert_eq!(payload, &OBJECT_CONTENT[..READ_LEN]);
        assert_eq!(descriptor.object().name, OBJECT_NAME);
        assert_eq!(descriptor.object().generation, OBJECT_GENERATION);

        let observed = observed_rx.await?;
        assert_eq!(observed.read_ranges.len(), 1);
        assert_eq!(observed.read_ranges[0].read_offset, 0);
        assert_eq!(observed.read_ranges[0].read_length, READ_LEN as i64);

        Ok(())
    }

    /// Verifies non-retryable error classification, confirming permanent errors fail
    /// immediately on the first attempt without retrying.
    #[tokio::test]
    async fn non_retryable_error() -> anyhow::Result<()> {
        // Arrange: Server returns permanent NotFound error
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_clone = attempts.clone();

        let mut mock = MockStorage::new();
        mock.expect_bidi_read_object().returning(move |_| {
            attempts_clone.fetch_add(1, Ordering::SeqCst);
            Err(TonicStatus::not_found("object not found"))
        });

        let (client, _server) = start_test_server(mock).await?;

        // Act: open object
        let result = client
            .open_object(BUCKET_NAME, "nonexistent-object")
            .send()
            .await;

        // Assert: fails immediately without retry
        assert!(result.is_err(), "expected open_object to fail on NotFound");
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            1,
            "non-retryable error must not be retried"
        );

        Ok(())
    }
}
