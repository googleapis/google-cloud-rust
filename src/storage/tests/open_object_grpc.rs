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
            let first = requests
                .recv()
                .await
                .expect(ERR_STREAM_CLOSED_PREMATURELY)
                .expect(ERR_RECV_ERROR);

            // Initial message should contain the object spec and range request
            assert!(first.read_object_spec.is_some(), "{first:?}");

            let [range] = first
                .read_ranges
                .try_into()
                .expect("expected exactly one range");

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
    let (endpoint, _server) = start(BIND_ADDRESS, mock).await?;
    let client = make_client(endpoint).await?;

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
            let open = requests
                .recv()
                .await
                .expect(ERR_STREAM_CLOSED_PREMATURELY)
                .expect(ERR_RECV_ERROR);

            // Initial message should contain the object spec and no range requests
            assert!(open.read_object_spec.is_some(), "{open:?}");
            assert!(open.read_ranges.is_empty(), "{open:?}");

            // Initial response contains only the object metadata
            tx.send(Ok(initial_response()))
                .await
                .expect("failed to send initial response");

            // Simulate the client requesting two distinct ranges sequentially
            for _ in 0..2 {
                let request = requests
                    .recv()
                    .await
                    .expect(ERR_STREAM_CLOSED_PREMATURELY)
                    .expect(ERR_RECV_ERROR);

                // Subsequent requests on the open stream must NOT send the object spec
                assert!(request.read_object_spec.is_none(), "{request:?}");

                let [range] = request
                    .read_ranges
                    .try_into()
                    .expect("expected exactly one range");

                let payload = slice_range(OBJECT_CONTENT, &range).to_vec();

                // Return the requested payload slice to the client
                tx.send(Ok(data_only_response(range, payload, true)))
                    .await
                    .expect("failed to send data response");
            }
        });
        Ok(TonicResponse::from(rx))
    });
    let (endpoint, _server) = start(BIND_ADDRESS, mock).await?;
    let client = make_client(endpoint).await?;

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
                let first = requests
                    .recv()
                    .await
                    .expect(ERR_STREAM_CLOSED_PREMATURELY)
                    .expect(ERR_RECV_ERROR);
                let [range] = first
                    .read_ranges
                    .clone()
                    .try_into()
                    .expect("expected exactly one range");

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
                let first = requests
                    .recv()
                    .await
                    .expect(ERR_STREAM_CLOSED_PREMATURELY)
                    .expect(ERR_RECV_ERROR);
                let [range] = first
                    .read_ranges
                    .clone()
                    .try_into()
                    .expect("expected exactly one range");

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
    let (endpoint, _server) = start(BIND_ADDRESS, mock).await?;
    let client = make_client(endpoint).await?;

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
