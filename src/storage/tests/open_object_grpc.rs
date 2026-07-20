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

//! Transport-level contract tests for Storage `open_object`.

use anyhow::Context as _;
use gaxi::grpc::tonic::{Response as TonicResponse, Result as TonicResult};
use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
use google_cloud_storage::client::Storage;
use google_cloud_storage::model_ext::ReadRange;
use google_cloud_storage::read_object::ReadObjectResponse;
use pretty_assertions::assert_eq;
use storage_grpc_mock::google::storage::v2::{
    BidiReadObjectResponse, ChecksummedData, Object as ProtoObject, ObjectRangeData,
    ReadRange as ProtoRange,
};
use storage_grpc_mock::{MockStorage, start};

const BIND_ADDRESS: &str = "127.0.0.1:0";
const BUCKET_NAME: &str = "projects/_/buckets/test-bucket";
const OBJECT_NAME: &str = "test-object";
const OBJECT_GENERATION: i64 = 123456;

const ERR_STREAM_CLOSED_PREMATURELY: &str = "gRPC stream closed before the request was received";
const ERR_RECV_ERROR: &str = "error while reading the request";

#[tokio::test]
async fn descriptor_sends_ranges_after_open_and_reads_multiple_messages() -> anyhow::Result<()> {
    // Arrange
    const OBJECT_CONTENT: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

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
            tx.send(Ok(initial_response(None)))
                .await
                .expect("failed to send initial response");

            // Simulate the client requesting two distinct ranges sequentially
            for _ in 0..2 {
                let mut request = requests
                    .recv()
                    .await
                    .expect(ERR_STREAM_CLOSED_PREMATURELY)
                    .expect(ERR_RECV_ERROR);

                // Subsequent requests on the open stream must NOT send the object spec
                assert!(request.read_object_spec.is_none(), "{request:?}");

                assert_eq!(
                    request.read_ranges.len(),
                    1,
                    "expected exactly one range: {request:?}"
                );
                let range = request.read_ranges.remove(0);

                let start = range.read_offset as usize;
                let end = start + range.read_length as usize;
                let payload = OBJECT_CONTENT[start..end].to_vec();

                // Return the requested payload slice to the client
                tx.send(Ok(data_response(range, payload, true)))
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
        read_all_bytes(&mut descriptor.read_range(ReadRange::segment(10, 5)).await).await?;
    let second_payload =
        read_all_bytes(&mut descriptor.read_range(ReadRange::segment(20, 6)).await).await?;

    // Assert
    assert_eq!(first_payload, &OBJECT_CONTENT[10..15]);
    assert_eq!(second_payload, &OBJECT_CONTENT[20..26]);
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

async fn read_all_bytes(stream: &mut ReadObjectResponse) -> anyhow::Result<Vec<u8>> {
    let mut payload = Vec::new();
    while let Some(chunk) = stream.next().await {
        payload.extend_from_slice(&chunk.context("range read failed")?);
    }
    Ok(payload)
}

fn initial_response(payload: Option<Vec<u8>>) -> BidiReadObjectResponse {
    let object_data_ranges = payload
        .map(|data| ObjectRangeData {
            read_range: Some(ProtoRange {
                // Arbitrarily set read_id to 1
                read_id: 1,
                ..ProtoRange::default()
            }),
            range_end: true,
            checksummed_data: Some(ChecksummedData {
                content: data,
                crc32c: None,
            }),
        })
        .into_iter()
        .collect();
    BidiReadObjectResponse {
        metadata: Some(ProtoObject {
            bucket: BUCKET_NAME.to_string(),
            name: OBJECT_NAME.to_string(),
            generation: OBJECT_GENERATION,
            ..ProtoObject::default()
        }),
        object_data_ranges,
        ..BidiReadObjectResponse::default()
    }
}

fn data_response(
    mut range: ProtoRange,
    payload: Vec<u8>,
    range_end: bool,
) -> BidiReadObjectResponse {
    range.read_length = payload.len() as i64;
    BidiReadObjectResponse {
        object_data_ranges: vec![ObjectRangeData {
            read_range: Some(range),
            range_end,
            checksummed_data: Some(ChecksummedData {
                content: payload,
                crc32c: None,
            }),
        }],
        ..BidiReadObjectResponse::default()
    }
}
