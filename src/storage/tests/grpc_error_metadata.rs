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

use anyhow::Result;
use bytes::Bytes;
use gaxi::grpc::tonic::{Code, MetadataMap, Status};
use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
use google_cloud_storage::client::Storage;
use pretty_assertions::assert_eq;
use prost::Message as _;
use storage_grpc_mock::google::rpc::Status as RpcStatus;
use storage_grpc_mock::google::storage::v2::{BidiReadHandle, BidiReadObjectRedirectedError};
use storage_grpc_mock::{MockStorage, start};

#[tokio::test]
async fn open_object_error_exposes_grpc_metadata_in_headers() -> Result<()> {
    // Arrange
    const BIND_ADDRESS: &str = "127.0.0.1:0";
    const BUCKET: &str = "projects/_/buckets/error-metadata-bucket";
    const OBJECT: &str = "error-metadata-object";
    const CUSTOM_METADATA_KEY: &str = "x-parity-probe";
    const CUSTOM_METADATA_VALUE: &str = "present";

    // Construct BidiRead error details
    let detail = BidiReadObjectRedirectedError {
        routing_token: Some("non-aborted-detail".to_string()),
        read_handle: Some(BidiReadHandle {
            handle: b"non-aborted-handle".to_vec(),
        }),
    };
    let wire_status = RpcStatus {
        code: Code::PermissionDenied as i32,
        message: "metadata parity probe".to_string(),
        details: vec![prost_types::Any::from_msg(&detail)?],
    };

    let mut metadata = MetadataMap::new();
    metadata.insert(CUSTOM_METADATA_KEY, CUSTOM_METADATA_VALUE.parse()?);

    let status = Status::with_details_and_metadata(
        Code::PermissionDenied,
        "metadata parity probe",
        Bytes::from(wire_status.encode_to_vec()),
        metadata,
    );

    // Set up the mock server with the error status
    let mut mock = MockStorage::new();
    mock.expect_bidi_read_object()
        .return_once(move |_| Err(status));
    let (endpoint, server) = start(BIND_ADDRESS, mock).await?;

    let client = Storage::builder()
        .with_credentials(Anonymous::new().build())
        .with_endpoint(endpoint)
        .build()
        .await?;

    // Act
    let error = client
        .open_object(BUCKET, OBJECT)
        .send()
        .await
        .expect_err("mock must return a permission error");

    // Assert
    assert!(error.status().is_some());
    let headers = error.http_headers().expect("error response headers");
    assert_eq!(
        headers
            .get(CUSTOM_METADATA_KEY)
            .and_then(|value| value.to_str().ok()),
        Some(CUSTOM_METADATA_VALUE)
    );

    // Clean up
    server.abort();
    let _ = server.await;
    Ok(())
}
