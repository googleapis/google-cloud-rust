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

use gaxi::grpc::tonic::{MetadataMap, Response, Status};
use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
use google_cloud_gax::error::rpc::Code;
use google_cloud_gax::options::RequestOptionsBuilder;
use google_cloud_storage::client::StorageControl;
use std::sync::{Arc, Mutex};
use storage_grpc_mock::google::storage::v2::Object;
use storage_grpc_mock::{MockStorage, start};
use tokio::task::JoinHandle;

const BUCKET_NAME: &str = "projects/_/buckets/test-bucket";
const OBJECT_NAME: &str = "test-object";
const IDEMPOTENCY_TOKEN_HEADER: &str = "x-goog-gcs-idempotency-token";

type Tokens = Arc<Mutex<Vec<Option<String>>>>;

/// Returns the idempotency token in the request metadata, if any.
fn token(metadata: &MetadataMap) -> Option<String> {
    metadata
        .get(IDEMPOTENCY_TOKEN_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
}

/// Starts `mock` and returns a client connected to it.
async fn client(mock: MockStorage) -> anyhow::Result<(StorageControl, JoinHandle<()>)> {
    let (endpoint, server) = start("127.0.0.1:0", mock).await?;
    let client = StorageControl::builder()
        .with_endpoint(endpoint)
        .with_credentials(Anonymous::default().build())
        .build()
        .await?;
    Ok((client, server))
}

#[tokio::test]
async fn delete_object_with_generation_sends_idempotency_token() -> anyhow::Result<()> {
    let tokens = Tokens::default();
    let captured = tokens.clone();
    let mut mock = MockStorage::new();
    mock.expect_delete_object().return_once(move |request| {
        captured.lock().unwrap().push(token(request.metadata()));
        Ok(Response::new(()))
    });

    let (client, _server) = client(mock).await?;
    client
        .delete_object()
        .set_bucket(BUCKET_NAME)
        .set_object(OBJECT_NAME)
        .set_generation(12345)
        .send()
        .await?;

    let tokens = tokens.lock().unwrap();
    let token = tokens[0]
        .as_deref()
        .expect("generation > 0 must send a token");
    assert!(uuid::Uuid::parse_str(token).is_ok(), "{token}");
    Ok(())
}

#[tokio::test]
async fn delete_object_unconditioned_omits_idempotency_token() -> anyhow::Result<()> {
    let tokens = Tokens::default();
    let captured = tokens.clone();
    let mut mock = MockStorage::new();
    mock.expect_delete_object().return_once(move |request| {
        captured.lock().unwrap().push(token(request.metadata()));
        Ok(Response::new(()))
    });

    let (client, _server) = client(mock).await?;
    client
        .delete_object()
        .set_bucket(BUCKET_NAME)
        .set_object(OBJECT_NAME)
        .send()
        .await?;

    assert_eq!(*tokens.lock().unwrap(), vec![None]);
    Ok(())
}

#[tokio::test]
async fn delete_object_unconditioned_does_not_retry() -> anyhow::Result<()> {
    let tokens = Tokens::default();
    let captured = tokens.clone();
    let mut mock = MockStorage::new();
    mock.expect_delete_object()
        .times(1)
        .return_once(move |request| {
            captured.lock().unwrap().push(token(request.metadata()));
            Err(Status::unavailable("try again"))
        });

    let (client, _server) = client(mock).await?;
    let err = client
        .delete_object()
        .set_bucket(BUCKET_NAME)
        .set_object(OBJECT_NAME)
        .send()
        .await
        .expect_err("unconditioned DeleteObject must not be retried");
    assert_eq!(
        err.status().map(|s| s.code),
        Some(Code::Unavailable),
        "{err:?}"
    );
    assert_eq!(*tokens.lock().unwrap(), vec![None]);
    Ok(())
}

#[tokio::test]
async fn delete_object_retry_reuses_idempotency_token() -> anyhow::Result<()> {
    let tokens = Tokens::default();
    let first = tokens.clone();
    let second = tokens.clone();
    let mut mock = MockStorage::new();
    let mut seq = mockall::Sequence::new();
    mock.expect_delete_object()
        .times(1)
        .in_sequence(&mut seq)
        .returning(move |request| {
            first.lock().unwrap().push(token(request.metadata()));
            Err(Status::unavailable("try again"))
        });
    mock.expect_delete_object()
        .times(1)
        .in_sequence(&mut seq)
        .returning(move |request| {
            second.lock().unwrap().push(token(request.metadata()));
            Ok(Response::new(()))
        });

    let (client, _server) = client(mock).await?;
    client
        .delete_object()
        .set_bucket(BUCKET_NAME)
        .set_object(OBJECT_NAME)
        .set_if_generation_match(54321)
        .send()
        .await?;

    let tokens = tokens.lock().unwrap();
    assert_eq!(tokens.len(), 2, "{tokens:?}");
    assert!(tokens[0].is_some(), "{tokens:?}");
    assert_eq!(
        tokens[0], tokens[1],
        "token must be identical across retries"
    );
    Ok(())
}

#[tokio::test]
async fn delete_object_override_idempotency_false_omits_token_and_does_not_retry()
-> anyhow::Result<()> {
    let tokens = Tokens::default();
    let captured = tokens.clone();
    let mut mock = MockStorage::new();
    mock.expect_delete_object()
        .times(1)
        .return_once(move |request| {
            captured.lock().unwrap().push(token(request.metadata()));
            Err(Status::unavailable("try again"))
        });

    let (client, _server) = client(mock).await?;
    let err = client
        .delete_object()
        .set_bucket(BUCKET_NAME)
        .set_object(OBJECT_NAME)
        .set_if_generation_match(54321)
        .with_idempotency(false)
        .send()
        .await
        .expect_err("with_idempotency(false) must disable retries");
    assert_eq!(
        err.status().map(|s| s.code),
        Some(Code::Unavailable),
        "{err:?}"
    );
    assert_eq!(*tokens.lock().unwrap(), vec![None]);
    Ok(())
}

// A conditioned `DeleteObject` is idempotent and therefore retryable, which makes
// it the right case to verify that `FAILED_PRECONDITION` (HTTP 412) is treated
// as a permanent error and fails on the first attempt.
#[tokio::test]
async fn delete_object_precondition_failure_is_not_retried() -> anyhow::Result<()> {
    let mut mock = MockStorage::new();
    mock.expect_delete_object()
        .times(1)
        .returning(|_| Err(Status::failed_precondition("generation mismatch")));

    let (client, _server) = client(mock).await?;
    let err = client
        .delete_object()
        .set_bucket(BUCKET_NAME)
        .set_object(OBJECT_NAME)
        .set_if_generation_match(54321)
        .send()
        .await
        .expect_err("FAILED_PRECONDITION is permanent and must not be retried");
    assert_eq!(
        err.status().map(|s| s.code),
        Some(Code::FailedPrecondition),
        "{err:?}"
    );
    Ok(())
}

// Reads are always idempotent: they are retried on transient errors, but never
// carry an idempotency token.
#[tokio::test]
async fn get_object_retries_without_idempotency_token() -> anyhow::Result<()> {
    let tokens = Tokens::default();
    let first = tokens.clone();
    let second = tokens.clone();
    let mut mock = MockStorage::new();
    let mut seq = mockall::Sequence::new();
    mock.expect_get_object()
        .times(1)
        .in_sequence(&mut seq)
        .returning(move |request| {
            first.lock().unwrap().push(token(request.metadata()));
            Err(Status::unavailable("try again"))
        });
    mock.expect_get_object()
        .times(1)
        .in_sequence(&mut seq)
        .returning(move |request| {
            second.lock().unwrap().push(token(request.metadata()));
            Ok(Response::new(Object {
                name: OBJECT_NAME.to_string(),
                bucket: BUCKET_NAME.to_string(),
                generation: 1,
                ..Default::default()
            }))
        });

    let (client, _server) = client(mock).await?;
    client
        .get_object()
        .set_bucket(BUCKET_NAME)
        .set_object(OBJECT_NAME)
        .send()
        .await?;

    assert_eq!(*tokens.lock().unwrap(), vec![None, None]);
    Ok(())
}
