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

use bytes::Bytes;
use google_cloud_auth::signer::{Result as SignResult, Signer, SigningProvider};
use google_cloud_storage::builder::storage::PostPolicyV4Builder;
use google_cloud_storage::post_policy_v4::{PolicyV4Fields, PostPolicyV4Condition};
use google_cloud_storage::signed_url::UrlStyle;
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
struct DeterministicSigner;

impl SigningProvider for DeterministicSigner {
    async fn client_email(&self) -> SignResult<String> {
        Ok("post-policy-test@example.com".to_string())
    }

    async fn sign(&self, content: &[u8]) -> SignResult<Bytes> {
        let mut signature = [0_u8; 32];
        for (index, byte) in content.iter().enumerate() {
            signature[index % signature.len()] ^= byte;
            signature[(index * 7) % signature.len()] =
                signature[(index * 7) % signature.len()].wrapping_add(*byte);
        }
        Ok(Bytes::copy_from_slice(&signature))
    }
}

fn test_signer() -> Signer {
    Signer::from(DeterministicSigner)
}

fn policy_builder(object: impl Into<String>) -> PostPolicyV4Builder {
    PostPolicyV4Builder::for_object("projects/_/buckets/post-policy-hardening", object)
        .with_expiration(Duration::from_secs(60))
}

#[tokio::test]
async fn post_policy_mutation_fuzz_public_api() -> anyhow::Result<()> {
    let signer = test_signer();
    let objects = [
        "",
        "a",
        "folder/object.txt",
        "$test-object-é",
        "emoji-😀",
        "line\nbreak",
        "quote\"backslash\\",
        " spaces ",
        "../relative",
        "%2F",
    ];
    let metadata_values = [
        "",
        "plain",
        "$test-object-é-metadata",
        "emoji-😀-metadata",
        "line\nbreak",
        "quote\"backslash\\",
    ];
    let endpoints = [
        None,
        Some("storage.googleapis.com"),
        Some("https://private.googleapis.com"),
        Some("http://localhost:9090"),
    ];
    let styles = [
        UrlStyle::PathStyle,
        UrlStyle::VirtualHostedStyle,
        UrlStyle::BucketBoundHostname,
    ];

    for (object_index, object) in objects.iter().enumerate() {
        for (metadata_index, metadata) in metadata_values.iter().enumerate() {
            for endpoint in endpoints {
                for style in styles {
                    let mut builder = policy_builder(*object)
                        .with_url_style(style)
                        .with_condition(PostPolicyV4Condition::starts_with(
                            "$key",
                            object.chars().take(2).collect::<String>(),
                        ))
                        .with_condition(PostPolicyV4Condition::content_length_range(
                            object_index as u64,
                            object_index as u64 + metadata_index as u64 + 1024,
                        ))
                        .with_fields(
                            PolicyV4Fields::new()
                                .with_content_type("application/octet-stream")
                                .with_metadata("x-goog-meta-fuzz", *metadata),
                        );
                    if let Some(endpoint) = endpoint {
                        builder = builder.with_endpoint(endpoint);
                    }

                    let policy = builder.sign_with(&signer).await?;
                    assert!(
                        policy.url.starts_with("http://") || policy.url.starts_with("https://"),
                        "{policy:?}"
                    );
                    if object.is_empty() {
                        assert!(!policy.fields.contains_key("key"), "{policy:?}");
                    } else {
                        assert_eq!(policy.fields.get("key").map(String::as_str), Some(*object));
                    }
                    assert!(policy.fields.contains_key("policy"), "{policy:?}");
                    assert!(policy.fields.contains_key("x-goog-signature"), "{policy:?}");
                    if metadata.is_empty() {
                        assert!(
                            !policy.fields.contains_key("x-goog-meta-fuzz"),
                            "{policy:?}"
                        );
                    } else {
                        assert_eq!(
                            policy.fields.get("x-goog-meta-fuzz").map(String::as_str),
                            Some(*metadata)
                        );
                    }
                }
            }
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn post_policy_concurrent_generation_is_stable() -> anyhow::Result<()> {
    let tasks = (0..24)
        .map(|task| {
            let signer = test_signer();
            tokio::spawn(async move {
                for iteration in 0..48 {
                    let object = format!("task-{task}/object-{iteration}");
                    let policy = policy_builder(object.clone())
                        .with_fields(
                            PolicyV4Fields::new()
                                .with_cache_control("no-cache")
                                .with_metadata("x-goog-meta-task", task.to_string()),
                        )
                        .sign_with(&signer)
                        .await?;
                    assert_eq!(policy.fields.get("key"), Some(&object));
                    assert!(policy.fields["policy"].len() > object.len());
                }
                Ok::<_, anyhow::Error>(())
            })
        })
        .collect::<Vec<_>>();

    for task in tasks {
        task.await??;
    }

    Ok(())
}

#[tokio::test]
async fn post_policy_signing_latency_smoke() -> anyhow::Result<()> {
    let signer = test_signer();
    let start = Instant::now();
    for iteration in 0..256 {
        let policy = policy_builder(format!("latency/object-{iteration}"))
            .with_fields(
                PolicyV4Fields::new()
                    .with_content_disposition("attachment")
                    .with_metadata("x-goog-meta-latency", iteration.to_string()),
            )
            .sign_with(&signer)
            .await?;
        assert!(policy.fields.contains_key("x-goog-signature"));
    }
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(10),
        "local policy generation should not be pathologically slow: {elapsed:?}"
    );

    Ok(())
}

#[tokio::test]
async fn post_policy_invalid_inputs_return_errors_instead_of_panics() -> anyhow::Result<()> {
    let signer = test_signer();

    for bucket in ["", "bucket", "projects/_/buckets/"] {
        let err = PostPolicyV4Builder::for_object(bucket, "object")
            .with_expiration(Duration::from_secs(60))
            .sign_with(&signer)
            .await
            .unwrap_err();
        assert!(err.is_invalid_parameter(), "{err:?}");
    }

    for endpoint in ["http://", "https://", "://bad-endpoint"] {
        let err = policy_builder("object")
            .with_endpoint(endpoint)
            .sign_with(&signer)
            .await
            .unwrap_err();
        assert!(err.is_invalid_parameter(), "{err:?}");
    }

    let err = policy_builder("object")
        .with_fields(PolicyV4Fields::new().with_metadata("x-not-goog-meta", "value"))
        .sign_with(&signer)
        .await
        .unwrap_err();
    assert!(err.is_invalid_parameter(), "{err:?}");

    Ok(())
}

#[test]
fn post_policy_signing_future_can_be_dropped_without_polling() {
    let signer = test_signer();
    let future = policy_builder("dropped-future").sign_with(&signer);
    drop(future);
}

#[test]
fn post_policy_implementation_contains_no_unsafe_blocks() {
    const SOURCE: &str = include_str!("../src/storage/post_policy_v4.rs");
    assert!(
        !SOURCE.contains("unsafe"),
        "post policy implementation should remain safe Rust"
    );
}
