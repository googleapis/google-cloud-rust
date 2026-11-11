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

//! End-to-end integration tests for Enterprise storage classes.

use crate::read_all;
use anyhow::Result;
use futures::FutureExt as _;
use google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder;
use google_cloud_gax::options::RequestOptionsBuilder as _;
use google_cloud_gax::retry_policy::RetryPolicyExt as _;
use google_cloud_storage::builder_ext::RewriteObjectExt as _;
use google_cloud_storage::client::{Storage, StorageControl};
use google_cloud_storage::model::bucket::iam_config::UniformBucketLevelAccess;
use google_cloud_storage::model::bucket::{HierarchicalNamespace, IamConfig};
use google_cloud_storage::model::{Bucket, Object};
use google_cloud_storage::model_ext::ReadRange;
use google_cloud_storage::retry_policy::RetryableErrors;
use google_cloud_test_utils::resource_names::random_bucket_id;
use google_cloud_test_utils::runtime_config::project_id;
use google_cloud_wkt::FieldMask;
use std::panic::AssertUnwindSafe;
use std::time::Duration;

const STANDARD_ENTERPRISE: &str = "STANDARD_ENTERPRISE";
const LR_STANDARD_ENTERPRISE: &str = "LR_STANDARD_ENTERPRISE";
const STANDARD_FLEXBACKUP_ENTERPRISE: &str = "STANDARD_FLEXBACKUP_ENTERPRISE";
const PAYLOAD: &str = "enterprise storage class end-to-end verification payload";

/// Executes end-to-end integration tests for GCS Enterprise storage classes.
///
/// # Example
/// ```
/// # async fn sample() -> anyhow::Result<()> {
/// integration_tests_storage::enterprise_storage_class::run().await?;
/// # Ok(())
/// # }
/// ```
///
/// Exercises bucket creation, default storage class updates, object uploads,
/// HTTP and gRPC object reads, and in-place storage class rewrites across
/// `STANDARD_ENTERPRISE` (flat and HNS), `LR_STANDARD_ENTERPRISE`, and
/// `STANDARD_FLEXBACKUP_ENTERPRISE` using [Storage] and [StorageControl].
///
/// Requires `GOOGLE_CLOUD_PROJECT` (and `GOOGLE_CLOUD_QUOTA_PROJECT` when local
/// credentials specify another quota project) to reference a project
/// allowlisted for Enterprise storage classes (such as `rust-sdk-testing`).
/// Optional `GOOGLE_CLOUD_TEST_HTTP_ENDPOINT` and
/// `GOOGLE_CLOUD_TEST_GRPC_ENDPOINT` environment variables override the default
/// production endpoints.
pub async fn run() -> Result<()> {
    let project = project_id()?;
    let clients = Clients::new().await?;
    storage_samples::cleanup_stale_buckets(&clients.control, &project).await;

    // 1. Flat regional STANDARD_ENTERPRISE: bucket update, inherited/explicit uploads,
    //    HTTP/gRPC reads, and in-place storage class rewrites.
    let flat_standard = BucketSpec {
        label: "Regional STANDARD_ENTERPRISE (flat)",
        location: "us-central1",
        storage_class: STANDARD_ENTERPRISE,
        hns: false,
    };
    with_bucket(&clients, &project, flat_standard, async |bucket| {
        verify_get_bucket(&clients.control, &bucket.name, STANDARD_ENTERPRISE).await?;
        verify_update_default_storage_class(
            &clients.control,
            &bucket.name,
            LR_STANDARD_ENTERPRISE,
            STANDARD_ENTERPRISE,
        )
        .await?;
        verify_inherited_object_upload_and_reads(
            &clients,
            &bucket.name,
            "inherited-standard-enterprise.txt",
            STANDARD_ENTERPRISE,
        )
        .await?;
        verify_explicit_object_storage_class(
            &clients,
            &bucket.name,
            "explicit-lr-enterprise.txt",
            LR_STANDARD_ENTERPRISE,
        )
        .await?;
        verify_rewrite_storage_class(
            &clients.control,
            &bucket.name,
            "inherited-standard-enterprise.txt",
            LR_STANDARD_ENTERPRISE,
            STANDARD_ENTERPRISE,
        )
        .await
    })
    .await?;

    // 2. HNS regional STANDARD_ENTERPRISE: BidiReadObject descriptor and range reader.
    let hns_standard = BucketSpec {
        label: "Regional STANDARD_ENTERPRISE (HNS)",
        location: "us-central1",
        storage_class: STANDARD_ENTERPRISE,
        hns: true,
    };
    with_bucket(&clients, &project, hns_standard, async |bucket| {
        let fetched =
            verify_get_bucket(&clients.control, &bucket.name, STANDARD_ENTERPRISE).await?;
        assert!(
            fetched
                .hierarchical_namespace
                .as_ref()
                .is_some_and(|h| h.enabled),
            "expected HNS enabled on bucket {fetched:?}"
        );
        verify_hns_bidi_open_and_read(
            &clients,
            &bucket.name,
            "hns/enterprise-bidi.txt",
            STANDARD_ENTERPRISE,
        )
        .await
    })
    .await?;

    // 3. Flat regional LR_STANDARD_ENTERPRISE: bucket CRUD and inherited object CRUD.
    let flat_lr = BucketSpec {
        label: "Regional LR_STANDARD_ENTERPRISE (flat)",
        location: "us-central1",
        storage_class: LR_STANDARD_ENTERPRISE,
        hns: false,
    };
    with_bucket(&clients, &project, flat_lr, async |bucket| {
        verify_get_bucket(&clients.control, &bucket.name, LR_STANDARD_ENTERPRISE).await?;
        verify_inherited_object_upload_and_reads(
            &clients,
            &bucket.name,
            "inherited-lr-enterprise.txt",
            LR_STANDARD_ENTERPRISE,
        )
        .await
    })
    .await?;

    // 4. Flat multi-region STANDARD_FLEXBACKUP_ENTERPRISE (`us`): bucket and object CRUD.
    let flat_flexbackup = BucketSpec {
        label: "Multi-region STANDARD_FLEXBACKUP_ENTERPRISE (flat)",
        location: "us",
        storage_class: STANDARD_FLEXBACKUP_ENTERPRISE,
        hns: false,
    };
    with_bucket(&clients, &project, flat_flexbackup, async |bucket| {
        verify_get_bucket(
            &clients.control,
            &bucket.name,
            STANDARD_FLEXBACKUP_ENTERPRISE,
        )
        .await?;
        verify_inherited_object_upload_and_reads(
            &clients,
            &bucket.name,
            "inherited-flexbackup-enterprise.txt",
            STANDARD_FLEXBACKUP_ENTERPRISE,
        )
        .await
    })
    .await?;

    tracing::info!("completed all Enterprise storage class integration test scenarios");
    Ok(())
}

struct Clients {
    http: Storage,
    grpc: Storage,
    control: StorageControl,
}

impl Clients {
    async fn new() -> Result<Self> {
        let http_endpoint = non_empty_env("GOOGLE_CLOUD_TEST_HTTP_ENDPOINT");
        let grpc_endpoint = non_empty_env("GOOGLE_CLOUD_TEST_GRPC_ENDPOINT");

        let mut http_builder = Storage::builder();
        if let Some(ref endpoint) = http_endpoint {
            tracing::info!("using HTTP Storage endpoint override: {endpoint}");
            http_builder = http_builder.with_endpoint(endpoint);
        }
        let http = http_builder.build().await?;

        let mut grpc_builder = Storage::builder();
        if let Some(ref endpoint) = grpc_endpoint {
            tracing::info!("using gRPC Storage endpoint override: {endpoint}");
            grpc_builder = grpc_builder.with_endpoint(endpoint);
        }
        let grpc = grpc_builder.build().await?;

        let backoff = ExponentialBackoffBuilder::new()
            .with_initial_delay(Duration::from_secs(2))
            .with_maximum_delay(Duration::from_secs(8))
            .build()?;
        let mut control_builder = StorageControl::builder()
            .with_backoff_policy(backoff)
            .with_retry_policy(RetryableErrors.with_attempt_limit(5));
        if let Some(ref endpoint) = grpc_endpoint {
            tracing::info!("using StorageControl endpoint override: {endpoint}");
            control_builder = control_builder.with_endpoint(endpoint);
        }
        let control = control_builder.build().await?;

        Ok(Self {
            http,
            grpc,
            control,
        })
    }
}

fn non_empty_env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.trim().is_empty())
}

#[derive(Clone, Copy, Debug)]
struct BucketSpec {
    label: &'static str,
    location: &'static str,
    storage_class: &'static str,
    hns: bool,
}

async fn with_bucket<F>(clients: &Clients, project: &str, spec: BucketSpec, f: F) -> Result<()>
where
    F: AsyncFnOnce(&Bucket) -> Result<()>,
{
    // Arrange.
    let bucket_id = random_bucket_id();
    tracing::info!(
        "creating Enterprise test bucket ({}) id={bucket_id}",
        spec.label
    );

    // Act.
    let created = create_enterprise_bucket(&clients.control, project, &bucket_id, spec).await?;

    // Assert.
    let expected_name = format!("projects/_/buckets/{bucket_id}");
    let run_result = AssertUnwindSafe(async {
        assert_eq!(created.name, expected_name);
        assert_eq!(created.storage_class, spec.storage_class);
        assert!(
            created.location.eq_ignore_ascii_case(spec.location),
            "expected location {}, got {}",
            spec.location,
            created.location
        );
        f(&created).await
    })
    .catch_unwind()
    .await;

    if let Err(err) = storage_samples::cleanup_bucket(
        clients.control.clone(),
        created.name.clone(),
        project.to_string(),
    )
    .await
    {
        tracing::warn!(
            "failed to delete Enterprise test bucket {} during teardown: {err:?}",
            created.name
        );
    }

    match run_result {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

async fn create_enterprise_bucket(
    control: &StorageControl,
    project: &str,
    bucket_id: &str,
    spec: BucketSpec,
) -> Result<Bucket> {
    let mut bucket = Bucket::new()
        .set_project(format!("projects/{project}"))
        .set_location(spec.location)
        .set_storage_class(spec.storage_class)
        .set_labels([("integration-test", "true")])
        .set_iam_config(
            IamConfig::new()
                .set_uniform_bucket_level_access(UniformBucketLevelAccess::new().set_enabled(true)),
        );
    if spec.hns {
        bucket = bucket.set_hierarchical_namespace(HierarchicalNamespace::new().set_enabled(true));
    }

    let created = control
        .create_bucket()
        .set_parent("projects/_")
        .set_bucket_id(bucket_id)
        .set_bucket(bucket)
        .with_idempotency(true)
        .send()
        .await?;
    Ok(created)
}

async fn verify_get_bucket(
    control: &StorageControl,
    bucket_name: &str,
    expected_storage_class: &str,
) -> Result<Bucket> {
    // Act.
    let got = control.get_bucket().set_name(bucket_name).send().await?;

    // Assert.
    assert_eq!(got.name, bucket_name);
    assert_eq!(got.storage_class, expected_storage_class);
    Ok(got)
}

async fn verify_update_default_storage_class(
    control: &StorageControl,
    bucket_name: &str,
    updated_class: &str,
    restored_class: &str,
) -> Result<()> {
    // Arrange.
    let initial = control.get_bucket().set_name(bucket_name).send().await?;
    let initial_metageneration = initial.metageneration;

    // Act: patch bucket default storage class to `updated_class`.
    let updated = control
        .update_bucket()
        .set_bucket(initial.set_storage_class(updated_class))
        .set_if_metageneration_match(initial_metageneration)
        .set_update_mask(FieldMask::default().set_paths(["storage_class"]))
        .with_idempotency(true)
        .send()
        .await?;

    // Assert.
    assert_eq!(updated.storage_class, updated_class);
    verify_get_bucket(control, bucket_name, updated_class).await?;

    // Act: restore bucket default storage class to `restored_class`.
    let updated_metageneration = updated.metageneration;
    let restored = control
        .update_bucket()
        .set_bucket(updated.set_storage_class(restored_class))
        .set_if_metageneration_match(updated_metageneration)
        .set_update_mask(FieldMask::default().set_paths(["storage_class"]))
        .with_idempotency(true)
        .send()
        .await?;

    // Assert.
    assert_eq!(restored.storage_class, restored_class);
    verify_get_bucket(control, bucket_name, restored_class).await?;
    Ok(())
}

async fn verify_inherited_object_upload_and_reads(
    clients: &Clients,
    bucket_name: &str,
    object_name: &str,
    expected_storage_class: &str,
) -> Result<()> {
    // Act: upload without setting per-object storage class so it inherits the bucket default.
    let uploaded = clients
        .http
        .write_object(bucket_name, object_name, PAYLOAD)
        .set_if_generation_match(0)
        .send_unbuffered()
        .await?;

    // Assert: upload metadata, HTTP ReadObject headers, gRPC GetObject, and gRPC BidiReadObject.
    assert_eq!(uploaded.storage_class, expected_storage_class);

    let response = clients
        .http
        .read_object(bucket_name, &uploaded.name)
        .set_generation(uploaded.generation)
        .send()
        .await?;
    assert_eq!(response.object().storage_class, expected_storage_class);
    let body = read_all(response).await?;
    assert_eq!(body, PAYLOAD.as_bytes());

    let got = clients
        .control
        .get_object()
        .set_bucket(bucket_name)
        .set_object(&uploaded.name)
        .set_generation(uploaded.generation)
        .send()
        .await?;
    assert_eq!(got.storage_class, expected_storage_class);

    let descriptor = clients
        .grpc
        .open_object(bucket_name, &uploaded.name)
        .set_generation(uploaded.generation)
        .send()
        .await?;
    assert_eq!(descriptor.object().storage_class, expected_storage_class);
    Ok(())
}

async fn verify_explicit_object_storage_class(
    clients: &Clients,
    bucket_name: &str,
    object_name: &str,
    explicit_storage_class: &str,
) -> Result<()> {
    // Act: upload with an explicit per-object Enterprise storage class override.
    let uploaded = clients
        .http
        .write_object(bucket_name, object_name, PAYLOAD)
        .set_if_generation_match(0)
        .set_storage_class(explicit_storage_class)
        .send_unbuffered()
        .await?;

    // Assert.
    assert_eq!(uploaded.storage_class, explicit_storage_class);

    let response = clients
        .http
        .read_object(bucket_name, &uploaded.name)
        .set_generation(uploaded.generation)
        .send()
        .await?;
    assert_eq!(response.object().storage_class, explicit_storage_class);
    let body = read_all(response).await?;
    assert_eq!(body, PAYLOAD.as_bytes());

    let got = clients
        .control
        .get_object()
        .set_bucket(bucket_name)
        .set_object(&uploaded.name)
        .set_generation(uploaded.generation)
        .send()
        .await?;
    assert_eq!(got.storage_class, explicit_storage_class);
    Ok(())
}

async fn verify_hns_bidi_open_and_read(
    clients: &Clients,
    bucket_name: &str,
    object_name: &str,
    expected_storage_class: &str,
) -> Result<()> {
    // Arrange.
    let uploaded = clients
        .http
        .write_object(bucket_name, object_name, PAYLOAD)
        .set_if_generation_match(0)
        .send_unbuffered()
        .await?;
    assert_eq!(uploaded.storage_class, expected_storage_class);

    // Act.
    let (descriptor, reader) = clients
        .grpc
        .open_object(bucket_name, &uploaded.name)
        .set_generation(uploaded.generation)
        .send_and_read(ReadRange::all())
        .await?;

    // Assert.
    assert_eq!(descriptor.object().storage_class, expected_storage_class);
    assert_eq!(reader.object().storage_class, expected_storage_class);
    let body = read_all(reader).await?;
    assert_eq!(body, PAYLOAD.as_bytes());
    Ok(())
}

async fn verify_rewrite_storage_class(
    control: &StorageControl,
    bucket_name: &str,
    object_name: &str,
    target_class: &str,
    restored_class: &str,
) -> Result<()> {
    // Act: rewrite object storage class in place to `target_class`.
    let rewritten = control
        .rewrite_object()
        .set_source_bucket(bucket_name)
        .set_source_object(object_name)
        .set_destination_bucket(bucket_name)
        .set_destination_name(object_name)
        .set_destination(Object::new().set_storage_class(target_class))
        .with_idempotency(true)
        .rewrite_until_done()
        .await?;

    // Assert.
    assert_eq!(rewritten.storage_class, target_class);
    let fetched = control
        .get_object()
        .set_bucket(bucket_name)
        .set_object(object_name)
        .send()
        .await?;
    assert_eq!(fetched.storage_class, target_class);

    // Act: rewrite object storage class back to `restored_class`.
    let restored = control
        .rewrite_object()
        .set_source_bucket(bucket_name)
        .set_source_object(object_name)
        .set_destination_bucket(bucket_name)
        .set_destination_name(object_name)
        .set_destination(Object::new().set_storage_class(restored_class))
        .with_idempotency(true)
        .rewrite_until_done()
        .await?;

    // Assert.
    assert_eq!(restored.storage_class, restored_class);
    let fetched_restored = control
        .get_object()
        .set_bucket(bucket_name)
        .set_object(object_name)
        .send()
        .await?;
    assert_eq!(fetched_restored.storage_class, restored_class);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use google_cloud_gax::Result as GaxResult;
    use google_cloud_gax::options::RequestOptions;
    use google_cloud_gax::response::Response;
    use google_cloud_storage::model::{
        CreateBucketRequest, GetBucketRequest, GetObjectRequest, RewriteObjectRequest,
        RewriteResponse, UpdateBucketRequest,
    };
    use std::sync::{Arc, Mutex};

    #[derive(Debug, Default)]
    struct EnterpriseControlStub {
        bucket_class: Mutex<String>,
        object_class: Mutex<String>,
        created_specs: Mutex<Vec<(String, String, bool)>>,
    }

    impl google_cloud_storage::stub::StorageControl for EnterpriseControlStub {
        async fn create_bucket(
            &self,
            req: CreateBucketRequest,
            _options: RequestOptions,
        ) -> GaxResult<Response<Bucket>> {
            let bucket = req.bucket.unwrap_or_default();
            let hns = bucket
                .hierarchical_namespace
                .as_ref()
                .is_some_and(|h| h.enabled);
            self.created_specs.lock().unwrap().push((
                bucket.location.clone(),
                bucket.storage_class.clone(),
                hns,
            ));
            *self.bucket_class.lock().unwrap() = bucket.storage_class.clone();
            Ok(Response::from(
                bucket
                    .set_name(format!("projects/_/buckets/{}", req.bucket_id))
                    .set_metageneration(1),
            ))
        }

        async fn get_bucket(
            &self,
            req: GetBucketRequest,
            _options: RequestOptions,
        ) -> GaxResult<Response<Bucket>> {
            let class = self.bucket_class.lock().unwrap().clone();
            Ok(Response::from(
                Bucket::new()
                    .set_name(req.name)
                    .set_storage_class(class)
                    .set_metageneration(1),
            ))
        }

        async fn update_bucket(
            &self,
            req: UpdateBucketRequest,
            _options: RequestOptions,
        ) -> GaxResult<Response<Bucket>> {
            let bucket = req.bucket.unwrap_or_default();
            assert_eq!(
                req.update_mask.map(|m| m.paths).unwrap_or_default(),
                vec!["storage_class".to_string()]
            );
            *self.bucket_class.lock().unwrap() = bucket.storage_class.clone();
            Ok(Response::from(bucket.set_metageneration(
                req.if_metageneration_match.unwrap_or(1) + 1,
            )))
        }

        async fn rewrite_object(
            &self,
            req: RewriteObjectRequest,
            _options: RequestOptions,
        ) -> GaxResult<Response<RewriteResponse>> {
            let class = req.destination.map(|d| d.storage_class).unwrap_or_default();
            *self.object_class.lock().unwrap() = class.clone();
            let obj = Object::new()
                .set_bucket(req.destination_bucket)
                .set_name(req.destination_name)
                .set_storage_class(class);
            Ok(Response::from(
                RewriteResponse::new().set_done(true).set_resource(obj),
            ))
        }

        async fn get_object(
            &self,
            req: GetObjectRequest,
            _options: RequestOptions,
        ) -> GaxResult<Response<Object>> {
            let class = self.object_class.lock().unwrap().clone();
            Ok(Response::from(
                Object::new()
                    .set_bucket(req.bucket)
                    .set_name(req.object)
                    .set_storage_class(class),
            ))
        }
    }

    #[tokio::test]
    async fn create_update_and_rewrite_enterprise_classes_via_stub() -> Result<()> {
        // Arrange.
        let stub = Arc::new(EnterpriseControlStub::default());
        let control = StorageControl::from_stub::<EnterpriseControlStub>(stub.clone());
        let spec = BucketSpec {
            label: "HNS Enterprise",
            location: "us-central1",
            storage_class: STANDARD_ENTERPRISE,
            hns: true,
        };

        // Act.
        let created = create_enterprise_bucket(&control, "test-proj", "my-bkt", spec).await?;
        verify_update_default_storage_class(
            &control,
            &created.name,
            LR_STANDARD_ENTERPRISE,
            STANDARD_ENTERPRISE,
        )
        .await?;
        verify_rewrite_storage_class(
            &control,
            &created.name,
            "obj.txt",
            STANDARD_FLEXBACKUP_ENTERPRISE,
            STANDARD_ENTERPRISE,
        )
        .await?;

        // Assert.
        assert_eq!(created.name, "projects/_/buckets/my-bkt");
        assert_eq!(created.storage_class, STANDARD_ENTERPRISE);
        assert_eq!(
            *stub.created_specs.lock().unwrap(),
            vec![(
                "us-central1".to_string(),
                STANDARD_ENTERPRISE.to_string(),
                true
            )]
        );
        assert_eq!(*stub.bucket_class.lock().unwrap(), STANDARD_ENTERPRISE);
        assert_eq!(*stub.object_class.lock().unwrap(), STANDARD_ENTERPRISE);

        Ok(())
    }
}
