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

//! Cross-SDK Conformance Tests for Bidirectional Read (Test Suite 1).
//!
//! Implements the formal test cases specified in the GCS Bidirectional Read
//! specification and the Rapid Cache Ultra (RCU) integration testing matrix.

use google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder;
use google_cloud_gax::options::RequestOptionsBuilder as _;
use google_cloud_gax::paginator::ItemPaginator as _;
use google_cloud_gax::retry_policy::RetryPolicyExt as _;
use google_cloud_lro::Poller as _;
use google_cloud_storage::client::{Storage, StorageControl};
use google_cloud_storage::model::bucket::iam_config::UniformBucketLevelAccess;
use google_cloud_storage::model::bucket::{
    CustomPlacementConfig, HierarchicalNamespace, IamConfig,
};
use google_cloud_storage::model::{Bucket, RapidCache};
use google_cloud_storage::model_ext::ReadRange;
use google_cloud_storage::read_object::ReadObjectResponse;
use google_cloud_storage::retry_policy::RetryableErrors;
use google_cloud_test_utils::resource_names::random_bucket_id;
use google_cloud_test_utils::runtime_config::{project_id, region_id, zone_id};
use std::time::Duration;

/// Default preprod endpoints for GCS conformance tests.
/// `PREPROD_GRPC_ENDPOINT` serves gRPC (`BidiReadObject`, `BidiWriteObject`, and `StorageControl`).
/// `PREPROD_HTTP_ENDPOINT` (`storage-preprod-test-unified`) serves HTTP JSON REST uploads (`write_object`)
/// within the exact same preprod storage universe.
const PREPROD_GRPC_ENDPOINT: &str = "https://storage-preprod-test-grpc.googleusercontent.com:443";
const PREPROD_HTTP_ENDPOINT: &str = "https://storage-preprod-test-unified.googleusercontent.com";

/// Runs the entire cross-SDK Bidirectional Read conformance test suite,
/// provisioning the minimal set of 4 buckets (1 per supported bucket topology).
///
/// Regional Rapid (RCU) is only exercised with HNS enabled: GCS rejects
/// `CreateRapidCache` on flat buckets with `FAILED_PRECONDITION` ("Rapid Cache
/// Ultra is only supported in hierarchical namespace buckets").
///
/// Whether the Zonal Rapid and Regional Rapid (RCU) tests run in a `colocated`
/// (`VM_zone == us-central1-a`) or `non-colocated` (`VM_zone != us-central1-a`)
/// topology depends on the zone of the GCE VM executing the test suite.
pub async fn run() -> anyhow::Result<()> {
    println!("\n=== Running Bidi Read Conformance Suite ===");

    // `client` uses preprod gRPC (`PREPROD_GRPC_ENDPOINT`) for `open_object` (`BidiReadObject`).
    // `write_client` uses preprod HTTP (`PREPROD_HTTP_ENDPOINT`) for `write_object` uploads.
    let client = &build_storage_client(Some(PREPROD_GRPC_ENDPOINT)).await?;
    let write_client = &build_storage_client(Some(PREPROD_HTTP_ENDPOINT)).await?;

    // Clean up any stale (>48h old) integration test buckets in preprod using `DisableRapidCache`.
    if let (Ok(project), Ok(control)) = (project_id(), build_storage_control_client().await) {
        cleanup_stale_buckets(&control, &project).await;
    }

    // 0. Bucketless test case (Test 4)
    println!("\n### No bucket");
    non_existent_bucket_read(client).await?;

    // 1. Regional Standard (Flat) — shared by non-bucket-dependent tests (2, 5) & flat standard test (1)
    with_regional_standard_bucket(false, "Regional Standard (flat)", |bucket| async move {
        read_post_stream_close(client, write_client, &bucket).await?;
        out_of_range(client, write_client, &bucket).await?;
        multiple_ranged_read_regional_standard_flat(client, write_client, &bucket).await?;
        Ok(())
    })
    .await?;

    // 2. Regional Standard (HNS)
    with_regional_standard_bucket(true, "Regional Standard (HNS)", |bucket| async move {
        multiple_ranged_read_regional_standard_hns(client, write_client, &bucket).await?;
        Ok(())
    })
    .await?;

    // 3. Zonal Rapid (us-central1-a; HNS is always enabled)
    with_zonal_rapid_bucket("Zonal Rapid", |bucket| async move {
        multiple_ranged_read_zonal_rapid(client, write_client, &bucket).await?;
        Ok(())
    })
    .await?;

    // 4. Regional Rapid (RCU - HNS required, cache in us-central1-a)
    with_regional_rapid_bucket("Regional Rapid / RCU (HNS)", |bucket| async move {
        multiple_ranged_read_regional_rapid_hns(client, write_client, &bucket).await?;
        Ok(())
    })
    .await?;

    println!("\n=== Bidi Read Conformance Suite Completed Successfully ===\n");
    Ok(())
}

// -----------------------------------------------------------------------------
// Client & Bucket Lifecycle Helpers
// -----------------------------------------------------------------------------

/// Single centralized factory for building `Storage` data clients in `conformance.rs`.
/// Uses `default_endpoint` unless overridden by `GOOGLE_CLOUD_TEST_STORAGE_ENDPOINT`.
async fn build_storage_client(default_endpoint: Option<&str>) -> anyhow::Result<Storage> {
    let mut builder = Storage::builder();
    if let Ok(env_ep) = std::env::var("GOOGLE_CLOUD_TEST_STORAGE_ENDPOINT") {
        builder = builder.with_endpoint(env_ep);
    } else if let Some(ep) = default_endpoint {
        builder = builder.with_endpoint(ep);
    }
    Ok(builder.build().await?)
}

/// Single centralized factory for building `StorageControl` clients in `conformance.rs`.
/// Uses `PREPROD_GRPC_ENDPOINT` (preprod) unless overridden by `GOOGLE_CLOUD_TEST_STORAGE_CONTROL_ENDPOINT`.
async fn build_storage_control_client() -> anyhow::Result<StorageControl> {
    let endpoint = std::env::var("GOOGLE_CLOUD_TEST_STORAGE_CONTROL_ENDPOINT")
        .unwrap_or_else(|_| PREPROD_GRPC_ENDPOINT.to_string());
    tracing::info!("StorageControl endpoint: {endpoint}");

    let client = StorageControl::builder()
        .with_endpoint(&endpoint)
        .with_backoff_policy(
            ExponentialBackoffBuilder::new()
                .with_initial_delay(Duration::from_secs(2))
                .with_maximum_delay(Duration::from_secs(8))
                .build()?,
        )
        .with_retry_policy(RetryableErrors.with_attempt_limit(5))
        .build()
        .await?;

    Ok(client)
}

/// Prints the section header that groups all test output for one bucket.
fn print_bucket_header(label: &str, bucket_name: &str) {
    println!("\n### {label}: {bucket_name}");
}

async fn with_regional_standard_bucket<F, Fut>(hns: bool, label: &str, f: F) -> anyhow::Result<()>
where
    F: FnOnce(String) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<()>>,
{
    let (control, bucket) = create_regional_standard_bucket(hns).await?;
    print_bucket_header(label, &bucket.name);
    let result = f(bucket.name.clone()).await;
    let _ = cleanup_bucket(control, bucket.name, false, hns).await;
    result
}

async fn with_zonal_rapid_bucket<F, Fut>(label: &str, f: F) -> anyhow::Result<()>
where
    F: FnOnce(String) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<()>>,
{
    let (control, bucket) = create_zonal_rapid_bucket().await?;
    print_bucket_header(label, &bucket.name);
    let result = f(bucket.name.clone()).await;
    let _ = cleanup_bucket(control, bucket.name, false, true).await;
    result
}

/// Regional Rapid (RCU) buckets always have HNS enabled; GCS rejects RCU on flat buckets.
async fn with_regional_rapid_bucket<F, Fut>(label: &str, f: F) -> anyhow::Result<()>
where
    F: FnOnce(String) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<()>>,
{
    let (control, bucket) = create_regional_rapid_bucket().await?;
    print_bucket_header(label, &bucket.name);
    let result = f(bucket.name.clone()).await;
    let _ = cleanup_bucket(control, bucket.name, true, true).await;
    result
}

async fn create_regional_standard_bucket(hns: bool) -> anyhow::Result<(StorageControl, Bucket)> {
    let project_id = project_id()?;
    let region = region_id();
    let control = build_storage_control_client().await?;

    let bucket_id = random_bucket_id();
    let mut bucket = Bucket::new()
        .set_project(format!("projects/{project_id}"))
        .set_location(&region)
        .set_labels([("integration-test", "true")])
        .set_iam_config(
            IamConfig::new()
                .set_uniform_bucket_level_access(UniformBucketLevelAccess::new().set_enabled(true)),
        );
    if hns {
        bucket = bucket.set_hierarchical_namespace(HierarchicalNamespace::new().set_enabled(true));
    }

    let created_bucket = control
        .create_bucket()
        .set_parent("projects/_")
        .set_bucket_id(bucket_id)
        .set_bucket(bucket)
        .with_idempotency(true)
        .send()
        .await?;
    tracing::info!(
        "create_regional_standard_bucket(hns={hns}, region={region}) created bucket: {}",
        created_bucket.name
    );

    Ok((control, created_bucket))
}

async fn create_zonal_rapid_bucket() -> anyhow::Result<(StorageControl, Bucket)> {
    let project_id = project_id()?;
    let region = region_id();
    let zone = zone_id();
    let control = build_storage_control_client().await?;

    let bucket_id = random_bucket_id();
    let bucket = Bucket::new()
        .set_project(format!("projects/{project_id}"))
        .set_location(&region)
        .set_custom_placement_config(CustomPlacementConfig::new().set_data_locations([&zone]))
        .set_storage_class("RAPID")
        .set_labels([("integration-test", "true")])
        .set_hierarchical_namespace(HierarchicalNamespace::new().set_enabled(true))
        .set_iam_config(
            IamConfig::new()
                .set_uniform_bucket_level_access(UniformBucketLevelAccess::new().set_enabled(true)),
        );

    let created_bucket = control
        .create_bucket()
        .set_parent("projects/_")
        .set_bucket_id(bucket_id)
        .set_bucket(bucket)
        .with_idempotency(true)
        .send()
        .await?;
    tracing::info!(
        "create_zonal_rapid_bucket(zone={zone}) created bucket: {}",
        created_bucket.name
    );

    Ok((control, created_bucket))
}

/// Creates an HNS regional bucket and attaches a `rapid-cache-ultra` cache in `zone_id()`.
/// If attaching the cache fails, the bucket is deleted before the error is returned.
async fn create_regional_rapid_bucket() -> anyhow::Result<(StorageControl, Bucket)> {
    let (control, created_bucket) = create_regional_standard_bucket(true).await?;
    let zone = zone_id();

    let rapid_cache = RapidCache::new()
        .set_name(format!("{}/rapidCaches/{zone}", created_bucket.name))
        .set_zone(&zone)
        .set_cache_type("rapid-cache-ultra");

    println!("attaching rapid-cache-ultra in {zone} (this can take a minute or more)...");
    let attached = control
        .create_rapid_cache()
        .set_parent(&created_bucket.name)
        .set_rapid_cache(rapid_cache)
        .poller()
        .until_done()
        .await;
    if let Err(e) = attached {
        // The LRO may fail after the cache was (partially) created, so check for caches too.
        let _ = cleanup_bucket(control, created_bucket.name, true, true).await;
        return Err(e.into());
    }

    Ok((control, created_bucket))
}

/// Cleans up a bucket in preprod by:
/// 1. Disabling any attached `RapidCache` instances via `disable_rapid_cache()` (if `has_rapid_cache` is true).
/// 2. Deleting all objects (including versions) in the bucket.
/// 3. Deleting any HNS folders / managed folders (if `is_hns` is true).
/// 4. Deleting the bucket itself.
async fn cleanup_bucket(
    control: StorageControl,
    bucket_name: String,
    has_rapid_cache: bool,
    is_hns: bool,
) -> anyhow::Result<()> {
    // 1. Disable any Rapid Caches via DisableRapidCache (only when attached)
    if has_rapid_cache {
        let mut rapid_caches = control
            .list_rapid_caches()
            .set_parent(&bucket_name)
            .by_item();
        while let Some(Ok(cache)) = rapid_caches.next().await {
            println!("  cleanup: disabling rapid cache {}", cache.name);
            let res = control
                .disable_rapid_cache()
                .set_name(&cache.name)
                .poller()
                .until_done()
                .await;
            if let Err(e) = res {
                // Ignore b/565175323: DisableRapidCache succeeds on the server, but returns an empty LRO response,
                // causing the client LRO poller to report "neither result nor error set in LRO result".
                let err_str = format!("{e:?}");
                if !err_str.contains("neither result nor error set in LRO result") {
                    tracing::warn!("disable_rapid_cache on {} returned: {e:?}", cache.name);
                }
            }
        }
    }

    // 2. Delete all objects in the bucket (required before bucket deletion)
    let mut objects = control
        .list_objects()
        .set_parent(&bucket_name)
        .set_versions(true)
        .by_item();
    while let Some(Ok(obj)) = objects.next().await {
        let _ = control
            .delete_object()
            .set_bucket(obj.bucket)
            .set_object(obj.name)
            .set_generation(obj.generation)
            .send()
            .await;
    }

    // 3. Delete any HNS folders / managed folders if present (only when HNS is enabled)
    if is_hns {
        let mut managed_folders = control
            .list_managed_folders()
            .set_parent(&bucket_name)
            .by_item();
        while let Some(Ok(folder)) = managed_folders.next().await {
            let _ = control
                .delete_managed_folder()
                .set_name(folder.name)
                .send()
                .await;
        }
        let mut folders = control.list_folders().set_parent(&bucket_name).by_item();
        while let Some(Ok(folder)) = folders.next().await {
            let _ = control.delete_folder().set_name(folder.name).send().await;
        }
    }

    // 4. Delete the bucket
    if let Err(e) = control
        .delete_bucket()
        .set_name(&bucket_name)
        .with_idempotency(true)
        .send()
        .await
    {
        println!("error deleting bucket {bucket_name}: {e:?}");
        return Err(e.into());
    }
    Ok(())
}

/// Cleans up stale (>48h old) `integration-test=true` buckets in the project using `cleanup_bucket`.
async fn cleanup_stale_buckets(control: &StorageControl, project_id: &str) {
    use std::time::{SystemTime, UNIX_EPOCH};
    let Ok(now) = SystemTime::now().duration_since(UNIX_EPOCH) else {
        return;
    };
    let stale_deadline = now.saturating_sub(Duration::from_secs(48 * 60 * 60));
    let stale_deadline = google_cloud_wkt::Timestamp::clamp(stale_deadline.as_secs() as i64, 0);

    let mut buckets = control
        .list_buckets()
        .set_parent(format!("projects/{project_id}"))
        .by_item();
    let mut stale = Vec::new();
    while let Some(Ok(bucket)) = buckets.next().await {
        if bucket
            .labels
            .get("integration-test")
            .is_some_and(|v| v == "true")
            && bucket.create_time.is_some_and(|t| t < stale_deadline)
        {
            stale.push(bucket.name);
        }
    }
    if !stale.is_empty() {
        println!(
            "cleaning up {} stale buckets (with DisableRapidCache)",
            stale.len()
        );
        for name in stale {
            // Stale buckets from prior runs may be of any type; check both caches and folders.
            let _ = cleanup_bucket(control.clone(), name, true, true).await;
        }
    }
}

/// Seeds a test object into `bucket_name` in preprod and returns its name.
///
/// Each bucket type accepts exactly one of the two upload paths (go/gcs-rapid-behavior-matrix),
/// so the caller selects it via `appendable`:
/// - `false`: JSON `write_object` via `write_client` (`PREPROD_HTTP_ENDPOINT`). Use for
///   `STANDARD` buckets (Regional Standard and Regional Rapid / RCU), which reject appendable writes.
/// - `true`: gRPC `open_appendable_object` (`BidiWriteObject`) via `grpc_client`
///   (`PREPROD_GRPC_ENDPOINT`). Use for Zonal Rapid (`RAPID`) buckets, which reject JSON uploads.
///   Requires `--cfg google_cloud_unstable_storage_bidi`.
async fn write_test_object(
    grpc_client: &Storage,
    write_client: &Storage,
    bucket_name: &str,
    object_name: &str,
    payload: String,
    appendable: bool,
) -> anyhow::Result<String> {
    if appendable {
        #[cfg(google_cloud_unstable_storage_bidi)]
        {
            let mut writer = grpc_client
                .open_appendable_object(bucket_name, object_name)
                .send()
                .await?;
            writer.append(bytes::Bytes::from(payload)).await?;
            let object = writer.finalize().await?;
            return Ok(object.name);
        }
        #[cfg(not(google_cloud_unstable_storage_bidi))]
        {
            let _ = grpc_client;
            anyhow::bail!("appendable uploads require `--cfg google_cloud_unstable_storage_bidi`");
        }
    }
    let object = write_client
        .write_object(bucket_name, object_name, payload)
        .set_if_generation_match(0)
        .send_unbuffered()
        .await?;
    Ok(object.name)
}

// -----------------------------------------------------------------------------
// Test Cases
// -----------------------------------------------------------------------------

// =============================================================================
// Non-bucket-type dependent test cases (Tests 2, 4, 5)
// =============================================================================

pub async fn read_post_stream_close(
    client: &Storage,
    write_client: &Storage,
    bucket: &str,
) -> anyhow::Result<()> {
    test_read_post_stream_close(client, write_client, bucket).await
}

pub async fn non_existent_bucket_read(client: &Storage) -> anyhow::Result<()> {
    test_non_existent_bucket_read(client).await
}

pub async fn out_of_range(
    client: &Storage,
    write_client: &Storage,
    bucket: &str,
) -> anyhow::Result<()> {
    test_out_of_range(client, write_client, bucket).await
}

// =============================================================================
// Bucket-type-dependent test cases (Test 1 permuted across topologies)
// =============================================================================

// --- 1. Regional Standard (HNS vs. Flat; colocation is not applicable) ---

pub async fn multiple_ranged_read_regional_standard_hns(
    client: &Storage,
    write_client: &Storage,
    bucket: &str,
) -> anyhow::Result<()> {
    test_multiple_ranged_read(client, write_client, bucket, false).await
}

pub async fn multiple_ranged_read_regional_standard_flat(
    client: &Storage,
    write_client: &Storage,
    bucket: &str,
) -> anyhow::Result<()> {
    test_multiple_ranged_read(client, write_client, bucket, false).await
}

// --- 2. Zonal Rapid (HNS is always enabled) ---

pub async fn multiple_ranged_read_zonal_rapid(
    client: &Storage,
    write_client: &Storage,
    bucket: &str,
) -> anyhow::Result<()> {
    // Zonal Rapid (`RAPID`) buckets only accept appendable objects written over gRPC.
    test_multiple_ranged_read(client, write_client, bucket, true).await
}

// --- 3. Regional Rapid / RCU (HNS only; GCS rejects RCU on flat buckets) ---

pub async fn multiple_ranged_read_regional_rapid_hns(
    client: &Storage,
    write_client: &Storage,
    bucket: &str,
) -> anyhow::Result<()> {
    test_multiple_ranged_read(client, write_client, bucket, false).await
}

/// Test Suite 1 - Test 1: Multiple Ranged Read
///
/// Tests reading an object across multiple concurrent range read streams over the
/// bidirectional gRPC stream session. Validates that concurrent streams drain properly
/// without deadlock, all received bytes match the expected slices, total length matches,
/// and CRC32C checksum integrity across all ranges matches.
///
/// `appendable` selects how the source object is seeded; see `write_test_object`.
pub async fn test_multiple_ranged_read(
    client: &Storage,
    write_client: &Storage,
    bucket_name: &str,
    appendable: bool,
) -> anyhow::Result<()> {
    println!("  [Test 1] Multiple Ranged Read ...");
    const TOTAL_SIZE: usize = 512 * 1024;
    let payload = String::from_iter(('a'..='z').cycle().take(TOTAL_SIZE));
    let object_name = format!("bidi_read/multi_range_source_{}.txt", random_bucket_id());

    let object_name = write_test_object(
        client,
        write_client,
        bucket_name,
        &object_name,
        payload.clone(),
        appendable,
    )
    .await?;

    let descriptor = client.open_object(bucket_name, &object_name).send().await?;

    // Define 4 non-overlapping segments covering the entire 512 KiB object:
    // Range 0: [0..64 KiB] (64 KiB)
    // Range 1: [64 KiB..192 KiB] (128 KiB)
    // Range 2: [192 KiB..384 KiB] (192 KiB)
    // Range 3: [384 KiB..512 KiB] (128 KiB)
    let range0 = ReadRange::segment(0, 64 * 1024);
    let range1 = ReadRange::segment(64 * 1024, 128 * 1024);
    let range2 = ReadRange::segment(192 * 1024, 192 * 1024);
    let range3 = ReadRange::segment(384 * 1024, 128 * 1024);

    let r0 = descriptor.read_range(range0).await;
    let r1 = descriptor.read_range(range1).await;
    let r2 = descriptor.read_range(range2).await;
    let r3 = descriptor.read_range(range3).await;

    // Concurrent draining is essential: all ranges share the underlying gRPC stream,
    // so sequential awaiting would cause buffer starvation and backpressure deadlocks.
    let (buf0, buf1, buf2, buf3) = tokio::try_join!(
        drain_reader(r0),
        drain_reader(r1),
        drain_reader(r2),
        drain_reader(r3),
    )?;

    let payload_bytes = payload.as_bytes();
    assert_eq!(buf0, &payload_bytes[0..64 * 1024]);
    assert_eq!(buf1, &payload_bytes[64 * 1024..192 * 1024]);
    assert_eq!(buf2, &payload_bytes[192 * 1024..384 * 1024]);
    assert_eq!(buf3, &payload_bytes[384 * 1024..512 * 1024]);

    let total_len = buf0.len() + buf1.len() + buf2.len() + buf3.len();
    assert_eq!(total_len, TOTAL_SIZE);

    let crc0 = crc32c::crc32c(&buf0);
    assert_eq!(crc0, crc32c::crc32c(&payload_bytes[0..64 * 1024]));
    let crc1 = crc32c::crc32c(&buf1);
    assert_eq!(crc1, crc32c::crc32c(&payload_bytes[64 * 1024..192 * 1024]));
    let crc2 = crc32c::crc32c(&buf2);
    assert_eq!(crc2, crc32c::crc32c(&payload_bytes[192 * 1024..384 * 1024]));
    let crc3 = crc32c::crc32c(&buf3);
    assert_eq!(crc3, crc32c::crc32c(&payload_bytes[384 * 1024..512 * 1024]));

    println!("  [Test 1] PASSED (512 KiB across 4 concurrent ranges)");
    Ok(())
}

/// Test Suite 1 - Test 2: Read Post Stream Close
///
/// Verifies stream lifecycle and session isolation:
/// 1. Verifies that once a range reader reaches EOF, subsequent calls to next() idempotently return None.
/// 2. Verifies that dropping an in-flight reader (aborting the range) leaves the underlying ObjectDescriptor
///    healthy and able to issue and read new ranges successfully.
pub async fn test_read_post_stream_close(
    client: &Storage,
    write_client: &Storage,
    bucket_name: &str,
) -> anyhow::Result<()> {
    println!("  [Test 2] Read Post Stream Close ...");
    let payload = String::from_iter(('a'..='z').cycle().take(100_000));
    let object_name = format!("bidi_read/post_close_source_{}.txt", random_bucket_id());

    // Runs on the Regional Standard (flat) bucket, so seed with a JSON upload.
    let object_name = write_test_object(
        client,
        write_client,
        bucket_name,
        &object_name,
        payload.clone(),
        false,
    )
    .await?;

    let descriptor = client.open_object(bucket_name, &object_name).send().await?;

    // 1. Read small range to completion (EOF)
    let mut reader = descriptor.read_range(ReadRange::head(100)).await;
    let mut data = Vec::new();
    while let Some(chunk) = reader.next().await.transpose()? {
        data.extend_from_slice(&chunk);
    }
    assert_eq!(data.len(), 100);

    // Verify idempotency of EOF: next() must consistently return None
    assert!(reader.next().await.is_none());
    assert!(reader.next().await.is_none());

    // 2. Cancellation / abort: drop an in-flight reader without reading it to completion
    let unconsumed_reader = descriptor.read_range(ReadRange::segment(500, 10_000)).await;
    drop(unconsumed_reader);

    // 3. Verify descriptor session remains fully functional for subsequent reads
    let mut subsequent_reader = descriptor.read_range(ReadRange::segment(200, 50)).await;
    let mut subsequent_data = Vec::new();
    while let Some(chunk) = subsequent_reader.next().await.transpose()? {
        subsequent_data.extend_from_slice(&chunk);
    }
    assert_eq!(subsequent_data.len(), 50);
    assert_eq!(subsequent_data, &payload.as_bytes()[200..250]);

    println!("  [Test 2] PASSED");
    Ok(())
}

/// Test Suite 1 - Test 4: Non-Existent Bucket Read
///
/// Tests opening a stream on a non-existent bucket. Verifies that an appropriate
/// error with NotFound status (HTTP 404) or PermissionDenied (allowlist check) is returned.
pub async fn test_non_existent_bucket_read(client: &Storage) -> anyhow::Result<()> {
    println!("  [Test 4] Non-Existent Bucket Read ...");
    let non_existent_bucket = format!(
        "projects/_/buckets/non-existent-bucket-{}",
        google_cloud_test_utils::resource_names::random_bucket_id()
    );

    let result = client
        .open_object(&non_existent_bucket, "non_existent_object.txt")
        .send()
        .await;

    match result {
        Ok(descriptor) => {
            let mut reader = descriptor.read_range(ReadRange::head(100)).await;
            let read_res = reader.next().await;
            match read_res {
                Some(Err(err)) => {
                    assert_is_not_found(&err);
                }
                other => anyhow::bail!("expected NotFound error on read_range, got {other:?}"),
            }
        }
        Err(err) => {
            assert_is_not_found(&err);
        }
    }

    println!("  [Test 4] PASSED");
    Ok(())
}

fn assert_is_not_found(err: &google_cloud_gax::error::Error) {
    if let Some(status) = err.status() {
        assert!(
            status.code == google_cloud_gax::error::rpc::Code::NotFound
                || status.code == google_cloud_gax::error::rpc::Code::PermissionDenied,
            "expected NotFound or PermissionDenied rpc code, got {status:?}"
        );
    } else if let Some(code) = err.http_status_code() {
        assert!(
            code == 404 || code == 403,
            "expected 404 or 403 HTTP status code, got {code}"
        );
    } else {
        panic!("expected NotFound or PermissionDenied status, got error: {err:?}");
    }
}

/// Test Suite 1 - Test 5: Out Of Range Read
///
/// Tests out-of-bounds range reads beyond object size (offset > size).
/// Ensures appropriate exception/EOF is returned for the invalid range while valid range reads
/// on the same session succeed.
pub async fn test_out_of_range(
    client: &Storage,
    write_client: &Storage,
    bucket_name: &str,
) -> anyhow::Result<()> {
    println!("  [Test 5] Out Of Range Read ...");
    let payload = String::from_iter(('a'..='z').cycle().take(10_000));
    let object_name = format!("bidi_read/out_of_range_source_{}.txt", random_bucket_id());

    // Runs on the Regional Standard (flat) bucket, so seed with a JSON upload.
    let object_name = write_test_object(
        client,
        write_client,
        bucket_name,
        &object_name,
        payload.clone(),
        false,
    )
    .await?;

    let descriptor = client.open_object(bucket_name, &object_name).send().await?;

    // 1. Session verification: Verify that a valid range read on this descriptor succeeds
    let mut valid_reader = descriptor.read_range(ReadRange::head(50)).await;
    let mut valid_data = Vec::new();
    while let Some(chunk) = valid_reader.next().await.transpose()? {
        valid_data.extend_from_slice(&chunk);
    }
    assert_eq!(valid_data.len(), 50);
    assert_eq!(valid_data, &payload.as_bytes()[0..50]);

    // 2. Request an out-of-bounds range: offset 50,000 when object size is only 10,000 bytes
    let mut oob_reader = descriptor
        .read_range(ReadRange::segment(50_000, 1_000))
        .await;

    let oob_res = oob_reader.next().await;
    match oob_res {
        None => {
            println!("    out-of-range read returned immediate EOF");
        }
        Some(Err(err)) => match find_rpc_status(&err) {
            Some(status) => {
                println!(
                    "    got expected error: {:?}: {}",
                    status.code, status.message
                );
                assert!(
                    matches!(
                        status.code,
                        google_cloud_gax::error::rpc::Code::OutOfRange
                            | google_cloud_gax::error::rpc::Code::InvalidArgument
                    ),
                    "unexpected status for out of range read: {status:?}"
                );
            }
            None => {
                let err_str = format!("{err:?}");
                assert!(
                    err_str.contains("OUT_OF_RANGE")
                        || err_str.contains("OutOfRange")
                        || err_str.contains("InvalidArgument"),
                    "unexpected error message for out of range: {err_str}"
                );
                println!("    got expected error: {err}");
            }
        },
        Some(Ok(data)) => {
            anyhow::bail!(
                "unexpected data returned for out of range read: {} bytes",
                data.len()
            );
        }
    }

    println!("  [Test 5] PASSED");
    Ok(())
}

/// Returns the first RPC status found in `err` or its source chain.
///
/// Bidi read failures surface as a transport error that wraps the service error
/// (as `Arc<Error>`), so the status is not on the outermost error.
fn find_rpc_status(
    err: &google_cloud_gax::error::Error,
) -> Option<google_cloud_gax::error::rpc::Status> {
    use google_cloud_gax::error::Error;
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(e) = current {
        let gax_err = e.downcast_ref::<Error>().or_else(|| {
            e.downcast_ref::<std::sync::Arc<Error>>()
                .map(|a| a.as_ref())
        });
        if let Some(status) = gax_err.and_then(|g| g.status()) {
            return Some(status.clone());
        }
        current = e.source();
    }
    None
}

async fn drain_reader(mut reader: ReadObjectResponse) -> anyhow::Result<Vec<u8>> {
    let mut buf = Vec::new();
    while let Some(chunk) = reader.next().await.transpose()? {
        buf.extend_from_slice(&chunk);
    }
    Ok(buf)
}
