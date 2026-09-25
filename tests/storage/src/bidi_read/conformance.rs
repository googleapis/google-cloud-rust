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

    let clients = &Clients::new().await?;
    cleanup_stale_buckets(&clients.control).await;

    // Test 4 needs no bucket.
    println!("\n### No bucket");
    test_non_existent_bucket_read(clients).await?;

    // Tests 2 and 5 do not depend on the bucket type, so they share the flat standard bucket.
    with_bucket(
        clients,
        BucketType::RegionalStandard { hns: false },
        |bucket| async move {
            test_read_post_stream_close(clients, &bucket).await?;
            test_out_of_range(clients, &bucket).await?;
            test_multiple_ranged_read(clients, &bucket, false).await
        },
    )
    .await?;

    with_bucket(
        clients,
        BucketType::RegionalStandard { hns: true },
        |bucket| async move { test_multiple_ranged_read(clients, &bucket, false).await },
    )
    .await?;

    // Zonal Rapid (`RAPID`) buckets only accept appendable objects written over gRPC.
    with_bucket(clients, BucketType::ZonalRapid, |bucket| async move {
        test_multiple_ranged_read(clients, &bucket, true).await
    })
    .await?;

    with_bucket(clients, BucketType::RegionalRapid, |bucket| async move {
        test_multiple_ranged_read(clients, &bucket, false).await
    })
    .await?;

    println!("\n=== Bidi Read Conformance Suite Completed Successfully ===\n");
    Ok(())
}

// -----------------------------------------------------------------------------
// Clients
// -----------------------------------------------------------------------------

/// All clients used by the suite, built once in `run()`.
struct Clients {
    /// Preprod gRPC data client: `open_object` (`BidiReadObject`) and
    /// `open_appendable_object` (`BidiWriteObject`).
    grpc: Storage,
    /// Preprod HTTP data client: JSON `write_object` uploads.
    http: Storage,
    /// Preprod `StorageControl` client: bucket, object, folder and cache management.
    control: StorageControl,
}

impl Clients {
    /// Data clients use their preprod default unless `GOOGLE_CLOUD_TEST_STORAGE_ENDPOINT` is set;
    /// the control client uses `PREPROD_GRPC_ENDPOINT` unless
    /// `GOOGLE_CLOUD_TEST_STORAGE_CONTROL_ENDPOINT` is set.
    async fn new() -> anyhow::Result<Self> {
        Ok(Self {
            grpc: build_storage_client(PREPROD_GRPC_ENDPOINT).await?,
            http: build_storage_client(PREPROD_HTTP_ENDPOINT).await?,
            control: build_storage_control_client().await?,
        })
    }
}

async fn build_storage_client(default_endpoint: &str) -> anyhow::Result<Storage> {
    let endpoint = std::env::var("GOOGLE_CLOUD_TEST_STORAGE_ENDPOINT")
        .unwrap_or_else(|_| default_endpoint.to_string());
    Ok(Storage::builder().with_endpoint(endpoint).build().await?)
}

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

// -----------------------------------------------------------------------------
// Bucket lifecycle
// -----------------------------------------------------------------------------

/// The bucket topologies exercised by the suite.
#[derive(Clone, Copy, Debug)]
enum BucketType {
    /// Regional bucket, `STANDARD` storage class.
    RegionalStandard { hns: bool },
    /// Zonal bucket in `zone_id()`, `RAPID` storage class; GCS requires HNS.
    ZonalRapid,
    /// Regional HNS bucket with a `rapid-cache-ultra` cache in `zone_id()`; GCS requires HNS.
    RegionalRapid,
}

impl BucketType {
    fn label(self) -> &'static str {
        match self {
            Self::RegionalStandard { hns: false } => "Regional Standard (flat)",
            Self::RegionalStandard { hns: true } => "Regional Standard (HNS)",
            Self::ZonalRapid => "Zonal Rapid",
            Self::RegionalRapid => "Regional Rapid / RCU (HNS)",
        }
    }

    fn hns(self) -> bool {
        match self {
            Self::RegionalStandard { hns } => hns,
            Self::ZonalRapid | Self::RegionalRapid => true,
        }
    }

    fn has_rapid_cache(self) -> bool {
        matches!(self, Self::RegionalRapid)
    }
}

/// Creates a bucket of `bucket_type`, runs `f` with its name, then cleans the bucket up,
/// even if `f` failed.
async fn with_bucket<F, Fut>(clients: &Clients, bucket_type: BucketType, f: F) -> anyhow::Result<()>
where
    F: FnOnce(String) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<()>>,
{
    let bucket = create_bucket(&clients.control, bucket_type).await?;
    println!("\n### {}: {}", bucket_type.label(), bucket.name);
    let result = f(bucket.name.clone()).await;
    let _ = cleanup_bucket(
        &clients.control,
        &bucket.name,
        bucket_type.has_rapid_cache(),
        bucket_type.hns(),
    )
    .await;
    result
}

/// Creates a bucket of `bucket_type`. For `RegionalRapid`, also attaches the cache; if that
/// fails, the bucket is deleted before the error is returned.
async fn create_bucket(
    control: &StorageControl,
    bucket_type: BucketType,
) -> anyhow::Result<Bucket> {
    let zone = zone_id();
    let mut bucket = Bucket::new()
        .set_project(format!("projects/{}", project_id()?))
        .set_location(region_id())
        .set_labels([("integration-test", "true")])
        .set_iam_config(
            IamConfig::new()
                .set_uniform_bucket_level_access(UniformBucketLevelAccess::new().set_enabled(true)),
        );
    if bucket_type.hns() {
        bucket = bucket.set_hierarchical_namespace(HierarchicalNamespace::new().set_enabled(true));
    }
    if let BucketType::ZonalRapid = bucket_type {
        bucket = bucket
            .set_custom_placement_config(CustomPlacementConfig::new().set_data_locations([&zone]))
            .set_storage_class("RAPID");
    }

    let bucket = control
        .create_bucket()
        .set_parent("projects/_")
        .set_bucket_id(random_bucket_id())
        .set_bucket(bucket)
        .with_idempotency(true)
        .send()
        .await?;
    tracing::info!("created {bucket_type:?} bucket: {}", bucket.name);

    if bucket_type.has_rapid_cache() {
        let rapid_cache = RapidCache::new()
            .set_name(format!("{}/rapidCaches/{zone}", bucket.name))
            .set_zone(&zone)
            .set_cache_type("rapid-cache-ultra");

        println!("attaching rapid-cache-ultra in {zone} (this can take a minute or more)...");
        let attached = control
            .create_rapid_cache()
            .set_parent(&bucket.name)
            .set_rapid_cache(rapid_cache)
            .poller()
            .until_done()
            .await;
        if let Err(e) = attached {
            // The LRO may fail after the cache was (partially) created, so check for caches too.
            let _ = cleanup_bucket(control, &bucket.name, true, true).await;
            return Err(e.into());
        }
    }

    Ok(bucket)
}

/// Cleans up a bucket in preprod by:
/// 1. Disabling any attached `RapidCache` instances via `disable_rapid_cache()` (if `has_rapid_cache` is true).
/// 2. Deleting all objects (including versions) in the bucket.
/// 3. Deleting any HNS folders / managed folders (if `is_hns` is true).
/// 4. Deleting the bucket itself.
async fn cleanup_bucket(
    control: &StorageControl,
    bucket_name: &str,
    has_rapid_cache: bool,
    is_hns: bool,
) -> anyhow::Result<()> {
    // 1. Disable any Rapid Caches via DisableRapidCache (only when attached)
    if has_rapid_cache {
        let mut rapid_caches = control
            .list_rapid_caches()
            .set_parent(bucket_name)
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
        .set_parent(bucket_name)
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
            .set_parent(bucket_name)
            .by_item();
        while let Some(Ok(folder)) = managed_folders.next().await {
            let _ = control
                .delete_managed_folder()
                .set_name(folder.name)
                .send()
                .await;
        }
        let mut folders = control.list_folders().set_parent(bucket_name).by_item();
        while let Some(Ok(folder)) = folders.next().await {
            let _ = control.delete_folder().set_name(folder.name).send().await;
        }
    }

    // 4. Delete the bucket
    if let Err(e) = control
        .delete_bucket()
        .set_name(bucket_name)
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
async fn cleanup_stale_buckets(control: &StorageControl) {
    use std::time::{SystemTime, UNIX_EPOCH};
    let Ok(project_id) = project_id() else {
        return;
    };
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
            let _ = cleanup_bucket(control, &name, true, true).await;
        }
    }
}

/// Seeds a test object into `bucket_name` in preprod and returns its name.
///
/// Each bucket type accepts exactly one of the two upload paths (go/gcs-rapid-behavior-matrix),
/// so the caller selects it via `appendable`:
/// - `false`: JSON `write_object` via `clients.http` (`PREPROD_HTTP_ENDPOINT`). Use for
///   `STANDARD` buckets (Regional Standard and Regional Rapid / RCU), which reject appendable writes.
/// - `true`: gRPC `open_appendable_object` (`BidiWriteObject`) via `clients.grpc`
///   (`PREPROD_GRPC_ENDPOINT`). Use for Zonal Rapid (`RAPID`) buckets, which reject JSON uploads.
///   Requires `--cfg google_cloud_unstable_storage_bidi`.
async fn write_test_object(
    clients: &Clients,
    bucket_name: &str,
    object_name: &str,
    payload: String,
    appendable: bool,
) -> anyhow::Result<String> {
    if appendable {
        #[cfg(google_cloud_unstable_storage_bidi)]
        {
            let mut writer = clients
                .grpc
                .open_appendable_object(bucket_name, object_name)
                .send()
                .await?;
            writer.append(bytes::Bytes::from(payload)).await?;
            let object = writer.finalize().await?;
            return Ok(object.name);
        }
        #[cfg(not(google_cloud_unstable_storage_bidi))]
        {
            anyhow::bail!("appendable uploads require `--cfg google_cloud_unstable_storage_bidi`");
        }
    }
    let object = clients
        .http
        .write_object(bucket_name, object_name, payload)
        .set_if_generation_match(0)
        .send_unbuffered()
        .await?;
    Ok(object.name)
}

// -----------------------------------------------------------------------------
// Test Cases
// -----------------------------------------------------------------------------

/// Test Suite 1 - Test 1: Multiple Ranged Read
///
/// Tests reading an object across multiple concurrent range read streams over the
/// bidirectional gRPC stream session. Validates that concurrent streams drain properly
/// without deadlock, all received bytes match the expected slices, total length matches,
/// and CRC32C checksum integrity across all ranges matches.
///
/// `appendable` selects how the source object is seeded; see `write_test_object`.
async fn test_multiple_ranged_read(
    clients: &Clients,
    bucket_name: &str,
    appendable: bool,
) -> anyhow::Result<()> {
    println!("  [Test 1] Multiple Ranged Read ...");
    const TOTAL_SIZE: usize = 512 * 1024;
    let payload = String::from_iter(('a'..='z').cycle().take(TOTAL_SIZE));
    let object_name = format!("bidi_read/multi_range_source_{}.txt", random_bucket_id());

    let object_name = write_test_object(
        clients,
        bucket_name,
        &object_name,
        payload.clone(),
        appendable,
    )
    .await?;

    let descriptor = clients
        .grpc
        .open_object(bucket_name, &object_name)
        .send()
        .await?;

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
async fn test_read_post_stream_close(clients: &Clients, bucket_name: &str) -> anyhow::Result<()> {
    println!("  [Test 2] Read Post Stream Close ...");
    let payload = String::from_iter(('a'..='z').cycle().take(100_000));
    let object_name = format!("bidi_read/post_close_source_{}.txt", random_bucket_id());

    // Runs on the Regional Standard (flat) bucket, so seed with a JSON upload.
    let object_name =
        write_test_object(clients, bucket_name, &object_name, payload.clone(), false).await?;

    let descriptor = clients
        .grpc
        .open_object(bucket_name, &object_name)
        .send()
        .await?;

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
async fn test_non_existent_bucket_read(clients: &Clients) -> anyhow::Result<()> {
    println!("  [Test 4] Non-Existent Bucket Read ...");
    let non_existent_bucket = format!(
        "projects/_/buckets/non-existent-bucket-{}",
        random_bucket_id()
    );

    let result = clients
        .grpc
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
async fn test_out_of_range(clients: &Clients, bucket_name: &str) -> anyhow::Result<()> {
    println!("  [Test 5] Out Of Range Read ...");
    let payload = String::from_iter(('a'..='z').cycle().take(10_000));
    let object_name = format!("bidi_read/out_of_range_source_{}.txt", random_bucket_id());

    // Runs on the Regional Standard (flat) bucket, so seed with a JSON upload.
    let object_name =
        write_test_object(clients, bucket_name, &object_name, payload.clone(), false).await?;

    let descriptor = clients
        .grpc
        .open_object(bucket_name, &object_name)
        .send()
        .await?;

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
