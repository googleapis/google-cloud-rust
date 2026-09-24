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
use google_cloud_storage::model::bucket::{HierarchicalNamespace, IamConfig};
use google_cloud_storage::model::{Bucket, RapidCache};
use google_cloud_storage::model_ext::ReadRange;
use google_cloud_storage::read_object::ReadObjectResponse;
use google_cloud_storage::retry_policy::RetryableErrors;
use google_cloud_test_utils::resource_names::random_bucket_id;
use google_cloud_test_utils::runtime_config::project_id;
use std::time::Duration;

/// Runs the entire cross-SDK Bidirectional Read conformance test suite.
pub async fn run() -> anyhow::Result<()> {
    println!("\n=== Running Bidi Read Conformance Suite ===");

    // Non-bucket-type dependent test cases (Tests 2, 4, 5)
    read_post_stream_close().await?;
    non_existent_bucket_read().await?;
    out_of_range().await?;

    // Bucket-type-dependent test cases (Tests 1 & 3 permuted)
    // 1. Regional Standard
    multiple_ranged_read_regional_standard_hns_colocated().await?;
    multiple_ranged_read_regional_standard_flat_colocated().await?;
    zero_copy_read_regional_standard_hns_colocated().await?;
    zero_copy_read_regional_standard_flat_colocated().await?;

    // 2. Zonal Rapid
    multiple_ranged_read_zonal_rapid_hns_colocated().await?;
    multiple_ranged_read_zonal_rapid_hns_non_colocated().await?;
    zero_copy_read_zonal_rapid_hns_colocated().await?;
    zero_copy_read_zonal_rapid_hns_non_colocated().await?;

    // 3. Regional Rapid (RCU)
    multiple_ranged_read_regional_rapid_hns_colocated().await?;
    multiple_ranged_read_regional_rapid_flat_colocated().await?;
    zero_copy_read_regional_rapid_hns_colocated().await?;
    zero_copy_read_regional_rapid_flat_colocated().await?;

    println!("=== Bidi Read Conformance Suite Completed Successfully ===\n");
    Ok(())
}

// -----------------------------------------------------------------------------
// Lifecycle Helpers (manage bucket provisioning -> test execution -> teardown)
// -----------------------------------------------------------------------------

const PREPROD_ENDPOINT: &str = "https://storage-preprod-test-grpc.googleusercontent.com:443";

/// Universal helper to build a `Storage` data client across all conformance test scenarios:
/// - `custom_endpoint: None` -> uses `GOOGLE_CLOUD_TEST_STORAGE_ENDPOINT` if set, otherwise default prod endpoint.
/// - `custom_endpoint: Some(url)` -> uses `GOOGLE_CLOUD_TEST_STORAGE_ENDPOINT` if set, otherwise `url`
///   (e.g., `"https://us-central1-b-storage.googleapis.com"` for non-colocated Zonal Rapid, or preprod URL for RCU).
async fn build_storage_client(custom_endpoint: Option<&str>) -> anyhow::Result<Storage> {
    let mut builder = Storage::builder();
    if let Ok(endpoint) = std::env::var("GOOGLE_CLOUD_TEST_STORAGE_ENDPOINT") {
        builder = builder.with_endpoint(endpoint);
    } else if let Some(endpoint) = custom_endpoint {
        builder = builder.with_endpoint(endpoint);
    }
    Ok(builder.build().await?)
}

async fn with_regional_standard_bucket<F, Fut>(hns: bool, f: F) -> anyhow::Result<()>
where
    F: FnOnce(Storage, String) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<()>>,
{
    let (control, bucket) = if hns {
        crate::create_test_hns_bucket().await?
    } else {
        crate::create_test_bucket().await?
    };
    let client = build_storage_client(None).await?;
    let result = f(client, bucket.name.clone()).await;
    let _ = storage_samples::cleanup_bucket(control, bucket.name, bucket.project).await;
    result
}

async fn with_zonal_rapid_bucket<F, Fut>(colocated: bool, f: F) -> anyhow::Result<()>
where
    F: FnOnce(Storage, String) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<()>>,
{
    let (control, bucket) = crate::create_test_rapid_bucket().await?;
    let endpoint = (!colocated).then_some("https://us-central1-b-storage.googleapis.com");
    let client = build_storage_client(endpoint).await?;
    let result = f(client, bucket.name.clone()).await;
    let _ = storage_samples::cleanup_bucket(control, bucket.name, bucket.project).await;
    result
}

async fn with_regional_rapid_bucket<F, Fut>(hns: bool, f: F) -> anyhow::Result<()>
where
    F: FnOnce(Storage, String) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<()>>,
{
    let (control, bucket) = create_test_regional_rapid_bucket(hns).await?;
    let endpoint = std::env::var("GOOGLE_CLOUD_TEST_STORAGE_CONTROL_ENDPOINT")
        .unwrap_or_else(|_| PREPROD_ENDPOINT.to_string());
    let client = build_storage_client(Some(&endpoint)).await?;
    let result = f(client, bucket.name.clone()).await;
    let _ = cleanup_regional_rapid_bucket(control, bucket.name, bucket.project).await;
    result
}

// Regional Rapid (RCU) Fixture & Control Client Helpers
async fn build_storage_control_client() -> anyhow::Result<StorageControl> {
    let endpoint = std::env::var("GOOGLE_CLOUD_TEST_STORAGE_CONTROL_ENDPOINT")
        .unwrap_or_else(|_| PREPROD_ENDPOINT.to_string());
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

async fn create_test_regional_rapid_bucket(hns: bool) -> anyhow::Result<(StorageControl, Bucket)> {
    let project_id = project_id()?;
    let control = build_storage_control_client().await?;
    storage_samples::cleanup_stale_buckets(&control, &project_id).await;

    let bucket_id = random_bucket_id();
    let mut bucket = Bucket::new()
        .set_project(format!("projects/{project_id}"))
        .set_location("us-central1")
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
    println!(
        "create_test_regional_rapid_bucket(hns={hns}) created base bucket: {:?}",
        created_bucket.name
    );

    let rapid_cache = RapidCache::new()
        .set_name(format!("{}/rapidCaches/us-central1-a", created_bucket.name))
        .set_zone("us-central1-a")
        .set_cache_type("rapid-cache-ultra");

    let _op = control
        .create_rapid_cache()
        .set_parent(&created_bucket.name)
        .set_rapid_cache(rapid_cache)
        .poller()
        .until_done()
        .await?;
    println!("create_test_regional_rapid_bucket: attached rapid-cache-ultra in us-central1-a");

    Ok((control, created_bucket))
}

async fn cleanup_regional_rapid_bucket(
    control: StorageControl,
    bucket_name: String,
    project_id: String,
) -> anyhow::Result<()> {
    let mut caches = control
        .list_rapid_caches()
        .set_parent(&bucket_name)
        .by_item();
    while let Some(item) = caches.next().await {
        if let Ok(cache) = item {
            tracing::info!("disabling rapid cache {}", cache.name);
            let _ = control
                .disable_rapid_cache()
                .set_name(cache.name)
                .poller()
                .until_done()
                .await;
        }
    }
    storage_samples::cleanup_bucket(control, bucket_name, project_id).await
}

// -----------------------------------------------------------------------------
// Test Cases
// -----------------------------------------------------------------------------

// =============================================================================
// Non-bucket-type dependent test cases (Tests 2, 4, 5)
// =============================================================================

pub async fn read_post_stream_close() -> anyhow::Result<()> {
    with_regional_standard_bucket(false, |client, bucket| async move {
        test_read_post_stream_close(&client, &bucket).await
    })
    .await
}

pub async fn non_existent_bucket_read() -> anyhow::Result<()> {
    let client = build_storage_client(None).await?;
    test_non_existent_bucket_read(&client).await
}

pub async fn out_of_range() -> anyhow::Result<()> {
    with_regional_standard_bucket(false, |client, bucket| async move {
        test_out_of_range(&client, &bucket).await
    })
    .await
}

// =============================================================================
// Bucket-type-dependent test cases (Tests 1 & 3 permuted)
// Format: [test-case-name]_[bucket_type]_[hns]_[colocated]
// =============================================================================

// --- 1. Regional Standard ---

pub async fn multiple_ranged_read_regional_standard_hns_colocated() -> anyhow::Result<()> {
    with_regional_standard_bucket(true, |client, bucket| async move {
        test_multiple_ranged_read(&client, &bucket).await
    })
    .await
}

pub async fn multiple_ranged_read_regional_standard_flat_colocated() -> anyhow::Result<()> {
    with_regional_standard_bucket(false, |client, bucket| async move {
        test_multiple_ranged_read(&client, &bucket).await
    })
    .await
}

pub async fn zero_copy_read_regional_standard_hns_colocated() -> anyhow::Result<()> {
    with_regional_standard_bucket(true, |client, bucket| async move {
        test_zero_copy_read(&client, &bucket).await
    })
    .await
}

pub async fn zero_copy_read_regional_standard_flat_colocated() -> anyhow::Result<()> {
    with_regional_standard_bucket(false, |client, bucket| async move {
        test_zero_copy_read(&client, &bucket).await
    })
    .await
}

// --- 2. Zonal Rapid ---

pub async fn multiple_ranged_read_zonal_rapid_hns_colocated() -> anyhow::Result<()> {
    with_zonal_rapid_bucket(true, |client, bucket| async move {
        test_multiple_ranged_read(&client, &bucket).await
    })
    .await
}

pub async fn multiple_ranged_read_zonal_rapid_hns_non_colocated() -> anyhow::Result<()> {
    with_zonal_rapid_bucket(false, |client, bucket| async move {
        test_multiple_ranged_read(&client, &bucket).await
    })
    .await
}

pub async fn zero_copy_read_zonal_rapid_hns_colocated() -> anyhow::Result<()> {
    with_zonal_rapid_bucket(true, |client, bucket| async move {
        test_zero_copy_read(&client, &bucket).await
    })
    .await
}

pub async fn zero_copy_read_zonal_rapid_hns_non_colocated() -> anyhow::Result<()> {
    with_zonal_rapid_bucket(false, |client, bucket| async move {
        test_zero_copy_read(&client, &bucket).await
    })
    .await
}

// --- 3. Regional Rapid (RCU) ---

pub async fn multiple_ranged_read_regional_rapid_hns_colocated() -> anyhow::Result<()> {
    with_regional_rapid_bucket(true, |client, bucket| async move {
        test_multiple_ranged_read(&client, &bucket).await
    })
    .await
}

pub async fn multiple_ranged_read_regional_rapid_flat_colocated() -> anyhow::Result<()> {
    with_regional_rapid_bucket(false, |client, bucket| async move {
        test_multiple_ranged_read(&client, &bucket).await
    })
    .await
}

pub async fn zero_copy_read_regional_rapid_hns_colocated() -> anyhow::Result<()> {
    with_regional_rapid_bucket(true, |client, bucket| async move {
        test_zero_copy_read(&client, &bucket).await
    })
    .await
}

pub async fn zero_copy_read_regional_rapid_flat_colocated() -> anyhow::Result<()> {
    with_regional_rapid_bucket(false, |client, bucket| async move {
        test_zero_copy_read(&client, &bucket).await
    })
    .await
}

/// Test Suite 1 - Test 1: Multiple Ranged Read
///
/// Tests reading an object across multiple concurrent range read streams over the
/// bidirectional gRPC stream session. Validates that concurrent streams drain properly
/// without deadlock, all received bytes match the expected slices, total length matches,
/// and CRC32C checksum integrity across all ranges matches.
pub async fn test_multiple_ranged_read(client: &Storage, bucket_name: &str) -> anyhow::Result<()> {
    println!("--- [Conformance 1/5] Testing Multiple Ranged Read ---");
    const TOTAL_SIZE: usize = 512 * 1024;
    let payload = String::from_iter(('a'..='z').cycle().take(TOTAL_SIZE));
    let object_name = "bidi_read/multi_range_source.txt";

    let write = client
        .write_object(bucket_name, object_name, payload.clone())
        .set_if_generation_match(0)
        .send_unbuffered()
        .await?;

    let descriptor = client.open_object(bucket_name, &write.name).send().await?;

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

    println!("SUCCESS on Conformance 1: Multiple Ranged Read (512 KiB across 4 concurrent ranges)");
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
    bucket_name: &str,
) -> anyhow::Result<()> {
    println!("--- [Conformance 2/5] Testing Read Post Stream Close ---");
    let payload = String::from_iter(('a'..='z').cycle().take(100_000));
    let object_name = "bidi_read/post_close_source.txt";

    let write = client
        .write_object(bucket_name, object_name, payload.clone())
        .set_if_generation_match(0)
        .send_unbuffered()
        .await?;

    let descriptor = client.open_object(bucket_name, &write.name).send().await?;

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

    println!("SUCCESS on Conformance 2: Read Post Stream Close");
    Ok(())
}

/// Test Suite 1 - Test 3: Zero-Copy Read
///
/// Tests concurrent zero-copy range reads, validating bytes::Bytes buffer access and memory safety.
pub async fn test_zero_copy_read(client: &Storage, bucket_name: &str) -> anyhow::Result<()> {
    println!("--- [Conformance 3/5] Testing Zero Copy Read ---");
    const SIZE: usize = 100_000;
    let payload = String::from_iter(('a'..='z').cycle().take(SIZE));
    let object_name = "bidi_read/zero_copy_source.txt";

    let write = client
        .write_object(bucket_name, object_name, payload.clone())
        .set_if_generation_match(0)
        .send_unbuffered()
        .await?;

    // Open connection and read first range with Fast Open (send_and_read)
    let (descriptor, reader1) = client
        .open_object(bucket_name, &write.name)
        .send_and_read(ReadRange::segment(0, 50_000))
        .await?;

    // Initiate second range on the open descriptor concurrently
    let reader2 = descriptor
        .read_range(ReadRange::segment(50_000, 50_000))
        .await;

    // Concurrently collect zero-copy bytes::Bytes chunks
    let (chunks1, chunks2) = tokio::try_join!(
        collect_zero_copy_chunks(reader1),
        collect_zero_copy_chunks(reader2),
    )?;

    // Verify chunk properties and reconstruct
    let mut combined1 = Vec::new();
    for chunk in &chunks1 {
        assert!(!chunk.is_empty(), "chunks should not be empty");
        combined1.extend_from_slice(chunk);
    }
    assert_eq!(combined1, &payload.as_bytes()[0..50_000]);

    let mut combined2 = Vec::new();
    for chunk in &chunks2 {
        assert!(!chunk.is_empty(), "chunks should not be empty");
        combined2.extend_from_slice(chunk);
    }
    assert_eq!(combined2, &payload.as_bytes()[50_000..100_000]);

    println!("SUCCESS on Conformance 3: Zero Copy Read");
    Ok(())
}

/// Test Suite 1 - Test 4: Non-Existent Bucket Read
///
/// Tests opening a stream on a non-existent bucket. Verifies that an appropriate
/// error with NotFound status (HTTP 404) or PermissionDenied (allowlist check) is returned.
pub async fn test_non_existent_bucket_read(client: &Storage) -> anyhow::Result<()> {
    println!("--- [Conformance 4/5] Testing Non Existent Bucket Read ---");
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

    println!("SUCCESS on Conformance 4: Non Existent Bucket Read");
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
pub async fn test_out_of_range(client: &Storage, bucket_name: &str) -> anyhow::Result<()> {
    println!("--- [Conformance 5/5] Testing Out Of Range Read ---");
    let payload = String::from_iter(('a'..='z').cycle().take(10_000));
    let object_name = "bidi_read/out_of_range_source.txt";

    let write = client
        .write_object(bucket_name, object_name, payload.clone())
        .set_if_generation_match(0)
        .send_unbuffered()
        .await?;

    let descriptor = client.open_object(bucket_name, &write.name).send().await?;

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
            println!("Out-of-range read returned immediate EOF");
        }
        Some(Err(err)) => {
            println!("Out-of-range read returned error as expected: {err:?}");
            let err_str = format!("{err:?}");
            assert!(
                err_str.contains("OUT_OF_RANGE")
                    || err_str.contains("OutOfRange")
                    || err_str.contains("InvalidArgument"),
                "unexpected error message for out of range: {err_str}"
            );
        }
        Some(Ok(data)) => {
            anyhow::bail!(
                "unexpected data returned for out of range read: {} bytes",
                data.len()
            );
        }
    }

    println!("SUCCESS on Conformance 5: Out Of Range Read");
    Ok(())
}

async fn drain_reader(mut reader: ReadObjectResponse) -> anyhow::Result<Vec<u8>> {
    let mut buf = Vec::new();
    while let Some(chunk) = reader.next().await.transpose()? {
        buf.extend_from_slice(&chunk);
    }
    Ok(buf)
}

async fn collect_zero_copy_chunks(
    mut reader: ReadObjectResponse,
) -> anyhow::Result<Vec<bytes::Bytes>> {
    let mut chunks = Vec::new();
    while let Some(chunk) = reader.next().await.transpose()? {
        chunks.push(chunk);
    }
    Ok(chunks)
}
