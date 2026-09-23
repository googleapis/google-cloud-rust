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

use google_cloud_storage::client::Storage;
use google_cloud_storage::model_ext::ReadRange;
use google_cloud_storage::read_object::ReadObjectResponse;

/// Runs all 5 live cloud conformance tests for Bidirectional Read.
pub async fn run(client: &Storage, bucket_name: &str) -> anyhow::Result<()> {
    run_with_scenario(client, bucket_name, "Default").await
}

/// Runs all 5 live cloud conformance tests for Bidirectional Read with a specific scenario name.
pub async fn run_with_scenario(
    client: &Storage,
    bucket_name: &str,
    scenario_name: &str,
) -> anyhow::Result<()> {
    println!("\n=== [Scenario: {scenario_name}] Running Bidi Read Conformance Suite ===");
    test_multiple_ranged_read(client, bucket_name).await?;
    test_read_post_stream_close(client, bucket_name).await?;
    test_zero_copy_read(client, bucket_name).await?;
    test_non_existent_bucket_read(client).await?;
    test_out_of_range(client, bucket_name).await?;
    println!("=== [Scenario: {scenario_name}] Conformance Suite Completed Successfully ===\n");
    Ok(())
}

// -----------------------------------------------------------------------------
// Lifecycle Helpers (manage bucket provisioning -> test execution -> teardown)
// -----------------------------------------------------------------------------

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
    let client = crate::build_storage_client().await?;
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
    let client = if colocated {
        crate::build_storage_client().await?
    } else {
        crate::build_non_colocated_storage_client("us-central1-b").await?
    };
    let result = f(client, bucket.name.clone()).await;
    let _ = storage_samples::cleanup_bucket(control, bucket.name, bucket.project).await;
    result
}

async fn with_regional_rapid_bucket<F, Fut>(hns: bool, f: F) -> anyhow::Result<()>
where
    F: FnOnce(Storage, String) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<()>>,
{
    let (control, bucket) = crate::create_test_regional_rapid_bucket(hns).await?;
    let client = crate::build_regional_rapid_storage_client().await?;
    let result = f(client, bucket.name.clone()).await;
    let _ = crate::cleanup_regional_rapid_bucket(control, bucket.name, bucket.project).await;
    result
}

// -----------------------------------------------------------------------------
// Self-Contained Test Runners (Invoked by driver.rs)
// -----------------------------------------------------------------------------

// Non-bucket-type dependent (Tests 2, 4, 5)
pub async fn run_read_post_stream_close() -> anyhow::Result<()> {
    with_regional_standard_bucket(false, |client, bucket| async move {
        test_read_post_stream_close(&client, &bucket).await
    })
    .await
}

pub async fn run_non_existent_bucket_read() -> anyhow::Result<()> {
    let client = crate::build_storage_client().await?;
    test_non_existent_bucket_read(&client).await
}

pub async fn run_out_of_range() -> anyhow::Result<()> {
    with_regional_standard_bucket(false, |client, bucket| async move {
        test_out_of_range(&client, &bucket).await
    })
    .await
}

// Bucket-type-dependent (Tests 1 & 3)
pub async fn run_multiple_ranged_read_regional_standard(hns: bool) -> anyhow::Result<()> {
    with_regional_standard_bucket(hns, |client, bucket| async move {
        test_multiple_ranged_read(&client, &bucket).await
    })
    .await
}

pub async fn run_zero_copy_read_regional_standard(hns: bool) -> anyhow::Result<()> {
    with_regional_standard_bucket(hns, |client, bucket| async move {
        test_zero_copy_read(&client, &bucket).await
    })
    .await
}

pub async fn run_multiple_ranged_read_zonal_rapid(colocated: bool) -> anyhow::Result<()> {
    with_zonal_rapid_bucket(colocated, |client, bucket| async move {
        test_multiple_ranged_read(&client, &bucket).await
    })
    .await
}

pub async fn run_zero_copy_read_zonal_rapid(colocated: bool) -> anyhow::Result<()> {
    with_zonal_rapid_bucket(colocated, |client, bucket| async move {
        test_zero_copy_read(&client, &bucket).await
    })
    .await
}

pub async fn run_multiple_ranged_read_regional_rapid(hns: bool) -> anyhow::Result<()> {
    with_regional_rapid_bucket(hns, |client, bucket| async move {
        test_multiple_ranged_read(&client, &bucket).await
    })
    .await
}

pub async fn run_zero_copy_read_regional_rapid(hns: bool) -> anyhow::Result<()> {
    with_regional_rapid_bucket(hns, |client, bucket| async move {
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
