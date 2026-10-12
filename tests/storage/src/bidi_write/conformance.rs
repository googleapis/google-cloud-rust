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

//! Cross-SDK conformance tests for bidirectional writes (appendable uploads).

use bytes::Bytes;
use futures::FutureExt as _;
use google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder;
use google_cloud_gax::options::RequestOptionsBuilder as _;
use google_cloud_gax::paginator::ItemPaginator as _;
use google_cloud_gax::retry_policy::RetryPolicyExt as _;
use google_cloud_lro::Poller as _;
use google_cloud_storage::appendable_object_writer::AppendableObjectWriter;
use google_cloud_storage::client::{Storage, StorageControl};
use google_cloud_storage::model::bucket::iam_config::UniformBucketLevelAccess;
use google_cloud_storage::model::bucket::{
    CustomPlacementConfig, HierarchicalNamespace, IamConfig,
};
use google_cloud_storage::model::{Bucket, Object, RapidCache};
use google_cloud_storage::model_ext::ReadRange;
use google_cloud_storage::retry_policy::RetryableErrors;
use google_cloud_test_utils::resource_names::{LowercaseAlphanumeric, random_bucket_id};
use google_cloud_test_utils::runtime_config::{project_id, region_id, zone_id};
use std::panic::AssertUnwindSafe;
use std::time::Duration;

/// Runs the bidi write conformance tests against each supported Rapid bucket type.
pub async fn run() -> anyhow::Result<()> {
    println!("\n========================================================");
    println!(" Running Bidi Write Conformance Integration Test Suite");
    println!("========================================================");

    let clients = Clients::new().await?;

    for bucket_type in [BucketType::ZonalRapid, BucketType::RegionalRapid] {
        with_bucket(&clients, bucket_type, async |bucket, bucket_type| {
            test_appendable_upload_empty_object(&clients, bucket, bucket_type).await?;
            test_multi_chunk_appendable_upload(&clients, bucket, bucket_type).await?;
            test_explicit_flush(&clients, bucket, bucket_type).await?;
            test_appendable_upload_takeover(&clients, bucket, bucket_type).await?;
            test_takeover_just_to_finalize(&clients, bucket, bucket_type).await
        })
        .await?;
    }

    println!("\n>>> All Bidi Write Conformance integration tests completed successfully! <<<\n");
    Ok(())
}

struct Clients {
    /// Appendable writes and bidi reads.
    grpc: Storage,
    /// Bucket, object, folder and cache management.
    control: StorageControl,
}

impl Clients {
    async fn new() -> anyhow::Result<Self> {
        let grpc_endpoint = std::env::var("GOOGLE_CLOUD_TEST_GRPC_ENDPOINT").map_err(|_| {
            anyhow::anyhow!("GOOGLE_CLOUD_TEST_GRPC_ENDPOINT environment variable must be set")
        })?;

        let grpc = Storage::builder()
            .with_endpoint(&grpc_endpoint)
            .build()
            .await?;

        let backoff = ExponentialBackoffBuilder::new()
            .with_initial_delay(Duration::from_secs(2))
            .with_maximum_delay(Duration::from_secs(8))
            .build()?;
        let control = StorageControl::builder()
            .with_endpoint(&grpc_endpoint)
            .with_backoff_policy(backoff)
            .with_retry_policy(RetryableErrors.with_attempt_limit(5))
            .build()
            .await?;

        Ok(Self { grpc, control })
    }
}

#[derive(Clone, Copy, Debug)]
enum BucketType {
    ZonalRapid,
    RegionalRapid,
}

impl BucketType {
    fn label(self) -> &'static str {
        match self {
            Self::ZonalRapid => "Zonal Rapid",
            Self::RegionalRapid => "Regional Rapid (HNS)",
        }
    }

    fn has_rapid_cache(self) -> bool {
        matches!(self, Self::RegionalRapid)
    }
}

/// Creates a bucket, runs `f` on it, and deletes the bucket even if `f` fails or panics.
async fn with_bucket(
    clients: &Clients,
    bucket_type: BucketType,
    f: impl AsyncFnOnce(&str, BucketType) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let bucket_id = random_bucket_id();
    println!("\n========================================================");
    println!(" Testing Bucket Type: {}", bucket_type.label());
    println!(" Bucket: {bucket_id}");
    println!("========================================================");
    let bucket = create_bucket(&clients.control, bucket_type, bucket_id).await?;
    // A failed `assert!` panics, so catch the unwind to make sure cleanup still runs.
    let result = AssertUnwindSafe(f(&bucket.name, bucket_type))
        .catch_unwind()
        .await;
    cleanup_bucket(
        &clients.control,
        &bucket.name,
        bucket_type.has_rapid_cache(),
    )
    .await;
    match result {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

/// For Regional Rapid, also attaches the cache, and deletes the bucket if that fails.
async fn create_bucket(
    control: &StorageControl,
    bucket_type: BucketType,
    bucket_id: String,
) -> anyhow::Result<Bucket> {
    let zone = zone_id();
    // Derive region from zone if zone follows standard `{region}-{zone_letter}` pattern,
    // otherwise fallback to `region_id()`. This prevents region/zone mismatch errors.
    let region = zone
        .rsplit_once('-')
        .map(|(r, _)| r.to_string())
        .unwrap_or_else(region_id);

    // Zonal Rapid and Regional Rapid are only supported with HNS enabled.
    let mut bucket = Bucket::new()
        .set_project(format!("projects/{}", project_id()?))
        .set_location(region)
        .set_labels([("integration-test", "true")])
        .set_hierarchical_namespace(HierarchicalNamespace::new().set_enabled(true))
        .set_iam_config(
            IamConfig::new()
                .set_uniform_bucket_level_access(UniformBucketLevelAccess::new().set_enabled(true)),
        );

    if matches!(bucket_type, BucketType::ZonalRapid) {
        bucket = bucket
            .set_custom_placement_config(CustomPlacementConfig::new().set_data_locations([&zone]))
            .set_storage_class("RAPID");
    }

    let bucket = control
        .create_bucket()
        .set_parent("projects/_")
        .set_bucket_id(bucket_id)
        .set_bucket(bucket)
        .with_idempotency(true)
        .send()
        .await?;

    if bucket_type.has_rapid_cache() {
        let rapid_cache = RapidCache::new()
            .set_name(format!("{}/rapidCaches/{zone}", bucket.name))
            .set_zone(&zone)
            .set_cache_type("rapid-cache-ultra");

        println!("Attaching rapid-cache-ultra in {zone} (this can take a minute or more)...");
        let attached = control
            .create_rapid_cache()
            .set_parent(&bucket.name)
            .set_rapid_cache(rapid_cache)
            .poller()
            .until_done()
            .await;
        if let Err(e) = attached {
            cleanup_bucket(control, &bucket.name, true).await;
            return Err(e.into());
        }
        println!("SUCCESS: attached rapid-cache-ultra in {zone}");
        // Give the control plane cache attachment time to propagate to the data plane.
        tokio::time::sleep(Duration::from_secs(5)).await;
    }

    Ok(bucket)
}

/// Deletes a bucket and everything in it. Failures are printed, not returned.
async fn cleanup_bucket(control: &StorageControl, bucket_name: &str, has_rapid_cache: bool) {
    if has_rapid_cache {
        disable_rapid_caches(control, bucket_name).await;
    }
    let result = match project_id() {
        Ok(project_id) => {
            storage_samples::cleanup_bucket(control.clone(), bucket_name.to_string(), project_id)
                .await
        }
        Err(e) => Err(e),
    };
    if let Err(e) = result {
        eprintln!("Warning: failed to delete bucket {bucket_name} during teardown: {e:?}");
    }
}

async fn disable_rapid_caches(control: &StorageControl, bucket_name: &str) {
    let mut caches = control
        .list_rapid_caches()
        .set_parent(bucket_name)
        .by_item();
    while let Some(cache) = caches.next().await {
        let cache = match cache {
            Ok(cache) => cache,
            Err(e) => {
                eprintln!(
                    "Warning: failed to list rapid caches in {bucket_name} during teardown: {e:?}"
                );
                return;
            }
        };
        println!("Disabling rapid cache {}...", cache.name);
        // TODO(b/565175323): Remove this timeout workaround once server-side LRO completion is fixed.
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            control
                .disable_rapid_cache()
                .set_name(&cache.name)
                .poller()
                .until_done(),
        )
        .await;
        match result {
            Ok(Ok(_)) => println!("SUCCESS: disabled rapid cache {}", cache.name),
            Ok(Err(e)) => {
                if !format!("{e:?}").contains("neither result nor error set in LRO result") {
                    eprintln!(
                        "Warning: failed to disable rapid cache {}: {e:?}",
                        cache.name
                    );
                } else {
                    println!("SUCCESS: disabled rapid cache {}", cache.name);
                }
            }
            Err(_) => {
                println!(
                    "SUCCESS: disabled rapid cache {} (timed out waiting for LRO)",
                    cache.name
                );
            }
        }
    }
}

fn random_object_name(prefix: &str) -> String {
    format!(
        "bidi_write/{prefix}_{}.txt",
        LowercaseAlphanumeric.random_string(16)
    )
}

fn test_payload(len: usize) -> Bytes {
    Bytes::from_iter((b'a'..=b'z').cycle().take(len))
}

/// Opens an appendable upload for the given bucket type.
///
/// For `RegionalRapid`, the bucket's default storage class is `STANDARD`, so the
/// object must explicitly specify `storage_class: "RAPID"` to write into the
/// attached zonal cache.
async fn open_appendable_object(
    client: &Storage,
    bucket_name: &str,
    object_name: &str,
    bucket_type: BucketType,
) -> anyhow::Result<AppendableObjectWriter> {
    let mut attempts = 0;
    loop {
        attempts += 1;
        let mut builder = client.open_appendable_object(bucket_name, object_name);
        if matches!(bucket_type, BucketType::RegionalRapid) {
            builder = builder.set_storage_class("RAPID");
        }
        match builder.send().await {
            Ok(writer) => return Ok(writer),
            Err(e)
                if attempts < 5
                    && (format!("{e:?}").contains("GRPC_APPENDABLE_OBJECT_NOT_SUPPORTED")
                        || format!("{e:?}").contains("RAPID_BUCKET_REQUIRED")) =>
            {
                println!(
                    "Waiting for rapid cache to propagate to data plane (attempt {attempts}/5)..."
                );
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
            Err(e) => return Err(e.into()),
        }
    }
}

/// Verifies the finalized `Object` metadata (name, size, cumulative CRC32C) and reads
/// back the object over gRPC to verify byte-for-byte content equivalence.
async fn verify_finalized_object(
    client: &Storage,
    bucket_name: &str,
    object_name: &str,
    object: &Object,
    expected: &[u8],
) -> anyhow::Result<()> {
    assert_eq!(object.name, object_name);
    assert_eq!(object.size, expected.len() as i64);
    assert_eq!(
        object.checksums.as_ref().and_then(|c| c.crc32c),
        Some(crc32c::crc32c(expected))
    );

    let (_descriptor, mut reader) = client
        .open_object(bucket_name, object_name)
        .send_and_read(ReadRange::all())
        .await?;
    let mut actual = Vec::new();
    while let Some(chunk) = reader.next().await.transpose()? {
        actual.extend_from_slice(&chunk);
    }
    assert_eq!(actual, expected);
    Ok(())
}

/// Opens an appendable upload and immediately finalizes it without writing bytes.
/// Asserts `object.size == 0`, CRC32C checksum of empty bytes (`0`), and empty read-back content.
async fn test_appendable_upload_empty_object(
    clients: &Clients,
    bucket_name: &str,
    bucket_type: BucketType,
) -> anyhow::Result<()> {
    println!(
        "\n--- Testing Appendable Upload Empty Object ({}) ---",
        bucket_type.label()
    );

    // Arrange
    let object_name = random_object_name("empty");

    // Act
    let writer =
        open_appendable_object(&clients.grpc, bucket_name, &object_name, bucket_type).await?;
    let object = writer.finalize().await?;

    // Assert
    verify_finalized_object(&clients.grpc, bucket_name, &object_name, &object, b"").await?;

    println!(
        "SUCCESS: Appendable Upload Empty Object ({})",
        bucket_type.label()
    );
    Ok(())
}

/// Writes multiple byte chunks with Appendable Upload, finalizes,
/// and asserts total size, cumulative CRC32C, and binary equivalence on read-back.
async fn test_multi_chunk_appendable_upload(
    clients: &Clients,
    bucket_name: &str,
    bucket_type: BucketType,
) -> anyhow::Result<()> {
    println!(
        "\n--- Testing Multi Chunk Appendable Upload & Finalization ({}) ---",
        bucket_type.label()
    );

    // Arrange
    const KIB: usize = 1024;
    let object_name = random_object_name("multi_chunk");
    let payload = test_payload(64 * KIB);
    let mid = payload.len() / 2;
    let chunk1 = payload.slice(..mid);
    let chunk2 = payload.slice(mid..);

    // Act
    let mut writer =
        open_appendable_object(&clients.grpc, bucket_name, &object_name, bucket_type).await?;
    writer.append(chunk1).await?;
    writer.append(chunk2).await?;
    let object = writer.finalize().await?;

    // Assert
    verify_finalized_object(&clients.grpc, bucket_name, &object_name, &object, &payload).await?;

    println!(
        "SUCCESS: Multi Chunk Appendable Upload & Finalization ({})",
        bucket_type.label()
    );
    Ok(())
}

/// Writes 1 byte, explicitly invokes `writer.flush()`, writes remaining data,
/// finalizes, and verifies final object size, CRC32C checksum, and read-back content.
async fn test_explicit_flush(
    clients: &Clients,
    bucket_name: &str,
    bucket_type: BucketType,
) -> anyhow::Result<()> {
    println!("\n--- Testing Explicit Flush ({}) ---", bucket_type.label());

    // Arrange
    let object_name = random_object_name("explicit_flush");
    let payload = test_payload(10_000);

    // Act & Assert: Intermediate Flush Checkpoint
    let mut writer =
        open_appendable_object(&clients.grpc, bucket_name, &object_name, bucket_type).await?;
    writer.append(payload.slice(..1)).await?;
    let persisted = writer.flush().await?;
    assert_eq!(persisted, 1);
    assert_eq!(writer.persisted_size(), 1);

    // Act: Append Remaining Data & Finalize
    writer.append(payload.slice(1..)).await?;
    let object = writer.finalize().await?;

    // Assert: Final Object Verification
    verify_finalized_object(&clients.grpc, bucket_name, &object_name, &object, &payload).await?;

    println!("SUCCESS: Explicit Flush ({})", bucket_type.label());
    Ok(())
}

/// Session 1 writes chunk 1 and closes without finalizing. Session 2 reopens the
/// same object generation, appends chunk 2, and finalizes, verifying cumulative
/// size, cumulative CRC32C, and read-back content.
async fn test_appendable_upload_takeover(
    clients: &Clients,
    bucket_name: &str,
    bucket_type: BucketType,
) -> anyhow::Result<()> {
    println!(
        "\n--- Testing Appendable Upload Takeover ({}) ---",
        bucket_type.label()
    );

    // Arrange
    let object_name = random_object_name("takeover");
    let payload = test_payload(10_000);
    let mid = (payload.len() / 2) + 1;
    let chunk1 = payload.slice(..mid);
    let chunk2 = payload.slice(mid..);
    let chunk1_len = chunk1.len() as i64;

    // Act: Session 1 (Write chunk 1 and close without finalizing)
    let mut writer1 =
        open_appendable_object(&clients.grpc, bucket_name, &object_name, bucket_type).await?;
    writer1.append(chunk1).await?;
    let generation = writer1.generation();
    let persisted = writer1.close().await?;

    // Assert: Session 1
    assert_eq!(persisted, chunk1_len);

    // Act: Session 2 (Reopen same object generation, append chunk 2, and finalize)
    let mut writer2 = clients
        .grpc
        .reopen_appendable_object(bucket_name, &object_name, generation)
        .send()
        .await?;
    assert_eq!(writer2.persisted_size(), chunk1_len);
    writer2.append(chunk2).await?;
    let object = writer2.finalize().await?;

    // Assert: Session 2 (Final Cumulative Object Verification)
    verify_finalized_object(&clients.grpc, bucket_name, &object_name, &object, &payload).await?;

    println!(
        "SUCCESS: Appendable Upload Takeover ({})",
        bucket_type.label()
    );
    Ok(())
}

/// Session 1 writes data and closes without finalizing. Session 2 takes over the
/// object generation and calls `finalize()` without appending data, verifying
/// object finalization, size, CRC32C, and read-back content.
async fn test_takeover_just_to_finalize(
    clients: &Clients,
    bucket_name: &str,
    bucket_type: BucketType,
) -> anyhow::Result<()> {
    println!(
        "\n--- Testing Takeover Just to Finalize ({}) ---",
        bucket_type.label()
    );

    // Arrange
    let object_name = random_object_name("takeover_finalize");
    let payload = test_payload(10_000);
    let payload_len = payload.len() as i64;

    // Act: Session 1 (Write full payload and close without finalizing)
    let mut writer1 =
        open_appendable_object(&clients.grpc, bucket_name, &object_name, bucket_type).await?;
    writer1.append(payload.clone()).await?;
    let generation = writer1.generation();
    let persisted = writer1.close().await?;

    // Assert: Session 1
    assert_eq!(persisted, payload_len);

    // Act: Session 2 (Reopen same object generation and finalize without appending)
    let writer2 = clients
        .grpc
        .reopen_appendable_object(bucket_name, &object_name, generation)
        .send()
        .await?;
    assert_eq!(writer2.persisted_size(), payload_len);
    let object = writer2.finalize().await?;

    // Assert: Session 2 (Final Object Verification)
    verify_finalized_object(&clients.grpc, bucket_name, &object_name, &object, &payload).await?;

    println!(
        "SUCCESS: Takeover Just to Finalize ({})",
        bucket_type.label()
    );
    Ok(())
}
