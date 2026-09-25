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

//! Cross-SDK conformance tests for bidirectional reads.

use google_cloud_gax::error::Error;
use google_cloud_gax::error::rpc::{Code, Status};
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

/// Preprod gRPC endpoint, used for bidi reads/writes and `StorageControl`.
const PREPROD_GRPC_ENDPOINT: &str = "https://storage-preprod-test-grpc.googleusercontent.com:443";
/// Preprod HTTP endpoint, used for JSON uploads.
const PREPROD_HTTP_ENDPOINT: &str = "https://storage-preprod-test-unified.googleusercontent.com";

/// Runs the bidi read conformance tests against each supported bucket type.
pub async fn run() -> anyhow::Result<()> {
    println!("\n=== Running Bidi Read Conformance Suite ===");

    let clients = &Clients::new().await?;
    cleanup_stale_buckets(&clients.control).await;

    println!("\n### No bucket");
    test_non_existent_bucket_read(clients).await?;

    // Tests that don't depend on the bucket type run once, on this bucket.
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

    // Zonal Rapid buckets only accept appendable objects.
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

/// Clients shared by all tests.
struct Clients {
    /// Bidi reads and appendable writes.
    grpc: Storage,
    /// JSON uploads.
    http: Storage,
    /// Bucket, object, folder and cache management.
    control: StorageControl,
}

impl Clients {
    /// `GOOGLE_CLOUD_TEST_STORAGE_ENDPOINT` overrides both data clients' endpoint, and
    /// `GOOGLE_CLOUD_TEST_STORAGE_CONTROL_ENDPOINT` overrides the control client's.
    async fn new() -> anyhow::Result<Self> {
        Ok(Self {
            grpc: build_storage_client(PREPROD_GRPC_ENDPOINT).await?,
            http: build_storage_client(PREPROD_HTTP_ENDPOINT).await?,
            control: build_storage_control_client(PREPROD_GRPC_ENDPOINT).await?,
        })
    }
}

async fn build_storage_client(default_endpoint: &str) -> anyhow::Result<Storage> {
    let endpoint = std::env::var("GOOGLE_CLOUD_TEST_STORAGE_ENDPOINT")
        .unwrap_or_else(|_| default_endpoint.to_string());
    Ok(Storage::builder().with_endpoint(endpoint).build().await?)
}

async fn build_storage_control_client(default_endpoint: &str) -> anyhow::Result<StorageControl> {
    let endpoint = std::env::var("GOOGLE_CLOUD_TEST_STORAGE_CONTROL_ENDPOINT")
        .unwrap_or_else(|_| default_endpoint.to_string());
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

#[derive(Clone, Copy, Debug)]
enum BucketType {
    RegionalStandard { hns: bool },
    ZonalRapid,
    RegionalRapid,
}

impl BucketType {
    fn label(self) -> &'static str {
        match self {
            Self::RegionalStandard { hns: false } => "Regional Standard (flat)",
            Self::RegionalStandard { hns: true } => "Regional Standard (HNS)",
            Self::ZonalRapid => "Zonal Rapid",
            Self::RegionalRapid => "Regional Rapid (HNS)",
        }
    }

    fn hns(self) -> bool {
        match self {
            Self::RegionalStandard { hns } => hns,
            // Zonal Rapid and Regional Rapid are only supported with HNS enabled.
            Self::ZonalRapid | Self::RegionalRapid => true,
        }
    }

    fn has_rapid_cache(self) -> bool {
        matches!(self, Self::RegionalRapid)
    }
}

/// Creates a bucket, runs `f` on it, and deletes the bucket even if `f` fails.
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

/// For Regional Rapid, also attaches the cache, and deletes the bucket if that fails.
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
            // The cache may have been partially created.
            let _ = cleanup_bucket(control, &bucket.name, true, true).await;
            return Err(e.into());
        }
    }

    Ok(bucket)
}

/// Deletes a bucket and everything in it. `has_rapid_cache` and `is_hns` skip the
/// steps that don't apply to the bucket.
async fn cleanup_bucket(
    control: &StorageControl,
    bucket_name: &str,
    has_rapid_cache: bool,
    is_hns: bool,
) -> anyhow::Result<()> {
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
                // b/565175323: the cache is disabled, but the LRO returns an empty result.
                let err_str = format!("{e:?}");
                if !err_str.contains("neither result nor error set in LRO result") {
                    tracing::warn!("disable_rapid_cache on {} returned: {e:?}", cache.name);
                }
            }
        }
    }

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

/// Deletes `integration-test=true` buckets older than 48 hours.
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
        println!("cleaning up {} stale buckets", stale.len());
        for name in stale {
            // Stale buckets may be of any type.
            let _ = cleanup_bucket(control, &name, true, true).await;
        }
    }
}

/// Set `appendable` for Zonal Rapid buckets, which only accept appendable objects; other
/// buckets only accept regular uploads. Appendable uploads need
/// `--cfg google_cloud_unstable_storage_bidi`.
async fn write_test_object(
    clients: &Clients,
    bucket_name: &str,
    object_name: &str,
    payload: String,
    appendable: bool,
) -> anyhow::Result<()> {
    if appendable {
        #[cfg(google_cloud_unstable_storage_bidi)]
        {
            let mut writer = clients
                .grpc
                .open_appendable_object(bucket_name, object_name)
                .send()
                .await?;
            writer.append(bytes::Bytes::from(payload)).await?;
            writer.finalize().await?;
            return Ok(());
        }
        #[cfg(not(google_cloud_unstable_storage_bidi))]
        {
            anyhow::bail!("appendable uploads require `--cfg google_cloud_unstable_storage_bidi`");
        }
    }
    clients
        .http
        .write_object(bucket_name, object_name, payload)
        .set_if_generation_match(0)
        .send_unbuffered()
        .await?;
    Ok(())
}

async fn test_multiple_ranged_read(
    clients: &Clients,
    bucket_name: &str,
    appendable: bool,
) -> anyhow::Result<()> {
    println!("  test_multiple_ranged_read ...");
    const KIB: u64 = 1024;
    let payload = String::from_iter(('a'..='z').cycle().take(512 * KIB as usize));
    let object_name = format!("bidi_read/multi_range_source_{}.txt", random_bucket_id());
    write_test_object(
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

    // Four non-overlapping (offset, length) ranges covering the whole object.
    let ranges = [
        (0, 64 * KIB),
        (64 * KIB, 128 * KIB),
        (192 * KIB, 192 * KIB),
        (384 * KIB, 128 * KIB),
    ];
    let mut readers = Vec::new();
    for (offset, len) in ranges {
        readers.push(descriptor.read_range(ReadRange::segment(offset, len)).await);
    }

    // The ranges share one stream, so they must be drained concurrently to avoid a deadlock.
    let buffers = futures::future::try_join_all(readers.into_iter().map(drain_reader)).await?;

    for ((offset, len), buf) in ranges.into_iter().zip(buffers) {
        let (start, end) = (offset as usize, (offset + len) as usize);
        assert_eq!(buf, &payload.as_bytes()[start..end], "range {start}..{end}");
    }

    println!("  test_multiple_ranged_read ok");
    Ok(())
}

async fn test_read_post_stream_close(clients: &Clients, bucket_name: &str) -> anyhow::Result<()> {
    println!("  test_read_post_stream_close ...");
    let payload = String::from_iter(('a'..='z').cycle().take(100_000));
    let object_name = format!("bidi_read/post_close_source_{}.txt", random_bucket_id());
    write_test_object(clients, bucket_name, &object_name, payload.clone(), false).await?;

    let descriptor = clients
        .grpc
        .open_object(bucket_name, &object_name)
        .send()
        .await?;

    let mut reader = descriptor.read_range(ReadRange::head(100)).await;
    let mut data = Vec::new();
    while let Some(chunk) = reader.next().await.transpose()? {
        data.extend_from_slice(&chunk);
    }
    assert_eq!(data, &payload.as_bytes()[0..100]);

    // A finished reader keeps returning `None`.
    assert!(reader.next().await.is_none());
    assert!(reader.next().await.is_none());

    // Dropping an unfinished reader must not break the descriptor.
    let unconsumed_reader = descriptor.read_range(ReadRange::segment(500, 10_000)).await;
    drop(unconsumed_reader);

    let subsequent_data =
        drain_reader(descriptor.read_range(ReadRange::segment(200, 50)).await).await?;
    assert_eq!(subsequent_data, &payload.as_bytes()[200..250]);

    println!("  test_read_post_stream_close ok");
    Ok(())
}

async fn test_non_existent_bucket_read(clients: &Clients) -> anyhow::Result<()> {
    println!("  test_non_existent_bucket_read ...");
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
            match reader.next().await {
                Some(Err(err)) => assert_is_not_found(&err),
                other => anyhow::bail!("expected NotFound error on read_range, got {other:?}"),
            }
        }
        Err(err) => assert_is_not_found(&err),
    }

    println!("  test_non_existent_bucket_read ok");
    Ok(())
}

async fn test_out_of_range(clients: &Clients, bucket_name: &str) -> anyhow::Result<()> {
    println!("  test_out_of_range ...");
    let payload = String::from_iter(('a'..='z').cycle().take(10_000));
    let object_name = format!("bidi_read/out_of_range_source_{}.txt", random_bucket_id());
    write_test_object(clients, bucket_name, &object_name, payload.clone(), false).await?;

    let descriptor = clients
        .grpc
        .open_object(bucket_name, &object_name)
        .send()
        .await?;

    let valid_data = drain_reader(descriptor.read_range(ReadRange::head(50)).await).await?;
    assert_eq!(valid_data, &payload.as_bytes()[0..50]);

    // The object is only 10,000 bytes.
    let mut oob_reader = descriptor
        .read_range(ReadRange::segment(50_000, 1_000))
        .await;

    match oob_reader.next().await {
        None => println!("    out-of-range read returned immediate EOF"),
        Some(Err(err)) => {
            let Some(status) = find_rpc_status(&err) else {
                anyhow::bail!("expected an RPC status for out of range read, got {err:?}");
            };
            println!(
                "    got expected error: {:?}: {}",
                status.code, status.message
            );
            assert!(
                matches!(status.code, Code::OutOfRange | Code::InvalidArgument),
                "unexpected status for out of range read: {status:?}"
            );
        }
        Some(Ok(data)) => anyhow::bail!(
            "unexpected data returned for out of range read: {} bytes",
            data.len()
        ),
    }

    println!("  test_out_of_range ok");
    Ok(())
}

async fn drain_reader(mut reader: ReadObjectResponse) -> anyhow::Result<Vec<u8>> {
    let mut buf = Vec::new();
    while let Some(chunk) = reader.next().await.transpose()? {
        buf.extend_from_slice(&chunk);
    }
    Ok(buf)
}

/// Also accepts `PermissionDenied` (allowlist check).
fn assert_is_not_found(err: &Error) {
    if let Some(status) = find_rpc_status(err) {
        assert!(
            matches!(status.code, Code::NotFound | Code::PermissionDenied),
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

/// Bidi read errors wrap the service error, so this searches the source chain for the status.
fn find_rpc_status(err: &Error) -> Option<Status> {
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
