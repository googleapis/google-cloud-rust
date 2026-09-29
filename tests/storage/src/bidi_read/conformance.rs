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

use bytes::Bytes;
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
use google_cloud_storage::object_descriptor::ObjectDescriptor;
use google_cloud_storage::read_object::ReadObjectResponse;
use google_cloud_storage::retry_policy::RetryableErrors;
use google_cloud_test_utils::resource_names::{LowercaseAlphanumeric, random_bucket_id};
use google_cloud_test_utils::runtime_config::{project_id, region_id, zone_id};
use std::time::Duration;

/// Runs the bidi read conformance tests against each supported bucket type.
pub async fn run() -> anyhow::Result<()> {
    println!("\n=== Running Bidi Read Conformance Suite ===");

    let clients = Clients::new().await?;

    test_non_existent_bucket_read(&clients).await?;

    with_bucket(
        &clients,
        BucketType::RegionalStandard { hns: false },
        async |bucket| {
            test_read_post_stream_close(&clients, bucket).await?;
            test_out_of_range(&clients, bucket).await?;
            test_multiple_ranged_read(&clients, bucket, false).await
        },
    )
    .await?;

    with_bucket(
        &clients,
        BucketType::RegionalStandard { hns: true },
        async |bucket| test_multiple_ranged_read(&clients, bucket, false).await,
    )
    .await?;

    with_bucket(&clients, BucketType::ZonalRapid, async |bucket| {
        test_multiple_ranged_read(&clients, bucket, true).await
    })
    .await?;

    with_bucket(&clients, BucketType::RegionalRapid, async |bucket| {
        test_multiple_ranged_read(&clients, bucket, false).await
    })
    .await?;

    println!("\n=== Bidi Read Conformance Suite Completed Successfully ===\n");
    Ok(())
}

struct Clients {
    /// Bidi reads and appendable writes.
    grpc: Storage,
    /// JSON uploads.
    http: Storage,
    /// Bucket, object, folder and cache management.
    control: StorageControl,
}

impl Clients {
    async fn new() -> anyhow::Result<Self> {
        let grpc_endpoint = std::env::var("GRPC_ENDPOINT")
            .map_err(|_| anyhow::anyhow!("GRPC_ENDPOINT environment variable must be set"))?;
        let http_endpoint = std::env::var("HTTP_ENDPOINT")
            .map_err(|_| anyhow::anyhow!("HTTP_ENDPOINT environment variable must be set"))?;

        let grpc = Storage::builder()
            .with_endpoint(&grpc_endpoint)
            .build()
            .await?;
        let http = Storage::builder()
            .with_endpoint(&http_endpoint)
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

        Ok(Self {
            grpc,
            http,
            control,
        })
    }
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

    fn is_hns(self) -> bool {
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
async fn with_bucket<F>(clients: &Clients, bucket_type: BucketType, f: F) -> anyhow::Result<()>
where
    F: AsyncFnOnce(&str) -> anyhow::Result<()>,
{
    let bucket_id = random_bucket_id();
    println!("\n### {}: {bucket_id}", bucket_type.label());
    let bucket = create_bucket(&clients.control, bucket_type, bucket_id).await?;
    let result = f(&bucket.name).await;
    cleanup_bucket(
        &clients.control,
        &bucket.name,
        bucket_type.has_rapid_cache(),
    )
    .await;
    result
}

/// For Regional Rapid, also attaches the cache, and deletes the bucket if that fails.
async fn create_bucket(
    control: &StorageControl,
    bucket_type: BucketType,
    bucket_id: String,
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
    if bucket_type.is_hns() {
        bucket = bucket.set_hierarchical_namespace(HierarchicalNamespace::new().set_enabled(true));
    }
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

        println!("attaching rapid-cache-ultra in {zone} (this can take a minute or more)...");
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
        println!("  cleanup: failed to delete bucket {bucket_name}: {e:?}");
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
                println!("  cleanup: failed to list rapid caches in {bucket_name}: {e:?}");
                return;
            }
        };
        println!("  cleanup: disabling rapid cache {}", cache.name);
        let result = control
            .disable_rapid_cache()
            .set_name(&cache.name)
            .poller()
            .until_done()
            .await;
        // b/565175323: the cache is disabled, but the LRO returns an empty result.
        if let Err(e) = result
            && !format!("{e:?}").contains("neither result nor error set in LRO result")
        {
            println!(
                "  cleanup: failed to disable rapid cache {}: {e:?}",
                cache.name
            );
        }
    }
}

/// Uploads `len` bytes of test data and opens the object for bidi reads.
///
/// Set `appendable` for Zonal Rapid buckets, which only accept appendable objects; other
/// buckets only accept regular uploads.
async fn upload_and_open(
    clients: &Clients,
    bucket_name: &str,
    len: usize,
    appendable: bool,
) -> anyhow::Result<(Bytes, ObjectDescriptor)> {
    let payload = Bytes::from_iter((b'a'..=b'z').cycle().take(len));
    let object_name = format!("bidi_read/{}.txt", LowercaseAlphanumeric.random_string(16));
    if appendable {
        write_appendable_object(&clients.grpc, bucket_name, &object_name, payload.clone()).await?;
    } else {
        clients
            .http
            .write_object(bucket_name, &object_name, payload.clone())
            .set_if_generation_match(0)
            .send_unbuffered()
            .await?;
    }
    let descriptor = clients
        .grpc
        .open_object(bucket_name, &object_name)
        .send()
        .await?;
    Ok((payload, descriptor))
}

#[cfg(google_cloud_unstable_storage_bidi)]
async fn write_appendable_object(
    client: &Storage,
    bucket_name: &str,
    object_name: &str,
    payload: Bytes,
) -> anyhow::Result<()> {
    let mut writer = client
        .open_appendable_object(bucket_name, object_name)
        .send()
        .await?;
    writer.append(payload).await?;
    writer.finalize().await?;
    Ok(())
}

#[cfg(not(google_cloud_unstable_storage_bidi))]
async fn write_appendable_object(_: &Storage, _: &str, _: &str, _: Bytes) -> anyhow::Result<()> {
    anyhow::bail!("appendable uploads require `--cfg google_cloud_unstable_storage_bidi`")
}

async fn test_multiple_ranged_read(
    clients: &Clients,
    bucket_name: &str,
    appendable: bool,
) -> anyhow::Result<()> {
    println!("  test_multiple_ranged_read ...");
    const KIB: u64 = 1024;
    let (payload, descriptor) =
        upload_and_open(clients, bucket_name, 512 * KIB as usize, appendable).await?;

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
    let buffers = futures::future::try_join_all(readers.iter_mut().map(drain_reader)).await?;

    for ((offset, len), buf) in ranges.into_iter().zip(buffers) {
        let (start, end) = (offset as usize, (offset + len) as usize);
        assert_eq!(buf, &payload[start..end], "range {start}..{end}");
    }

    println!("  test_multiple_ranged_read ok");
    Ok(())
}

async fn test_read_post_stream_close(clients: &Clients, bucket_name: &str) -> anyhow::Result<()> {
    println!("  test_read_post_stream_close ...");
    let (payload, descriptor) = upload_and_open(clients, bucket_name, 100_000, false).await?;

    let mut reader = descriptor.read_range(ReadRange::head(100)).await;
    assert_eq!(drain_reader(&mut reader).await?, &payload[0..100]);

    assert!(reader.next().await.is_none());
    assert!(reader.next().await.is_none());

    // Dropping an unfinished reader must not break the descriptor.
    let unconsumed_reader = descriptor.read_range(ReadRange::segment(500, 10_000)).await;
    drop(unconsumed_reader);

    let mut reader = descriptor.read_range(ReadRange::segment(200, 50)).await;
    assert_eq!(drain_reader(&mut reader).await?, &payload[200..250]);

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

    let err = match result {
        Err(err) => err,
        Ok(descriptor) => {
            let mut reader = descriptor.read_range(ReadRange::head(100)).await;
            match reader.next().await {
                Some(Err(err)) => err,
                other => panic!("expected NotFound error on read_range, got {other:?}"),
            }
        }
    };
    assert_is_not_found(&err);

    println!("  test_non_existent_bucket_read ok");
    Ok(())
}

async fn test_out_of_range(clients: &Clients, bucket_name: &str) -> anyhow::Result<()> {
    println!("  test_out_of_range ...");
    let (payload, descriptor) = upload_and_open(clients, bucket_name, 10_000, false).await?;

    let mut reader = descriptor.read_range(ReadRange::head(50)).await;
    assert_eq!(drain_reader(&mut reader).await?, &payload[0..50]);

    let mut oob_reader = descriptor
        .read_range(ReadRange::segment(50_000, 1_000))
        .await;

    match oob_reader.next().await {
        None => {}
        Some(Err(err)) => {
            let Some(status) = find_rpc_status(&err) else {
                panic!("expected an RPC status for out of range read, got {err:?}");
            };
            assert!(
                matches!(status.code, Code::OutOfRange | Code::InvalidArgument),
                "unexpected status for out of range read: {status:?}"
            );
        }
        Some(Ok(data)) => panic!(
            "unexpected data returned for out of range read: {} bytes",
            data.len()
        ),
    }

    println!("  test_out_of_range ok");
    Ok(())
}

async fn drain_reader(reader: &mut ReadObjectResponse) -> anyhow::Result<Vec<u8>> {
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
    std::iter::successors(Some(err as &(dyn std::error::Error + 'static)), |e| {
        e.source()
    })
    .filter_map(|e| {
        e.downcast_ref::<Error>().or_else(|| {
            e.downcast_ref::<std::sync::Arc<Error>>()
                .map(|a| a.as_ref())
        })
    })
    .find_map(|e| e.status().cloned())
}
