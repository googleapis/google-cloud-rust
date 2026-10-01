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
use futures::FutureExt as _;
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
use std::panic::AssertUnwindSafe;
use std::time::Duration;

/// Runs the bidi read conformance tests against each supported bucket type.
pub async fn run() -> anyhow::Result<()> {
    println!("\n========================================================");
    println!(" Running Bidi Read Conformance Integration Test Suite");
    println!("========================================================");

    let clients = Clients::new().await?;

    test_non_existent_bucket_read(&clients).await?;

    with_bucket(
        &clients,
        BucketType::RegionalStandard { hns: false },
        async |bucket, bucket_type| {
            test_read_post_stream_close(&clients, bucket, bucket_type).await?;
            test_out_of_range(&clients, bucket, bucket_type).await?;
            test_multiple_ranged_read(&clients, bucket, bucket_type).await
        },
    )
    .await?;

    // Only the data path is exercised on Regional Standard (HNS). The error-path tests already
    // run on HNS through the Zonal Rapid and Regional Rapid buckets, which always enable HNS.
    with_bucket(
        &clients,
        BucketType::RegionalStandard { hns: true },
        async |bucket, bucket_type| test_multiple_ranged_read(&clients, bucket, bucket_type).await,
    )
    .await?;

    with_bucket(
        &clients,
        BucketType::ZonalRapid,
        async |bucket, bucket_type| {
            test_read_post_stream_close(&clients, bucket, bucket_type).await?;
            test_out_of_range(&clients, bucket, bucket_type).await?;
            test_multiple_ranged_read(&clients, bucket, bucket_type).await
        },
    )
    .await?;

    with_bucket(
        &clients,
        BucketType::RegionalRapid,
        async |bucket, bucket_type| {
            test_read_post_stream_close(&clients, bucket, bucket_type).await?;
            test_out_of_range(&clients, bucket, bucket_type).await?;
            test_multiple_ranged_read(&clients, bucket, bucket_type).await
        },
    )
    .await?;

    println!("\n>>> All Bidi Read Conformance integration tests completed successfully! <<<\n");
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
        let grpc_endpoint = std::env::var("GOOGLE_CLOUD_TEST_GRPC_ENDPOINT").map_err(|_| {
            anyhow::anyhow!("GOOGLE_CLOUD_TEST_GRPC_ENDPOINT environment variable must be set")
        })?;
        let http_endpoint = std::env::var("GOOGLE_CLOUD_TEST_HTTP_ENDPOINT").map_err(|_| {
            anyhow::anyhow!("GOOGLE_CLOUD_TEST_HTTP_ENDPOINT environment variable must be set")
        })?;

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

    fn is_appendable(self) -> bool {
        matches!(self, Self::ZonalRapid)
    }
}

/// Creates a bucket, runs `f` on it, and deletes the bucket even if `f` fails or panics.
async fn with_bucket<F>(clients: &Clients, bucket_type: BucketType, f: F) -> anyhow::Result<()>
where
    F: AsyncFnOnce(&str, BucketType) -> anyhow::Result<()>,
{
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
            eprintln!(
                "Warning: failed to disable rapid cache {}: {e:?}",
                cache.name
            );
        } else {
            println!("SUCCESS: disabled rapid cache {}", cache.name);
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

async fn write_appendable_object(
    #[allow(unused_variables)] client: &Storage,
    #[allow(unused_variables)] bucket_name: &str,
    #[allow(unused_variables)] object_name: &str,
    #[allow(unused_variables)] payload: Bytes,
) -> anyhow::Result<()> {
    #[cfg(google_cloud_unstable_storage_bidi)]
    {
        let mut writer = client
            .open_appendable_object(bucket_name, object_name)
            .send()
            .await?;
        writer.append(payload).await?;
        writer.finalize().await?;
        Ok(())
    }
    #[cfg(not(google_cloud_unstable_storage_bidi))]
    {
        anyhow::bail!("appendable uploads require `--cfg google_cloud_unstable_storage_bidi`")
    }
}

async fn test_multiple_ranged_read(
    clients: &Clients,
    bucket_name: &str,
    bucket_type: BucketType,
) -> anyhow::Result<()> {
    println!(
        "\n--- Testing Multiple Ranged Read ({}) ---",
        bucket_type.label()
    );
    const KIB: u64 = 1024;
    let (payload, descriptor) = upload_and_open(
        clients,
        bucket_name,
        512 * KIB as usize,
        bucket_type.is_appendable(),
    )
    .await?;

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

    println!(
        "SUCCESS: Multiple Ranged Read ({}) -> 4 concurrent ranges verified",
        bucket_type.label()
    );
    Ok(())
}

async fn test_read_post_stream_close(
    clients: &Clients,
    bucket_name: &str,
    bucket_type: BucketType,
) -> anyhow::Result<()> {
    println!(
        "\n--- Testing Read Post Stream Close ({}) ---",
        bucket_type.label()
    );
    let (payload, descriptor) =
        upload_and_open(clients, bucket_name, 100_000, bucket_type.is_appendable()).await?;

    let mut reader = descriptor.read_range(ReadRange::head(100)).await;
    assert_eq!(drain_reader(&mut reader).await?, &payload[0..100]);

    assert!(reader.next().await.is_none());
    assert!(reader.next().await.is_none());

    // Dropping an unfinished reader must not break the descriptor.
    let unconsumed_reader = descriptor.read_range(ReadRange::segment(500, 10_000)).await;
    drop(unconsumed_reader);

    let mut reader = descriptor.read_range(ReadRange::segment(200, 50)).await;
    assert_eq!(drain_reader(&mut reader).await?, &payload[200..250]);

    println!("SUCCESS: Read Post Stream Close ({})", bucket_type.label());
    Ok(())
}

async fn test_non_existent_bucket_read(clients: &Clients) -> anyhow::Result<()> {
    println!("\n--- Testing Non-Existent Bucket Read ---");
    // `random_bucket_id()` is a valid, max-length bucket id that is not expected to exist.
    let non_existent_bucket = format!("projects/_/buckets/{}", random_bucket_id());

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

    println!("SUCCESS: expected NotFound or PermissionDenied received for non-existent bucket");
    Ok(())
}

/// Verifies that an out-of-bounds read returns `OutOfRange` or `InvalidArgument`,
/// while concurrent valid range reads on the same session succeed.
///
/// Both ranges are dispatched concurrently on the same `ObjectDescriptor` session:
/// the valid range completes and yields the expected bytes, while the out-of-bounds range
/// yields an `OutOfRange` / `InvalidArgument` error.
async fn test_out_of_range(
    clients: &Clients,
    bucket_name: &str,
    bucket_type: BucketType,
) -> anyhow::Result<()> {
    println!(
        "\n--- Testing Out Of Range Read ({}) ---",
        bucket_type.label()
    );
    let (payload, descriptor) =
        upload_and_open(clients, bucket_name, 10_000, bucket_type.is_appendable()).await?;

    let valid_fut = async {
        let mut reader = descriptor.read_range(ReadRange::head(50)).await;
        drain_reader(&mut reader).await
    };
    let oob_fut = async {
        let mut oob_reader = descriptor
            .read_range(ReadRange::segment(50_000, 1_000))
            .await;
        oob_reader.next().await
    };

    let (valid_res, oob_res) = tokio::join!(valid_fut, oob_fut);

    assert_eq!(valid_res?, &payload[0..50]);

    match oob_res {
        None => {
            panic!("expected OutOfRange or InvalidArgument error for out of range read, got None")
        }
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

    println!(
        "SUCCESS: Out Of Range Read ({}) -> expected OutOfRange or InvalidArgument received",
        bucket_type.label()
    );
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
