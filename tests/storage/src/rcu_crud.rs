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

use google_cloud_gax::Result as GaxResult;
use google_cloud_gax::error::rpc::Code;
use google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder;
use google_cloud_gax::options::RequestOptionsBuilder;
use google_cloud_gax::paginator::ItemPaginator as _;
use google_cloud_gax::retry_policy::RetryPolicyExt;
use google_cloud_lro::Poller;
use google_cloud_storage::client::StorageControl;
use google_cloud_storage::model::bucket::iam_config::UniformBucketLevelAccess;
use google_cloud_storage::model::bucket::{HierarchicalNamespace, IamConfig};
use google_cloud_storage::model::{Bucket, RapidCache};
use google_cloud_storage::retry_policy::RetryableErrors;
use google_cloud_test_utils::resource_names::random_bucket_id;
use google_cloud_test_utils::runtime_config::zone_id;
use google_cloud_wkt::{Duration, FieldMask};
use std::time::Duration as StdDuration;

/// Creates a StorageControl client. Defaults to the Preprod endpoint
/// (`https://storage-preprod-test-grpc.googleusercontent.com:443`) unless overridden
/// by `GOOGLE_CLOUD_TEST_STORAGE_CONTROL_ENDPOINT`.
pub async fn create_client() -> anyhow::Result<StorageControl> {
    let endpoint =
        std::env::var("GOOGLE_CLOUD_TEST_STORAGE_CONTROL_ENDPOINT").unwrap_or_else(|_| {
            "https://storage-preprod-test-grpc.googleusercontent.com:443".to_string()
        });
    println!("StorageControl endpoint: {endpoint}");

    let client = StorageControl::builder()
        .with_endpoint(&endpoint)
        .with_backoff_policy(
            ExponentialBackoffBuilder::new()
                .with_initial_delay(StdDuration::from_secs(2))
                .with_maximum_delay(StdDuration::from_secs(8))
                .build()
                .unwrap(),
        )
        .with_retry_policy(RetryableErrors.with_attempt_limit(5))
        .build()
        .await?;

    Ok(client)
}

/// Creates an HNS-enabled bucket for RCU testing.
pub async fn create_test_hns_bucket(client: &StorageControl) -> anyhow::Result<Bucket> {
    let project_id = std::env::var("GOOGLE_CLOUD_PROJECT")?;
    let bucket_id = random_bucket_id();

    let create = client
        .create_bucket()
        .set_parent("projects/_")
        .set_bucket_id(bucket_id)
        .set_bucket(
            Bucket::new()
                .set_project(format!("projects/{project_id}"))
                .set_location("us-central1")
                .set_labels([("integration-test", "true")])
                .set_hierarchical_namespace(HierarchicalNamespace::new().set_enabled(true))
                .set_iam_config(IamConfig::new().set_uniform_bucket_level_access(
                    UniformBucketLevelAccess::new().set_enabled(true),
                )),
        )
        .with_idempotency(true)
        .send()
        .await?;
    println!("create_test_hns_bucket(): {create:?}");
    Ok(create)
}

/// Purges any lingering rapid caches configured on the bucket.
pub async fn purge_rapid_caches(client: &StorageControl, bucket_name: &str) {
    let mut stream = client.list_rapid_caches().set_parent(bucket_name).by_item();
    let mut to_disable = Vec::new();
    while let Some(item) = stream.next().await {
        if let Ok(cache) = item {
            to_disable.push(cache.name);
        }
    }
    for name in to_disable {
        if let Err(e) = client.disable_rapid_cache().set_name(&name).send().await {
            eprintln!("Warning: failed to disable rapid cache {name} during teardown: {e:?}");
        }
    }
}

/// Cleans up test resources: purges rapid caches and deletes the test bucket.
pub async fn cleanup_bucket(client: &StorageControl, bucket_name: &str) -> anyhow::Result<()> {
    purge_rapid_caches(client, bucket_name).await;

    let mut attempts = 0;
    loop {
        attempts += 1;
        match client.delete_bucket().set_name(bucket_name).send().await {
            Ok(()) => {
                println!("Successfully deleted test bucket {bucket_name}");
                return Ok(());
            }
            Err(e) if attempts < 5 => {
                eprintln!(
                    "Retrying bucket delete for {bucket_name} (attempt {attempts}/5) after error: {e:?}"
                );
                tokio::time::sleep(StdDuration::from_secs(2)).await;
            }
            Err(e) => {
                eprintln!("Failed to delete bucket {bucket_name} after {attempts} attempts: {e:?}");
                return Err(e.into());
            }
        }
    }
}

/// Executes all 9 integration test cases for Rapid Cache Ultra (RCU) CRUD management APIs.
pub async fn run(client: StorageControl, bucket_name: &str) -> anyhow::Result<()> {
    let zone = zone_id();
    let zone = if zone.is_empty() {
        "us-central1-a"
    } else {
        &zone
    };
    println!("\n========================================================");
    println!(" Running RCU CRUD Integration Test Suite");
    println!(" Bucket: {bucket_name}");
    println!(" Zone:   {zone}");
    println!("========================================================");

    test_create_rapid_cache(&client, bucket_name, zone).await?;
    test_create_rapid_cache_invalid_config(&client, bucket_name).await?;
    test_create_rapid_cache_duplicate(&client, bucket_name, zone).await?;
    test_get_rapid_cache(&client, bucket_name, zone).await?;
    test_get_rapid_cache_non_existent(&client, bucket_name).await?;
    test_list_rapid_caches(&client, bucket_name, zone).await?;
    test_update_rapid_cache(&client, bucket_name, zone).await?;
    test_disable_rapid_cache(&client, bucket_name, zone).await?;
    test_disable_rapid_cache_non_existent(&client, bucket_name).await?;

    println!("\n>>> All 9 RCU CRUD integration tests completed successfully! <<<\n");
    Ok(())
}

pub async fn test_create_rapid_cache(
    client: &StorageControl,
    bucket_name: &str,
    zone: &str,
) -> anyhow::Result<RapidCache> {
    println!("\n--- [Test 1/9] Testing CreateRapidCache (zone: {zone}) ---");
    let cache_name = format!("{bucket_name}/rapidCaches/{zone}");
    let cache = client
        .create_rapid_cache()
        .set_parent(bucket_name)
        .set_rapid_cache(
            RapidCache::new()
                .set_name(&cache_name)
                .set_zone(zone)
                .set_cache_type("rapid-cache-ultra")
                .set_admission_policy("admit-on-first-miss")
                .set_ttl(Duration::clamp(86400, 0)),
        )
        .poller()
        .until_done()
        .await?;

    assert_eq!(cache.name, cache_name);
    assert_eq!(cache.zone, zone);
    assert_eq!(cache.cache_type, "rapid-cache-ultra");
    assert_eq!(cache.state.to_lowercase(), "running");
    assert!(cache.create_time.is_some());
    println!("SUCCESS on Test 1: CreateRapidCache -> {cache_name}");
    Ok(cache)
}

pub async fn test_create_rapid_cache_invalid_config(
    client: &StorageControl,
    bucket_name: &str,
) -> anyhow::Result<()> {
    println!("\n--- [Test 2/9] Testing CreateRapidCache with invalid configuration ---");
    let invalid_zone = "invalid-zone-123";
    let cache_name = format!("{bucket_name}/rapidCaches/{invalid_zone}");
    let result: GaxResult<RapidCache> = client
        .create_rapid_cache()
        .set_parent(bucket_name)
        .set_rapid_cache(
            RapidCache::new()
                .set_name(&cache_name)
                .set_zone(invalid_zone)
                .set_cache_type("rapid-cache-ultra")
                .set_ttl(Duration::clamp(86400, 0)),
        )
        .poller()
        .until_done()
        .await;

    match result {
        Ok(c) => {
            anyhow::bail!("expected InvalidArgument error for invalid zone, but succeeded: {c:?}")
        }
        Err(e) => {
            let code = e.status().map(|s| s.code);
            assert_eq!(
                code,
                Some(Code::InvalidArgument),
                "expected InvalidArgument status, got {e:?}"
            );
            println!("SUCCESS on Test 2: expected InvalidArgument received for invalid zone");
        }
    }
    Ok(())
}

pub async fn test_create_rapid_cache_duplicate(
    client: &StorageControl,
    bucket_name: &str,
    zone: &str,
) -> anyhow::Result<()> {
    println!("\n--- [Test 3/9] Testing CreateRapidCache duplicate in zone {zone} ---");
    let cache_name = format!("{bucket_name}/rapidCaches/{zone}");
    let result: GaxResult<RapidCache> = client
        .create_rapid_cache()
        .set_parent(bucket_name)
        .set_rapid_cache(
            RapidCache::new()
                .set_name(&cache_name)
                .set_zone(zone)
                .set_cache_type("rapid-cache-ultra")
                .set_ttl(Duration::clamp(86400, 0)),
        )
        .poller()
        .until_done()
        .await;

    match result {
        Ok(c) => {
            anyhow::bail!("expected AlreadyExists error for duplicate cache, but succeeded: {c:?}")
        }
        Err(e) => {
            let code = e.status().map(|s| s.code);
            assert_eq!(
                code,
                Some(Code::AlreadyExists),
                "expected AlreadyExists status, got {e:?}"
            );
            println!("SUCCESS on Test 3: expected AlreadyExists received for duplicate cache");
        }
    }
    Ok(())
}

pub async fn test_get_rapid_cache(
    client: &StorageControl,
    bucket_name: &str,
    zone: &str,
) -> anyhow::Result<RapidCache> {
    println!("\n--- [Test 4/9] Testing GetRapidCache (zone: {zone}) ---");
    let cache_name = format!("{bucket_name}/rapidCaches/{zone}");
    let cache = client
        .get_rapid_cache()
        .set_name(&cache_name)
        .send()
        .await?;

    assert_eq!(cache.name, cache_name);
    assert_eq!(cache.zone, zone);
    assert_eq!(cache.cache_type, "rapid-cache-ultra");
    assert_eq!(cache.state.to_lowercase(), "running");
    assert!(cache.create_time.is_some());
    println!("SUCCESS on Test 4: GetRapidCache -> state: {}", cache.state);
    Ok(cache)
}

pub async fn test_get_rapid_cache_non_existent(
    client: &StorageControl,
    bucket_name: &str,
) -> anyhow::Result<()> {
    println!("\n--- [Test 5/9] Testing GetRapidCache for non-existent cache ---");
    let non_existent_name = format!("{bucket_name}/rapidCaches/us-central1-z");
    let result = client
        .get_rapid_cache()
        .set_name(&non_existent_name)
        .send()
        .await;

    match result {
        Ok(c) => {
            anyhow::bail!("expected NotFound error for non-existent cache, but succeeded: {c:?}")
        }
        Err(e) => {
            let code = e.status().map(|s| s.code);
            assert_eq!(
                code,
                Some(Code::NotFound),
                "expected NotFound status, got {e:?}"
            );
            println!("SUCCESS on Test 5: expected NotFound received for non-existent cache");
        }
    }
    Ok(())
}

pub async fn test_list_rapid_caches(
    client: &StorageControl,
    bucket_name: &str,
    expected_zone: &str,
) -> anyhow::Result<()> {
    println!("\n--- [Test 6/9] Testing ListRapidCaches ---");
    let mut stream = client.list_rapid_caches().set_parent(bucket_name).by_item();

    let mut caches = Vec::new();
    while let Some(item) = stream.next().await {
        caches.push(item?);
    }

    assert!(
        !caches.is_empty(),
        "expected at least one rapid cache in list"
    );
    let expected_name = format!("{bucket_name}/rapidCaches/{expected_zone}");
    assert!(
        caches.iter().any(|c| c.name == expected_name),
        "list did not contain expected cache {expected_name}: {caches:?}"
    );
    println!(
        "SUCCESS on Test 6: ListRapidCaches found {} cache(s)",
        caches.len()
    );
    Ok(())
}

pub async fn test_update_rapid_cache(
    client: &StorageControl,
    bucket_name: &str,
    zone: &str,
) -> anyhow::Result<RapidCache> {
    println!("\n--- [Test 7/9] Testing UpdateRapidCache (zone: {zone}) ---");
    let cache_name = format!("{bucket_name}/rapidCaches/{zone}");
    let new_ttl = Duration::clamp(172800, 0); // 48 hours

    let updated_config = RapidCache::new()
        .set_name(&cache_name)
        .set_zone(zone)
        .set_cache_type("rapid-cache-ultra")
        .set_ttl(new_ttl);

    let updated = client
        .update_rapid_cache()
        .set_rapid_cache(updated_config)
        .set_update_mask(FieldMask::default().set_paths(["ttl"]))
        .poller()
        .until_done()
        .await?;

    assert_eq!(updated.ttl, Some(new_ttl));

    // Verify update was persisted via get_rapid_cache
    let fetched = client
        .get_rapid_cache()
        .set_name(&cache_name)
        .send()
        .await?;
    assert_eq!(fetched.ttl, Some(new_ttl));
    println!("SUCCESS on Test 7: UpdateRapidCache TTL updated to 48h");
    Ok(updated)
}

pub async fn test_disable_rapid_cache(
    client: &StorageControl,
    bucket_name: &str,
    zone: &str,
) -> anyhow::Result<()> {
    println!("\n--- [Test 8/9] Testing DisableRapidCache (zone: {zone}) ---");
    let cache_name = format!("{bucket_name}/rapidCaches/{zone}");
    let result = client
        .disable_rapid_cache()
        .set_name(&cache_name)
        .poller()
        .until_done()
        .await;

    match result {
        Ok(disabled) => {
            assert_eq!(disabled.state.to_lowercase(), "disabled");
            println!(
                "SUCCESS on Test 8: DisableRapidCache -> state: {}",
                disabled.state
            );
        }
        Err(e) => {
            // TODO(b/552228787): Fix this part when server-side bug is resolved.
            let err_msg = format!("{e:?}");
            if err_msg.contains("neither result nor error set in LRO result") {
                println!("Note on Test 8: Encountered server bug (b/552228787).");
                // Verify that the cache was indeed disabled and removed from active list
                let mut list_stream = client.list_rapid_caches().set_parent(bucket_name).by_item();
                let mut active_caches = Vec::new();
                while let Some(item) = list_stream.next().await {
                    if let Ok(c) = item {
                        active_caches.push(c.name);
                    }
                }
                assert!(
                    !active_caches.iter().any(|n| n == &cache_name),
                    "Expected disabled cache {cache_name} to no longer appear in active list, but found: {active_caches:?}"
                );
                println!(
                    "SUCCESS on Test 8: Verified cache was disabled on backend (no longer in active list)"
                );
            } else {
                return Err(e.into());
            }
        }
    }
    Ok(())
}

pub async fn test_disable_rapid_cache_non_existent(
    client: &StorageControl,
    bucket_name: &str,
) -> anyhow::Result<()> {
    println!("\n--- [Test 9/9] Testing DisableRapidCache for non-existent cache ---");
    let non_existent_name = format!("{bucket_name}/rapidCaches/us-central1-z");
    let result: GaxResult<RapidCache> = client
        .disable_rapid_cache()
        .set_name(&non_existent_name)
        .poller()
        .until_done()
        .await;

    match result {
        Ok(c) => {
            anyhow::bail!("expected NotFound error for non-existent cache, but succeeded: {c:?}")
        }
        Err(e) => {
            let code = e.status().map(|s| s.code);
            assert_eq!(
                code,
                Some(Code::NotFound),
                "expected NotFound status, got {e:?}"
            );
            println!("SUCCESS on Test 9: expected NotFound received for non-existent cache");
        }
    }
    Ok(())
}
