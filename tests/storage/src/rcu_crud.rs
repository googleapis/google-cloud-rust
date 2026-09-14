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
use google_cloud_gax::paginator::ItemPaginator as _;
use google_cloud_lro::Poller;
use google_cloud_storage::client::StorageControl;
use google_cloud_storage::model::RapidCache;
use google_cloud_test_utils::runtime_config::zone_id;
use google_cloud_wkt::{Duration, FieldMask};

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

/// Test Case 1: Create Rapid Cache
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

/// Test Case 2: Create Rapid Cache - Invalid Config
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

/// Test Case 3: Create Duplicate Rapid Cache
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

/// Test Case 4: Get Rapid Cache
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

/// Test Case 5: Get Non-existent Rapid Cache
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

/// Test Case 6: List Rapid Caches
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

/// Test Case 7: Update Rapid Cache
pub async fn test_update_rapid_cache(
    client: &StorageControl,
    bucket_name: &str,
    zone: &str,
) -> anyhow::Result<RapidCache> {
    println!("\n--- [Test 7/9] Testing UpdateRapidCache (zone: {zone}) ---");
    let cache_name = format!("{bucket_name}/rapidCaches/{zone}");
    let new_ttl = Duration::clamp(172800, 0); // 48 hours

    // Note: Cross-language testing revealed that cache_type must be specified
    // to avoid a backend HTTP 500 error.
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

/// Test Case 8: Disable Rapid Cache
pub async fn test_disable_rapid_cache(
    client: &StorageControl,
    bucket_name: &str,
    zone: &str,
) -> anyhow::Result<()> {
    println!("\n--- [Test 8/9] Testing DisableRapidCache (zone: {zone}) ---");
    let cache_name = format!("{bucket_name}/rapidCaches/{zone}");
    let disabled = client
        .disable_rapid_cache()
        .set_name(&cache_name)
        .poller()
        .until_done()
        .await?;

    assert_eq!(disabled.state.to_lowercase(), "disabled");
    println!("SUCCESS on Test 8: DisableRapidCache -> state: {}", disabled.state);
    Ok(())
}

/// Test Case 9: Disable Non-existent Cache
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
