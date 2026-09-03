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

use google_cloud_storage::client::{Storage, StorageControl};
use std::path::Path;
use std::time::{Duration, Instant};
use tokio::fs::File;

/// Result of a single benchmark run iteration.
#[derive(Debug, Clone)]
pub struct IterationResult {
    /// Total elapsed time for the upload operation.
    pub total_elapsed: Duration,
    /// Precomputation duration (Option B only).
    pub precompute_duration: Option<Duration>,
}

/// Scenario A: Baseline 1-Pass Unbuffered Stream (Option A)
/// - Continuous stream without precomputed checksum.
/// - Client-side validation only, 0 RAM buffer.
pub async fn scenario_option_a(
    client: &Storage,
    bucket_name: &str,
    object_name: &str,
    file_path: &Path,
    object_size: usize,
) -> anyhow::Result<IterationResult> {
    let file = File::open(file_path).await?;
    let start_time = Instant::now();

    let object = client
        .write_object(bucket_name, object_name, file)
        .send_unbuffered()
        .await?;

    let total_elapsed = start_time.elapsed();

    if object.size as usize != object_size {
        anyhow::bail!(
            "persisted size mismatch: expected {}, got {}",
            object_size,
            object.size
        );
    }

    Ok(IterationResult {
        total_elapsed,
        precompute_duration: None,
    })
}

/// Scenario B: 2-Pass Unbuffered Stream (Option B)
/// - Pass 1: Local hash computation (`precompute_checksums()`).
/// - Pass 2: Continuous stream with server-side validation, 0 RAM buffer.
pub async fn scenario_option_b(
    client: &Storage,
    bucket_name: &str,
    object_name: &str,
    file_path: &Path,
    object_size: usize,
) -> anyhow::Result<IterationResult> {
    let file = File::open(file_path).await?;
    let total_start = Instant::now();

    let precompute_start = Instant::now();
    let write_builder = client
        .write_object(bucket_name, object_name, file)
        .precompute_checksums()
        .await?;
    let precompute_duration = precompute_start.elapsed();

    let object = write_builder.send_unbuffered().await?;
    let total_elapsed = total_start.elapsed();

    if object.size as usize != object_size {
        anyhow::bail!(
            "persisted size mismatch: expected {}, got {}",
            object_size,
            object.size
        );
    }

    Ok(IterationResult {
        total_elapsed,
        precompute_duration: Some(precompute_duration),
    })
}

/// Scenario C: 1-Pass Chunked Buffered Upload (Option C)
/// - Sequential 8 MiB chunk PUT requests.
/// - Server-side validation attached to final chunk, 8 MiB RAM buffer.
pub async fn scenario_option_c(
    client: &Storage,
    bucket_name: &str,
    object_name: &str,
    file_path: &Path,
    object_size: usize,
) -> anyhow::Result<IterationResult> {
    let file = File::open(file_path).await?;
    let start_time = Instant::now();

    let object = client
        .write_object(bucket_name, object_name, file)
        .send_buffered()
        .await?;

    let total_elapsed = start_time.elapsed();

    if object.size as usize != object_size {
        anyhow::bail!(
            "persisted size mismatch: expected {}, got {}",
            object_size,
            object.size
        );
    }

    Ok(IterationResult {
        total_elapsed,
        precompute_duration: None,
    })
}

/// Cleans up a test object from GCS.
pub async fn cleanup_object(
    control: &StorageControl,
    bucket_name: &str,
    object_name: &str,
) -> anyhow::Result<()> {
    control
        .delete_object()
        .set_bucket(bucket_name)
        .set_object(object_name)
        .send()
        .await?;
    Ok(())
}
