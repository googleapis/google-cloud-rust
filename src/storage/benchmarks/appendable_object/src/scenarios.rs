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

use super::source::StatelessSource;
use google_cloud_storage::client::Storage;
use google_cloud_storage::model::Object;
use std::time::Instant;

/// Scenario 1: Basic Steady-State
/// Returns the elapsed time of the iteration.
pub async fn scenario_1_basic_steady_state(
    client: &Storage,
    bucket_name: &str,
    object_name: &str,
    object_size: usize,
    chunk_size: usize,
) -> anyhow::Result<std::time::Duration> {
    // Arrange.
    if chunk_size == 0 {
        anyhow::bail!("chunk_size cannot be 0");
    }

    // Prepare data source and payload chunks.
    let mut source = StatelessSource::new();

    let chunk = source.next_chunk(chunk_size);
    let remainder = object_size % chunk_size;
    let remainder_chunk = if remainder > 0 {
        Some(source.next_chunk(remainder))
    } else {
        None
    };

    // Calculate chunk count and expected checksums.
    let num_full_chunks = object_size / chunk_size;
    let chunk_crc = crc32c::crc32c(&chunk);
    let remainder_crc = remainder_chunk.as_ref().map(|rc| crc32c::crc32c(rc));

    // Act.
    let mut writer = client
        .open_appendable_object(bucket_name, object_name)
        .send()
        .await?;

    let start_time = Instant::now();

    for _ in 0..num_full_chunks {
        writer.append(chunk.clone()).await?;
    }
    if let Some(rc) = remainder_chunk {
        writer.append(rc).await?;
    }
    let object: Object = writer.finalize().await?;
    let elapsed = start_time.elapsed();

    // Assert.
    if object.size as usize != object_size {
        anyhow::bail!(
            "persisted size mismatch: expected {}, got {}",
            object_size,
            object.size
        );
    }

    // Verify object-level checksum.
    if let Some(server_crc) = object.checksums.as_ref().and_then(|c| c.crc32c) {
        let mut expected_crc = 0u32;
        for _ in 0..num_full_chunks {
            expected_crc = crc32c::crc32c_combine(expected_crc, chunk_crc, chunk.len());
        }
        if let Some(r_crc) = remainder_crc {
            expected_crc = crc32c::crc32c_combine(expected_crc, r_crc, remainder);
        }
        if server_crc != expected_crc {
            anyhow::bail!(
                "CRC32C checksum mismatch: expected {expected_crc:#010x}, got {server_crc:#010x}"
            );
        }
    }

    Ok(elapsed)
}
