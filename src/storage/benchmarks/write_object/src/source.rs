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

use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::path::PathBuf;
use tempfile::NamedTempFile;
use tokio::io::AsyncWriteExt;

/// Creates a temporary file of the given size populated with pseudo-random bytes.
/// Returns the path to the temporary file and the NamedTempFile handle.
pub async fn create_temp_test_file(size_bytes: usize) -> anyhow::Result<(NamedTempFile, PathBuf)> {
    let temp_file = NamedTempFile::new()?;
    let path = temp_file.path().to_path_buf();

    // Use a 1 MiB chunk of pseudo-random data written repeatedly to disk
    let chunk_size = 1024 * 1024; // 1 MiB
    let mut rng = StdRng::seed_from_u64(42);
    let mut pattern = vec![0u8; chunk_size.min(size_bytes)];
    rng.fill(&mut pattern[..]);

    let mut async_file = tokio::fs::File::create(&path).await?;
    let mut remaining = size_bytes;
    while remaining > 0 {
        let to_write = remaining.min(pattern.len());
        async_file.write_all(&pattern[..to_write]).await?;
        remaining -= to_write;
    }
    async_file.flush().await?;

    Ok((temp_file, path))
}
