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

use bytes::Bytes;
use google_cloud_storage::client::{Storage, StorageControl};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::fs::File;
use std::path::{Path, PathBuf};
use tempfile::NamedTempFile;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

#[cfg(unix)]
use std::os::unix::io::AsRawFd;

const WARMUP_PAYLOAD_SIZE: usize = 512 * 1024; // 512 KiB

/// Performs a one-time global warmup by uploading and deleting a 512 KiB payload.
/// Primes OAuth authentication tokens, DNS resolution, and the TLS connection pool.
/// If an authentication or permission error occurs, returns a fatal error with remediation advice.
pub async fn perform_global_warmup(
    client: &Storage,
    control: &StorageControl,
    bucket: &str,
) -> anyhow::Result<()> {
    let warmup_object_name = format!("bench-warmup-{}", Uuid::new_v4());
    let warmup_data = Bytes::from(vec![0u8; WARMUP_PAYLOAD_SIZE]);

    let upload_res = client
        .write_object(bucket, &warmup_object_name, warmup_data)
        .send_unbuffered()
        .await;

    if let Err(e) = upload_res {
        eprintln!("\n============================================================");
        eprintln!("FATAL ERROR: Pre-flight warmup check failed!");
        eprintln!("Failed to upload warmup payload (512 KiB) to bucket: {bucket}");
        eprintln!("Error details: {e:#}");
        eprintln!("------------------------------------------------------------");
        eprintln!("Troubleshooting Suggestions:");
        eprintln!("1. Authentication: Ensure your credentials are valid by running:");
        eprintln!("   gcloud auth application-default login");
        eprintln!("2. Bucket Access: Ensure the target bucket exists and your account has");
        eprintln!("   'Storage Object Admin' (or 'Storage Object Creator') permissions:");
        eprintln!("   export GOOGLE_CLOUD_RUST_BENCHMARKS_BUCKET=\"<your-bucket-name>\"");
        eprintln!("============================================================\n");
        anyhow::bail!("Warmup pre-flight check failed: {e}");
    }

    if let Err(e) = control
        .delete_object()
        .set_bucket(bucket)
        .set_object(&warmup_object_name)
        .send()
        .await
    {
        eprintln!("Warning: Failed to delete warmup object {warmup_object_name}: {e}");
    }

    Ok(())
}

/// Creates a temporary file of the given size populated with pseudo-random bytes.
/// The file is created in `temp_dir` on physical SSD storage.
/// Returns the path to the temporary file and the NamedTempFile handle.
pub async fn create_temp_test_file(
    size_bytes: usize,
    temp_dir: &str,
) -> anyhow::Result<(NamedTempFile, PathBuf)> {
    // Ensure parent directory exists
    std::fs::create_dir_all(temp_dir)?;

    let temp_file = NamedTempFile::new_in(temp_dir)?;
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

/// Evicts the given file's data from the OS page cache (RAM) to simulate a cold physical disk read.
pub fn drop_file_from_page_cache(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let std_file = File::open(path)?;
        let fd = std_file.as_raw_fd();
        // Sync dirty pages to disk first.
        let _ = unsafe { libc::fdatasync(fd) };
        // Tell the OS kernel to discard cached pages for the entire file range.
        let ret = unsafe { libc::posix_fadvise(fd, 0, 0, libc::POSIX_FADV_DONTNEED) };
        if ret != 0 {
            return Err(std::io::Error::from_raw_os_error(ret));
        }
    }
    Ok(())
}
