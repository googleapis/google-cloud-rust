use crate::source::StatelessSource;
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
    if chunk_size == 0 {
        anyhow::bail!("chunk_size cannot be 0");
    }

    let mut source = StatelessSource::new();

    let mut chunks = Vec::new();
    let mut bytes_generated = 0;
    while bytes_generated < object_size {
        let size = std::cmp::min(chunk_size, object_size - bytes_generated);
        chunks.push(source.next_chunk(size));
        bytes_generated += size;
    }

    let mut writer = client
        .open_appendable_object(bucket_name, object_name)
        .send()
        .await?;

    let start_time = Instant::now();

    for chunk in chunks {
        writer.append(chunk).await?;
    }
    let object: Object = writer.finalize().await?;
    if object.size as usize != object_size {
        anyhow::bail!(
            "persisted size mismatch: expected {}, got {}",
            object_size,
            object.size
        );
    }
    let elapsed = start_time.elapsed();

    Ok(elapsed)
}
