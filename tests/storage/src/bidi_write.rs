// Copyright 2025 Google LLC
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
use google_cloud_storage::client::Storage;
use std::time::SystemTime;

pub async fn run(bucket_name: &str) -> anyhow::Result<()> {
    let client = Storage::builder().build().await?;
    test_bidi_write_single_block(&client, bucket_name).await?;
    test_bidi_write_chunked_appends(&client, bucket_name).await?;
    test_bidi_write_resume_append(&client, bucket_name).await?;
    test_bidi_write_poison_stream(&client, bucket_name).await?;
    Ok(())
}

async fn test_bidi_write_single_block(client: &Storage, bucket: &str) -> anyhow::Result<()> {
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let object_name = format!("test_bidi_write_single_block_{now}");

    let mut writer = client
        .open_appendable_object(bucket, &object_name)
        .send()
        .await?;

    writer.append(Bytes::from("hello world")).await?;
    let obj = writer.finalize().await?;

    assert_eq!(obj.size, 11);
    assert_eq!(obj.name, object_name);

    Ok(())
}

async fn test_bidi_write_chunked_appends(client: &Storage, bucket: &str) -> anyhow::Result<()> {
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let object_name = format!("test_bidi_write_chunked_appends_{now}");

    let mut writer = client
        .open_appendable_object(bucket, &object_name)
        .send()
        .await?;

    writer.append(Bytes::from("chunk1")).await?;
    writer.append(Bytes::from("chunk2")).await?;
    writer.flush().await?;
    writer.append(Bytes::from("chunk3")).await?;
    let obj = writer.finalize().await?;

    assert_eq!(obj.size, 18);
    assert_eq!(obj.name, object_name);

    Ok(())
}

async fn test_bidi_write_resume_append(client: &Storage, bucket: &str) -> anyhow::Result<()> {
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let object_name = format!("test_bidi_write_resume_append_{now}");

    let mut writer = client
        .open_appendable_object(bucket, &object_name)
        .send()
        .await?;

    writer.append(Bytes::from("hello ")).await?;
    let generation = writer.generation();
    let persisted_size = writer.close().await?;
    assert_eq!(persisted_size, 6);

    // Reopen
    let mut writer2 = client
        .reopen_appendable_object(bucket, &object_name, generation)
        .send()
        .await?;

    assert_eq!(writer2.persisted_size(), 6);
    writer2.append(Bytes::from("world")).await?;
    let obj = writer2.finalize().await?;

    assert_eq!(obj.size, 11);
    assert_eq!(obj.name, object_name);

    Ok(())
}

async fn test_bidi_write_poison_stream(client: &Storage, bucket: &str) -> anyhow::Result<()> {
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let object_name = format!("test_bidi_write_poison_stream_{now}");

    let mut writer = client
        .open_appendable_object(bucket, &object_name)
        .send()
        .await?;

    writer.append(Bytes::from("hello ")).await?;
    writer.flush().await?; // Ensure we get the generation by flushing at least once
    let generation = writer.generation();
    writer
        .append(Bytes::from("poison data that won't be flushed"))
        .await?;

    // Explicitly drop the writer without finalizing or closing, poisoning the stream.
    drop(writer);

    // Give the server a moment to recognize the dropped connection (not strictly necessary but good practice)
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Reopen using the generation
    let mut writer2 = client
        .reopen_appendable_object(bucket, &object_name, generation)
        .send()
        .await?;

    // We only flushed "hello ", so persisted size might be 6 (though the server might have acked the rest, let's just assert it succeeds in reopening and we can append more).
    writer2.append(Bytes::from("world")).await?;
    let obj = writer2.finalize().await?;

    // Since we didn't flush the poison data, it may or may not be there. GCS makes no guarantees about unflushed data on disconnect.
    // The main test is that we can successfully reopen and finalize after a dropped connection.
    assert!(obj.size >= 11);
    assert_eq!(obj.name, object_name);

    Ok(())
}
