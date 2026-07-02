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
use google_cloud_storage::client::Storage;
use std::time::SystemTime;

pub async fn run(bucket_name: &str) -> anyhow::Result<()> {
    let client = Storage::builder().build().await?;
    test_bidi_write_single_block(&client, bucket_name).await?;
    test_bidi_write_chunked_appends(&client, bucket_name).await?;
    test_bidi_write_resume_append(&client, bucket_name).await?;
    test_bidi_write_drop_stream(&client, bucket_name).await?;
    Ok(())
}

async fn check_object_contents(
    client: &Storage,
    bucket: &str,
    object: &str,
    expected: &[u8],
) -> anyhow::Result<()> {
    let mut resp = client.read_object(bucket, object).send().await?;
    let mut buf = Vec::new();
    while let Some(chunk) = resp.next().await {
        buf.extend_from_slice(&chunk?);
    }
    assert_eq!(buf, expected);
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
    let object_metadata = writer.finalize().await?;

    assert_eq!(object_metadata.size, 11);
    assert_eq!(object_metadata.name, object_name);
    check_object_contents(client, bucket, &object_name, b"hello world").await?;

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
    let object_metadata = writer.finalize().await?;

    assert_eq!(object_metadata.size, 18);
    assert_eq!(object_metadata.name, object_name);
    check_object_contents(client, bucket, &object_name, b"chunk1chunk2chunk3").await?;

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

    // Reopen.
    let mut writer2 = client
        .reopen_appendable_object(bucket, &object_name, generation)
        .send()
        .await?;

    assert_eq!(writer2.persisted_size(), 6);
    writer2.append(Bytes::from("world")).await?;
    let object_metadata = writer2.finalize().await?;

    assert_eq!(object_metadata.size, 11);
    assert_eq!(object_metadata.name, object_name);
    check_object_contents(client, bucket, &object_name, b"hello world").await?;

    Ok(())
}

async fn test_bidi_write_drop_stream(client: &Storage, bucket: &str) -> anyhow::Result<()> {
    // Test reopen and finalize after a dropped connection. We'll append some
    // data and drop the connection without flushing or finalizing. Since we
    // didn't flush the data, it may or may not be persisted.
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let object_name = format!("test_bidi_write_drop_stream_{now}");

    let mut writer = client
        .open_appendable_object(bucket, &object_name)
        .send()
        .await?;

    writer.append(Bytes::from("hello ")).await?;
    writer.flush().await?;
    let generation = writer.generation();
    writer
        .append(Bytes::from("data that won't be flushed"))
        .await?;

    // Explicitly drop the writer without flush/finalize/close.
    drop(writer);

    // Give the server a moment to recognize the dropped connection.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Reopen using the generation.
    let mut writer2 = client
        .reopen_appendable_object(bucket, &object_name, generation)
        .send()
        .await?;

    // We only flushed "hello " (6 bytes). The data may or may not have
    // been persisted, so the persisted size must be at least 6.
    assert!(writer2.persisted_size() >= 6);

    writer2.append(Bytes::from("world")).await?;
    let object_metadata = writer2.finalize().await?;

    assert!(object_metadata.size >= 11);
    assert_eq!(object_metadata.name, object_name);

    // We can't strictly check exact contents because the data may or may not
    // have been persisted, but we know it starts with "hello " and ends with
    // "world".
    let mut resp = client.read_object(bucket, &object_name).send().await?;
    let mut buf = Vec::new();
    while let Some(chunk) = resp.next().await {
        buf.extend_from_slice(&chunk?);
    }
    assert!(buf.starts_with(b"hello "));
    assert!(buf.ends_with(b"world"));

    Ok(())
}
