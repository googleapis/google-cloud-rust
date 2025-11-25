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

use super::read_all;
use crate::Result;
use storage::client::Storage;
use storage::model_ext::{KeyAes256, ReadRange};

pub async fn run(
    builder: storage::builder::storage::ClientBuilder,
    bucket_name: &str,
) -> anyhow::Result<()> {
    let client = builder.build().await?;
    let pending: Vec<std::pin::Pin<Box<dyn Future<Output = Result<()>>>>> = vec![
        Box::pin(customer_supplied_encryption(&client, bucket_name)),
        Box::pin(large_file(&client, bucket_name)),
        Box::pin(ranged_reads(&client, bucket_name)),
        Box::pin(read_gzip(&client, bucket_name)),
    ];
    let result: Result<Vec<_>> = futures::future::join_all(pending.into_iter())
        .await
        .into_iter()
        .collect();
    let _ = result?;
    Ok(())
}

pub async fn customer_supplied_encryption(client: &Storage, bucket_name: &str) -> Result<()> {
    tracing::info!("testing insert_object() with key");
    const CONTENTS: &str = "the quick brown fox jumps over the lazy dog";
    let key = vec![b'a'; 32];
    let insert = client
        .write_object(bucket_name, "csek/quick.text", CONTENTS)
        .set_key(KeyAes256::new(&key)?)
        .send_unbuffered()
        .await?;
    tracing::info!("success with insert={insert:?}");

    tracing::info!("testing read_object() with key");
    let mut resp = client
        .read_object(bucket_name, &insert.name)
        .set_key(KeyAes256::new(&key)?)
        .send()
        .await?;
    let mut contents = Vec::new();
    while let Some(chunk) = resp.next().await.transpose()? {
        contents.extend_from_slice(&chunk);
    }
    let contents = bytes::Bytes::from(contents);
    assert_eq!(contents, CONTENTS.as_bytes());
    tracing::info!("success with contents={contents:?}");

    Ok(())
}

pub async fn large_file(client: &Storage, bucket_name: &str) -> Result<()> {
    // Create a large enough file that will require multiple chunks to download.
    const BLOCK_SIZE: usize = 500;
    let mut contents = Vec::new();
    for i in 0..16 {
        contents.extend_from_slice(&[i as u8; BLOCK_SIZE]);
    }

    tracing::info!("testing insert_object()");
    let insert = client
        .write_object(
            bucket_name,
            "large_file/quick.text",
            bytes::Bytes::from_owner(contents.clone()),
        )
        .send_unbuffered()
        .await?;
    tracing::info!("success with insert={insert:?}");

    tracing::info!("testing read_object() streaming");
    let mut resp = client.read_object(bucket_name, &insert.name).send().await?;

    // This should take multiple chunks to download.
    let mut got = bytes::BytesMut::new();
    let mut count = 0;
    while let Some(chunk) = resp.next().await.transpose()? {
        got.extend_from_slice(&chunk);
        count += 1;
    }
    assert_eq!(got, contents);
    assert!(count > 1, "{count:?}");
    tracing::info!("success with large contents");

    // Use futures::StreamExt for the download.
    tracing::info!("testing read_object() using into_stream()");
    use futures::StreamExt;
    let mut stream = client
        .read_object(bucket_name, &insert.name)
        .send()
        .await?
        .into_stream()
        .enumerate();

    // This should take multiple chunks to download.
    got.clear();
    let mut iteration = 0;
    while let Some((i, chunk)) = stream.next().await {
        got.extend_from_slice(&chunk?);
        iteration = i;
    }
    assert_eq!(got, contents);
    assert!(iteration > 1, "{iteration:?}");
    tracing::info!("success with into_stream() large contents");

    Ok(())
}

pub async fn ranged_reads(client: &Storage, bucket_name: &str) -> Result<()> {
    tracing::info!("ranged reads test, using bucket {bucket_name}");
    const VEXING: &str = "how vexingly quick daft zebras jump";

    let object = client
        .write_object(bucket_name, "ranged_reads", VEXING)
        .set_if_generation_match(0)
        .send_unbuffered()
        .await?;
    tracing::info!("created object {object:?}");

    let want = VEXING.as_bytes();
    tracing::info!("running with ReadRange::head");
    let response = client
        .read_object(&object.bucket, &object.name)
        .set_generation(object.generation)
        .set_read_range(ReadRange::head(4))
        .send()
        .await?;
    let got = read_all(response).await?;
    assert_eq!(&got, &want[0..4]);

    tracing::info!("running with ReadRange::tail");
    let response = client
        .read_object(&object.bucket, &object.name)
        .set_generation(object.generation)
        .set_read_range(ReadRange::tail(4))
        .send()
        .await?;
    let got = read_all(response).await?;
    assert_eq!(&got, &want[(VEXING.len() - 4)..]);

    tracing::info!("running with ReadRange::offset");
    let response = client
        .read_object(&object.bucket, &object.name)
        .set_generation(object.generation)
        .set_read_range(ReadRange::offset(4))
        .send()
        .await?;
    let got = read_all(response).await?;
    assert_eq!(&got, &want[4..]);

    tracing::info!("running with ReadRange::segment");
    let response = client
        .read_object(&object.bucket, &object.name)
        .set_generation(object.generation)
        .set_read_range(ReadRange::segment(4, 4))
        .send()
        .await?;
    let got = read_all(response).await?;
    assert_eq!(&got, &want[4..8]);

    tracing::info!("DONE");
    Ok(())
}

async fn read_gzip(client: &Storage, bucket_name: &str) -> anyhow::Result<()> {
    use flate2::write::GzEncoder;
    use std::io::Write;

    const CONTENT: &str = r#"
    Four score and seven years ago our fathers brought forth on this continent a new
    nation, conceived in liberty, and dedicated to the proposition that all men are
    created equal.

    Now we are engaged in a great civil war, testing whether that nation, or any
    nation so conceived and so dedicated, can long endure. We are met on a great
    battlefield of that war. We have come to dedicate a portion of that field as a
    final resting place for those who here gave their lives that that nation might
    live. It is altogether fitting and proper that we should do this.

    But in a larger sense we cannot dedicate, we cannot consecrate, we cannot hallow
    this ground. The brave men, living and dead, who struggled here have consecrated
    it, far above our poor power to add or detract. The world will little note, nor
    long remember, what we say here, but it can never forget what they did here. It
    is for us the living, rather, to be dedicated here to the unfinished work which
    they who fought here have thus far so nobly advanced. It is rather for us to be
    here dedicated to the great task remaining before us,that from these honored
    dead we take increased devotion to that cause for which they gave the last full
    measure of devotion, that we here highly resolve that these dead shall not have
    died in vain, that this nation, under God, shall have a new birth of freedom,
    and that government of the people, by the people, for the people, shall not
    perish from the earth.
    "#;

    let mut e = GzEncoder::new(Vec::new(), flate2::Compression::default());
    e.write_all(CONTENT.as_bytes())?;
    let compressed = e.finish()?;

    tracing::info!("Uploading compressed object");
    let object = client
        .write_object(
            bucket_name,
            "address.txt.gz",
            bytes::Bytes::from_owner(compressed.clone()),
        )
        .set_content_encoding("gzip")
        .set_content_type("text/plain")
        .send_unbuffered()
        .await?;
    tracing::info!("Compressed object uploaded: {object:?}");

    tracing::info!("Reading compressed object");
    let response = client
        .read_object(&object.bucket, &object.name)
        .set_generation(object.generation)
        .send()
        .await?;
    let highlights = response.object();
    tracing::info!("Compressed object read: {:?}", highlights);
    assert_eq!(highlights.content_encoding, "gzip", "{highlights:?}");
    assert_eq!(highlights.content_type, "text/plain", "{highlights:?}");
    assert_eq!(highlights.size as usize, compressed.len(), "{highlights:?}");
    assert_eq!(highlights.generation, object.generation, "{highlights:?}");
    assert_eq!(
        highlights.metageneration, object.metageneration,
        "{highlights:?}"
    );
    assert_eq!(highlights.etag, object.etag, "{highlights:?}");
    assert_eq!(highlights.checksums, object.checksums, "{highlights:?}");
    assert_eq!(
        highlights.storage_class, object.storage_class,
        "{highlights:?}"
    );
    let got = super::read_all(response).await?;
    assert_eq!(got, compressed);

    tracing::info!("Reading decompressed object");
    let response = client
        .read_object(&object.bucket, &object.name)
        .set_generation(object.generation)
        .with_automatic_decompression(true)
        .send()
        .await?;
    let highlights = response.object();
    tracing::info!("Decompressed object read: {:?}", highlights);
    assert_eq!(highlights.content_encoding, "gzip", "{highlights:?}");
    assert_eq!(highlights.content_type, "text/plain", "{highlights:?}");
    assert_eq!(highlights.size as usize, compressed.len(), "{highlights:?}");
    assert_eq!(highlights.generation, object.generation, "{highlights:?}");
    assert_eq!(
        highlights.metageneration, object.metageneration,
        "{highlights:?}"
    );
    assert_eq!(highlights.checksums, object.checksums, "{highlights:?}");
    assert_eq!(
        highlights.storage_class, object.storage_class,
        "{highlights:?}"
    );
    let got = super::read_all(response).await?;
    assert_eq!(String::from_utf8(got), Ok(CONTENT.to_string()));

    tracing::info!("Reading compressed object head");
    let response = client
        .read_object(&object.bucket, &object.name)
        .set_generation(object.generation)
        .set_read_range(ReadRange::head(16))
        .send()
        .await?;
    let got = super::read_all(response).await?;
    assert_eq!(got, compressed[0..16]);

    tracing::info!("Reading compressed object tail");
    let response = client
        .read_object(&object.bucket, &object.name)
        .set_generation(object.generation)
        .set_read_range(ReadRange::tail(16))
        .send()
        .await?;
    tracing::info!("Compressed object read: {:?}", response.object());
    let got = super::read_all(response).await?;
    assert_eq!(got, compressed[(compressed.len() - 16)..]);

    tracing::info!("Reading compressed object offset");
    let response = client
        .read_object(&object.bucket, &object.name)
        .set_generation(object.generation)
        .set_read_range(ReadRange::offset(16))
        .send()
        .await?;
    tracing::info!("Compressed object read: {:?}", response.object());
    let got = super::read_all(response).await?;
    assert_eq!(got, compressed[16..]);

    tracing::info!("Reading compressed object range");
    let response = client
        .read_object(&object.bucket, &object.name)
        .set_generation(object.generation)
        .set_read_range(ReadRange::segment(16, 16))
        .send()
        .await?;
    tracing::info!("Compressed object read: {:?}", response.object());
    let got = super::read_all(response).await?;
    assert_eq!(got, compressed[16..32]);

    Ok(())
}
