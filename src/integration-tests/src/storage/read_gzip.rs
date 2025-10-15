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

use storage::model_ext::ReadRange;

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

pub async fn test(bucket: &storage::model::Bucket) -> anyhow::Result<()> {
    use flate2::write::GzEncoder;
    use std::io::Write;

    // Create a temporary bucket for the test.

    let client = storage::client::Storage::builder().build().await?;
    let mut e = GzEncoder::new(Vec::new(), flate2::Compression::default());
    e.write_all(CONTENT.as_bytes())?;
    let compressed = e.finish()?;

    tracing::info!("Uploading compressed object");
    let object = client
        .write_object(
            &bucket.name,
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
    println!("highlights = {highlights:?}");
    println!("object = {object:?}");
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
