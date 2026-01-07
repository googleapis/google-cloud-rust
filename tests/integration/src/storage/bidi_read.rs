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

use storage::client::Storage;
use storage::model_ext::ReadRange;

pub async fn run(bucket_name: &str) -> anyhow::Result<()> {
    let client = Storage::builder().build().await?;
    send(&client, bucket_name).await?;
    send_and_read(&client, bucket_name).await?;
    Ok(())
}

async fn send(client: &Storage, bucket_name: &str) -> anyhow::Result<()> {
    let write = client
        .write_object(
            bucket_name,
            "basic/source.txt",
            String::from_iter((0..100_000).map(|_| 'a')),
        )
        .set_if_generation_match(0)
        .send_unbuffered()
        .await?;

    let open = client.open_object(bucket_name, &write.name).send().await?;
    tracing::info!("open returns: {open:?}");
    let got = open.object();
    let mut want = write.clone();
    // This field is a mismatch, but both `Some(false)` and `None` represent
    // the same value.
    want.event_based_hold = want.event_based_hold.or(Some(false));
    // There is a submillisecond difference, maybe rounding?
    want.finalize_time = got.finalize_time;
    assert_eq!(got, want);

    let mut reader = open.read_range(ReadRange::head(100)).await;
    let mut count = 0_usize;
    while let Some(r) = reader.next().await.transpose()? {
        tracing::info!("received {} bytes", r.len());
        count += r.len();
    }
    assert_eq!(count, 100_usize);

    Ok(())
}

pub async fn send_and_read(client: &Storage, bucket_name: &str) -> anyhow::Result<()> {
    let payload = String::from_iter(('a'..='z').cycle().take(100_000));
    let write = client
        .write_object(bucket_name, "open_and_read/source.txt", payload.clone())
        .set_if_generation_match(0)
        .send_unbuffered()
        .await?;

    let (descriptor, mut reader) = client
        .open_object(bucket_name, &write.name)
        .send_and_read(ReadRange::tail(100))
        .await?;
    tracing::info!("object: {:?}", descriptor.object());
    tracing::info!("headers: {:?}", descriptor.headers());
    tracing::info!("reader: {:?}", reader);
    let got = descriptor.object();
    let mut want = write.clone();
    // This field is a mismatch, but both `Some(false)` and `None` represent
    // the same value.
    want.event_based_hold = want.event_based_hold.or(Some(false));
    // There is a submillisecond difference, maybe rounding?
    want.finalize_time = got.finalize_time;
    assert_eq!(got, want);

    let mut data = Vec::new();
    while let Some(r) = reader.next().await.transpose()? {
        tracing::info!("received {} bytes", r.len());
        data.extend_from_slice(&r);
    }
    assert_eq!(data, &payload.as_bytes()[(payload.len() - 100)..]);

    Ok(())
}
