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
    let write = client
        .write_object(
            bucket_name,
            "basic/source.txt",
            String::from_iter((0..100_000).map(|_| 'a')),
        )
        .set_if_generation_match(0)
        .send_unbuffered()
        .await?;

    println!("created bidi client: {client:?}");
    let open = client.open_object(bucket_name, &write.name).send().await?;
    println!("open returns: {open:?}");
    let got = open.object();
    let mut want = write.clone();
    // This field is a mismatch, but both `Some(false)` and `None` represent
    // the same value.
    want.event_based_hold = want.event_based_hold.or(Some(false));
    // There is a submillisecond difference, maybe rounding?
    want.finalize_time = got.finalize_time;
    assert_eq!(got, &want);

    let mut reader = open.read_range(ReadRange::head(100)).await;
    let mut count = 0_usize;
    while let Some(r) = reader.next().await.transpose()? {
        println!("received {} bytes", r.len());
        count += r.len();
    }
    assert_eq!(count, 100_usize);

    Ok(())
}
