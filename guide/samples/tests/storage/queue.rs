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

// ANCHOR: all
// ANCHOR: impl-streaming-source
use google_cloud_storage::streaming_source::StreamingSource;
// ANCHOR_END: impl-streaming-source
// ANCHOR: wrapper-struct
use tokio::sync::mpsc::{self, Receiver};
#[derive(Debug)]
struct QueueSource(Receiver<bytes::Bytes>);
// ANCHOR_END: wrapper-struct
// ANCHOR: impl-streaming-source
impl StreamingSource for QueueSource {
    type Error = std::convert::Infallible;
    async fn next(&mut self) -> Option<Result<bytes::Bytes, Self::Error>> {
        self.0.recv().await.map(Ok)
    }
}
// ANCHOR_END: impl-streaming-source

// ANCHOR: begin-sample-function
pub async fn queue(bucket_name: &str, object_name: &str) -> anyhow::Result<()> {
    // ANCHOR_END: begin-sample-function
    // ANCHOR: client
    use google_cloud_storage::client::Storage;
    let client = Storage::builder().build().await?;
    // ANCHOR_END: client

    // ANCHOR: create-queue
    let (sender, receiver) = mpsc::channel::<bytes::Bytes>(32);
    // ANCHOR_END: create-queue
    // ANCHOR: create-upload
    let upload = client
        .upload_object(bucket_name, object_name, QueueSource(receiver))
        .send_buffered();
    // ANCHOR_END: create-upload
    // ANCHOR: create-task
    let task = tokio::spawn(upload);
    // ANCHOR_END: create-task

    // ANCHOR: send-data
    for _ in 0..1000 {
        let line = "I will not write funny examples in class\n";
        sender
            .send(bytes::Bytes::from_static(line.as_bytes()))
            .await?;
    }
    // ANCHOR_END: send-data
    // ANCHOR: close
    drop(sender);
    // ANCHOR_END: close
    // ANCHOR: wait
    let object = task.await??;
    println!("object successfully uploaded {object:?}");
    // ANCHOR_END: wait

    // ANCHOR: end-sample-function
    Ok(())
}
// ANCHOR_END: end-sample-function
// ANCHOR_END: all
