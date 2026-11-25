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

// [START rust_storage_queue_all] ANCHOR: all
// [START rust_storage_queue_impl_streaming_source] ANCHOR: impl-streaming-source
use google_cloud_storage::streaming_source::StreamingSource;
// [END rust_storage_queue_impl_streaming_source] ANCHOR_END: impl-streaming-source
// [START rust_storage_queue_wrapper_struct] ANCHOR: wrapper-struct
use tokio::sync::mpsc::{self, Receiver};
#[derive(Debug)]
struct QueueSource(Receiver<bytes::Bytes>);
// [END rust_storage_queue_wrapper_struct] ANCHOR_END: wrapper-struct
// [START rust_storage_queue_impl_streaming_source] ANCHOR: impl-streaming-source
impl StreamingSource for QueueSource {
    type Error = std::convert::Infallible;
    async fn next(&mut self) -> Option<Result<bytes::Bytes, Self::Error>> {
        self.0.recv().await.map(Ok)
    }
}
// [END rust_storage_queue_impl_streaming_source] ANCHOR_END: impl-streaming-source

// [START rust_storage_queue_begin_sample_function] ANCHOR: begin-sample-function
pub async fn queue(bucket_name: &str, object_name: &str) -> anyhow::Result<()> {
    // [END rust_storage_queue_begin_sample_function] ANCHOR_END: begin-sample-function
    // [START rust_storage_queue_client] ANCHOR: client
    use google_cloud_storage::client::Storage;
    let client = Storage::builder().build().await?;
    // [END rust_storage_queue_client] ANCHOR_END: client

    // [START rust_storage_queue_create_queue] ANCHOR: create-queue
    let (sender, receiver) = mpsc::channel::<bytes::Bytes>(32);
    // [END rust_storage_queue_create_queue] ANCHOR_END: create-queue
    // [START rust_storage_queue_create_writer] ANCHOR: create-writer
    let upload = client
        .write_object(bucket_name, object_name, QueueSource(receiver))
        .send_buffered();
    // [END rust_storage_queue_create_writer] ANCHOR_END: create-writer
    // [START rust_storage_queue_create_task] ANCHOR: create-task
    let task = tokio::spawn(upload);
    // [END rust_storage_queue_create_task] ANCHOR_END: create-task

    // [START rust_storage_queue_send_data] ANCHOR: send-data
    for _ in 0..1000 {
        let line = "I will not write funny examples in class\n";
        sender
            .send(bytes::Bytes::from_static(line.as_bytes()))
            .await?;
    }
    // [END rust_storage_queue_send_data] ANCHOR_END: send-data
    // [START rust_storage_queue_close] ANCHOR: close
    drop(sender);
    // [END rust_storage_queue_close] ANCHOR_END: close
    // [START rust_storage_queue_wait] ANCHOR: wait
    let object = task.await??;
    println!("object successfully uploaded {object:?}");
    // [END rust_storage_queue_wait] ANCHOR_END: wait

    // [START rust_storage_queue_end_sample_function] ANCHOR: end-sample-function
    Ok(())
}
// [END rust_storage_queue_end_sample_function] ANCHOR_END: end-sample-function
// [END rust_storage_queue_all] ANCHOR_END: all
