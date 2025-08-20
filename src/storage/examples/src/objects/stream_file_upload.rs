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

// [START storage_stream_file_upload]
use bytes::Bytes;
use google_cloud_storage::{client::Storage, streaming_source::StreamingSource};

pub async fn sample(client: &Storage, bucket_id: &str) -> anyhow::Result<()> {
    const NAME: &str = "object-to-upload.txt";
    let payload = Payload(100);
    let object = client
        .write_object(format!("projects/_/buckets/{bucket_id}"), NAME, payload)
        .set_if_generation_match(0)
        .send_buffered()
        .await?;
    println!("successfully uploaded object {NAME} to bucket {bucket_id}: {object:?}");
    Ok(())
}

struct Payload(i32);

impl StreamingSource for Payload {
    type Error = std::convert::Infallible;
    async fn next(&mut self) -> Option<Result<bytes::Bytes, Self::Error>> {
        if self.0 <= 0 {
            return None;
        }
        let value = format!("still have to send {} lines\n", self.0);
        self.0 -= 1;
        Some(Ok(Bytes::from_owner(value)))
    }
}
// [END storage_stream_file_upload]
