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

// [START storage_custom_headers]
use google_cloud_gax::options::RequestOptionsBuilder;
use google_cloud_storage::client::Storage;
use google_cloud_storage::client::StorageControl;
use http::header::{HeaderName, HeaderValue};

pub async fn sample(bucket_id: &str) -> anyhow::Result<()> {
    const NAME: &str = "hello-world.txt";

    // Set a client-level custom header on the Storage data plane client.
    let client = Storage::builder()
        .with_custom_header(
            HeaderName::from_static("x-custom-header"),
            HeaderValue::from_static("custom-value"),
        )
        .build()
        .await?;

    let reader = client
        .read_object(format!("projects/_/buckets/{bucket_id}"), NAME)
        .send()
        .await?;
    println!("Object highlights: {:?}", reader.object());

    // Set a request-level custom header on a StorageControl RPC request.
    let control = StorageControl::builder().build().await?;
    let bucket = control
        .get_bucket()
        .set_name(format!("projects/_/buckets/{bucket_id}"))
        .with_custom_header(
            HeaderName::from_static("x-custom-header"),
            HeaderValue::from_static("custom-value"),
        )
        .send()
        .await?;
    println!("Bucket {bucket_id} metadata is {bucket:?}");

    Ok(())
}
// [END storage_custom_headers]
