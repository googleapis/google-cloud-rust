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

// [START storage_generate_upload_signed_url_v4]
use google_cloud_storage::builder::storage::SignedUrlBuilder;
use google_cloud_storage::http::Method;
use std::time::Duration;

pub async fn sample(bucket_name: &str, object_name: &str) -> anyhow::Result<()> {
    let signer = google_cloud_auth::credentials::Builder::default().build_signer()?;

    let signed_url =
        SignedUrlBuilder::for_object(format!("projects/_/buckets/{bucket_name}"), object_name)
            .with_method(Method::PUT)
            .with_expiration(Duration::from_secs(15 * 60)) // 15 minutes
            .with_header("content-type", "application/octet-stream")
            .sign_with(&signer)
            .await?;

    println!("Generated PUT signed URL:");
    println!("{signed_url}");
    println!("You can use this URL with any user agent, for example:");
    println!(
        "curl -X PUT -H 'Content-Type: application/octet-stream' --upload-file my-file '{signed_url}'",
    );

    Ok(())
}
// [END storage_generate_upload_signed_url_v4]
