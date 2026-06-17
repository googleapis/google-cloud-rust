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

// [START storage_generate_signed_post_policy_v4]
use google_cloud_storage::builder::storage::PostPolicyV4Builder;
use std::time::Duration;

pub async fn sample(bucket_name: &str, object_name: &str) -> anyhow::Result<()> {
    let signer = google_cloud_auth::credentials::Builder::default().build_signer()?;

    let policy = PostPolicyV4Builder::for_object(bucket_name, object_name)
        .with_expiration(Duration::from_secs(30 * 60)) // 30 minutes
        .with_field("Content-Type", "text/plain")
        .with_starts_with("$key", "")
        .with_content_length_range(1, 10 * 1024 * 1024) // 1 byte to 10 MiB
        .sign_with(&signer)
        .await?;

    // Create an HTML form with the computed policy
    let mut form = format!("<form action='{}' method='POST' enctype='multipart/form-data'>\n", policy.url);
    for (key, value) in &policy.fields {
        form.push_str(&format!("  <input name='{}' value='{}' type='hidden' />\n", key, value));
    }
    form.push_str("  <input type='file' name='file' /><br />\n");
    form.push_str("  <input type='submit' value='Upload File' /><br />\n");
    form.push_str("</form>");

    println!("Generated POST Policy HTML Form:");
    println!("{form}");

    Ok(())
}
// [END storage_generate_signed_post_policy_v4]
