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

// [START storage_add_cors_configuration]
use google_cloud_storage::client::StorageControl;
use google_cloud_storage::model::bucket::Cors;
use google_cloud_wkt::FieldMask;

pub async fn sample(client: &StorageControl, bucket_id: &str) -> anyhow::Result<()> {
    let bucket = client
        .get_bucket()
        .set_name(format!("projects/_/buckets/{bucket_id}"))
        .send()
        .await?;
    let metageneration = bucket.metageneration;
    let mut cors = bucket.cors.clone();
    cors.push(
        Cors::new()
            .set_origin(["http://example.appspot.com".to_string()])
            .set_method(["GET".to_string(), "HEAD".to_string(), "DELETE".to_string()])
            .set_response_header(["Content-Type".to_string()])
            .set_max_age_seconds(3600),
    );

    let bucket = client
        .update_bucket()
        .set_bucket(bucket.set_cors(cors))
        .set_if_metageneration_match(metageneration)
        .set_update_mask(FieldMask::default().set_paths(["cors"]))
        .send()
        .await?;
    println!("successfully updated bucket CORS for {bucket_id}: {bucket:?}");
    Ok(())
}
// [END storage_add_cors_configuration]
