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

// ANCHOR: quickstart
pub async fn quickstart(project_id: &str, bucket_id: &str) -> anyhow::Result<()> {
    // ANCHOR: control-client
    use google_cloud_storage as gcs;
    use google_cloud_storage::client::StorageControl;
    let control = StorageControl::builder().build().await?;
    // ANCHOR_END: control-client
    // ANCHOR: control-bucket-required
    let bucket = control
        .create_bucket()
        .set_parent("projects/_")
        .set_bucket_id(bucket_id)
        .set_bucket(
            // ANCHOR: control-bucket-ubla
            gcs::model::Bucket::new()
                .set_project(format!("projects/{project_id}"))
                // ANCHOR_END: control-bucket-required
                .set_iam_config(
                    gcs::model::bucket::IamConfig::new().set_uniform_bucket_level_access(
                        gcs::model::bucket::iam_config::UniformBucketLevelAccess::new()
                            .set_enabled(true),
                    ),
                ),
            // ANCHOR_END: control-bucket-ubla
            // ANCHOR: control-bucket-required
        )
        // ANCHOR_END: control-bucket-required
        // ANCHOR: control-bucket-send
        .send()
        .await?;
    println!("bucket successfully created {bucket:?}");
    // ANCHOR_END: control-bucket-send

    // ANCHOR: client
    use google_cloud_storage::client::Storage;
    let client = Storage::builder().build().await?;
    // ANCHOR_END: client

    // ANCHOR: upload
    let object = client
        .write_object(&bucket.name, "hello.txt", "Hello World!")
        .send_buffered()
        .await?;
    println!("object successfully uploaded {object:?}");
    // ANCHOR_END: upload

    // ANCHOR: download
    use google_cloud_storage::read_object_response::ReadObjectResponse;
    let mut reader = client.read_object(&bucket.name, "hello.txt").send().await?;
    let mut contents = Vec::new();
    while let Some(chunk) = reader.next().await.transpose()? {
        contents.extend_from_slice(&chunk);
    }
    println!(
        "object contents successfully downloaded {:?}",
        bytes::Bytes::from_owner(contents)
    );
    // ANCHOR_END: download

    // ANCHOR: cleanup
    control
        .delete_object()
        .set_bucket(&bucket.name)
        .set_object(&object.name)
        .set_generation(object.generation)
        .send()
        .await?;
    control
        .delete_bucket()
        .set_name(&bucket.name)
        .send()
        .await?;
    // ANCHOR_END: cleanup

    Ok(())
}
// ANCHOR_END: quickstart
