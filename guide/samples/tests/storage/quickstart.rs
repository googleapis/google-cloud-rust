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

// [START rust_storage_quickstart] ANCHOR: quickstart
pub async fn sample(project_id: &str, bucket_id: &str) -> anyhow::Result<()> {
    // [START rust_storage_quickstart_control_client] ANCHOR: control-client
    use google_cloud_storage as gcs;
    use google_cloud_storage::client::StorageControl;
    let control = StorageControl::builder().build().await?;
    // [END rust_storage_quickstart_control_client] ANCHOR_END: control-client
    // [START rust_storage_quickstart_control_bucket_required] ANCHOR: control-bucket-required
    let bucket = control
        .create_bucket()
        .set_parent("projects/_")
        .set_bucket_id(bucket_id)
        .set_bucket(
            // [START rust_storage_quickstart_control_bucket_ubla] ANCHOR: control-bucket-ubla
            gcs::model::Bucket::new()
                .set_project(format!("projects/{project_id}"))
                // [END rust_storage_quickstart_control_bucket_required] ANCHOR_END: control-bucket-required
                .set_iam_config(
                    gcs::model::bucket::IamConfig::new().set_uniform_bucket_level_access(
                        gcs::model::bucket::iam_config::UniformBucketLevelAccess::new()
                            .set_enabled(true),
                    ),
                ),
            // [END rust_storage_quickstart_control_bucket_ubla] ANCHOR_END: control-bucket-ubla
            // [START rust_storage_quickstart_control_bucket_required] ANCHOR: control-bucket-required
        )
        // [END rust_storage_quickstart_control_bucket_required] ANCHOR_END: control-bucket-required
        // [START rust_storage_quickstart_control_bucket_send] ANCHOR: control-bucket-send
        .send()
        .await?;
    println!("bucket successfully created {bucket:?}");
    // [END rust_storage_quickstart_control_bucket_send] ANCHOR_END: control-bucket-send

    // [START rust_storage_quickstart_client] ANCHOR: client
    use google_cloud_storage::client::Storage;
    let client = Storage::builder().build().await?;
    // [END rust_storage_quickstart_client] ANCHOR_END: client

    // [START rust_storage_quickstart_upload] ANCHOR: upload
    let object = client
        .write_object(&bucket.name, "hello.txt", "Hello World!")
        .send_buffered()
        .await?;
    println!("object successfully uploaded {object:?}");
    // [END rust_storage_quickstart_upload] ANCHOR_END: upload

    // [START rust_storage_quickstart_download] ANCHOR: download
    let mut reader = client.read_object(&bucket.name, "hello.txt").send().await?;
    let mut contents = Vec::new();
    while let Some(chunk) = reader.next().await.transpose()? {
        contents.extend_from_slice(&chunk);
    }
    println!(
        "object contents successfully downloaded {:?}",
        bytes::Bytes::from_owner(contents)
    );
    // [END rust_storage_quickstart_download] ANCHOR_END: download

    // [START rust_storage_quickstart_cleanup] ANCHOR: cleanup
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
    // [END rust_storage_quickstart_cleanup] ANCHOR_END: cleanup

    Ok(())
}
// [END rust_storage_quickstart] ANCHOR_END: quickstart
