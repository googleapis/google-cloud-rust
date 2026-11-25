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

// [START rust_storage_rewrite_object_all] ANCHOR: all
use gcs::Result;
use gcs::builder::storage_control::RewriteObject;
use gcs::client::StorageControl;
use gcs::model::Object;
use gcs::retry_policy::RetryableErrors;
use google_cloud_gax::options::RequestOptionsBuilder;
use google_cloud_gax::retry_policy::RetryPolicyExt as _;
use google_cloud_storage as gcs;

pub async fn rewrite_object(bucket_name: &str) -> anyhow::Result<()> {
    let source_object = upload(bucket_name).await?;

    // [START rust_storage_rewrite_object_client] ANCHOR: client
    let control = StorageControl::builder()
        .with_retry_policy(RetryableErrors.with_attempt_limit(5))
        .build()
        .await?;
    // [END rust_storage_rewrite_object_client] ANCHOR_END: client

    // [START rust_storage_rewrite_object_builder] ANCHOR: builder
    let mut builder = control
        .rewrite_object()
        .with_idempotency(true) // retry transient errors
        .set_source_bucket(bucket_name)
        .set_source_object(&source_object.name)
        .set_destination_bucket(bucket_name)
        .set_destination_name("rewrite-object-clone");
    // [END rust_storage_rewrite_object_builder] ANCHOR_END: builder

    // Optionally limit the max bytes written per request.
    // [START rust_storage_rewrite_object_limit_bytes_per_call] ANCHOR: limit-bytes-per-call
    builder = builder.set_max_bytes_rewritten_per_call(1024 * 1024);
    // [END rust_storage_rewrite_object_limit_bytes_per_call] ANCHOR_END: limit-bytes-per-call

    // Optionally change the storage class to force GCS to copy bytes
    // [START rust_storage_rewrite_object_change_storage_class] ANCHOR: change-storage-class
    builder = builder.set_destination(Object::new().set_storage_class("NEARLINE"));
    // [END rust_storage_rewrite_object_change_storage_class] ANCHOR_END: change-storage-class

    // [START rust_storage_rewrite_object_loop] ANCHOR: loop
    let dest_object = loop {
        let progress = make_one_request(builder.clone()).await?;
        match progress {
            // [START rust_storage_rewrite_object_set_rewrite_token] ANCHOR: set-rewrite-token
            RewriteProgress::Incomplete(rewrite_token) => {
                builder = builder.set_rewrite_token(rewrite_token);
            }
            // [END rust_storage_rewrite_object_set_rewrite_token] ANCHOR_END: set-rewrite-token
            RewriteProgress::Done(object) => break object,
        };
    };
    println!("dest_object={dest_object:?}");
    // [END rust_storage_rewrite_object_loop] ANCHOR_END: loop

    cleanup(control, bucket_name, &source_object.name, &dest_object.name).await;
    Ok(())
}

// [START rust_storage_rewrite_object_make_one_request] ANCHOR: make-one-request
enum RewriteProgress {
    // This holds the rewrite token
    Incomplete(String),
    Done(Box<Object>),
}

async fn make_one_request(builder: RewriteObject) -> Result<RewriteProgress> {
    let resp = builder.send().await?;
    if resp.done {
        println!(
            "DONE:     total_bytes_rewritten={}; object_size={}",
            resp.total_bytes_rewritten, resp.object_size
        );
        return Ok(RewriteProgress::Done(Box::new(
            resp.resource
                .expect("A `done` response must have an object."),
        )));
    }
    println!(
        "PROGRESS: total_bytes_rewritten={}; object_size={}",
        resp.total_bytes_rewritten, resp.object_size
    );
    Ok(RewriteProgress::Incomplete(resp.rewrite_token))
}
// [END rust_storage_rewrite_object_make_one_request] ANCHOR_END: make-one-request

// Upload an object to rewrite
async fn upload(bucket_name: &str) -> anyhow::Result<Object> {
    let storage = gcs::client::Storage::builder().build().await?;
    // We need the size to exceed 1MiB to exercise the rewrite token logic.
    let payload = bytes::Bytes::from(vec![65_u8; 3 * 1024 * 1024]);
    let object = storage
        .write_object(bucket_name, "rewrite-object-source", payload)
        .send_unbuffered()
        .await?;
    Ok(object)
}

// Clean up the resources created in this sample
async fn cleanup(control: StorageControl, bucket_name: &str, o1: &str, o2: &str) {
    let _ = control
        .delete_object()
        .set_bucket(bucket_name)
        .set_object(o1)
        .send()
        .await;
    let _ = control
        .delete_object()
        .set_bucket(bucket_name)
        .set_object(o2)
        .send()
        .await;
    let _ = control.delete_bucket().set_name(bucket_name).send().await;
}
// [END rust_storage_rewrite_object_all] ANCHOR_END: all

pub async fn rewrite_object_until_done(bucket_name: &str) -> anyhow::Result<()> {
    let source_object = upload(bucket_name).await?;

    let control = StorageControl::builder()
        .with_retry_policy(RetryableErrors.with_attempt_limit(5))
        .build()
        .await?;

    let builder = control
        .rewrite_object()
        .with_idempotency(true) // retry transient errors
        .set_source_bucket(bucket_name)
        .set_source_object(&source_object.name)
        .set_destination_bucket(bucket_name)
        .set_destination_name("rewrite-object-clone");

    // [START rust_storage_rewrite_object_rewrite_until_done] ANCHOR: rewrite-until-done
    use google_cloud_storage::builder_ext::RewriteObjectExt;
    let dest_object = builder.rewrite_until_done().await?;
    println!("dest_object={dest_object:?}");
    // [END rust_storage_rewrite_object_rewrite_until_done] ANCHOR_END: rewrite-until-done
    Ok(())
}
