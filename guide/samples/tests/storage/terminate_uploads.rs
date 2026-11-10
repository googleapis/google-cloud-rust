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

// [START rust_storage_terminate_uploads_all] ANCHOR: all
// [START rust_storage_terminate_uploads_my_error_type] ANCHOR: my-error-type
#[derive(Debug)]
pub enum MyError {
    ExpectedProblem,
    OhNoes,
}
// [END rust_storage_terminate_uploads_my_error_type] ANCHOR_END: my-error-type

// [START rust_storage_terminate_uploads_my_error_impl_error] ANCHOR: my-error-impl-error
impl std::error::Error for MyError {}
// [END rust_storage_terminate_uploads_my_error_impl_error] ANCHOR_END: my-error-impl-error

// [START rust_storage_terminate_uploads_my_error_impl_display] ANCHOR: my-error-impl-display
impl std::fmt::Display for MyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ExpectedProblem => write!(f, "this kind of thing happens"),
            Self::OhNoes => write!(f, "oh noes! something terrible happened"),
        }
    }
}
// [END rust_storage_terminate_uploads_my_error_impl_display] ANCHOR_END: my-error-impl-display

// [START rust_storage_terminate_uploads_my_source] ANCHOR: my-source
#[derive(Debug, Default)]
struct MySource(u32);
// [END rust_storage_terminate_uploads_my_source] ANCHOR_END: my-source

// [START rust_storage_terminate_uploads_my_source_impl_all] ANCHOR: my-source-impl-all
// [START rust_storage_terminate_uploads_my_source_impl] ANCHOR: my-source-impl
impl google_cloud_storage::streaming_source::StreamingSource for MySource {
    // [END rust_storage_terminate_uploads_my_source_impl] ANCHOR_END: my-source-impl
    // [START rust_storage_terminate_uploads_my_source_impl_error] ANCHOR: my-source-impl-error
    type Error = MyError;
    // [END rust_storage_terminate_uploads_my_source_impl_error] ANCHOR_END: my-source-impl-error
    // [START rust_storage_terminate_uploads_my_source_impl_next] ANCHOR: my-source-impl-next
    async fn next(&mut self) -> Option<Result<bytes::Bytes, Self::Error>> {
        self.0 += 1;
        match self.0 {
            42 => Some(Err(MyError::ExpectedProblem)),
            n if n > 42 => None,
            n => Some(Ok(bytes::Bytes::from_owner(format!(
                "test data for the example {n}\n"
            )))),
        }
    }
    // [END rust_storage_terminate_uploads_my_source_impl_next] ANCHOR_END: my-source-impl-next
}
// [END rust_storage_terminate_uploads_my_source_impl_all] ANCHOR_END: my-source-impl-all

// [START rust_storage_terminate_uploads_attempt_upload_client] ANCHOR: attempt-upload-client
pub async fn attempt_upload(bucket_name: &str) -> anyhow::Result<()> {
    use google_cloud_storage::client::Storage;
    let client = Storage::builder().build().await?;
    // [END rust_storage_terminate_uploads_attempt_upload_client] ANCHOR_END: attempt-upload-client
    // [START rust_storage_terminate_uploads_attempt_upload_upload] ANCHOR: attempt-upload-upload
    let upload = client
        .write_object(bucket_name, "expect-error", MySource::default())
        .send_buffered()
        .await;
    // [END rust_storage_terminate_uploads_attempt_upload_upload] ANCHOR_END: attempt-upload-upload
    // [START rust_storage_terminate_uploads_attempt_upload_inspect_err] ANCHOR: attempt-upload-inspect-err
    println!("Upload result {upload:?}");
    let err = upload.expect_err("the source is supposed to terminate the upload");
    assert!(err.is_serialization(), "{err:?}");
    use std::error::Error as _;
    assert!(err.source().is_some_and(|e| e.is::<MyError>()), "{err:?}");
    // [END rust_storage_terminate_uploads_attempt_upload_inspect_err] ANCHOR_END: attempt-upload-inspect-err
    // [START rust_storage_terminate_uploads_attempt_upload_end] ANCHOR: attempt-upload-end
    Ok(())
}
// [END rust_storage_terminate_uploads_attempt_upload_end] ANCHOR_END: attempt-upload-end
// [END rust_storage_terminate_uploads_all] ANCHOR_END: all
