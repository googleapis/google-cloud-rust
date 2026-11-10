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

// [START rust_storage_mocking_all] ANCHOR: all
use gcs::client::Storage;
use gcs::model::Object;
use google_cloud_storage as gcs;

// [END rust_storage_mocking_all] ANCHOR_END: all
// [START rust_storage_mocking_prod_only_interface] ANCHOR: prod-only-interface
pub async fn my_function(_client: Storage) {}
// [END rust_storage_mocking_prod_only_interface] ANCHOR_END: prod-only-interface

// [START rust_storage_mocking_testable_interface] ANCHOR: testable-interface
pub async fn my_testable_function<T>(_client: Storage<T>)
where
    T: gcs::stub::Storage + 'static,
{
}
// [END rust_storage_mocking_testable_interface] ANCHOR_END: testable-interface

// [START rust_storage_mocking_all] ANCHOR: all
// [START rust_storage_mocking_count_newlines] ANCHOR: count-newlines
// Downloads an object from GCS and counts the total lines.
pub async fn count_newlines<T>(
    client: &Storage<T>,
    bucket_id: &str,
    object_id: &str,
) -> gcs::Result<usize>
where
    T: gcs::stub::Storage + 'static,
{
    let mut count = 0;
    let mut reader = client
        .read_object(format!("projects/_/buckets/{bucket_id}"), object_id)
        .set_generation(42)
        .send()
        .await?;
    while let Some(buffer) = reader.next().await.transpose()? {
        count += buffer.into_iter().filter(|c| *c == b'\n').count();
    }
    Ok(count)
}
// [END rust_storage_mocking_count_newlines] ANCHOR_END: count-newlines

// [START rust_storage_mocking_upload] ANCHOR: upload
// Uploads an object to GCS.
pub async fn upload<T>(client: &Storage<T>, bucket_id: &str, object_id: &str) -> gcs::Result<Object>
where
    T: gcs::stub::Storage + 'static,
{
    client
        .write_object(
            format!("projects/_/buckets/{bucket_id}"),
            object_id,
            "payload",
        )
        .set_if_generation_match(42)
        .send_unbuffered()
        .await
}
// [END rust_storage_mocking_upload] ANCHOR_END: upload

#[cfg(test)]
mod tests {
    use super::{count_newlines, upload};
    use gcs::Result;
    use gcs::model::{Object, ReadObjectRequest};
    use gcs::model_ext::{ObjectHighlights, WriteObjectRequest};
    use gcs::read_object::ReadObjectResponse;
    use gcs::request_options::RequestOptions;
    use gcs::streaming_source::{BytesSource, Payload, Seek, StreamingSource};
    use google_cloud_storage as gcs;

    // [START rust_storage_mocking_mockall] ANCHOR: mockall
    mockall::mock! {
        #[derive(Debug)]
        Storage {}
        impl gcs::stub::Storage for Storage {
            async fn read_object(&self, _req: ReadObjectRequest, _options: RequestOptions) -> Result<ReadObjectResponse>;
            async fn write_object_buffered<P: StreamingSource + Send + Sync + 'static>(
                &self,
                _payload: P,
                _req: WriteObjectRequest,
                _options: RequestOptions,
            ) -> Result<Object>;
            async fn write_object_unbuffered<P: StreamingSource + Seek + Send + Sync + 'static>(
                &self,
                _payload: P,
                _req: WriteObjectRequest,
                _options: RequestOptions,
            ) -> Result<Object>;
        }
    }
    // [END rust_storage_mocking_mockall] ANCHOR_END: mockall

    // [START rust_storage_mocking_fake_read_object_resp] ANCHOR: fake-read-object-resp
    fn fake_response(size: usize) -> ReadObjectResponse {
        let mut contents = String::new();
        for i in 0..size {
            contents.push_str(&format!("{i}\n"))
        }
        ReadObjectResponse::from_source(ObjectHighlights::default(), bytes::Bytes::from(contents))
    }
    // [END rust_storage_mocking_fake_read_object_resp] ANCHOR_END: fake-read-object-resp

    // [START rust_storage_mocking_test_count_lines] ANCHOR: test-count-lines
    #[tokio::test]
    async fn test_count_lines() -> anyhow::Result<()> {
        let mut mock = MockStorage::new();
        mock.expect_read_object().return_once({
            move |r, _| {
                // Verify contents of the request
                assert_eq!(r.generation, 42);
                assert_eq!(r.bucket, "projects/_/buckets/my-bucket");
                assert_eq!(r.object, "my-object");

                // Return a `ReadObjectResponse`
                Ok(fake_response(100))
            }
        });
        let client = gcs::client::Storage::from_stub(mock);

        let count = count_newlines(&client, "my-bucket", "my-object").await?;
        assert_eq!(count, 100);

        Ok(())
    }
    // [END rust_storage_mocking_test_count_lines] ANCHOR_END: test-count-lines

    // [START rust_storage_mocking_test_upload] ANCHOR: test-upload
    #[tokio::test]
    async fn test_upload() -> anyhow::Result<()> {
        let mut mock = MockStorage::new();
        // [START rust_storage_mocking_expect_unbuffered] ANCHOR: expect-unbuffered
        mock.expect_write_object_unbuffered()
            // [END rust_storage_mocking_expect_unbuffered] ANCHOR_END: expect-unbuffered
            .return_once(
                // [START rust_storage_mocking_explicit_payload_type] ANCHOR: explicit-payload-type
                |_payload: Payload<BytesSource>, r, _| {
                    // [END rust_storage_mocking_explicit_payload_type] ANCHOR_END: explicit-payload-type
                    // Verify contents of the request
                    assert_eq!(r.spec.if_generation_match, Some(42));
                    let o = r.spec.resource.unwrap_or_default();
                    assert_eq!(o.bucket, "projects/_/buckets/my-bucket");
                    assert_eq!(o.name, "my-object");

                    // Return the object
                    Ok(Object::default()
                        .set_bucket("projects/_/buckets/my-bucket")
                        .set_name("my-object")
                        .set_generation(42))
                },
            );
        let client = gcs::client::Storage::from_stub(mock);

        let object = upload(&client, "my-bucket", "my-object").await?;
        assert_eq!(object.generation, 42);

        Ok(())
    }
    // [END rust_storage_mocking_test_upload] ANCHOR_END: test-upload
}
// [END rust_storage_mocking_all] ANCHOR_END: all
