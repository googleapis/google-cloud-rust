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

#[cfg(test)]
mod tests {
    use crate::client::Storage;
    use crate::model::Object;
    use auth::credentials::anonymous::Builder as Anonymous;
    use storage_grpc_mock::google::storage::v2::{BidiReadObjectResponse, Object as ProtoObject};
    use storage_grpc_mock::{MockStorage, start};

    // Verify `open_object()` meets normal Send, Sync, requirements.
    #[tokio::test]
    async fn test_open_object_is_send_and_static() -> anyhow::Result<()> {
        let client = Storage::builder()
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        fn need_send<T: Send>(_val: &T) {}
        fn need_sync<T: Sync>(_val: &T) {}
        fn need_static<T: 'static>(_val: &T) {}

        let open = client.read_object("projects/_/buckets/test-bucket", "test-object");
        need_send(&open);
        need_sync(&open);
        need_static(&open);

        let open = client
            .open_object("projects/_/buckets/test-bucket", "test-object")
            .send();
        need_send(&open);
        need_static(&open);
        Ok(())
    }

    #[tokio::test]
    async fn open_object_normal() -> anyhow::Result<()> {
        const BUCKET_NAME: &str = "projects/_/buckets/test-bucket";

        let (tx, rx) = tokio::sync::mpsc::channel::<tonic::Result<BidiReadObjectResponse>>(1);
        let initial = BidiReadObjectResponse {
            metadata: Some(ProtoObject {
                bucket: BUCKET_NAME.to_string(),
                name: "test-object".to_string(),
                generation: 123456,
                size: 42,
                ..ProtoObject::default()
            }),
            ..BidiReadObjectResponse::default()
        };
        tx.send(Ok(initial.clone())).await?;

        let mut mock = MockStorage::new();
        mock.expect_bidi_read_object()
            .return_once(|_| Ok(tonic::Response::from(rx)));
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;

        let client = Storage::builder()
            .with_endpoint(endpoint)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;
        let descriptor = client
            .open_object(BUCKET_NAME, "test-object")
            .send()
            .await?;

        let got = descriptor.object();
        let want = Object::new()
            .set_bucket(BUCKET_NAME)
            .set_name("test-object")
            .set_generation(123456)
            .set_size(42);
        assert_eq!(got, &want);

        Ok(())
    }
}
