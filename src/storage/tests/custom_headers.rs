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

#[cfg(test)]
mod tests {
    use gaxi::grpc::tonic::{Response as TonicResponse, Result as TonicResult};
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    use google_cloud_gax::options::RequestOptionsBuilder;
    use google_cloud_storage::client::{Storage, StorageControl};
    use http::header::{HeaderName, HeaderValue};
    use httptest::{Expectation, Server, all_of, matchers::*, responders::status_code};
    use scoped_env::ScopedEnv;
    use serial_test::serial;
    use storage_grpc_mock::google::storage::v2::{
        BidiReadObjectResponse, Bucket as ProtoBucket, Object as ProtoObject,
    };
    use storage_grpc_mock::{MockStorage, start};

    const BUCKET_NAME: &str = "projects/_/buckets/test-bucket";
    const OBJECT_NAME: &str = "test-object";

    #[tokio::test]
    #[serial]
    async fn read_object_with_custom_header() -> anyhow::Result<()> {
        // Disable HTTP proxy environment variables so reqwest connects directly to the local mock server.
        let _proxy_guard = [
            "http_proxy",
            "https_proxy",
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "all_proxy",
            "ALL_PROXY",
        ]
        .map(ScopedEnv::remove);

        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("GET", "/storage/v1/b/test-bucket/o/test-object"),
                request::headers(contains(("x-custom-header", "custom-value"))),
                request::query(url_decoded(contains(("alt", "media")))),
            ])
            .times(1)
            .respond_with(
                status_code(200)
                    .body("custom header read content")
                    .append_header("x-goog-generation", 123456),
            ),
        );

        let client = Storage::builder()
            .with_endpoint(format!("http://{}", server.addr()))
            .with_credentials(Anonymous::new().build())
            .with_custom_header(
                HeaderName::from_static("x-custom-header"),
                HeaderValue::from_static("custom-value"),
            )
            .build()
            .await?;

        let mut reader = client.read_object(BUCKET_NAME, OBJECT_NAME).send().await?;

        let mut got = Vec::new();
        while let Some(chunk) = reader.next().await.transpose()? {
            got.extend_from_slice(&chunk);
        }
        assert_eq!(got, b"custom header read content");

        Ok(())
    }

    #[tokio::test]
    async fn open_object_with_custom_header() -> anyhow::Result<()> {
        let (tx, rx) = tokio::sync::mpsc::channel::<TonicResult<BidiReadObjectResponse>>(1);
        let initial = BidiReadObjectResponse {
            metadata: Some(ProtoObject {
                bucket: BUCKET_NAME.to_string(),
                name: OBJECT_NAME.to_string(),
                generation: 123456,
                ..ProtoObject::default()
            }),
            ..BidiReadObjectResponse::default()
        };
        tx.send(Ok(initial)).await?;

        let mut mock = MockStorage::new();
        mock.expect_bidi_read_object().return_once(|request| {
            let meta = request.metadata();
            assert_eq!(
                meta.get("x-custom-header").and_then(|v| v.to_str().ok()),
                Some("custom-value")
            );
            Ok(TonicResponse::from(rx))
        });
        let (endpoint, _server) = start("127.0.0.1:0", mock).await?;

        let client = Storage::builder()
            .with_credentials(Anonymous::new().build())
            .with_endpoint(endpoint)
            .with_custom_header(
                HeaderName::from_static("x-custom-header"),
                HeaderValue::from_static("custom-value"),
            )
            .build()
            .await?;

        let _descriptor = client.open_object(BUCKET_NAME, OBJECT_NAME).send().await?;

        Ok(())
    }

    #[tokio::test]
    async fn storage_control_with_custom_header() -> anyhow::Result<()> {
        let mut mock = MockStorage::new();
        mock.expect_get_bucket()
            .withf(|req| {
                let meta = req.metadata();
                meta.get("x-custom-header").and_then(|v| v.to_str().ok()) == Some("custom-value")
            })
            .times(1)
            .returning(|_| Ok(TonicResponse::new(ProtoBucket::default())));

        let (endpoint, _server) = start("127.0.0.1:0", mock).await?;

        let client = StorageControl::builder()
            .with_endpoint(endpoint)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        let _ = client
            .get_bucket()
            .set_name(BUCKET_NAME)
            .with_custom_header(
                HeaderName::from_static("x-custom-header"),
                HeaderValue::from_static("custom-value"),
            )
            .send()
            .await?;

        Ok(())
    }
}
