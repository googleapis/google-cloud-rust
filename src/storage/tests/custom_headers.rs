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
    use google_cloud_gax::retry_policy::NeverRetry;
    use google_cloud_storage::client::{Storage, StorageControl};
    use http::header::{HeaderName, HeaderValue};
    use httptest::{Expectation, Server, all_of, matchers::*, responders::status_code};
    use storage_grpc_mock::google::storage::v2::{
        BidiReadObjectResponse, Bucket as ProtoBucket, Object as ProtoObject,
    };
    use storage_grpc_mock::{MockStorage, start};

    const BUCKET_NAME: &str = "projects/_/buckets/test-bucket";
    const OBJECT_NAME: &str = "test-object";

    #[tokio::test]
    async fn read_object_with_custom_header() -> anyhow::Result<()> {
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
            // httptest answers unmatched requests with a 500; don't retry it.
            .with_retry_policy(NeverRetry)
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

        let (header_tx, header_rx) = tokio::sync::oneshot::channel();
        let mut mock = MockStorage::new();
        mock.expect_bidi_read_object().return_once(|request| {
            let _ = header_tx.send(request.metadata().get("x-custom-header").cloned());
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

        let got = header_rx.await?;
        assert_eq!(
            got.as_ref().and_then(|v| v.to_str().ok()),
            Some("custom-value")
        );

        Ok(())
    }

    #[tokio::test]
    async fn storage_control_with_custom_header() -> anyhow::Result<()> {
        let (header_tx, header_rx) = tokio::sync::oneshot::channel();
        let mut mock = MockStorage::new();
        mock.expect_get_bucket().return_once(|request| {
            let _ = header_tx.send(request.metadata().get("x-custom-header").cloned());
            Ok(TonicResponse::new(ProtoBucket::default()))
        });
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

        let got = header_rx.await?;
        assert_eq!(
            got.as_ref().and_then(|v| v.to_str().ok()),
            Some("custom-value")
        );

        Ok(())
    }
}
