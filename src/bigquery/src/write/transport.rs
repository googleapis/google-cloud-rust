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

pub(super) use super::generated::gapic_storage::transport::BigQueryWrite as Transport;
use crate::Result;
use crate::google::cloud::bigquery::storage::v1::{AppendRowsRequest, AppendRowsResponse};
use gaxi::grpc::tonic::{Response as TonicResponse, Streaming};
use tokio::sync::mpsc::Receiver;
use tokio_stream::wrappers::ReceiverStream;

mod info {
    use std::sync::LazyLock;

    const NAME: &str = env!("CARGO_PKG_NAME");
    const VERSION: &str = env!("CARGO_PKG_VERSION");
    pub(super) static X_GOOG_API_CLIENT_HEADER: LazyLock<String> = LazyLock::new(|| {
        let ac = gaxi::api_header::XGoogApiClient {
            name: NAME,
            version: VERSION,
            library_type: gaxi::api_header::GCCL,
        };
        ac.grpc_header_value()
    });
}

impl Transport {
    pub(crate) async fn append_rows(
        &self,
        request_params: &str,
        request_rx: Receiver<AppendRowsRequest>,
        options: crate::RequestOptions,
    ) -> Result<TonicResponse<Streaming<AppendRowsResponse>>> {
        use gaxi::grpc::tonic::{Extensions, GrpcMethod};
        let request = ReceiverStream::new(request_rx);
        let extensions = {
            let mut e = Extensions::new();
            e.insert(GrpcMethod::new(
                "google.cloud.bigquery.storage.v1.BigQueryWrite",
                "AppendRows",
            ));
            e
        };
        let path = http::uri::PathAndQuery::from_static(
            "/google.cloud.bigquery.storage.v1.BigQueryWrite/AppendRows",
        );
        self.inner
            .bidi_stream(
                extensions,
                path,
                request,
                options,
                &info::X_GOOG_API_CLIENT_HEADER,
                request_params,
            )
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::google::cloud::bigquery::storage::v1::append_rows_response::{
        AppendResult, Response,
    };
    use crate::write::test::*;
    use bigquery_grpc_mock::{MockBigQueryWrite, start};

    #[tokio::test]
    async fn append_rows() -> anyhow::Result<()> {
        let (response_tx, response_rx) = tokio::sync::mpsc::channel(1);
        let expected = AppendRowsResponse {
            response: Some(Response::AppendResult(AppendResult { offset: Some(1024) })),
            write_stream: "projects/p/datasets/d/tables/t/streams/s".to_string(),
            ..Default::default()
        };
        response_tx.send(Ok(convert(&expected))).await?;

        let (request_tx, request_rx) = tokio::sync::mpsc::channel(1);
        let request = AppendRowsRequest {
            write_stream: "projects/p/datasets/d/tables/t/streams/s".to_string(),
            ..Default::default()
        };
        request_tx.send(request).await?;

        let mut mock = MockBigQueryWrite::new();
        mock.expect_append_rows().return_once(|request| {
            let metadata = request.metadata();
            assert_eq!(
                metadata
                    .get("x-goog-request-params")
                    .expect("routing header missing"),
                "write_stream=projects/p/datasets/d/tables/t/streams/s"
            );
            Ok(TonicResponse::from(response_rx))
        });
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
        let transport = test_transport(endpoint).await?;
        let mut stream = transport
            .append_rows(
                "write_stream=projects/p/datasets/d/tables/t/streams/s",
                request_rx,
                crate::RequestOptions::default(),
            )
            .await?
            .into_inner();

        assert_eq!(stream.message().await?, Some(expected));

        drop(response_tx);
        assert_eq!(stream.message().await?, None);

        Ok(())
    }
}
