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

use gaxi::observability::RequestRecorder;
use gaxi::options::InstrumentationClientInfo;
use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
use google_cloud_storage::client::StorageControl;
use integration_tests_o11y::storage_grpc_tracing_common::{
    setup_o11y, verify_logs, verify_metrics,
};
use storage_grpc_mock::{MockStorage, start};
use tonic::{Code, Status};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_reports_server_error() -> anyhow::Result<()> {
    let setup = setup_o11y().await?;

    let mut mock = MockStorage::new();
    mock.expect_delete_bucket()
        .return_once(|_| Err(Status::new(Code::NotFound, "Object not found")));

    let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
    let endpoint = endpoint.trim_end_matches('/');

    let client = StorageControl::builder()
        .with_endpoint(endpoint)
        .with_credentials(Anonymous::new().build())
        .with_tracing()
        .build()
        .await?;

    let _ = {
        let mut info = InstrumentationClientInfo::default();
        info.service_name = "storage";
        info.client_version = "1.10.0";
        info.client_artifact = "google-cloud-storage";
        info.default_host = "storage.googleapis.com";
        let recorder = RequestRecorder::new(info);
        recorder
            .scope(async {
                client
                    .delete_bucket()
                    .set_name("projects/_/buckets/test-bucket")
                    .send()
                    .await
            })
            .await
    };

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let _ = setup.provider.force_flush();
    let _ = setup.meter_provider.force_flush();
    let _ = setup.logger_provider.force_flush();

    let (_, _, request) = setup
        .mock_collector
        .traces
        .lock()
        .expect("never poisoned")
        .pop()
        .expect("should have received at least one trace request")
        .into_parts();

    let mut all_spans = Vec::new();
    for rs in request.resource_spans {
        for ss in rs.scope_spans {
            all_spans.extend(ss.spans);
        }
    }

    let client_span = all_spans
        .iter()
        .find(|s| {
            (s.name == "delete_bucket" || s.name == "google.storage.v2.Storage/DeleteBucket")
                && (s.kind == 1 || s.kind == 3)
        })
        .expect("Should have a DeleteBucket span");

    verify_metrics(&setup.mock_collector);
    verify_logs(&setup.mock_collector, client_span);

    Ok(())
}
