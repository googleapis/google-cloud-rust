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

use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
use google_cloud_storage::client::StorageControl;
use integration_tests_o11y::storage_grpc_tracing_common::setup_o11y;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use storage_grpc_mock::{MockStorage, start};
use tonic::{Code, Status};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_can_be_disabled() -> anyhow::Result<()> {
    let setup = setup_o11y().await?;

    let mut mock = MockStorage::new();
    mock.expect_delete_bucket()
        .return_once(|_| Err(Status::new(Code::NotFound, "Object not found")));

    let (endpoint, _server) = start("0.0.0.0:0", mock).await?;
    let endpoint = endpoint.trim_end_matches('/');

    // Intentionally omit .with_tracing()
    let client = StorageControl::builder()
        .with_endpoint(endpoint)
        .with_credentials(Anonymous::new().build())
        .build()
        .await?;

    let _ = client
        .delete_bucket()
        .set_name("projects/_/buckets/test-bucket")
        .send()
        .await;

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let _ = setup.provider.force_flush();

    let mut traces_lock = setup.mock_collector.traces.lock().expect("never poisoned");
    // Verify no spans with CLIENT kind exist
    for request in traces_lock.drain(..) {
        let req: tonic::Request<ExportTraceServiceRequest> = request;
        let (_, _, req) = req.into_parts();
        for rs in req.resource_spans {
            for ss in rs.scope_spans {
                for span in ss.spans {
                    assert_ne!(span.kind, 3, "Should not emit CLIENT spans when disabled");
                }
            }
        }
    }

    Ok(())
}
