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

use super::{new_credentials, set_up_providers, wait_for_trace};
use google_cloud_storage::client::Storage;
use google_cloud_storage::model_ext::ReadRange;
use google_cloud_test_utils::runtime_config::project_id;
use opentelemetry::trace::TraceContextExt;
use std::collections::BTreeSet;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

const ROOT_SPAN_NAME: &str = "e2e-storage-test";

pub async fn run() -> anyhow::Result<()> {
    let project_id = project_id()?;
    // Create a trace with a number of interesting spans from the
    // `google-cloud-storage` client.
    let trace_id = send_trace(&project_id).await?;
    let required = BTreeSet::from_iter([
        ROOT_SPAN_NAME,
        "google.storage.v2.Storage/ListBuckets",
        "create_bucket",
        "google.storage.v2.Storage/CreateBucket",
        "google_cloud_storage::client::Storage::write_object",
        "POST /upload/storage/v1/b/{bucket}/o",
        "PUT /upload/storage/v1/b/{bucket}/o",
        // These appear in the spans, but the test does not worry about entries
        // with the same name
        //   "google_cloud_storage::client::Storage::write_object",
        //   "POST /upload/storage/v1/b/{bucket}/o",
        "google_cloud_storage::client::Storage::read_object",
        "GET /storage/v1/b/{bucket}/o/{object}",
        "google_cloud_storage::client::Storage::open_object",
        "google.storage.v2.Storage/BidiReadObject",
        "get_bucket",
        "google.storage.v2.Storage/GetBucket",
        "list_objects",
        "google.storage.v2.Storage/ListObjects",
        "delete_object",
        "google.storage.v2.Storage/DeleteObject",
        "list_anywhere_caches",
        "google.storage.control.v2.StorageControl/ListAnywhereCaches",
        "delete_bucket",
        "google.storage.v2.Storage/DeleteBucket",
    ]);
    let trace = wait_for_trace(&project_id, &trace_id, &required).await?;

    // Verify the expected spans appear in the trace:
    let span_names = trace
        .spans
        .iter()
        .map(|s| s.name.as_str())
        .collect::<BTreeSet<_>>();
    let missing = required.difference(&span_names).collect::<Vec<_>>();
    assert!(missing.is_empty(), "missing={missing:?}\n\n{trace:?}",);

    Ok(())
}

async fn send_trace(project_id: &str) -> anyhow::Result<String> {
    // 1. Setup Telemetry (Real Google Cloud Destination)
    let id = Uuid::new_v4();
    let credentials = new_credentials(project_id).await?;
    let (provider, _meter_provider, _) = set_up_providers(
        project_id,
        ROOT_SPAN_NAME,
        id.to_string(),
        credentials.clone(),
    )
    .await?;

    // 2. Generate Trace
    // Start a root span
    let root_span = tracing::info_span!("e2e_root", { "otel.name" } = ROOT_SPAN_NAME);
    let trace_id = root_span
        .context()
        .span()
        .span_context()
        .trace_id()
        .to_string();

    use tracing::Instrument;
    let _ = client_library_operations().instrument(root_span).await;

    println!(
        "View generated trace in Console: https://console.cloud.google.com/traces/explorer;traceId={}?project={}",
        trace_id, project_id
    );

    // 4. Force flush to ensure spans are sent.
    if let Err(e) = provider.force_flush() {
        tracing::error!("error flushing provider: {e:}");
    }
    Ok(trace_id)
}

// Run some StorageControl and Storage operations.
async fn client_library_operations() -> anyhow::Result<()> {
    let (control, bucket) = storage_samples::create_test_bucket().await?;
    let _ = storage_data_operations(&bucket.name).await;
    if let Err(e) = storage_samples::cleanup_bucket(control, bucket.name.clone()).await {
        tracing::error!("error cleaning up test bucket {}: {e:?}", bucket.name);
    };
    Ok(())
}

async fn storage_data_operations(bucket_name: &str) -> anyhow::Result<()> {
    let client = Storage::builder().with_tracing().build().await?;

    const CONTENTS: &str = "the quick brown fox jumps over the lazy dog";
    let body = (0..100)
        .map(|i| format!("{i:08} {CONTENTS:1000}"))
        .collect::<Vec<_>>()
        .join("\n");
    tracing::info!("uploading small object with send_buffered");
    let _ = client
        .write_object(bucket_name, "unused.txt", body.clone())
        .set_content_type("text/plain")
        .set_content_language("en")
        .set_storage_class("STANDARD")
        .with_resumable_upload_threshold(0_usize)
        .send_buffered()
        .await?;
    tracing::info!("uploading small object with send_unbuffered");
    let insert = client
        .write_object(bucket_name, "quick.txt", body.clone())
        .set_metadata([("verify-metadata-works", "yes")])
        .set_content_type("text/plain")
        .set_content_language("en")
        .set_storage_class("STANDARD")
        .send_unbuffered()
        .await?;
    {
        tracing::info!("reading small object");
        let mut response = client.read_object(bucket_name, &insert.name).send().await?;
        while response.next().await.transpose()?.is_some() {}
    }
    {
        tracing::info!("opening small object");
        let descriptor = client
            .open_object(&insert.bucket, &insert.name)
            .send()
            .await?;
        tracing::info!("reading from open object");
        let mut response = descriptor.read_range(ReadRange::head(16)).await;
        while response.next().await.transpose()?.is_some() {}
    }
    tracing::info!("all Storage operations done");

    Ok(())
}
