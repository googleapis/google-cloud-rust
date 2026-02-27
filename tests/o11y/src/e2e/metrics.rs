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

use super::resource_detector::TestResourceDetector;
use super::{MetricService, set_up_meter_provider, set_up_tracer_provider, try_get_metric};
use google_cloud_test_utils::runtime_config::project_id;
use opentelemetry::KeyValue;
use rand::RngExt;
use rand::distr::Uniform;
use std::time::Duration;

const METRIC_NAME: &str = "workload.googleapis.com/test.e2e.metric";

pub async fn run() -> anyhow::Result<()> {
    let id = uuid::Uuid::new_v4().to_string();
    let project_id = project_id()?;
    let _tracer_provider = set_up_tracer_provider(&project_id).await?;
    let provider =
        set_up_meter_provider(&project_id, TestResourceDetector::new(&project_id)).await?;

    let client = MetricService::builder().build().await?;

    let meter = opentelemetry::global::meter(env!("CARGO_PKG_NAME"));
    let metric = meter.f64_histogram(METRIC_NAME).with_unit("s").build();
    let generator = Uniform::new(Duration::from_millis(0), Duration::from_secs(120))?;

    // This may take several attempts, as inserting values in a timeseries is rate limited.
    let mut found = None;
    for delay in [0, 10, 60, 120, 120, 120].map(Duration::from_secs) {
        tokio::time::sleep(delay).await;
        for d in rand::rng().sample_iter(&generator).take(5_000) {
            metric.record(d.as_secs_f64(), &[KeyValue::new("testId", id.clone())]);
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        // Ignore errors because it may have flushed recently.
        let _ = provider.force_flush();
        found = try_get_metric(&client, &project_id, METRIC_NAME, ("testId", id.as_str())).await?;
        if found.is_some() {
            break;
        }
    }
    let found = found.expect("timeseries is found in Cloud Monitoring");
    println!("found metric: {found:?}");
    assert!(!found.time_series.is_empty(), "{found:?}");

    Ok(())
}
