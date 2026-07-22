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

use google_cloud_monitoring_v3::client::MetricService;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::metrics::Temporality;
use opentelemetry_sdk::metrics::data::ResourceMetrics;
use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Debug)]
pub(crate) struct GcpMonitoringExporter {
    #[allow(dead_code)]
    client: Arc<MetricService>,
    #[allow(dead_code)]
    project_name: String,
}

impl GcpMonitoringExporter {
    pub(crate) fn new(client: MetricService, project_id: &str) -> Self {
        Self {
            client: Arc::new(client),
            project_name: format!("projects/{}", project_id),
        }
    }
}

impl PushMetricExporter for GcpMonitoringExporter {
    async fn export(&self, _metrics: &ResourceMetrics) -> OTelSdkResult {
        // TODO: Map ResourceMetrics to CreateTimeSeriesRequest and send via self.client
        Ok(())
    }

    fn force_flush(&self) -> OTelSdkResult {
        Ok(())
    }

    fn shutdown(&self) -> OTelSdkResult {
        Ok(())
    }

    fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
        self.shutdown()
    }

    fn temporality(&self) -> Temporality {
        Temporality::Cumulative
    }
}
