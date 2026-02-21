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

use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest as MetricsRequest, ExportMetricsServiceResponse,
    metrics_service_server::{MetricsService, MetricsServiceServer},
};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest as TraceRequest, ExportTraceServiceResponse,
    trace_service_server::{TraceService, TraceServiceServer},
};
use tonic::{Request, Response, Status};

use std::sync::{Arc, Mutex};

#[derive(Default, Clone)]
pub struct MockCollector {
    pub traces: Arc<Mutex<Vec<tonic::Request<TraceRequest>>>>,
    pub metrics: Arc<Mutex<Vec<tonic::Request<MetricsRequest>>>>,
}

#[tonic::async_trait]
impl TraceService for MockCollector {
    async fn export(
        &self,
        request: Request<TraceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        self.traces.lock().unwrap().push(request);
        Ok(Response::new(ExportTraceServiceResponse {
            partial_success: None,
        }))
    }
}

#[tonic::async_trait]
impl MetricsService for MockCollector {
    async fn export(
        &self,
        request: Request<MetricsRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        self.metrics.lock().unwrap().push(request);
        Ok(Response::new(ExportMetricsServiceResponse {
            partial_success: None,
        }))
    }
}

impl MockCollector {
    pub async fn start(&self) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let endpoint = format!("http://{}", addr);

        let this = self.clone();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(TraceServiceServer::new(this.clone()))
                .add_service(MetricsServiceServer::new(this.clone()))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap()
        });

        endpoint
    }
}
