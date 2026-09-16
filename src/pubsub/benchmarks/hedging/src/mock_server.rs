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

use rand::RngExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::task::JoinHandle;

use pubsub_grpc_mock::google::pubsub::v1::publisher_server::{Publisher, PublisherServer};
use pubsub_grpc_mock::google::pubsub::v1::{
    DeleteTopicRequest, DetachSubscriptionRequest, DetachSubscriptionResponse, GetTopicRequest,
    ListTopicSnapshotsRequest, ListTopicSnapshotsResponse, ListTopicSubscriptionsRequest,
    ListTopicSubscriptionsResponse, ListTopicsRequest, ListTopicsResponse, PublishRequest,
    PublishResponse, Topic, UpdateTopicRequest,
};

#[derive(Debug, Clone)]
pub struct MockServerConfig {
    pub fast_latency: Duration,
    pub fast_ratio: f64,
    pub degraded_latency: Duration,
    pub degraded_ratio: f64,
    pub stall_latency: Duration,
}

impl Default for MockServerConfig {
    fn default() -> Self {
        Self {
            fast_latency: Duration::from_millis(5),
            fast_ratio: 0.95,
            degraded_latency: Duration::from_millis(300),
            degraded_ratio: 0.04,
            stall_latency: Duration::from_secs(4),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct MockServerStats {
    pub total_publish_requests: Arc<AtomicU64>,
    pub hedged_publish_requests: Arc<AtomicU64>,
}

impl MockServerStats {
    pub fn reset(&self) {
        self.total_publish_requests.store(0, Ordering::Relaxed);
        self.hedged_publish_requests.store(0, Ordering::Relaxed);
    }
}

#[derive(Clone)]
struct MockPublisherService {
    config: MockServerConfig,
    stats: MockServerStats,
    id_counter: Arc<AtomicU64>,
}

#[tonic::async_trait]
impl Publisher for MockPublisherService {
    async fn create_topic(
        &self,
        _request: tonic::Request<Topic>,
    ) -> Result<tonic::Response<Topic>, tonic::Status> {
        Err(tonic::Status::unimplemented("unimplemented in mock server"))
    }

    async fn update_topic(
        &self,
        _request: tonic::Request<UpdateTopicRequest>,
    ) -> Result<tonic::Response<Topic>, tonic::Status> {
        Err(tonic::Status::unimplemented("unimplemented in mock server"))
    }

    async fn publish(
        &self,
        request: tonic::Request<PublishRequest>,
    ) -> Result<tonic::Response<PublishResponse>, tonic::Status> {
        self.stats
            .total_publish_requests
            .fetch_add(1, Ordering::Relaxed);

        if request
            .metadata()
            .get("x-goog-pubsub-client-telemetry")
            .is_some()
        {
            self.stats
                .hedged_publish_requests
                .fetch_add(1, Ordering::Relaxed);
        }

        let roll: f64 = rand::rng().random();
        let sleep_duration = if roll < self.config.fast_ratio {
            self.config.fast_latency
        } else if roll < self.config.fast_ratio + self.config.degraded_ratio {
            self.config.degraded_latency
        } else {
            self.config.stall_latency
        };

        if !sleep_duration.is_zero() {
            tokio::time::sleep(sleep_duration).await;
        }

        let inner = request.into_inner();
        let count = inner.messages.len();
        let mut message_ids = Vec::with_capacity(count);
        for _ in 0..count {
            let id = self.id_counter.fetch_add(1, Ordering::Relaxed);
            message_ids.push(format!("msg-{id}"));
        }

        Ok(tonic::Response::new(PublishResponse { message_ids }))
    }

    async fn get_topic(
        &self,
        _request: tonic::Request<GetTopicRequest>,
    ) -> Result<tonic::Response<Topic>, tonic::Status> {
        Err(tonic::Status::unimplemented("unimplemented in mock server"))
    }

    async fn list_topics(
        &self,
        _request: tonic::Request<ListTopicsRequest>,
    ) -> Result<tonic::Response<ListTopicsResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unimplemented in mock server"))
    }

    async fn list_topic_subscriptions(
        &self,
        _request: tonic::Request<ListTopicSubscriptionsRequest>,
    ) -> Result<tonic::Response<ListTopicSubscriptionsResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unimplemented in mock server"))
    }

    async fn list_topic_snapshots(
        &self,
        _request: tonic::Request<ListTopicSnapshotsRequest>,
    ) -> Result<tonic::Response<ListTopicSnapshotsResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unimplemented in mock server"))
    }

    async fn delete_topic(
        &self,
        _request: tonic::Request<DeleteTopicRequest>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        Err(tonic::Status::unimplemented("unimplemented in mock server"))
    }

    async fn detach_subscription(
        &self,
        _request: tonic::Request<DetachSubscriptionRequest>,
    ) -> Result<tonic::Response<DetachSubscriptionResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unimplemented in mock server"))
    }
}

pub struct MockServerHandle {
    pub stats: MockServerStats,
    pub server_task: JoinHandle<()>,
}

pub async fn start_mock_server(
    config: MockServerConfig,
) -> anyhow::Result<(String, MockServerHandle)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    let stats = MockServerStats::default();
    let service = MockPublisherService {
        config,
        stats: stats.clone(),
        id_counter: Arc::new(AtomicU64::new(1)),
    };

    let server_task = tokio::spawn(async move {
        let stream = tokio_stream::wrappers::TcpListenerStream::new(listener);
        let _ = tonic::transport::Server::builder()
            .add_service(PublisherServer::new(service))
            .serve_with_incoming(stream)
            .await;
    });

    let uri = format!("http://127.0.0.1:{}", addr.port());
    Ok((uri, MockServerHandle { stats, server_task }))
}
