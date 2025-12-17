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

use super::google::pubsub::v1;
use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

#[mockall::automock]
#[async_trait]
pub trait Subscriber {
    // In the mock we use an easy-to-create type (mpsc::Receiver) for the
    // streams. The `impl` adapts between these concrete types and the more
    // general types / traits that Tonic uses.

    async fn streaming_pull(
        &self,
        request: tonic::Request<mpsc::Receiver<tonic::Result<v1::StreamingPullRequest>>>,
    ) -> tonic::Result<tonic::Response<mpsc::Receiver<tonic::Result<v1::StreamingPullResponse>>>>;

    // The unary RPCs just use the normal types.
    async fn acknowledge(
        &self,
        request: tonic::Request<v1::AcknowledgeRequest>,
    ) -> tonic::Result<tonic::Response<()>>;
    async fn modify_ack_deadline(
        &self,
        request: tonic::Request<v1::ModifyAckDeadlineRequest>,
    ) -> tonic::Result<tonic::Response<()>>;
    async fn create_subscription(
        &self,
        request: tonic::Request<v1::Subscription>,
    ) -> tonic::Result<tonic::Response<v1::Subscription>>;
    async fn get_subscription(
        &self,
        request: tonic::Request<v1::GetSubscriptionRequest>,
    ) -> tonic::Result<tonic::Response<v1::Subscription>>;
    async fn update_subscription(
        &self,
        request: tonic::Request<v1::UpdateSubscriptionRequest>,
    ) -> tonic::Result<tonic::Response<v1::Subscription>>;
    async fn list_subscriptions(
        &self,
        request: tonic::Request<v1::ListSubscriptionsRequest>,
    ) -> tonic::Result<tonic::Response<v1::ListSubscriptionsResponse>>;
    async fn delete_subscription(
        &self,
        request: tonic::Request<v1::DeleteSubscriptionRequest>,
    ) -> tonic::Result<tonic::Response<()>>;
    async fn pull(
        &self,
        request: tonic::Request<v1::PullRequest>,
    ) -> tonic::Result<tonic::Response<v1::PullResponse>>;
    async fn modify_push_config(
        &self,
        request: tonic::Request<v1::ModifyPushConfigRequest>,
    ) -> tonic::Result<tonic::Response<()>>;
    async fn get_snapshot(
        &self,
        request: tonic::Request<v1::GetSnapshotRequest>,
    ) -> tonic::Result<tonic::Response<v1::Snapshot>>;
    async fn list_snapshots(
        &self,
        request: tonic::Request<v1::ListSnapshotsRequest>,
    ) -> tonic::Result<tonic::Response<v1::ListSnapshotsResponse>>;
    async fn create_snapshot(
        &self,
        request: tonic::Request<v1::CreateSnapshotRequest>,
    ) -> tonic::Result<tonic::Response<v1::Snapshot>>;
    async fn update_snapshot(
        &self,
        request: tonic::Request<v1::UpdateSnapshotRequest>,
    ) -> tonic::Result<tonic::Response<v1::Snapshot>>;
    async fn delete_snapshot(
        &self,
        request: tonic::Request<v1::DeleteSnapshotRequest>,
    ) -> tonic::Result<tonic::Response<()>>;
    async fn seek(
        &self,
        request: tonic::Request<v1::SeekRequest>,
    ) -> tonic::Result<tonic::Response<v1::SeekResponse>>;
}

#[async_trait]
impl v1::subscriber_server::Subscriber for MockSubscriber {
    type StreamingPullStream = ReceiverStream<tonic::Result<v1::StreamingPullResponse>>;
    async fn streaming_pull(
        &self,
        request: tonic::Request<tonic::Streaming<v1::StreamingPullRequest>>,
    ) -> tonic::Result<tonic::Response<Self::StreamingPullStream>> {
        let request = adapt_streaming_request(request);
        let response = self::Subscriber::streaming_pull(self, request).await?;
        let (metadata, receiver, extensions) = response.into_parts();
        Ok(tonic::Response::from_parts(
            metadata,
            ReceiverStream::new(receiver),
            extensions,
        ))
    }
    async fn acknowledge(
        &self,
        request: tonic::Request<v1::AcknowledgeRequest>,
    ) -> tonic::Result<tonic::Response<()>> {
        self::Subscriber::acknowledge(self, request).await
    }
    async fn modify_ack_deadline(
        &self,
        request: tonic::Request<v1::ModifyAckDeadlineRequest>,
    ) -> tonic::Result<tonic::Response<()>> {
        self::Subscriber::modify_ack_deadline(self, request).await
    }
    async fn create_subscription(
        &self,
        request: tonic::Request<v1::Subscription>,
    ) -> tonic::Result<tonic::Response<v1::Subscription>> {
        self::Subscriber::create_subscription(self, request).await
    }
    async fn get_subscription(
        &self,
        request: tonic::Request<v1::GetSubscriptionRequest>,
    ) -> tonic::Result<tonic::Response<v1::Subscription>> {
        self::Subscriber::get_subscription(self, request).await
    }
    async fn update_subscription(
        &self,
        request: tonic::Request<v1::UpdateSubscriptionRequest>,
    ) -> tonic::Result<tonic::Response<v1::Subscription>> {
        self::Subscriber::update_subscription(self, request).await
    }
    async fn list_subscriptions(
        &self,
        request: tonic::Request<v1::ListSubscriptionsRequest>,
    ) -> tonic::Result<tonic::Response<v1::ListSubscriptionsResponse>> {
        self::Subscriber::list_subscriptions(self, request).await
    }
    async fn delete_subscription(
        &self,
        request: tonic::Request<v1::DeleteSubscriptionRequest>,
    ) -> tonic::Result<tonic::Response<()>> {
        self::Subscriber::delete_subscription(self, request).await
    }
    async fn pull(
        &self,
        request: tonic::Request<v1::PullRequest>,
    ) -> tonic::Result<tonic::Response<v1::PullResponse>> {
        self::Subscriber::pull(self, request).await
    }
    async fn modify_push_config(
        &self,
        request: tonic::Request<v1::ModifyPushConfigRequest>,
    ) -> tonic::Result<tonic::Response<()>> {
        self::Subscriber::modify_push_config(self, request).await
    }
    async fn get_snapshot(
        &self,
        request: tonic::Request<v1::GetSnapshotRequest>,
    ) -> tonic::Result<tonic::Response<v1::Snapshot>> {
        self::Subscriber::get_snapshot(self, request).await
    }
    async fn list_snapshots(
        &self,
        request: tonic::Request<v1::ListSnapshotsRequest>,
    ) -> tonic::Result<tonic::Response<v1::ListSnapshotsResponse>> {
        self::Subscriber::list_snapshots(self, request).await
    }
    async fn create_snapshot(
        &self,
        request: tonic::Request<v1::CreateSnapshotRequest>,
    ) -> tonic::Result<tonic::Response<v1::Snapshot>> {
        self::Subscriber::create_snapshot(self, request).await
    }
    async fn update_snapshot(
        &self,
        request: tonic::Request<v1::UpdateSnapshotRequest>,
    ) -> tonic::Result<tonic::Response<v1::Snapshot>> {
        self::Subscriber::update_snapshot(self, request).await
    }
    async fn delete_snapshot(
        &self,
        request: tonic::Request<v1::DeleteSnapshotRequest>,
    ) -> tonic::Result<tonic::Response<()>> {
        self::Subscriber::delete_snapshot(self, request).await
    }
    async fn seek(
        &self,
        request: tonic::Request<v1::SeekRequest>,
    ) -> tonic::Result<tonic::Response<v1::SeekResponse>> {
        self::Subscriber::seek(self, request).await
    }
}

fn adapt_streaming_request<T>(
    request: tonic::Request<tonic::Streaming<T>>,
) -> tonic::Request<mpsc::Receiver<tonic::Result<T>>>
where
    T: Send + 'static,
{
    let (tx, rx) = mpsc::channel(1);
    let (metadata, extensions, stream) = request.into_parts();
    forward(tx, stream);
    tonic::Request::from_parts(metadata, extensions, rx)
}

fn forward<T>(tx: mpsc::Sender<tonic::Result<T>>, mut stream: tonic::Streaming<T>)
where
    T: Send + 'static,
{
    tokio::spawn(async move {
        while let Some(r) = stream.message().await.transpose() {
            let _ = tx.send(r).await; // Ignore errors caused by closed streams.
        }
    });
}
