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

use super::google::iam;
use super::google::storage::v2;
use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

#[mockall::automock]
#[async_trait]
pub trait Storage {
    // In the mock we use an easy-to-create type (mpsc::Receiver) for the
    // streams. The `impl` adapts between these concrete types and the more
    // general types / traits that Tonic uses.

    async fn read_object(
        &self,
        request: tonic::Request<v2::ReadObjectRequest>,
    ) -> tonic::Result<tonic::Response<mpsc::Receiver<tonic::Result<v2::ReadObjectResponse>>>>;

    async fn write_object(
        &self,
        request: tonic::Request<mpsc::Receiver<tonic::Result<v2::WriteObjectRequest>>>,
    ) -> tonic::Result<tonic::Response<v2::WriteObjectResponse>>;

    async fn bidi_read_object(
        &self,
        request: tonic::Request<mpsc::Receiver<tonic::Result<v2::BidiReadObjectRequest>>>,
    ) -> tonic::Result<tonic::Response<mpsc::Receiver<tonic::Result<v2::BidiReadObjectResponse>>>>;
    async fn bidi_write_object(
        &self,
        request: tonic::Request<mpsc::Receiver<tonic::Result<v2::BidiWriteObjectRequest>>>,
    ) -> tonic::Result<tonic::Response<mpsc::Receiver<tonic::Result<v2::BidiWriteObjectResponse>>>>;

    // The unary RPCs just use the normal types.
    async fn delete_bucket(
        &self,
        request: tonic::Request<v2::DeleteBucketRequest>,
    ) -> tonic::Result<tonic::Response<()>>;
    async fn get_bucket(
        &self,
        request: tonic::Request<v2::GetBucketRequest>,
    ) -> tonic::Result<tonic::Response<v2::Bucket>>;
    async fn create_bucket(
        &self,
        request: tonic::Request<v2::CreateBucketRequest>,
    ) -> tonic::Result<tonic::Response<v2::Bucket>>;
    async fn list_buckets(
        &self,
        request: tonic::Request<v2::ListBucketsRequest>,
    ) -> tonic::Result<tonic::Response<v2::ListBucketsResponse>>;
    async fn lock_bucket_retention_policy(
        &self,
        request: tonic::Request<v2::LockBucketRetentionPolicyRequest>,
    ) -> tonic::Result<tonic::Response<v2::Bucket>>;
    async fn get_iam_policy(
        &self,
        request: tonic::Request<iam::v1::GetIamPolicyRequest>,
    ) -> tonic::Result<tonic::Response<iam::v1::Policy>>;
    async fn set_iam_policy(
        &self,
        request: tonic::Request<iam::v1::SetIamPolicyRequest>,
    ) -> tonic::Result<tonic::Response<iam::v1::Policy>>;
    async fn test_iam_permissions(
        &self,
        request: tonic::Request<iam::v1::TestIamPermissionsRequest>,
    ) -> tonic::Result<tonic::Response<iam::v1::TestIamPermissionsResponse>>;
    async fn update_bucket(
        &self,
        request: tonic::Request<v2::UpdateBucketRequest>,
    ) -> tonic::Result<tonic::Response<v2::Bucket>>;
    async fn compose_object(
        &self,
        request: tonic::Request<v2::ComposeObjectRequest>,
    ) -> tonic::Result<tonic::Response<v2::Object>>;
    async fn delete_object(
        &self,
        request: tonic::Request<v2::DeleteObjectRequest>,
    ) -> tonic::Result<tonic::Response<()>>;
    async fn restore_object(
        &self,
        request: tonic::Request<v2::RestoreObjectRequest>,
    ) -> tonic::Result<tonic::Response<v2::Object>>;
    async fn cancel_resumable_write(
        &self,
        request: tonic::Request<v2::CancelResumableWriteRequest>,
    ) -> tonic::Result<tonic::Response<v2::CancelResumableWriteResponse>>;
    async fn get_object(
        &self,
        request: tonic::Request<v2::GetObjectRequest>,
    ) -> tonic::Result<tonic::Response<v2::Object>>;
    async fn update_object(
        &self,
        request: tonic::Request<v2::UpdateObjectRequest>,
    ) -> tonic::Result<tonic::Response<v2::Object>>;
    async fn list_objects(
        &self,
        request: tonic::Request<v2::ListObjectsRequest>,
    ) -> tonic::Result<tonic::Response<v2::ListObjectsResponse>>;
    async fn rewrite_object(
        &self,
        request: tonic::Request<v2::RewriteObjectRequest>,
    ) -> tonic::Result<tonic::Response<v2::RewriteResponse>>;
    async fn start_resumable_write(
        &self,
        request: tonic::Request<v2::StartResumableWriteRequest>,
    ) -> tonic::Result<tonic::Response<v2::StartResumableWriteResponse>>;
    async fn query_write_status(
        &self,
        request: tonic::Request<v2::QueryWriteStatusRequest>,
    ) -> tonic::Result<tonic::Response<v2::QueryWriteStatusResponse>>;
    async fn move_object(
        &self,
        request: tonic::Request<v2::MoveObjectRequest>,
    ) -> tonic::Result<tonic::Response<v2::Object>>;
}

#[async_trait]
impl v2::storage_server::Storage for MockStorage {
    type ReadObjectStream = ReceiverStream<tonic::Result<v2::ReadObjectResponse>>;
    async fn read_object(
        &self,
        request: tonic::Request<v2::ReadObjectRequest>,
    ) -> tonic::Result<tonic::Response<Self::ReadObjectStream>> {
        let response = self::Storage::read_object(self, request).await?;
        let (metadata, receiver, extensions) = response.into_parts();
        Ok(tonic::Response::from_parts(
            metadata,
            ReceiverStream::new(receiver),
            extensions,
        ))
    }

    async fn write_object(
        &self,
        request: tonic::Request<tonic::Streaming<v2::WriteObjectRequest>>,
    ) -> tonic::Result<tonic::Response<v2::WriteObjectResponse>> {
        let request = adapt_streaming_request(request);
        self::Storage::write_object(self, request).await
    }

    type BidiReadObjectStream = ReceiverStream<tonic::Result<v2::BidiReadObjectResponse>>;
    async fn bidi_read_object(
        &self,
        request: tonic::Request<tonic::Streaming<v2::BidiReadObjectRequest>>,
    ) -> tonic::Result<tonic::Response<Self::BidiReadObjectStream>> {
        let request = adapt_streaming_request(request);
        let response = self::Storage::bidi_read_object(self, request).await?;
        let (metadata, receiver, extensions) = response.into_parts();
        Ok(tonic::Response::from_parts(
            metadata,
            ReceiverStream::new(receiver),
            extensions,
        ))
    }

    type BidiWriteObjectStream = ReceiverStream<tonic::Result<v2::BidiWriteObjectResponse>>;
    async fn bidi_write_object(
        &self,
        request: tonic::Request<tonic::Streaming<v2::BidiWriteObjectRequest>>,
    ) -> tonic::Result<tonic::Response<Self::BidiWriteObjectStream>> {
        let request = adapt_streaming_request(request);
        let response = self::Storage::bidi_write_object(self, request).await?;
        let (metadata, receiver, extensions) = response.into_parts();
        Ok(tonic::Response::from_parts(
            metadata,
            ReceiverStream::new(receiver),
            extensions,
        ))
    }

    async fn delete_bucket(
        &self,
        request: tonic::Request<v2::DeleteBucketRequest>,
    ) -> tonic::Result<tonic::Response<()>> {
        self::Storage::delete_bucket(self, request).await
    }
    async fn get_bucket(
        &self,
        request: tonic::Request<v2::GetBucketRequest>,
    ) -> tonic::Result<tonic::Response<v2::Bucket>> {
        self::Storage::get_bucket(self, request).await
    }
    async fn create_bucket(
        &self,
        request: tonic::Request<v2::CreateBucketRequest>,
    ) -> tonic::Result<tonic::Response<v2::Bucket>> {
        self::Storage::create_bucket(self, request).await
    }
    async fn list_buckets(
        &self,
        request: tonic::Request<v2::ListBucketsRequest>,
    ) -> tonic::Result<tonic::Response<v2::ListBucketsResponse>> {
        self::Storage::list_buckets(self, request).await
    }
    async fn lock_bucket_retention_policy(
        &self,
        request: tonic::Request<v2::LockBucketRetentionPolicyRequest>,
    ) -> tonic::Result<tonic::Response<v2::Bucket>> {
        self::Storage::lock_bucket_retention_policy(self, request).await
    }
    async fn get_iam_policy(
        &self,
        request: tonic::Request<iam::v1::GetIamPolicyRequest>,
    ) -> tonic::Result<tonic::Response<iam::v1::Policy>> {
        self::Storage::get_iam_policy(self, request).await
    }
    async fn set_iam_policy(
        &self,
        request: tonic::Request<iam::v1::SetIamPolicyRequest>,
    ) -> tonic::Result<tonic::Response<iam::v1::Policy>> {
        self::Storage::set_iam_policy(self, request).await
    }
    async fn test_iam_permissions(
        &self,
        request: tonic::Request<iam::v1::TestIamPermissionsRequest>,
    ) -> tonic::Result<tonic::Response<iam::v1::TestIamPermissionsResponse>> {
        self::Storage::test_iam_permissions(self, request).await
    }
    async fn update_bucket(
        &self,
        request: tonic::Request<v2::UpdateBucketRequest>,
    ) -> tonic::Result<tonic::Response<v2::Bucket>> {
        self::Storage::update_bucket(self, request).await
    }
    async fn compose_object(
        &self,
        request: tonic::Request<v2::ComposeObjectRequest>,
    ) -> tonic::Result<tonic::Response<v2::Object>> {
        self::Storage::compose_object(self, request).await
    }
    async fn delete_object(
        &self,
        request: tonic::Request<v2::DeleteObjectRequest>,
    ) -> tonic::Result<tonic::Response<()>> {
        self::Storage::delete_object(self, request).await
    }
    async fn restore_object(
        &self,
        request: tonic::Request<v2::RestoreObjectRequest>,
    ) -> tonic::Result<tonic::Response<v2::Object>> {
        self::Storage::restore_object(self, request).await
    }
    async fn cancel_resumable_write(
        &self,
        request: tonic::Request<v2::CancelResumableWriteRequest>,
    ) -> tonic::Result<tonic::Response<v2::CancelResumableWriteResponse>> {
        self::Storage::cancel_resumable_write(self, request).await
    }
    async fn get_object(
        &self,
        request: tonic::Request<v2::GetObjectRequest>,
    ) -> tonic::Result<tonic::Response<v2::Object>> {
        self::Storage::get_object(self, request).await
    }
    async fn update_object(
        &self,
        request: tonic::Request<v2::UpdateObjectRequest>,
    ) -> tonic::Result<tonic::Response<v2::Object>> {
        self::Storage::update_object(self, request).await
    }
    async fn list_objects(
        &self,
        request: tonic::Request<v2::ListObjectsRequest>,
    ) -> tonic::Result<tonic::Response<v2::ListObjectsResponse>> {
        self::Storage::list_objects(self, request).await
    }
    async fn rewrite_object(
        &self,
        request: tonic::Request<v2::RewriteObjectRequest>,
    ) -> tonic::Result<tonic::Response<v2::RewriteResponse>> {
        self::Storage::rewrite_object(self, request).await
    }
    async fn start_resumable_write(
        &self,
        request: tonic::Request<v2::StartResumableWriteRequest>,
    ) -> tonic::Result<tonic::Response<v2::StartResumableWriteResponse>> {
        self::Storage::start_resumable_write(self, request).await
    }
    async fn query_write_status(
        &self,
        request: tonic::Request<v2::QueryWriteStatusRequest>,
    ) -> tonic::Result<tonic::Response<v2::QueryWriteStatusResponse>> {
        self::Storage::query_write_status(self, request).await
    }
    async fn move_object(
        &self,
        request: tonic::Request<v2::MoveObjectRequest>,
    ) -> tonic::Result<tonic::Response<v2::Object>> {
        self::Storage::move_object(self, request).await
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
