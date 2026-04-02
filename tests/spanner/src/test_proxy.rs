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

use spanner_grpc_mock::google::spanner::v1 as spanner_v1;
use spanner_grpc_mock::google::spanner::v1::spanner_client::SpannerClient;

#[tonic::async_trait]
pub trait SpannerInterceptor: Send + Sync + 'static {
    fn emulator_client(&self) -> SpannerClient<tonic::transport::Channel>;

    async fn create_session(
        &self,
        request: tonic::Request<spanner_v1::CreateSessionRequest>,
    ) -> std::result::Result<tonic::Response<spanner_v1::Session>, tonic::Status> {
        self.emulator_client().create_session(request).await
    }

    async fn batch_create_sessions(
        &self,
        request: tonic::Request<spanner_v1::BatchCreateSessionsRequest>,
    ) -> std::result::Result<tonic::Response<spanner_v1::BatchCreateSessionsResponse>, tonic::Status>
    {
        self.emulator_client().batch_create_sessions(request).await
    }

    async fn get_session(
        &self,
        request: tonic::Request<spanner_v1::GetSessionRequest>,
    ) -> std::result::Result<tonic::Response<spanner_v1::Session>, tonic::Status> {
        self.emulator_client().get_session(request).await
    }

    async fn list_sessions(
        &self,
        request: tonic::Request<spanner_v1::ListSessionsRequest>,
    ) -> std::result::Result<tonic::Response<spanner_v1::ListSessionsResponse>, tonic::Status> {
        self.emulator_client().list_sessions(request).await
    }

    async fn delete_session(
        &self,
        request: tonic::Request<spanner_v1::DeleteSessionRequest>,
    ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
        self.emulator_client().delete_session(request).await
    }

    async fn execute_sql(
        &self,
        request: tonic::Request<spanner_v1::ExecuteSqlRequest>,
    ) -> std::result::Result<tonic::Response<spanner_v1::ResultSet>, tonic::Status> {
        self.emulator_client().execute_sql(request).await
    }

    async fn execute_streaming_sql(
        &self,
        request: tonic::Request<spanner_v1::ExecuteSqlRequest>,
    ) -> std::result::Result<
        tonic::Response<tonic::codec::Streaming<spanner_v1::PartialResultSet>>,
        tonic::Status,
    > {
        self.emulator_client().execute_streaming_sql(request).await
    }

    async fn execute_batch_dml(
        &self,
        request: tonic::Request<spanner_v1::ExecuteBatchDmlRequest>,
    ) -> std::result::Result<tonic::Response<spanner_v1::ExecuteBatchDmlResponse>, tonic::Status>
    {
        self.emulator_client().execute_batch_dml(request).await
    }

    async fn read(
        &self,
        request: tonic::Request<spanner_v1::ReadRequest>,
    ) -> std::result::Result<tonic::Response<spanner_v1::ResultSet>, tonic::Status> {
        self.emulator_client().read(request).await
    }

    async fn streaming_read(
        &self,
        request: tonic::Request<spanner_v1::ReadRequest>,
    ) -> std::result::Result<
        tonic::Response<tonic::codec::Streaming<spanner_v1::PartialResultSet>>,
        tonic::Status,
    > {
        self.emulator_client().streaming_read(request).await
    }

    async fn begin_transaction(
        &self,
        request: tonic::Request<spanner_v1::BeginTransactionRequest>,
    ) -> std::result::Result<tonic::Response<spanner_v1::Transaction>, tonic::Status> {
        self.emulator_client().begin_transaction(request).await
    }

    async fn commit(
        &self,
        request: tonic::Request<spanner_v1::CommitRequest>,
    ) -> std::result::Result<tonic::Response<spanner_v1::CommitResponse>, tonic::Status> {
        self.emulator_client().commit(request).await
    }

    async fn rollback(
        &self,
        request: tonic::Request<spanner_v1::RollbackRequest>,
    ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
        self.emulator_client().rollback(request).await
    }

    async fn partition_query(
        &self,
        request: tonic::Request<spanner_v1::PartitionQueryRequest>,
    ) -> std::result::Result<tonic::Response<spanner_v1::PartitionResponse>, tonic::Status> {
        self.emulator_client().partition_query(request).await
    }

    async fn partition_read(
        &self,
        request: tonic::Request<spanner_v1::PartitionReadRequest>,
    ) -> std::result::Result<tonic::Response<spanner_v1::PartitionResponse>, tonic::Status> {
        self.emulator_client().partition_read(request).await
    }

    async fn batch_write(
        &self,
        request: tonic::Request<spanner_v1::BatchWriteRequest>,
    ) -> std::result::Result<
        tonic::Response<tonic::codec::Streaming<spanner_v1::BatchWriteResponse>>,
        tonic::Status,
    > {
        self.emulator_client().batch_write(request).await
    }
}

pub struct InterceptedSpanner<T>(pub T);

#[tonic::async_trait]
impl<T: SpannerInterceptor> spanner_v1::spanner_server::Spanner for InterceptedSpanner<T> {
    async fn create_session(
        &self,
        request: tonic::Request<spanner_v1::CreateSessionRequest>,
    ) -> std::result::Result<tonic::Response<spanner_v1::Session>, tonic::Status> {
        self.0.create_session(request).await
    }

    async fn batch_create_sessions(
        &self,
        request: tonic::Request<spanner_v1::BatchCreateSessionsRequest>,
    ) -> std::result::Result<tonic::Response<spanner_v1::BatchCreateSessionsResponse>, tonic::Status>
    {
        self.0.batch_create_sessions(request).await
    }

    async fn get_session(
        &self,
        request: tonic::Request<spanner_v1::GetSessionRequest>,
    ) -> std::result::Result<tonic::Response<spanner_v1::Session>, tonic::Status> {
        self.0.get_session(request).await
    }

    async fn list_sessions(
        &self,
        request: tonic::Request<spanner_v1::ListSessionsRequest>,
    ) -> std::result::Result<tonic::Response<spanner_v1::ListSessionsResponse>, tonic::Status> {
        self.0.list_sessions(request).await
    }

    async fn delete_session(
        &self,
        request: tonic::Request<spanner_v1::DeleteSessionRequest>,
    ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
        self.0.delete_session(request).await
    }

    async fn execute_sql(
        &self,
        request: tonic::Request<spanner_v1::ExecuteSqlRequest>,
    ) -> std::result::Result<tonic::Response<spanner_v1::ResultSet>, tonic::Status> {
        self.0.execute_sql(request).await
    }

    type ExecuteStreamingSqlStream = tonic::codec::Streaming<spanner_v1::PartialResultSet>;

    async fn execute_streaming_sql(
        &self,
        request: tonic::Request<spanner_v1::ExecuteSqlRequest>,
    ) -> std::result::Result<tonic::Response<Self::ExecuteStreamingSqlStream>, tonic::Status> {
        self.0.execute_streaming_sql(request).await
    }

    async fn execute_batch_dml(
        &self,
        request: tonic::Request<spanner_v1::ExecuteBatchDmlRequest>,
    ) -> std::result::Result<tonic::Response<spanner_v1::ExecuteBatchDmlResponse>, tonic::Status>
    {
        self.0.execute_batch_dml(request).await
    }

    async fn read(
        &self,
        request: tonic::Request<spanner_v1::ReadRequest>,
    ) -> std::result::Result<tonic::Response<spanner_v1::ResultSet>, tonic::Status> {
        self.0.read(request).await
    }

    type StreamingReadStream = tonic::codec::Streaming<spanner_v1::PartialResultSet>;

    async fn streaming_read(
        &self,
        request: tonic::Request<spanner_v1::ReadRequest>,
    ) -> std::result::Result<tonic::Response<Self::StreamingReadStream>, tonic::Status> {
        self.0.streaming_read(request).await
    }

    async fn begin_transaction(
        &self,
        request: tonic::Request<spanner_v1::BeginTransactionRequest>,
    ) -> std::result::Result<tonic::Response<spanner_v1::Transaction>, tonic::Status> {
        self.0.begin_transaction(request).await
    }

    async fn commit(
        &self,
        request: tonic::Request<spanner_v1::CommitRequest>,
    ) -> std::result::Result<tonic::Response<spanner_v1::CommitResponse>, tonic::Status> {
        self.0.commit(request).await
    }

    async fn rollback(
        &self,
        request: tonic::Request<spanner_v1::RollbackRequest>,
    ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
        self.0.rollback(request).await
    }

    async fn partition_query(
        &self,
        request: tonic::Request<spanner_v1::PartitionQueryRequest>,
    ) -> std::result::Result<tonic::Response<spanner_v1::PartitionResponse>, tonic::Status> {
        self.0.partition_query(request).await
    }

    async fn partition_read(
        &self,
        request: tonic::Request<spanner_v1::PartitionReadRequest>,
    ) -> std::result::Result<tonic::Response<spanner_v1::PartitionResponse>, tonic::Status> {
        self.0.partition_read(request).await
    }

    type BatchWriteStream = tonic::codec::Streaming<spanner_v1::BatchWriteResponse>;

    async fn batch_write(
        &self,
        request: tonic::Request<spanner_v1::BatchWriteRequest>,
    ) -> std::result::Result<tonic::Response<Self::BatchWriteStream>, tonic::Status> {
        self.0.batch_write(request).await
    }
}
