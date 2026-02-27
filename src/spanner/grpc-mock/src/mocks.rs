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

use crate::google::spanner::v1::*;
use tonic::{Request, Response, Status};
use async_trait::async_trait;
use std::pin::Pin;
use tokio_stream::Stream;

mockall::mock! {
    pub Spanner {}

    #[async_trait]
    impl spanner_server::Spanner for Spanner {
        async fn create_session(&self, request: Request<CreateSessionRequest>) -> Result<Response<Session>, Status>;
        async fn batch_create_sessions(&self, request: Request<BatchCreateSessionsRequest>) -> Result<Response<BatchCreateSessionsResponse>, Status>;
        async fn get_session(&self, request: Request<GetSessionRequest>) -> Result<Response<Session>, Status>;
        async fn list_sessions(&self, request: Request<ListSessionsRequest>) -> Result<Response<ListSessionsResponse>, Status>;
        async fn delete_session(&self, request: Request<DeleteSessionRequest>) -> Result<Response<()>, Status>;
        async fn execute_sql(&self, request: Request<ExecuteSqlRequest>) -> Result<Response<ResultSet>, Status>;
        
        type ExecuteStreamingSqlStream = Pin<Box<dyn Stream<Item = Result<PartialResultSet, Status>> + Send + 'static>>;
        async fn execute_streaming_sql(&self, request: Request<ExecuteSqlRequest>) -> Result<Response<<Self as spanner_server::Spanner>::ExecuteStreamingSqlStream>, Status>;
        
        async fn execute_batch_dml(&self, request: Request<ExecuteBatchDmlRequest>) -> Result<Response<ExecuteBatchDmlResponse>, Status>;
        async fn read(&self, request: Request<ReadRequest>) -> Result<Response<ResultSet>, Status>;

        type StreamingReadStream = Pin<Box<dyn Stream<Item = Result<PartialResultSet, Status>> + Send + 'static>>;
        async fn streaming_read(&self, request: Request<ReadRequest>) -> Result<Response<<Self as spanner_server::Spanner>::StreamingReadStream>, Status>;

        async fn begin_transaction(&self, request: Request<BeginTransactionRequest>) -> Result<Response<Transaction>, Status>;
        async fn commit(&self, request: Request<CommitRequest>) -> Result<Response<CommitResponse>, Status>;
        async fn rollback(&self, request: Request<RollbackRequest>) -> Result<Response<()>, Status>;
        async fn partition_query(&self, request: Request<PartitionQueryRequest>) -> Result<Response<PartitionResponse>, Status>;
        async fn partition_read(&self, request: Request<PartitionReadRequest>) -> Result<Response<PartitionResponse>, Status>;

        type BatchWriteStream = Pin<Box<dyn Stream<Item = Result<BatchWriteResponse, Status>> + Send + 'static>>;
        async fn batch_write(&self, request: Request<BatchWriteRequest>) -> Result<Response<<Self as spanner_server::Spanner>::BatchWriteStream>, Status>;
    }
}
