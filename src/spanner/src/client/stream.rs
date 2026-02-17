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

use crate::google::spanner::v1::PartialResultSet;
use crate::google::spanner::v1::BatchWriteResponse;
use gaxi::grpc::tonic::Result as TonicResult;
use gaxi::grpc::tonic::Streaming;

/// Representation for the `ExecuteStreamingSql` RPC stream.
#[derive(Debug)]
pub struct ServerStream {
    pub(crate) inner: Streaming<crate::google::spanner::v1::PartialResultSet>,
}

impl ServerStream {
    pub(crate) fn new(inner: Streaming<crate::google::spanner::v1::PartialResultSet>) -> Self {
        Self { inner }
    }

    /// Fetches the next `PartialResultSet` from the stream.
    ///
    /// Returns `Ok(Some(PartialResultSet))` when a message is successfully received,
    /// `Ok(None)` when the stream concludes naturally, or `Err(_)` on RPC errors.
    pub async fn next_message(&mut self) -> TonicResult<Option<PartialResultSet>> {
        self.inner.message().await
    }
}

/// Representation for the `BatchWrite` RPC stream.
#[derive(Debug)]
pub struct BatchWriteStream {
    pub(crate) inner: Streaming<crate::google::spanner::v1::BatchWriteResponse>,
}

impl BatchWriteStream {
    pub(crate) fn new(inner: Streaming<crate::google::spanner::v1::BatchWriteResponse>) -> Self {
        Self { inner }
    }

    /// Fetches the next `BatchWriteResponse` from the stream.
    ///
    /// Returns `Ok(Some(BatchWriteResponse))` when a message is successfully received,
    /// `Ok(None)` when the stream concludes naturally, or `Err(_)` on RPC errors.
    pub async fn next_message(&mut self) -> TonicResult<Option<BatchWriteResponse>> {
        self.inner.message().await
    }
}
