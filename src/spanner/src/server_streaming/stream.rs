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

#![allow(dead_code)]

use crate::google::spanner::v1::BatchWriteResponse;
use crate::google::spanner::v1::PartialResultSet;
use gaxi::grpc::from_status::to_gax_error;
use gaxi::grpc::tonic::Streaming;

/// Representation for the `ExecuteStreamingSql` RPC stream.
#[derive(Debug)]
pub(crate) struct PartialResultSetStream {
    pub(crate) inner: Streaming<crate::google::spanner::v1::PartialResultSet>,
}

impl PartialResultSetStream {
    pub(crate) fn new(inner: Streaming<crate::google::spanner::v1::PartialResultSet>) -> Self {
        Self { inner }
    }

    /// Fetches the next `PartialResultSet` from the stream.
    ///
    /// Returns `Some(Ok(PartialResultSet))` when a message is successfully received,
    /// `None` when the stream concludes naturally, or `Some(Err(_))` on RPC errors.
    pub(crate) async fn next_message(&mut self) -> Option<crate::Result<PartialResultSet>> {
        self.inner.message().await.map_err(to_gax_error).transpose()
    }
}

/// Representation for the `BatchWrite` RPC stream.
#[derive(Debug)]
pub(crate) struct BatchWriteStream {
    pub(crate) inner: Streaming<crate::google::spanner::v1::BatchWriteResponse>,
}

impl BatchWriteStream {
    pub(crate) fn new(inner: Streaming<crate::google::spanner::v1::BatchWriteResponse>) -> Self {
        Self { inner }
    }

    /// Fetches the next `BatchWriteResponse` from the stream.
    ///
    /// Returns `Some(Ok(BatchWriteResponse))` when a message is successfully received,
    /// `None` when the stream concludes naturally, or `Some(Err(_))` on RPC errors.
    pub(crate) async fn next_message(&mut self) -> Option<crate::Result<BatchWriteResponse>> {
        self.inner.message().await.map_err(to_gax_error).transpose()
    }
}
