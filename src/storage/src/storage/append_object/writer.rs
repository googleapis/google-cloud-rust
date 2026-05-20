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

use crate::model::Object;
use crate::storage::request_options::RequestOptions;
use crate::Result;

/// A writer for appending data to an object.
///
/// This writer handles the underlying bidirectional streaming RPC to append data
/// to a GCS object.
///
/// TODO(#5716): This is a work in progress. Logic will be implemented soon.
#[cfg(google_cloud_unstable_storage_bidi)]
#[derive(Debug)]
pub struct AppendableObjectWriter {
    pub(crate) stub: std::sync::Arc<dyn AppendableStorage>,
    pub(crate) bucket: String,
    pub(crate) object: String,
    pub(crate) params: Option<crate::model::CommonObjectRequestParams>,
    pub(crate) if_metageneration_match: Option<i64>,
    pub(crate) if_metageneration_not_match: Option<i64>,
    pub(crate) options: RequestOptions,
}

#[cfg(google_cloud_unstable_storage_bidi)]
impl AppendableObjectWriter {
    /// Appends bytes to the object. Coalesces small byte chunks into a buffer.
    pub async fn append(&mut self, _bytes: impl Into<bytes::Bytes>) -> Result<()> {
        unimplemented!()
    }

    /// Drains buffer, send bytes to server and await server's response.
    pub async fn flush(&mut self) -> Result<()> {
        unimplemented!()
    }

    /// Drains buffer, sends bytes to server with `flush=true, state_lookup=true`
    /// and await server's response. Returns the final `Object`.
    pub async fn finalize(self) -> Result<Object> {
        unimplemented!()
    }

    /// Drains buffer, send bytes to server and await server's response. Then
    /// close the stream without finalizing.
    /// Returns the new `persisted_size`.
    /// WARNING: Because this consumes `self`, cancelling this future (e.g., via
    /// a timeout) drops the writer and loses unflushed bytes.
    pub async fn close(self) -> Result<i64> {
        unimplemented!()
    }

    // Accessors

    pub fn persisted_size(&self) -> i64 {
        unimplemented!()
    }

    pub fn generation(&self) -> i64 {
        unimplemented!()
    }

    pub fn object(&self) -> Option<Object> {
        unimplemented!()
    }
}
