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

use crate::Result;
use crate::model::Object;
use bytes::Bytes;

/// A handle to an in-progress appendable object upload.
#[derive(Debug)]
#[non_exhaustive]
pub struct AppendableObjectWriter {
    // TODO: The request channel, shared response state, and coalescing/replay
    // buffers will be added later.
}

impl AppendableObjectWriter {
    /// Appends bytes to the object.
    ///
    /// Small chunks are coalesced before being sent,
    /// so a call does not necessarily produce wire traffic immediately. Appending
    /// an empty buffer is a no-op.
    pub async fn append(&mut self, _bytes: impl Into<Bytes>) -> Result<()> {
        unimplemented!()
    }

    /// Flushes any pending data to the server and awaits durable persistence on
    /// the server.
    pub async fn flush(&mut self) -> Result<()> {
        unimplemented!()
    }

    /// Finalizes the upload, sending any pending data. Returns the final [Object][crate::model::Object].
    pub async fn finalize(self) -> Result<Object> {
        unimplemented!()
    }

    /// Relinquishes the writer without finalizing, draining any pending data to the server.
    /// Returns the final persisted size of the object.
    pub async fn close(self) -> Result<i64> {
        unimplemented!()
    }

    /// Returns the latest durable offset confirmed by the server.
    pub fn persisted_size(&self) -> i64 {
        unimplemented!()
    }

    /// Returns the generation of the object being appended to.
    pub fn generation(&self) -> i64 {
        unimplemented!()
    }

    /// Returns the latest known object metadata.
    pub fn object(&self) -> Option<Object> {
        unimplemented!()
    }
}
