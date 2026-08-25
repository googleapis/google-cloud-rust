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

use super::{BufferedWriter, CommittedWriter, PendingWriter};
use crate::model::ArrowSchema;
use crate::model::write_stream::Type;
use crate::write::transport::Transport;
use std::sync::Arc;

mod sealed {
    use super::*;
    pub trait Sealed {}
    impl Sealed for PendingWriter {}
    impl Sealed for CommittedWriter {}
    impl Sealed for BufferedWriter {}
}

/// A trait for strongly-typed stream writers that can be attached to an existing stream.
pub trait AttachableWriter: sealed::Sealed + Sized {
    #[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
    const STREAM_TYPE: Type;

    #[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
    fn build(inner: Arc<Transport>, write_stream: String, schema: ArrowSchema) -> Self;
}

impl AttachableWriter for PendingWriter {
    const STREAM_TYPE: Type = Type::Pending;

    fn build(inner: Arc<Transport>, write_stream: String, schema: ArrowSchema) -> Self {
        Self::new(inner, write_stream, schema)
    }
}

impl AttachableWriter for CommittedWriter {
    const STREAM_TYPE: Type = Type::Committed;

    fn build(inner: Arc<Transport>, write_stream: String, schema: ArrowSchema) -> Self {
        Self::new(inner, write_stream, schema)
    }
}

impl AttachableWriter for BufferedWriter {
    const STREAM_TYPE: Type = Type::Buffered;

    fn build(inner: Arc<Transport>, write_stream: String, schema: ArrowSchema) -> Self {
        Self::new(inner, write_stream, schema)
    }
}
