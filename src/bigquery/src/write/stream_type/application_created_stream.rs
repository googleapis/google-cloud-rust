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

use super::{BufferedStream, CommittedStream, PendingStream, Stream};
use crate::model::write_stream::Type;
use crate::write::format::DataFormat;
use crate::write::transport::Transport;
use crate::write::{BufferedWriter, CommittedWriter, PendingWriter};
use std::sync::Arc;

/// Marker trait for [application-created stream] types.
/// - [`PendingStream`]
/// - [`CommittedStream`]
/// - [`BufferedStream`]
///
/// These streams can be created or attached to.
///
/// This trait is sealed and cannot be implemented for types outside this crate.
///
/// [application-created stream]: https://docs.cloud.google.com/bigquery/docs/write-api-grpc#application-created_streams
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not an application-created stream type",
    label = "expected `PendingStream`, `CommittedStream`, or `BufferedStream`",
    note = "default streams are managed by BigQuery and cannot be created via `create_stream`; use `Write::open_default_stream` instead"
)]
pub trait ApplicationCreatedStream: Stream + sealed::ApplicationCreatedStream {}

impl ApplicationCreatedStream for PendingStream {}
impl ApplicationCreatedStream for CommittedStream {}
impl ApplicationCreatedStream for BufferedStream {}

pub(crate) mod sealed {
    use super::*;

    /// Sealed trait for application-created write stream types.
    pub trait ApplicationCreatedStream: Stream {
        const STREAM_TYPE: Type;
        fn build<F>(inner: Arc<Transport>, write_stream: String, format: F) -> Self::Writer<F>
        where
            F: DataFormat,
            Self: Stream;
    }

    impl ApplicationCreatedStream for PendingStream {
        const STREAM_TYPE: Type = Type::Pending;

        fn build<F: DataFormat>(
            inner: Arc<Transport>,
            write_stream: String,
            format: F,
        ) -> PendingWriter<F> {
            PendingWriter::<F>::new(inner, write_stream, format)
        }
    }
    impl ApplicationCreatedStream for CommittedStream {
        const STREAM_TYPE: Type = Type::Committed;

        fn build<F: DataFormat>(
            inner: Arc<Transport>,
            write_stream: String,
            format: F,
        ) -> CommittedWriter<F> {
            CommittedWriter::<F>::new(inner, write_stream, format)
        }
    }
    impl ApplicationCreatedStream for BufferedStream {
        const STREAM_TYPE: Type = Type::Buffered;

        fn build<F: DataFormat>(
            inner: Arc<Transport>,
            write_stream: String,
            format: F,
        ) -> BufferedWriter<F> {
            BufferedWriter::<F>::new(inner, write_stream, format)
        }
    }
}
