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

use super::{BufferedStream, CommittedStream, DefaultStream, PendingStream};
use crate::write::format::DataFormat;
use crate::write::writer_builder::WriterBuilder;
use crate::write::{BufferedWriter, CommittedWriter, DefaultWriter, PendingWriter};

/// Trait mapping a write stream type ([`DefaultStream`], [`PendingStream`], [`CommittedStream`],
/// [`BufferedStream`]) to its corresponding writer type.
///
/// This trait is sealed and cannot be implemented for types outside this crate.
pub trait Stream: sealed::Stream {
    /// The writer type constructed for this stream type and data format `F`.
    type Writer<F>;
}

impl Stream for DefaultStream {
    type Writer<F> = DefaultWriter<F>;
}

impl Stream for PendingStream {
    type Writer<F> = PendingWriter<F>;
}

impl Stream for CommittedStream {
    type Writer<F> = CommittedWriter<F>;
}

impl Stream for BufferedStream {
    type Writer<F> = BufferedWriter<F>;
}

pub(crate) mod sealed {
    use super::*;
    use crate::write::error::WriterBuilderError;

    /// Sealed trait for all write stream types.
    pub trait Stream: Sized {
        fn build<F>(
            builder: WriterBuilder<Self>,
            write_stream: String,
            format: F,
        ) -> impl Future<Output = Result<Self::Writer<F>, WriterBuilderError>> + Send
        where
            F: DataFormat,
            Self: super::Stream;
    }

    impl Stream for DefaultStream {
        async fn build<F>(
            builder: WriterBuilder<Self>,
            write_stream: String,
            format: F,
        ) -> Result<DefaultWriter<F>, WriterBuilderError>
        where
            F: DataFormat,
            Self: super::Stream,
        {
            builder.make_default_writer(write_stream, format).await
        }
    }

    impl Stream for PendingStream {
        async fn build<F>(
            builder: WriterBuilder<Self>,
            write_stream: String,
            format: F,
        ) -> Result<PendingWriter<F>, WriterBuilderError>
        where
            F: DataFormat,
            Self: super::Stream,
        {
            Ok(PendingWriter::new(builder.inner, write_stream, format))
        }
    }

    impl Stream for CommittedStream {
        async fn build<F>(
            builder: WriterBuilder<Self>,
            write_stream: String,
            format: F,
        ) -> Result<CommittedWriter<F>, WriterBuilderError>
        where
            F: DataFormat,
            Self: super::Stream,
        {
            Ok(CommittedWriter::new(builder.inner, write_stream, format))
        }
    }

    impl Stream for BufferedStream {
        async fn build<F>(
            builder: WriterBuilder<Self>,
            write_stream: String,
            format: F,
        ) -> Result<BufferedWriter<F>, WriterBuilderError>
        where
            F: DataFormat,
            Self: super::Stream,
        {
            Ok(BufferedWriter::new(builder.inner, write_stream, format))
        }
    }
}
