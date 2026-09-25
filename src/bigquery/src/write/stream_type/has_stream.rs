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

use super::{BufferedStream, CommittedStream, DefaultStream, PendingStream, Stream};
use crate::write::{BufferedWriter, CommittedWriter, DefaultWriter, PendingWriter};

/// Trait mapping a writer type ([`DefaultWriter`], [`PendingWriter`], [`CommittedWriter`],
/// [`BufferedWriter`]) back to its corresponding [`Stream`].
///
/// This trait is sealed and cannot be implemented for types outside this crate.
#[diagnostic::on_unimplemented(
    message = "cannot infer the writer or stream type",
    label = "type annotations needed for this writer",
    note = "annotate the variable type (e.g. `let writer: PendingWriter<Arrow> = ...`) or specify a stream type via turbofish (e.g. `client.create_stream::<PendingStream, _>(...)`)"
)]
pub trait HasStream: sealed::HasStream {
    /// The stream type marker corresponding to this writer.
    type Stream: Stream;
}

impl<F> HasStream for DefaultWriter<F> {
    type Stream = DefaultStream;
}

impl<F> HasStream for PendingWriter<F> {
    type Stream = PendingStream;
}

impl<F> HasStream for CommittedWriter<F> {
    type Stream = CommittedStream;
}

impl<F> HasStream for BufferedWriter<F> {
    type Stream = BufferedStream;
}

pub(crate) mod sealed {
    use super::*;

    /// Sealed trait for mapping a writer back to its stream type.
    pub trait HasStream {}

    impl<F> HasStream for DefaultWriter<F> {}
    impl<F> HasStream for PendingWriter<F> {}
    impl<F> HasStream for CommittedWriter<F> {}
    impl<F> HasStream for BufferedWriter<F> {}
}
