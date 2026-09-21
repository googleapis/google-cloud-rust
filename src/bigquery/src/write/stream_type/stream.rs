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

    /// Sealed trait for all write stream types.
    pub trait Stream: Sized {}

    impl Stream for DefaultStream {}

    impl Stream for PendingStream {}

    impl Stream for CommittedStream {}

    impl Stream for BufferedStream {}
}
