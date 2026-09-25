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

//! Defines marker traits and types for the stream writers.
//!
//! The types and traits in this module exist to enable an idiomatic build
//! pattern for the `*Writer` types.
//!
//! Application code never needs to name these types, and can always use the
//! `*Writer` types directly.
//!
//! ```
//! use google_cloud_bigquery::write::format::Arrow;
//! use google_cloud_bigquery::write::PendingWriter;
//! # use google_cloud_bigquery::client::Write;
//! # use google_cloud_bigquery::model::ArrowSchema;
//! # async fn sample(client: Write, table: &str, schema: ArrowSchema) -> anyhow::Result<()> {
//! let w: PendingWriter<Arrow> = client
//!     .create_stream(table)
//!     .build_arrow(schema)
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! With that being said, application code can choose to use the stream markers
//! with turbofish notation, such as:
//!
//! ```
//! use google_cloud_bigquery::write::stream_type::PendingStream;
//! # use google_cloud_bigquery::client::Write;
//! # use google_cloud_bigquery::model::ArrowSchema;
//! # async fn sample(client: Write, table: &str, schema: ArrowSchema) -> anyhow::Result<()> {
//! let w = client
//!     .create_stream::<PendingStream, _>(table)
//!     .build_arrow(schema)
//!     .await?;
//! # Ok(())
//! # }
//! ```

mod application_created_stream;
mod has_stream;
mod markers;
mod stream;

pub use application_created_stream::ApplicationCreatedStream;
pub use has_stream::HasStream;
pub use markers::{BufferedStream, CommittedStream, DefaultStream, PendingStream};
pub use stream::Stream;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::write::format::{Arrow, Proto};
    use crate::write::{BufferedWriter, CommittedWriter, DefaultWriter, PendingWriter};
    use static_assertions::{assert_impl_all, assert_not_impl_any};

    macro_rules! assert_format_mappings {
        ($F:ty) => {
            assert_impl_all!(DefaultStream: Stream<Writer<$F> = DefaultWriter<$F>>);
            assert_impl_all!(PendingStream: Stream<Writer<$F> = PendingWriter<$F>>);
            assert_impl_all!(CommittedStream: Stream<Writer<$F> = CommittedWriter<$F>>);
            assert_impl_all!(BufferedStream: Stream<Writer<$F> = BufferedWriter<$F>>);

            assert_impl_all!(DefaultWriter<$F>: HasStream<Stream = DefaultStream>);
            assert_impl_all!(PendingWriter<$F>: HasStream<Stream = PendingStream>);
            assert_impl_all!(CommittedWriter<$F>: HasStream<Stream = CommittedStream>);
            assert_impl_all!(BufferedWriter<$F>: HasStream<Stream = BufferedStream>);
        };
    }

    #[test]
    fn stream_and_writer_mappings() {
        assert_format_mappings!(Arrow);
        assert_format_mappings!(Proto);

        assert_impl_all!(PendingStream: ApplicationCreatedStream);
        assert_impl_all!(CommittedStream: ApplicationCreatedStream);
        assert_impl_all!(BufferedStream: ApplicationCreatedStream);
        assert_not_impl_any!(DefaultStream: ApplicationCreatedStream);
    }
}
