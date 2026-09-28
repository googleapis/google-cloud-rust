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

/// Marker type representing a [default stream].
///
/// The default stream is designed for streaming scenarios where you have
/// continuously arriving data. It has the following characteristics:
///
/// - Data written to the default stream is available immediately for query.
/// - The default stream supports at-least-once semantics.
/// - You don't need to explicitly create the default stream.
///
/// [default stream]: https://docs.cloud.google.com/bigquery/docs/write-api-grpc#default_stream
#[derive(Clone, Copy, Debug)]
pub struct DefaultStream;

/// Marker type representing a [pending stream].
///
/// In a pending stream, records are buffered in a pending state until you
/// commit the stream. When you commit a stream, all of the pending data
/// becomes available for reading atomically. Use this type for batch
/// workloads, as an alternative to BigQuery load jobs.
///
/// [pending stream]: https://docs.cloud.google.com/bigquery/docs/write-api-grpc#pending_type
#[derive(Clone, Copy, Debug)]
pub struct PendingStream;

/// Marker type representing a [committed stream].
///
/// In a committed stream, records are available for reading immediately as you
/// write them to the stream. Use this type for streaming workloads that need
/// minimal read latency and exactly-once semantics through the use of stream
/// offsets.
///
/// [committed stream]: https://docs.cloud.google.com/bigquery/docs/write-api-grpc#committed_type
#[derive(Clone, Copy, Debug)]
pub struct CommittedStream;

/// Marker type representing a [buffered stream].
///
/// In a buffered stream, row-level commits are provided, and records are
/// buffered until the rows are committed by flushing the stream. This is an
/// advanced stream type; if you have small batches that you want to guarantee
/// appear together, consider using a [committed stream][CommittedStream] and
/// sending each batch in one request.
///
/// [buffered stream]: https://docs.cloud.google.com/bigquery/docs/write-api-grpc#buffered_type
#[derive(Clone, Copy, Debug)]
pub struct BufferedStream;
