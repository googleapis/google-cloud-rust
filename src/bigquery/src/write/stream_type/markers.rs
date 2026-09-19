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
/// [default stream]: https://docs.cloud.google.com/bigquery/docs/write-api-grpc#default_stream
#[derive(Clone, Copy, Debug)]
pub struct DefaultStream;

/// Marker type representing a [pending stream].
///
/// [pending stream]: https://docs.cloud.google.com/bigquery/docs/write-api-grpc#pending_type
#[derive(Clone, Copy, Debug)]
pub struct PendingStream;

/// Marker type representing a [committed stream].
///
/// [committed stream]: https://docs.cloud.google.com/bigquery/docs/write-api-grpc#committed_type
#[derive(Clone, Copy, Debug)]
pub struct CommittedStream;

/// Marker type representing a [buffered stream].
///
/// [buffered stream]: https://docs.cloud.google.com/bigquery/docs/write-api-grpc#buffered_type
#[derive(Clone, Copy, Debug)]
pub struct BufferedStream;
