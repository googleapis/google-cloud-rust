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

use crate::model::ObjectChecksums;
use crate::storage::ChecksumMismatch;

/// Computes a checksum or hash for [Cloud Storage] transfers.
///
/// We want to minimize code complexity in our implementation of data integrity
/// checks for uploads and downloads. This trait defines a composable interface
/// to support:
/// - No checksums (`Null`): the client library does not compute any checksums,
///   and therefore does not validate checksums either.
/// - Precomputed checksums (`Precomputed`): the client library assumes the
///   application provided checksums in the object metadata.
/// - Only crc32c (`Crc32c` or `Crc32c<Null>`)`: compute (and validate) only
///   crc32c checksums.
/// - Only MD5 (`Md5` or `Md5<Null>`): compute (and validate) only MD5 hashes.
/// - Both: (`Crc32c<Md5>` or `Md5<Crc32>`): compute (and validate) both crc32
///   checksums and MD5 hashes.
///
/// The application should have no need to interact with these types directly,
/// or even name them. They are used only as implementation details. They may
/// be visible in debug messages.
pub trait ChecksumEngine: std::fmt::Debug + sealed::ChecksumEngine {
    fn update(&mut self, offset: u64, data: &bytes::Bytes);
    fn finalize(&self) -> ObjectChecksums;
}

mod sealed {
    pub trait ChecksumEngine {}
}

pub(crate) mod details;
