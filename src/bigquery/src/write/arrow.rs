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

use super::format::Arrow;

/// DEPRECATED - do not use.
///
/// This type is about to be deleted. See:
/// <https://github.com/googleapis/google-cloud-rust/issues/6855>
pub type DefaultWriter = super::DefaultWriter<Arrow>;

/// DEPRECATED - do not use.
///
/// This type is about to be deleted. See:
/// <https://github.com/googleapis/google-cloud-rust/issues/6855>
pub type BufferedWriter = super::BufferedWriter<Arrow>;

/// DEPRECATED - do not use.
///
/// This type is about to be deleted. See:
/// <https://github.com/googleapis/google-cloud-rust/issues/6855>
pub type CommittedWriter = super::CommittedWriter<Arrow>;

/// DEPRECATED - do not use.
///
/// This type is about to be deleted. See:
/// <https://github.com/googleapis/google-cloud-rust/issues/6855>
pub type PendingWriter = super::PendingWriter<Arrow>;

/// DEPRECATED - do not use.
///
/// This type is about to be deleted. See:
/// <https://github.com/googleapis/google-cloud-rust/issues/6855>
pub type WriterBuilder = super::WriterBuilder<Arrow>;
