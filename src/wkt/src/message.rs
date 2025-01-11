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

//! Define traits required of all messages.

/// A trait that must be implemented by all messages.
///
/// Messages sent to and received from Google Cloud services may be wrapped in
/// [Any][crate::any::Any]. `Any` uses a `@type` field to encoding the type
/// name and then validates extraction and insertion against this type.
pub trait Message {
    /// The typename of this message.
    fn typename() -> &'static str;
}
