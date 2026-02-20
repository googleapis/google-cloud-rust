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

pub(crate) mod actor;
pub(crate) mod backoff_policy;
pub(crate) mod base_publisher;
pub(crate) mod batch;
pub(crate) mod builder;
pub(crate) mod client_builder;
pub(crate) mod constants;
pub(crate) mod implementation;
pub(crate) mod model_ext;
pub(crate) mod options;
pub(crate) mod retry_policy;

/// Contains clients for publishing messages.
pub mod client {
    pub use super::base_publisher::BasePublisher;
}
