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

/// Handlers for acknowledging or rejecting messages.
pub mod handler;

/// Defines the return interface for
/// [Subscriber::streaming_pull][crate::client::Subscriber::streaming_pull].
pub mod session;

pub(super) mod builder;
pub(super) mod client;
pub(super) mod client_builder;
mod keepalive;
mod lease_loop;
mod lease_state;
mod leaser;
#[allow(dead_code)] // TODO(#4097) - use the retry policy
mod retry_policy;
mod stream;
mod stub;
mod transport;
