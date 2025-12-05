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

/// A trait representing leaser actions
///
/// We stub out the interface, in order to test the lease management.
#[async_trait::async_trait]
pub(crate) trait Leaser {
    /// Acknowledge a batch of messages.
    async fn ack(&self, ack_ids: Vec<String>);
    /// Negatively acknowledge a batch of messages.
    async fn nack(&self, ack_ids: Vec<String>);
    /// Extend lease deadlines for a batch of messages.
    async fn extend(&self, ack_ids: Vec<String>);
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    mockall::mock! {
        #[derive(Debug)]
        pub(crate) Leaser {}
        #[async_trait::async_trait]
        impl Leaser for Leaser {
            async fn ack(&self, ack_ids: Vec<String>);
            async fn nack(&self, ack_ids: Vec<String>);
            async fn extend(&self, ack_ids: Vec<String>);
        }
    }
}
