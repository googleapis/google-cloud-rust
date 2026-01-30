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

pub mod global;
pub mod locational;

#[cfg(test)]
mod tests {
    use secretmanager_openapi_v1::model::{Secret, secret_manager_service::CreateSecretRequest};
    use static_assertions::{assert_impl_all, assert_not_impl_any};

    // The generator introduces synthetic messages for the requests in
    // OpenAPI-based services. Those should not have serialization or
    // deserialization functions.
    #[test]
    fn synthetic_message_serialization() {
        assert_impl_all!(CreateSecretRequest: std::fmt::Debug);
        assert_not_impl_any!(CreateSecretRequest: serde::Serialize);
        assert_not_impl_any!(CreateSecretRequest: serde::de::DeserializeOwned);
        assert_impl_all!(Secret: std::fmt::Debug);
        assert_impl_all!(Secret: serde::Serialize);
        assert_impl_all!(Secret: serde::de::DeserializeOwned);
    }
}
