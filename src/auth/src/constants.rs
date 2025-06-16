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

pub(crate) const DEFAULT_SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";
pub(crate) const GOOGLE_CLOUD_QUOTA_PROJECT_VAR: &str = "GOOGLE_CLOUD_QUOTA_PROJECT";
/// Token Exchange OAuth Grant Type
pub(crate) const TOKEN_EXCHANGE_GRANT_TYPE: &str =
    "urn:ietf:params:oauth:grant-type:token-exchange";
/// Access Token Oauth Token Type
pub(crate) const ACCESS_TOKEN_TYPE: &str = "urn:ietf:params:oauth:token-type:access_token";
/// JWT OAuth Token Type
pub(crate) const JWT_TOKEN_TYPE: &str = "urn:ietf:params:oauth:token-type:jwt";
/// SAML2 Token OAuth Token Type
pub(crate) const SAML2_TOKEN_TYPE: &str = "urn:ietf:params:oauth:token-type:saml2";
