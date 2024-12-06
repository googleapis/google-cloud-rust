// Copyright 2024 Google LLC
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

/// Represents an auth token.
#[derive(Debug)]
pub struct Token {
    /// The actual token string.  This is the value used in Authorization header.
    pub token: String,

    /// The type of the token.  Common types include "Bearer".
    pub token_type: String,

    /// The instant at which the token expires. If `None`, the token does not
    /// expire (or its expiration is unknown).
    pub expires_at: Option<time::OffsetDateTime>,

    /// Optional metadata associated with the token. This might include
    /// information like granted scopes or other claims.
    pub metadata: Option<std::collections::HashMap<String, String>>,
}
