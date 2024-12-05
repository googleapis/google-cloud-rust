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

use std::collections::HashMap;

use async_trait::async_trait;

pub type Result<T> = std::result::Result<T, crate::errors::AuthError>;

#[async_trait]
pub trait Credential: Send + Sync {
    async fn get_token(&mut self) -> Result<crate::token::Token>;
    async fn get_headers(&mut self) -> Result<HashMap<String, String>>;
    fn get_quota_project_id(&self) -> Result<String>;
    fn get_universe_domain(&self) -> Result<String>;
}
