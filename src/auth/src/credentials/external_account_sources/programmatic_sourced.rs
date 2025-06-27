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

use std::sync::Arc;

use crate::credentials::subject_token::{self, SubjectToken};
use crate::errors::CredentialsError;

#[derive(Debug, Clone)]
pub(crate) struct ProgrammaticSourcedCredentials {
    subject_token_provider: Arc<dyn subject_token::dynamic::SubjectTokenProvider>,
}

impl ProgrammaticSourcedCredentials {
    pub(crate) fn new(
        subject_token_provider: Arc<dyn subject_token::dynamic::SubjectTokenProvider>,
    ) -> Self {
        Self {
            subject_token_provider,
        }
    }
}

#[async_trait::async_trait]
impl subject_token::dynamic::SubjectTokenProvider for ProgrammaticSourcedCredentials {
    async fn subject_token(&self) -> Result<SubjectToken, CredentialsError> {
        return self.subject_token_provider.subject_token().await;
    }
}
