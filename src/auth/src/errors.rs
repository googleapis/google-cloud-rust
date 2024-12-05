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

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug)]
pub struct AuthError {
    is_retryable: bool,
    source: BoxError,
}

impl AuthError {
    pub fn new(is_retryable: bool, source: BoxError) -> Self {
        AuthError {
            is_retryable,
            source,
        }
    }

    pub fn is_retryable(&self) -> bool {
        self.is_retryable
    }
}

impl std::error::Error for AuthError {}

impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Retryable:{}, Source:{}", self.is_retryable, self.source)
    }
}

#[derive(Debug)]
pub struct InnerAuthError {
    message: String,
    kind: InnerAuthErrorKind,
}

impl std::error::Error for InnerAuthError {}

impl std::fmt::Display for InnerAuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Message: {}, Kind: {:?}", self.message, self.kind)
    }
}

impl InnerAuthError {
    pub fn new(message: String, kind: InnerAuthErrorKind) -> Self {
        InnerAuthError { message, kind }
    }
}

#[derive(Debug)]
pub enum InnerAuthErrorKind {
    DefaultCredentialsError, // Errors during ADC
    InvalidOptionsError,     // Errors interpreting options
}
