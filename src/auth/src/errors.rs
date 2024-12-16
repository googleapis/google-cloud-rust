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

use std::error::Error;
use std::fmt::{Display, Formatter, Result};

pub(crate) type BoxError = Box<dyn Error + Send + Sync>;

/// Represents an error creating or using a [Credential](crate::credentials::Credential).
///
/// This error type indicates issues encountered while trying to create or use a
/// `Credential`.
#[derive(Debug)]
pub struct CredentialError {
    /// A boolean value indicating whether the error is retryable.
    ///
    /// If `true`, the operation that resulted in this error might succeed upon
    /// retry. Applications and client libraries should use
    /// [Exponential backoff] and [retry budgets] in their retry loops.
    ///
    /// [Exponential backoff]: https://en.wikipedia.org/wiki/Exponential_backoff
    /// [retry budgets]: https://docs.rs/tower/latest/tower/retry/budget/index.html
    is_retryable: bool,

    /// The underlying source of the error.
    ///
    /// This provides more specific information about the cause of the failure.
    source: BoxError,
}

impl CredentialError {
    /// Creates a new `CredentialError`.
    ///
    /// # Arguments
    /// * `is_retryable` - A boolean indicating whether the error is retryable.
    /// * `source` - The underlying error that caused the auth failure.
    pub fn new(is_retryable: bool, source: BoxError) -> Self {
        CredentialError {
            is_retryable,
            source,
        }
    }

    /// Returns `true` if the error is retryable; otherwise returns `false`.
    pub fn is_retryable(&self) -> bool {
        self.is_retryable
    }
}

impl std::error::Error for CredentialError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&*self.source)
    }
}

impl Display for CredentialError {
    /// Formats the error message to include retryability and source.
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        let msg = if self.is_retryable {
            "but future attempts may succeed"
        } else {
            "and future attempts will not succeed"
        };
        write!(
            f,
            "cannot create access token, {}, source:{}",
            msg, self.source
        )
    }
}

/// InnerAuthError enum is designed to enumerate specific auth error types.
///
/// This allows distinguishing various causes of auth failures which can be used
/// for more fine-grained error handling.
#[derive(thiserror::Error, Debug)]
pub enum InnerAuthError {
    // TODO(#389) - define error types here
}
