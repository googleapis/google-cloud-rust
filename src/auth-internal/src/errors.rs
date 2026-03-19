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

use std::error::Error;
use std::fmt::{Display, Formatter, Result};

/// A minimal error type for credentials provider.
#[derive(Debug)]
pub struct InternalCredentialsError {
    is_transient: bool,
    message: Option<String>,
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl Display for InternalCredentialsError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        if let Some(message) = &self.message {
            write!(f, "{}", message)?;
        }
        if let Some(source) = &self.source {
            write!(f, "{}", source)?;
        }
        Ok(())
    }
}

impl Error for InternalCredentialsError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_ref()
            .map(|e| e.as_ref() as &(dyn Error + 'static))
    }
}

impl InternalCredentialsError {
    #[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
    pub fn from_msg<T: Into<String>>(is_transient: bool, message: T) -> Self {
        Self {
            is_transient,
            message: Some(message.into()),
            source: None,
        }
    }

    #[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
    pub fn from_source<T: Error + Send + Sync + 'static>(is_transient: bool, source: T) -> Self {
        Self {
            is_transient,
            source: Some(Box::new(source)),
            message: None,
        }
    }

    pub fn is_transient(&self) -> bool {
        self.is_transient
    }
}
