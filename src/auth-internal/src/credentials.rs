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

use async_trait::async_trait;
use http::{Extensions, HeaderMap};
use std::fmt;

/// Represents an Entity Tag for a [CacheableResource].
#[derive(Clone, Debug, PartialEq, Default)]
pub struct EntityTag(pub u64);

/// Represents a resource that can be cached, along with its [EntityTag].
#[derive(Clone, PartialEq, Debug)]
pub enum CacheableResource<T> {
    NotModified,
    New { entity_tag: EntityTag, data: T },
}

/// A minimal error type for credentials provider.
#[derive(Debug)]
pub struct InternalCredentialsError {
    pub is_transient: bool,
    pub message: String,
    pub source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl fmt::Display for InternalCredentialsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)?;
        if let Some(source) = &self.source {
            write!(f, ": {}", source)?;
        }
        Ok(())
    }
}

impl std::error::Error for InternalCredentialsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|e| e.as_ref() as &(dyn std::error::Error + 'static))
    }
}

impl InternalCredentialsError {
    pub fn new<M: Into<String>>(is_transient: bool, message: M) -> Self {
        Self {
            is_transient,
            message: message.into(),
            source: None,
        }
    }

    pub fn with_source<M: Into<String>, E: Into<Box<dyn std::error::Error + Send + Sync>>>(
        is_transient: bool,
        message: M,
        source: E,
    ) -> Self {
        Self {
            is_transient,
            message: message.into(),
            source: Some(source.into()),
        }
    }
}

/// A minimal trait for `gax-internal` to fetch authentication headers.
#[async_trait]
pub trait InternalCredentials: Send + Sync + fmt::Debug {
    async fn headers(
        &self,
        extensions: Extensions,
    ) -> Result<CacheableResource<HeaderMap>, InternalCredentialsError>;

    async fn universe_domain(&self) -> Option<String>;
}
