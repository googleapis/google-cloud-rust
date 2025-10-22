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

//! Anonymous credentials.
//!
//! These credentials do not provide any authentication information. They are
//! useful for accessing public resources that do not require authentication.

use crate::credentials::dynamic::CredentialsProvider;
use crate::credentials::{CacheableResource, Credentials, EntityTag, Result};
use http::{Extensions, HeaderMap};
use std::sync::Arc;

#[derive(Debug)]
struct AnonymousCredentials {
    entity_tag: EntityTag,
}

/// A builder for creating anonymous credentials.
#[derive(Debug, Default)]
pub struct Builder {}

impl Builder {
    /// Creates a new builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns a [Credentials] instance.
    pub fn build(self) -> Credentials {
        Credentials {
            inner: Arc::new(AnonymousCredentials {
                entity_tag: EntityTag::new(),
            }),
        }
    }
}

#[async_trait::async_trait]
impl CredentialsProvider for AnonymousCredentials {
    async fn headers(&self, extensions: Extensions) -> Result<CacheableResource<HeaderMap>> {
        match extensions.get::<EntityTag>() {
            Some(tag) if self.entity_tag.eq(tag) => Ok(CacheableResource::NotModified),
            _ => Ok(CacheableResource::New {
                data: HeaderMap::new(),
                entity_tag: self.entity_tag.clone(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

    #[tokio::test]
    async fn create_anonymous_credentials() -> TestResult {
        let creds = Builder::new().build();
        let mut extensions = Extensions::new();
        let cached_headers = creds.headers(extensions.clone()).await.unwrap();
        let (headers, entity_tag) = match cached_headers {
            CacheableResource::New { entity_tag, data } => (data, entity_tag),
            CacheableResource::NotModified => unreachable!("expecting new headers"),
        };
        assert!(headers.is_empty());

        extensions.insert(entity_tag);
        let cached_headers = creds.headers(extensions).await.unwrap();
        match cached_headers {
            CacheableResource::New { .. } => unreachable!("expecting cached headers"),
            CacheableResource::NotModified => {}
        }
        Ok(())
    }
}
