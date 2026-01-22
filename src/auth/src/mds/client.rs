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

use reqwest::Client as ReqwestClient;

/// A client for GCP Compute Engine Metadata Service (MDS).
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct Client {
    endpoint: String,
    inner: ReqwestClient,
    /// True if the endpoint was NOT overridden by env var or constructor arg.
    pub(crate) is_default_endpoint: bool,
}

impl Client {
    #[allow(dead_code)]
    /// Creates a new client for the Metadata Service.
    pub(crate) fn new(endpoint_override: Option<String>) -> Self {
        let (endpoint, is_default_endpoint) = Self::resolve_endpoint(endpoint_override);
        let endpoint = endpoint.trim_end_matches('/').to_string();

        Self {
            endpoint,
            inner: ReqwestClient::new(),
            is_default_endpoint,
        }
    }

    /// Determine the endpoint and whether it was overridden
    fn resolve_endpoint(endpoint_override: Option<String>) -> (String, bool) {
        if let Ok(host) = std::env::var(super::GCE_METADATA_HOST_ENV_VAR) {
            // Check GCE_METADATA_HOST environment variable first
            (format!("http://{host}"), false)
        } else if let Some(e) = endpoint_override {
            // Else, check if an endpoint was provided to the mds::Builder
            (e, false)
        } else {
            // Else, use the default metadata root
            (super::METADATA_ROOT.to_string(), true)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use scoped_env::ScopedEnv;
    use serial_test::{parallel, serial};

    #[test]
    #[parallel]
    fn test_resolve_endpoint_default() {
        let client = Client::new(None);
        assert_eq!(client.endpoint, "http://metadata.google.internal");
    }

    #[test]
    #[parallel]
    fn test_resolve_endpoint_override() {
        let client = Client::new(Some("http://custom.endpoint".to_string()));
        assert_eq!(client.endpoint, "http://custom.endpoint");
    }

    #[test]
    #[serial]
    fn test_resolve_endpoint_env_var() {
        let _s = ScopedEnv::set(super::super::GCE_METADATA_HOST_ENV_VAR, "env.var.host");
        let client = Client::new(None);
        assert_eq!(client.endpoint, "http://env.var.host");
    }

    #[test]
    #[serial]
    fn test_resolve_endpoint_priority() {
        let _s = ScopedEnv::set(super::super::GCE_METADATA_HOST_ENV_VAR, "env.priority.host");
        // Env var should take precedence over constructor argument
        let client = Client::new(Some("http://custom.endpoint".to_string()));
        assert_eq!(client.endpoint, "http://env.priority.host");
    }
}
