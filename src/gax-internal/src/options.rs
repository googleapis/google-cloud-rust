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

pub use auth::credentials::Credentials;

// The client configuration for [crate::http::ReqwestClient] and [crate::grpc::Client].
pub type ClientConfig = gax::client_builder::internal::ClientConfig<Credentials>;

pub(crate) const LOGGING_VAR: &str = "GOOGLE_CLOUD_RUST_LOGGING";

/// Information about the client library used for instrumentation.
#[derive(Copy, Clone, Debug)]
pub struct InstrumentationClientInfo {
    /// The short service name, e.g., "appengine", "run", "firestore".
    pub service_name: &'static str,
    /// The version of the client library.
    pub client_version: &'static str,
    /// The name of the client library artifact (e.g., crate name).
    pub client_artifact: &'static str,
    /// The default hostname of the service.
    pub default_host: &'static str,
}

// Returns true if the environment or client configuration enables tracing.
pub fn tracing_enabled(config: &ClientConfig) -> bool {
    if config.tracing {
        return true;
    }
    std::env::var(LOGGING_VAR)
        .map(|v| v == "true")
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use scoped_env::ScopedEnv;

    // This test must run serially because it manipulates the environment.
    #[test]
    #[serial_test::serial]
    fn config_tracing() {
        let _e = ScopedEnv::remove(LOGGING_VAR);
        let config = ClientConfig::default();
        assert!(!tracing_enabled(&config), "expected tracing to be disabled");
        let mut config = ClientConfig::default();
        config.tracing = true;
        let config = config;
        assert!(tracing_enabled(&config), "expected tracing to be enabled");

        let _e = ScopedEnv::set(LOGGING_VAR, "true");
        let config = ClientConfig::default();
        assert!(tracing_enabled(&config), "expected tracing to be enabled");

        let _e = ScopedEnv::set(LOGGING_VAR, "not-true");
        let config = ClientConfig::default();
        assert!(!tracing_enabled(&config), "expected tracing to be disabled");
    }
}
