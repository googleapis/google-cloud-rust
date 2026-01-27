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

/// Enables tracing for the application.
pub fn enable_tracing() -> ::tracing::subscriber::DefaultGuard {
    use tracing_subscriber::fmt::format::FmtSpan;
    #[cfg(feature = "log-integration-tests")]
    let max_level = tracing::Level::INFO;
    #[cfg(not(feature = "log-integration-tests"))]
    let max_level = tracing::Level::WARN;
    let builder = tracing_subscriber::fmt()
        .with_level(true)
        .with_thread_ids(true)
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .with_max_level(max_level);
    let builder = builder.with_max_level(max_level);
    let subscriber = builder.finish();

    tracing::subscriber::set_default(subscriber)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_default() {
        let _guard = enable_tracing();
        let default = tracing::Dispatch::default();
        assert!(
            default.is::<tracing_subscriber::FmtSubscriber>(),
            "{default:?}"
        );
    }
}
