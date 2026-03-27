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

use crate::options::InstrumentationClientInfo;
use google_cloud_gax::options::RequestOptions;
use std::time::Duration;

/// Captures the "start of request" information needed to generate client request signals.
///
/// # Example
/// ```
/// # use google_cloud_gax_internal::observability::RequestStart;
/// use google_cloud_gax_internal::options::InstrumentationClientInfo;
/// use google_cloud_gax::options::RequestOptions;
///
/// async fn some_method(options: &RequestOptions) {
///     let start = RequestStart::new(get_info(), options, "some_method");
///     // use `start` and google_cloud_gax_internal::observability::ClientInstrumentationExt
///     // to add golden signals to futures such as:
///     let future = make_request();
/// }
///
/// fn make_request() -> google_cloud_gax::Result<String> {
/// # panic!();
/// }
/// fn get_info() -> &'static InstrumentationClientInfo {
/// // ... details omitted ...
/// # panic!();
/// }
/// ```
///
/// To generate the request duration and the error log signals we need to
/// capture some information when the request starts, such as the initial
/// timestamp and the core attributes of the request.
#[derive(Clone, Copy, Debug)]
pub struct RequestStart {
    start: tokio::time::Instant,
    info: InstrumentationClientInfo,
    url_template: &'static str,
    method: &'static str,
    disable_actionable_error_logging: bool,
}

impl RequestStart {
    /// Creates a new instance, capturing the relevant data.
    pub fn new(
        info: &InstrumentationClientInfo,
        options: &RequestOptions,
        method: &'static str,
    ) -> Self {
        use google_cloud_gax::options::internal::{PathTemplate, RequestOptionsExt};
        let start = tokio::time::Instant::now();
        let url_template = options
            .get_extension::<PathTemplate>()
            .map(|p| p.0)
            .unwrap_or_default();
        let disable_actionable_error_logging = options
            .get_extension::<super::SuppressActionableErrorLog>()
            .is_some();
        Self {
            start,
            info: *info,
            method,
            url_template,
            disable_actionable_error_logging,
        }
    }

    /// Returns the elapsed time since the call to [new()][RequestStart::new].
    pub(crate) fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }

    pub(crate) fn info(&self) -> &InstrumentationClientInfo {
        &self.info
    }

    pub(crate) fn url_template(&self) -> &'static str {
        self.url_template
    }

    pub(crate) fn method(&self) -> &'static str {
        self.method
    }

    pub(crate) fn disable_actionable_error_logging(&self) -> bool {
        self.disable_actionable_error_logging
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::*;
    use super::*;
    use google_cloud_gax::options::RequestOptions;
    use google_cloud_gax::options::internal::{PathTemplate, RequestOptionsExt};

    #[tokio::test(start_paused = true)]
    async fn elapsed() -> anyhow::Result<()> {
        let options = RequestOptions::default().insert_extension(PathTemplate(TEST_URL_TEMPLATE));
        let start = RequestStart::new(&TEST_INFO, &options, TEST_METHOD);
        // Use a long pause so it gets recorded as such.
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(start.elapsed(), Duration::from_millis(500));
        assert_eq!(start.info(), &TEST_INFO);
        assert_eq!(start.url_template(), TEST_URL_TEMPLATE);
        assert_eq!(start.method(), TEST_METHOD);
        Ok(())
    }
}
