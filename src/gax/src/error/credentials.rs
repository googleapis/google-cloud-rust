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

use std::error::Error;
use std::fmt::{Debug, Display, Formatter, Result};
use std::sync::Arc;

type ArcError = Arc<dyn Error + Send + Sync>;

/// Represents an error using [Credentials].
///
/// The Google Cloud client libraries may experience problems using credentials
/// to create the necessary authentication headers. For example, a temporary
/// failure to retrieve or create [access tokens]. Note that these failures may
/// happen even after the credentials files are successfully loaded and parsed.
///
/// Applications rarely need to create instances of this error type. The
/// exception might be when testing application code, where the application is
/// mocking a client library behavior. Such tests are extremely rare, most
/// applications should only work with the [Error][crate::error::Error] type.
///
/// # Example
/// ```
/// use google_cloud_gax::error::CredentialsError;
/// let mut headers = fetch_headers();
/// while let Err(e) = &headers {
///     if e.is_transient() {
///         headers = fetch_headers();
///     }
/// }
///
/// fn fetch_headers() -> Result<http::HeaderMap, CredentialsError> {
///   # Ok(http::HeaderMap::new())
/// }
/// ```
///
/// [access tokens]: https://cloud.google.com/docs/authentication/token-types
/// [Credentials]: https://docs.rs/google-cloud-auth/latest/google_cloud_auth/credentials/struct.Credential.html
#[derive(Clone, Debug)]
pub struct CredentialsError {
    is_transient: bool,
    message: Option<String>,
    source: Option<ArcError>,
}

impl CredentialsError {
    /// Creates a new `CredentialsError`.
    ///
    /// This function is only intended for use in the client libraries
    /// implementation. Application may use this in mocks, though we do not
    /// recommend that you write tests for specific error cases. Most tests
    /// should use the generic [Error][crate::error::Error] type.
    ///
    /// # Example
    /// ```
    /// use google_cloud_gax::error::CredentialsError;
    /// let mut headers = fetch_headers();
    /// while let Err(e) = &headers {
    ///     if e.is_transient() {
    ///         headers = fetch_headers();
    ///     }
    /// }
    ///
    /// fn fetch_headers() -> Result<http::HeaderMap, CredentialsError> {
    ///   # Ok(http::HeaderMap::new())
    /// }
    /// ```
    ///
    /// # Parameters
    /// * `is_transient` - if true, the operation may succeed in future attempts.
    /// * `source` - The underlying error that caused the auth failure.
    #[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
    pub fn from_source<T: Error + Send + Sync + 'static>(is_transient: bool, source: T) -> Self {
        CredentialsError {
            is_transient,
            source: Some(Arc::new(source)),
            message: None,
        }
    }

    /// Creates a new `CredentialsError`.
    ///
    /// This function is only intended for use in the client libraries
    /// implementation. Application may use this in mocks, though we do not
    /// recommend that you write tests for specific error cases. Most tests
    /// should use the generic [Error][crate::error::Error] type.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_gax::error::CredentialsError;
    /// let err = CredentialsError::from_msg(
    ///     true, "simulated retryable error while trying to create credentials");
    /// assert!(err.is_transient());
    /// assert!(format!("{err}").contains("simulated retryable error"));
    /// ```
    ///
    /// # Parameters
    /// * `is_transient` - if true, the operation may succeed in future attempts.
    /// * `message` - The underlying error that caused the auth failure.
    #[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
    pub fn from_msg<T: Into<String>>(is_transient: bool, message: T) -> Self {
        CredentialsError {
            is_transient,
            message: Some(message.into()),
            source: None,
        }
    }

    /// Creates a new `CredentialsError`.
    ///
    /// This function is only intended for use in the client libraries
    /// implementation. Application may use this in mocks, though we do not
    /// recommend that you write tests for specific error cases. Most tests
    /// should use the generic [Error][crate::error::Error] type.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_gax::error::CredentialsError;
    /// let source = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "cannot connect");
    /// let err = CredentialsError::new(
    ///     true,
    ///     "simulated retryable error while trying to create credentials",
    ///     source);
    /// assert!(err.is_transient());
    /// assert!(format!("{err}").contains("simulated retryable error"));
    /// ```
    ///
    /// # Parameters
    /// * `is_transient` - if true, the operation may succeed in future attempts.
    /// * `message` - The underlying error that caused the auth failure.
    #[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
    pub fn new<M, S>(is_transient: bool, message: M, source: S) -> Self
    where
        M: Into<String>,
        S: std::error::Error + Send + Sync + 'static,
    {
        CredentialsError {
            is_transient,
            message: Some(message.into()),
            source: Some(Arc::new(source)),
        }
    }

    /// Returns true if the error is transient and may succeed in future attempts.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_gax::error::CredentialsError;
    /// let mut headers = fetch_headers();
    /// while let Err(e) = &headers {
    ///     if e.is_transient() {
    ///         headers = fetch_headers();
    ///     }
    /// }
    ///
    /// fn fetch_headers() -> Result<http::HeaderMap, CredentialsError> {
    ///   # Ok(http::HeaderMap::new())
    /// }
    /// ```
    pub fn is_transient(&self) -> bool {
        self.is_transient
    }
}

impl std::error::Error for CredentialsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|arc| arc.as_ref() as &(dyn std::error::Error + 'static))
    }
}

const TRANSIENT_MSG: &str = "but future attempts may succeed";
const PERMANENT_MSG: &str = "and future attempts will not succeed";

impl Display for CredentialsError {
    /// Formats the error message to include retryability and source.
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        let msg = if self.is_transient {
            TRANSIENT_MSG
        } else {
            PERMANENT_MSG
        };
        match &self.message {
            None => write!(f, "cannot create auth headers {msg}"),
            Some(m) => write!(f, "{m} {msg}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;

    #[test_case(true)]
    #[test_case(false)]
    fn from_source(transient: bool) {
        let source = wkt::TimestampError::OutOfRange;
        let got = CredentialsError::from_source(transient, source);
        assert_eq!(got.is_transient(), transient, "{got:?}");
        assert!(
            got.source()
                .and_then(|e| e.downcast_ref::<wkt::TimestampError>())
                .is_some(),
            "{got:?}"
        );
        assert!(
            got.to_string().contains("cannot create auth headers"),
            "{got:?}"
        );
    }

    #[test_case(true)]
    #[test_case(false)]
    fn from_str(transient: bool) {
        let got = CredentialsError::from_msg(transient, "test-only");
        assert_eq!(got.is_transient(), transient, "{got:?}");
        assert!(got.source().is_none(), "{got:?}");
        assert!(got.to_string().contains("test-only"), "{got}");
    }

    #[test_case(true)]
    #[test_case(false)]
    fn new(transient: bool) {
        let source = wkt::TimestampError::OutOfRange;
        let got = CredentialsError::new(transient, "additional information", source);
        assert_eq!(got.is_transient(), transient, "{got:?}");
        assert!(
            got.source()
                .and_then(|e| e.downcast_ref::<wkt::TimestampError>())
                .is_some(),
            "{got:?}"
        );
        assert!(
            got.to_string().contains("additional information"),
            "{got:?}"
        );
    }

    #[test]
    fn fmt() {
        let e = CredentialsError::from_msg(true, "test-only-err-123");
        let got = format!("{e}");
        assert!(got.contains("test-only-err-123"), "{got}");
        assert!(got.contains(TRANSIENT_MSG), "{got}");

        let e = CredentialsError::from_msg(false, "test-only-err-123");
        let got = format!("{e}");
        assert!(got.contains("test-only-err-123"), "{got}");
        assert!(got.contains(PERMANENT_MSG), "{got}");
    }
}
