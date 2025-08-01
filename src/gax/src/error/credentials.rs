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
use std::fmt::{Debug, Display, Formatter, Result as FmtResult};
use std::sync::Arc;
use std::time::Duration;

type ArcError = Arc<dyn Error + Send + Sync>;

#[derive(Clone, Debug)]
enum Retryability {
    Permanent,
    Transient { retry_in: Option<Duration> },
}

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
    retryability: Retryability,
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
        let retryability = if is_transient {
            Retryability::Transient { retry_in: None }
        } else {
            Retryability::Permanent
        };
        CredentialsError {
            retryability,
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
        let retryability = if is_transient {
            Retryability::Transient { retry_in: None }
        } else {
            Retryability::Permanent
        };
        CredentialsError {
            retryability,
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
        let retryability = if is_transient {
            Retryability::Transient { retry_in: None }
        } else {
            Retryability::Permanent
        };
        CredentialsError {
            retryability,
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
        matches!(self.retryability, Retryability::Transient { .. })
    }

    /// Sets the duration to wait before a retry attempt may succeed.
    ///
    /// This is only meaningful for transient errors. If the error is not
    /// transient, this function has no effect.
    pub fn with_retry_in(mut self, duration: Duration) -> Self {
        if let Retryability::Transient { retry_in: r } = &mut self.retryability {
            *r = Some(duration);
        }
        self
    }

    /// Returns the duration to wait before a retry attempt may succeed.
    ///
    /// Returns a `PermanentError` if the error is not transient.
    pub fn retry_in(&self) -> Result<Duration, PermanentError> {
        match self.retryability {
            Retryability::Transient { retry_in } => {
                retry_in.ok_or(PermanentError::new("retry duration not set"))
            }
            Retryability::Permanent => Err(PermanentError::new("error is permanent")),
        }
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
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let msg = if self.is_transient() {
            TRANSIENT_MSG
        } else {
            PERMANENT_MSG
        };
        match &self.message {
            None => write!(f, "cannot create auth headers {msg}"),
            Some(m) => write!(f, "{m} {msg}"),
        }?;
        if let Retryability::Transient { retry_in } = self.retryability {
            if let Some(duration) = retry_in {
                write!(f, ", retry in {:?}", duration)?;
            }
        }
        Ok(())
    }
}

/// An error returned when `retry_in` is called on a permanent `CredentialsError`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PermanentError {
    message: String,
}

impl PermanentError {
    fn new(message: &str) -> Self {
        Self {
            message: message.to_string(),
        }
    }
}

impl Display for PermanentError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}", self.message)
    }
}

impl Error for PermanentError {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
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

    #[test]
    fn with_retry_in() {
        let duration = Duration::from_secs(10);
        let err = CredentialsError::from_msg(true, "transient").with_retry_in(duration);
        assert_eq!(err.retry_in().unwrap(), duration);

        // should not set for non-transient
        let err = CredentialsError::from_msg(false, "permanent").with_retry_in(duration);
        assert!(err.retry_in().is_err());
    }

    #[test]
    fn retry_in_on_permanent_error() {
        let err = CredentialsError::from_msg(false, "permanent");
        let result = err.retry_in();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "error is permanent");
    }

    #[test]
    fn retry_in_on_transient_without_time() {
        let err = CredentialsError::from_msg(true, "transient");
        let result = err.retry_in();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "retry duration not set");
    }

    #[test]
    fn fmt_with_retry_in() {
        let duration = Duration::from_secs(10);
        let e = CredentialsError::from_msg(true, "test-only-err-123").with_retry_in(duration);
        let got = format!("{e}");
        assert!(got.contains("test-only-err-123"), "{got}");
        assert!(got.contains(TRANSIENT_MSG), "{got}");
        assert!(got.contains(", retry in 10s"), "{got}");
    }
}
