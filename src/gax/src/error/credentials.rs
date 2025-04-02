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

/// Represents an error creating or using a [Credential].
///
/// The Google Cloud client libraries may experience problems creating
/// credentials and/or using them. An example of problems creating credentials
/// may be a badly formatted or missing files key files. An example of problems
/// using credentials may be a temporary failure to retrieve or create
/// [access tokens]. Note that the latter kind of errors may happen even after
/// the credential files are successfully loaded and parsed.
///
/// Applications rarely need to create instances of this error type. The
/// exception might be when testing application code, where the application is
/// mocking a client library behavior. Such tests are extremely rare, most
/// applications should only work with the [Error][crate::error::Error] type.
///
/// # Example
/// ```
/// # use google_cloud_gax::error::CredentialError;
/// let err = CredentialError::from_str(
///     true, "simulated retryable error while trying to create credentials");
/// assert!(err.is_retryable());
/// assert!(format!("{err}").contains("simulated retryable error"));
/// ```
///
/// [access tokens]: https://cloud.google.com/docs/authentication/token-types
/// [Credential]: https://docs.rs/google-cloud-auth/latest/google_cloud_auth/credentials/struct.Credential.html
#[derive(Clone, Debug)]
pub struct CredentialError {
    /// A boolean value indicating whether the error is retryable.
    ///
    /// If `true`, the operation that resulted in this error might succeed upon
    /// retry.
    is_retryable: bool,

    /// The underlying source of the error.
    ///
    /// This provides more specific information about the cause of the failure.
    source: CredentialErrorImpl,
}

#[derive(Clone, Debug)]
enum CredentialErrorImpl {
    SimpleMessage(String),
    Source(Arc<dyn Error + Send + Sync>),
}

impl CredentialError {
    /// Creates a new `CredentialError`.
    ///
    /// This function is only intended for use in the client libraries
    /// implementation. Application may use this in mocks, though we do not
    /// recommend that your write tests for specific error cases. Most tests
    /// should use the generic type [Error][crate::error::Error] type.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_gax::error::CredentialError;
    /// # use google_cloud_gax::error::Error;
    /// let err = CredentialError::new(
    ///     false, Error::other("simulated non-retryable error while trying to create credentials"));
    /// assert!(!err.is_retryable());
    /// assert!(format!("{err}").contains("simulated non-retryable error"));
    /// ```
    /// # Arguments
    /// * `is_retryable` - A boolean indicating whether the error is retryable.
    /// * `source` - The underlying error that caused the auth failure.
    pub fn new<T: Error + Send + Sync + 'static>(is_retryable: bool, source: T) -> Self {
        CredentialError {
            is_retryable,
            source: CredentialErrorImpl::Source(Arc::new(source)),
        }
    }

    /// Creates a new `CredentialError`.
    ///
    /// This function is only intended for use in the client libraries
    /// implementation. Application may use this in mocks, though we do not
    /// recommend that your write tests for specific error cases. Most tests
    /// should use the generic type [Error][crate::error::Error] type.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_gax::error::CredentialError;
    /// let err = CredentialError::from_str(
    ///     true, "simulated retryable error while trying to create credentials");
    /// assert!(err.is_retryable());
    /// assert!(format!("{err}").contains("simulated retryable error"));
    /// ```
    ///
    /// # Arguments
    /// * `is_retryable` - A boolean indicating whether the error is retryable.
    /// * `message` - The underlying error that caused the auth failure.
    pub fn from_str<T: Into<String>>(is_retryable: bool, message: T) -> Self {
        CredentialError::new(
            is_retryable,
            CredentialErrorImpl::SimpleMessage(message.into()),
        )
    }

    /// Returns `true` if the error is retryable; otherwise returns `false`.
    pub fn is_retryable(&self) -> bool {
        self.is_retryable
    }
}

impl std::error::Error for CredentialErrorImpl {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self {
            CredentialErrorImpl::SimpleMessage(_) => None,
            CredentialErrorImpl::Source(source) => Some(source),
        }
    }
}

impl Display for CredentialErrorImpl {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match &self {
            CredentialErrorImpl::SimpleMessage(message) => write!(f, "{}", message),
            CredentialErrorImpl::Source(source) => write!(f, "{}", source),
        }
    }
}

impl std::error::Error for CredentialError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.source()
    }
}

const RETRYABLE_MSG: &str = "but future attempts may succeed";
const NON_RETRYABLE_MSG: &str = "and future attempts will not succeed";

impl Display for CredentialError {
    /// Formats the error message to include retryability and source.
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        let msg = if self.is_retryable {
            RETRYABLE_MSG
        } else {
            NON_RETRYABLE_MSG
        };
        write!(
            f,
            "cannot create access token, {}, source:{}",
            msg, self.source
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::collections::HashMap;
    use test_case::test_case;

    #[test_case(true)]
    #[test_case(false)]
    fn new(retryable: bool) {
        let source = crate::error::HttpError::new(
            404,
            HashMap::new(),
            Some(bytes::Bytes::from_static("test-only".as_bytes())),
        );
        let got = CredentialError::new(retryable, source);
        assert_eq!(got.is_retryable(), retryable, "{got}");
        assert!(got.source().is_some(), "{got}");
        assert!(format!("{got}").contains("test-only"), "{got}");
    }

    #[test_case(true)]
    #[test_case(false)]
    fn from_str(retryable: bool) {
        let got = CredentialError::from_str(retryable, "test-only");
        assert_eq!(got.is_retryable(), retryable, "{got}");
        assert!(got.source().is_some(), "{got}");
        assert!(format!("{got}").contains("test-only"), "{got}");
    }

    #[test]
    fn fmt() {
        let e = CredentialError::from_str(true, "test-only-err-123");
        let got = format!("{e}");
        assert!(got.contains("test-only-err-123"), "{got}");
        assert!(got.contains(RETRYABLE_MSG), "{got}");

        let e = CredentialError::from_str(false, "test-only-err-123");
        let got = format!("{e}");
        assert!(got.contains("test-only-err-123"), "{got}");
        assert!(got.contains(NON_RETRYABLE_MSG), "{got}");
    }

    #[test]
    fn source() {
        let got = CredentialErrorImpl::SimpleMessage("test-only".into());
        assert!(got.source().is_none(), "{got}");
        let got = CredentialErrorImpl::Source(Arc::new(crate::error::Error::other("test-only")));
        assert!(got.source().is_some(), "{got}");
    }
}
