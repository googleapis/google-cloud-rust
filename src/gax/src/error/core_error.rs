// Copyright 2024 Google LLC
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

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// The core error returned by all client libraries.
#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
    source: BoxError,
}

impl Error {
    /// Creates a new [Error] with the given [ErrorKind] and source error.
    pub fn new<T: Into<BoxError>>(kind: ErrorKind, source: T) -> Self {
        Error {
            kind,
            source: source.into(),
        }
    }

    /// A helper to create a new [ErrorKind::Serde] error.
    pub fn serde<T: Into<BoxError>>(source: T) -> Self {
        Error::new(ErrorKind::Serde, source)
    }

    /// A helper to create a new [ErrorKind::Authentication] error.
    pub fn authentication<T: Into<BoxError>>(source: T) -> Self {
        Error::new(ErrorKind::Authentication, source)
    }

    /// A helper to create a new [ErrorKind::Io] error.
    pub fn io<T: Into<BoxError>>(source: T) -> Self {
        Error::new(ErrorKind::Io, source)
    }

    /// A helper to create a new [ErrorKind::Rpc] error.
    pub fn rpc<T: Into<BoxError>>(source: T) -> Self {
        Error::new(ErrorKind::Rpc, source)
    }

    /// A helper to create a new [ErrorKind::Other] error.
    pub fn other<T: Into<BoxError>>(source: T) -> Self {
        Error::new(ErrorKind::Other, source)
    }

    /// Returns the [ErrorKind] associated with this error.
    pub fn kind(&self) -> ErrorKind {
        self.kind.clone()
    }

    /// Recurses through the source error chain and returns a reference to the
    /// inner value if it is of type `T`, or `None` if no such inner value is
    /// found.
    ///
    /// # Examples
    ///
    /// ```
    /// # use gcp_sdk_gax::error::Error;
    /// # use gcp_sdk_gax::error::HttpError;
    /// # use std::collections::HashMap;
    /// let error = Error::rpc(HttpError::new(404, HashMap::new(), None));
    /// if let Some(e) = error.as_inner::<HttpError>() {
    ///     assert_eq!(e.status_code(), 404);
    /// }
    /// ```
    pub fn as_inner<T: std::error::Error + Send + Sync + 'static>(&self) -> Option<&T> {
        let mut error = self.source.as_ref() as &(dyn std::error::Error);
        loop {
            match error.downcast_ref::<T>() {
                Some(e) => return Some(e),
                None => error = error.source()?,
            }
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.kind, self.source)
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}

/// The type of error held by an [Error] instance.
#[derive(Clone, Debug, PartialEq, Default)]
pub enum ErrorKind {
    /// A serialization or deserialization error.
    Serde,
    /// An authentication error.
    Authentication,
    /// An I/O error.
    Io,
    /// An error related to making a RPC.
    Rpc,
    /// A uncategorized error.
    #[default]
    Other,
}

impl std::fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorKind::Serde => write!(
                f,
                "a problem occurred during serialization or deserialization"
            ),
            ErrorKind::Authentication => write!(f, "a problem occurred during authentication"),
            ErrorKind::Io => write!(f, "a problem occurred during I/O"),
            ErrorKind::Rpc => write!(f, "a problem occurred while making a RPC"),
            ErrorKind::Other => write!(f, "a problem occurred"),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use test_case::test_case;

    #[test]
    fn error_matches_kind() {
        use std::error::Error as E;
        let error = Error::serde("source".to_string());
        assert_eq!(error.kind(), ErrorKind::Serde);
        assert!(error.source().is_some(), "missing source for {error:?}");
        let error = Error::authentication("source".to_string());
        assert_eq!(error.kind(), ErrorKind::Authentication);
        assert!(error.source().is_some(), "missing source for {error:?}");
        let error = Error::io("source".to_string());
        assert_eq!(error.kind(), ErrorKind::Io);
        assert!(error.source().is_some(), "missing source for {error:?}");
        let error = Error::rpc("source".to_string());
        assert_eq!(error.kind(), ErrorKind::Rpc);
        assert!(error.source().is_some(), "missing source for {error:?}");
        let error = Error::other("source".to_string());
        assert_eq!(error.kind(), ErrorKind::Other);
        assert!(error.source().is_some(), "missing source for {error:?}");
    }

    #[test_case(ErrorKind::Serde)]
    #[test_case(ErrorKind::Authentication)]
    #[test_case(ErrorKind::Io)]
    #[test_case(ErrorKind::Rpc)]
    #[test_case(ErrorKind::Other)]
    fn error_display_includes_kind_and_source(kind: ErrorKind) {
        let kind_msg = format!("{kind}");
        let error = Error::new(kind, "test-error-msg".to_string());
        let msg = format!("{error}");
        assert!(
            msg.contains("test-error-msg"),
            "missing error message in {msg:?}"
        );
        assert!(msg.contains(&kind_msg), "missing kind message in {msg:?}");
    }
}
