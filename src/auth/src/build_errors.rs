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

//! Errors created during credentials construction.

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// The error type for [Credentials] builders.
///
/// Applications rarely need to create instances of this error type. The
/// exception might be when testing application code, where the application is
/// mocking a client library behavior.
///
/// [Credentials]: super::credentials::Credentials
#[derive(thiserror::Error, Debug)]
#[error(transparent)]
pub struct Error(ErrorKind);

impl Error {
    /// A problem finding or opening the credentials file.
    pub fn is_loading(&self) -> bool {
        matches!(self.0, ErrorKind::Loading(_))
    }

    /// A problem parsing a credentials JSON specification.
    pub fn is_parsing(&self) -> bool {
        matches!(self.0, ErrorKind::Parsing(_))
    }

    /// The credentials type is invalid or unknown.
    pub fn is_unknown_type(&self) -> bool {
        matches!(self.0, ErrorKind::UnknownType(_))
    }

    /// A required field was missing from the builder.
    pub fn is_missing_field(&self) -> bool {
        matches!(self.0, ErrorKind::MissingField(_))
    }

    #[cfg(feature = "idtoken")]
    /// The credential type is not supported for the given use case.
    pub fn is_not_supported(&self) -> bool {
        matches!(self.0, ErrorKind::NotSupported(_))
    }

    /// Create an error representing problems loading or reading a credentials
    /// file.
    pub(crate) fn loading<T>(source: T) -> Error
    where
        T: Into<BoxError>,
    {
        Error(ErrorKind::Loading(source.into()))
    }

    /// A problem parsing a credentials specification.
    pub(crate) fn parsing<T>(source: T) -> Error
    where
        T: Into<BoxError>,
    {
        Error(ErrorKind::Parsing(source.into()))
    }

    /// The credential type is unknown or invalid.
    pub(crate) fn unknown_type<T>(source: T) -> Error
    where
        T: Into<BoxError>,
    {
        Error(ErrorKind::UnknownType(source.into()))
    }

    /// A required field was missing from the builder.
    pub(crate) fn missing_field(field: &'static str) -> Error {
        Error(ErrorKind::MissingField(field))
    }

    /// The given credential type is not supported.
    pub(crate) fn not_supported<T>(credential_type: T) -> Error
    where
        T: Into<BoxError>,
    {
        Error(ErrorKind::NotSupported(credential_type.into()))
    }
}

#[derive(thiserror::Error, Debug)]
enum ErrorKind {
    #[error("could not find or open the credentials file {0}")]
    Loading(#[source] BoxError),
    #[error("cannot parse the credentials file {0}")]
    Parsing(#[source] BoxError),
    #[error("unknown or invalid credentials type {0}")]
    UnknownType(#[source] BoxError),
    #[error("missing required field: {0}")]
    MissingField(&'static str),    
    #[error("credentials type not supported: {0}")]
    NotSupported(#[source] BoxError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error as _;

    #[test]
    fn constructors() {
        let error = Error::loading("test message");
        assert!(error.is_loading(), "{error:?}");
        assert!(error.source().is_some(), "{error:?}");
        assert!(error.to_string().contains("test message"), "{error}");

        let error = Error::parsing("test message");
        assert!(error.is_parsing(), "{error:?}");
        assert!(error.source().is_some(), "{error:?}");
        assert!(error.to_string().contains("test message"), "{error}");

        let error = Error::unknown_type("test message");
        assert!(error.is_unknown_type(), "{error:?}");
        assert!(error.source().is_some(), "{error:?}");
        assert!(error.to_string().contains("test message"), "{error}");

        let error = Error::missing_field("test field");
        assert!(error.is_missing_field(), "{error:?}");
        assert!(error.source().is_none(), "{error:?}");
        assert!(error.to_string().contains("test field"), "{error}");
    }
}
