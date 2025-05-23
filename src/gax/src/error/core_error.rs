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

use super::CredentialsError;
use super::rpc::Status;
use http::HeaderMap;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// The core error returned by all client libraries.
#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
}

impl Error {
    /// An error with the information returned by Google Cloud services.
    ///
    /// # Example
    /// ```
    /// use google_cloud_gax::error::Error;
    /// use google_cloud_gax::error::rpc::{Code, Status};
    /// let error = Error::service(None, None, Status::default().set_code(Code::NotFound).set_message("NOT FOUND"));
    /// assert!(error.status().is_some());
    /// assert!(error.http_status_code().is_none());
    /// assert!(error.http_headers().is_none());
    /// ```
    pub fn service(status_code: Option<u16>, headers: Option<HeaderMap>, status: Status) -> Self {
        let kind = ErrorKind::Service {
            status_code,
            headers: headers.map(Box::new),
            payload: ServiceErrorPayload::Status(status),
        };
        Self { kind }
    }

    /// The [Status] payload associated with this error.
    ///
    /// # Examples
    /// ```
    /// use google_cloud_gax::error::{Error, rpc::{Code, Status}};
    /// let e = search_for_thing("the thing");
    /// if let Some(status) = e.status() {
    ///     if status.code == Code::NotFound {
    ///         println!("cannot find the thing, more details in {:?}", status.details);
    ///     }
    /// }
    ///
    /// fn search_for_thing(name: &str) -> Error {
    ///     # Error::service(None, None, Status::default().set_code(Code::NotFound))
    /// }
    /// ```
    ///
    /// Google Cloud services return a detailed `Status` message including a
    /// numeric code for the error type, a human-readable message, and a
    /// sequence of details which may include localization messages, or more
    /// information about what caused the failure.
    ///
    /// See [AIP-193] for background information about the error model in Google
    /// Cloud services.
    ///
    /// [AIP-193]: https://google.aip.dev/193
    pub fn status(&self) -> Option<&Status> {
        match &self.kind {
            ErrorKind::Service {
                payload: ServiceErrorPayload::Status(s),
                ..
            } => Some(s),
            _ => None,
        }
    }

    /// The HTTP status code, if any, associated with this error.
    ///
    /// # Example
    /// ```
    /// use google_cloud_gax::error::{Error, rpc::{Code, Status}};
    /// let e = search_for_thing("the thing");
    /// if let Some(code) = e.http_status_code() {
    ///     if code == 404 {
    ///         println!("cannot find the thing, more details in {e}");
    ///     }
    /// }
    ///
    /// fn search_for_thing(name: &str) -> Error {
    ///     # Error::http(400, http::HeaderMap::new(), bytes::Bytes::from_static(b"NOT FOUND"))
    /// }
    /// ```
    ///
    /// Sometimes the error is generated before it reaches any Google Cloud
    /// service. For example, your proxy or the Google load balancers may
    /// generate errors without the detailed payload described in [AIP-193].
    /// In such cases the client library returns the status code, headers, and
    /// http payload.
    ///
    /// Note that `http_status_code()`, `http_headers()`, `http_payload()`, and
    /// `status()` are represented as different fields, because they may be
    /// set in some errors but not others.
    pub fn http_status_code(&self) -> Option<u16> {
        match &self.kind {
            ErrorKind::Service {
                status_code: Some(code),
                ..
            } => Some(*code),
            _ => None,
        }
    }

    /// The headers, if any, associated with this error.
    ///
    /// # Example
    /// ```
    /// use google_cloud_gax::error::{Error, rpc::{Code, Status}};
    /// let e = search_for_thing("the thing");
    /// if let Some(headers) = e.http_headers() {
    ///     if let Some(id) = headers.get("x-guploader-uploadid") {
    ///         println!("this can speed up troubleshooting the Google Cloud Storage support team {id:?}");
    ///     }
    /// }
    ///
    /// fn search_for_thing(name: &str) -> Error {
    ///     # let mut map = http::HeaderMap::new();
    ///     # map.insert("x-guploader-uploadid", http::HeaderValue::from_static("placeholder"));
    ///     # Error::http(400, map, bytes::Bytes::from_static(b"NOT FOUND"))
    /// }
    /// ```
    ///
    /// Sometimes the error may have headers associated with it. Some services
    /// include information useful for troubleshooting or for the support team
    /// in the headers. Over gRPC this is called `metadata`, the Google Cloud
    /// client libraries for Rust normalize this to a [http::HeaderMap].
    ///
    /// Many errors do not have this information, e.g. errors detected before
    /// the request is set, or timeouts. Some RPCs also return "partial"
    /// errors, which do not include such information.
    ///
    /// Note that `http_status_code()`, `http_headers()`, `http_payload()`, and
    /// `status()` are represented as different fields, because they may be
    /// set in some errors but not others.
    pub fn http_headers(&self) -> Option<&http::HeaderMap> {
        match &self.kind {
            ErrorKind::Service {
                headers: Some(h), ..
            } => Some(h),
            _ => None,
        }
    }

    /// The payload, if any, associated with this error.
    ///
    /// # Example
    /// ```
    /// use google_cloud_gax::error::{Error, rpc::{Code, Status}};
    /// let e = search_for_thing("the thing");
    /// if let Some(payload) = e.http_payload() {
    ///    println!("the error included some extra payload {payload:?}");
    /// }
    ///
    /// fn search_for_thing(name: &str) -> Error {
    ///     # Error::http(400, http::HeaderMap::new(), bytes::Bytes::from_static(b"NOT FOUND"))
    /// }
    /// ```
    ///
    /// Sometimes the error may have headers associated with it. Some services
    /// include information useful for troubleshooting or for the support team
    /// in the headers. Over gRPC this is called `metadata`, the Google Cloud
    /// client libraries for Rust normalize this to a [http::HeaderMap].
    ///
    /// Many errors do not have this information, e.g. errors detected before
    /// the request is set, or timeouts. Some RPCs also return "partial"
    /// errors, which do not include such information.
    ///
    /// Note that `http_status_code()`, `http_headers()`, `http_payload()`, and
    /// `status()` are represented as different fields, because they may be
    /// set in some errors but not others.
    pub fn http_payload(&self) -> Option<&bytes::Bytes> {
        match &self.kind {
            ErrorKind::Service {
                payload: ServiceErrorPayload::Bytes(b),
                ..
            } => Some(b),
            _ => None,
        }
    }

    /// The error was generated before the RPC started and is transient.
    pub(crate) fn is_transient_and_before_rpc(&self) -> bool {
        match &self.kind {
            ErrorKind::Binding(_) => false,
            ErrorKind::Connect(_) => false,
            ErrorKind::Authentication(e) if e.is_retryable() => true,
            _ => false,
        }
    }

    /// The error was generated after I/O started.
    pub(crate) fn is_io(&self) -> bool {
        matches!(&self.kind, ErrorKind::Io(_) | ErrorKind::Timeout(_))
    }

    // TODO(#2221) - remove once the migration is completed.
    #[doc(hidden)]
    pub fn serde<T: Into<BoxError>>(source: T) -> Self {
        Self {
            kind: ErrorKind::Serialization(source.into()),
        }
    }

    #[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
    pub fn is_deserialization(&self) -> bool {
        matches!(self.kind, ErrorKind::Deserialization(_))
    }

    #[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
    pub fn is_timeout(&self) -> bool {
        matches!(self.kind, ErrorKind::Timeout(_))
    }

    #[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
    pub fn binding<T: Into<BoxError>>(source: T) -> Self {
        Self {
            kind: ErrorKind::Binding(source.into()),
        }
    }

    #[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
    pub fn ser<T: Into<BoxError>>(source: T) -> Self {
        Self {
            kind: ErrorKind::Serialization(source.into()),
        }
    }

    #[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
    pub fn authentication(source: CredentialsError) -> Self {
        Self {
            kind: ErrorKind::Authentication(source),
        }
    }

    #[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
    pub fn connect<T: Into<BoxError>>(source: T) -> Self {
        Self {
            kind: ErrorKind::Connect(source.into()),
        }
    }

    #[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
    pub fn io<T: Into<BoxError>>(source: T) -> Self {
        Self {
            kind: ErrorKind::Io(source.into()),
        }
    }

    #[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
    pub fn timeout<T: Into<BoxError>>(source: T) -> Self {
        Self {
            kind: ErrorKind::Timeout(source.into()),
        }
    }

    #[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
    pub fn exhausted<T: Into<BoxError>>(source: T) -> Self {
        Self {
            kind: ErrorKind::Exhausted(source.into()),
        }
    }

    #[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
    pub fn http(status_code: u16, headers: HeaderMap, payload: bytes::Bytes) -> Self {
        let kind = ErrorKind::Service {
            status_code: Some(status_code),
            headers: Some(Box::new(headers)),
            payload: ServiceErrorPayload::Bytes(payload),
        };
        Self { kind }
    }

    #[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
    pub fn deser<T: Into<BoxError>>(source: T) -> Self {
        Self {
            kind: ErrorKind::Deserialization(source.into()),
        }
    }

    // TODO(#2221) - remove once the migration is completed.
    #[doc(hidden)]
    pub fn other<T: Into<BoxError>>(source: T) -> Self {
        Self {
            kind: ErrorKind::Other(source.into()),
        }
    }

    fn display_service_error(
        f: &mut std::fmt::Formatter,
        status_code: &Option<u16>,
        _headers: &Option<Box<HeaderMap>>,
        payload: &ServiceErrorPayload,
    ) -> std::fmt::Result {
        match payload {
            ServiceErrorPayload::Status(s) => {
                // TODO(#2221) - more complete error messages
                write!(
                    f,
                    "the service returned an error: {}, code={}, details={:?}",
                    s.message, s.code, s.details
                )
            }
            ServiceErrorPayload::Bytes(b) => {
                // TODO(#2221) - more complete error messages
                write!(f, "an HTTP error, code={:?}, payload={b:?}", status_code)
            }
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.kind {
            ErrorKind::Binding(e) => {
                write!(f, "cannot find a matching binding to send the request: {e}")
            }
            ErrorKind::Connect(e) => {
                write!(f, "cannot connect to the service: {e}")
            }
            ErrorKind::Serialization(e) => write!(f, "cannot serialize the request: {e}"),
            ErrorKind::Authentication(e) => {
                write!(f, "cannot create the authentication headers: {e}")
            }
            ErrorKind::Io(e) => write!(
                f,
                "an I/O problem sending the request or receiving the response: {e}"
            ),
            ErrorKind::Timeout(e) => write!(f, "the request exceeded the request deadline: {e}"),
            ErrorKind::Exhausted(e) => write!(
                f,
                "a policy was exhausted before getting a successful response: {e}"
            ),
            ErrorKind::Service {
                status_code,
                headers,
                payload,
            } => Self::display_service_error(f, status_code, headers, payload),
            ErrorKind::Deserialization(e) => write!(f, "cannot deserialize the response: {e}"),
            ErrorKind::Other(e) => write!(f, "an unclassified problem making a request: {e}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.kind {
            ErrorKind::Binding(e) => Some(e.as_ref()),
            ErrorKind::Connect(e) => Some(e.as_ref()),
            ErrorKind::Serialization(e) => Some(e.as_ref()),
            ErrorKind::Authentication(e) => Some(e),
            ErrorKind::Io(e) => Some(e.as_ref()),
            ErrorKind::Timeout(e) => Some(e.as_ref()),
            ErrorKind::Exhausted(e) => Some(e.as_ref()),
            ErrorKind::Service { .. } => None,
            ErrorKind::Deserialization(e) => Some(e.as_ref()),
            ErrorKind::Other(e) => Some(e.as_ref()),
        }
    }
}

#[derive(Debug)]
enum ServiceErrorPayload {
    Bytes(bytes::Bytes),
    Status(super::rpc::Status),
}

/// The type of error held by an [Error] instance.
#[derive(Debug)]
enum ErrorKind {
    Binding(BoxError),
    Connect(BoxError),
    Serialization(BoxError),
    Authentication(CredentialsError),
    Io(BoxError),
    Timeout(BoxError),
    Exhausted(BoxError),
    Service {
        status_code: Option<u16>,
        headers: Option<Box<HeaderMap>>,
        payload: ServiceErrorPayload,
    },
    Deserialization(BoxError),
    /// A uncategorized error.
    Other(BoxError),
}

#[cfg(test)]
mod test {
    // TODO(#2221) - add some tests for `Display`
}
