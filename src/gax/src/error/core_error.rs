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
use std::error::Error as StdError;

type BoxError = Box<dyn StdError + Send + Sync>;

/// The core error returned by all client libraries.
///
/// The client libraries report errors from multiple sources. For example, the
/// service may return an error, the transport may be unable to create the
/// necessary connection to make a request, the request may timeout before a
/// response is received, the retry policy may be exhausted, or the library may
/// be unable to format the request due to invalid or missing application
/// application inputs.
///
/// Most applications will just return the error or log it, without any further
/// action. However, some applications may need to interrogate the error
/// details. This type offers a series of predicates to determine the error
/// kind. The type also offers accessors to query the most common error details.
/// Applications can query the error [source][std::error::Error::source] for
/// deeper information.
///
/// # Example
/// ```
/// use google_cloud_gax::error::Error;
/// match example_function() {
///     Err(e) if matches!(e.status(), Some(_)) => {
///         println!("service error {e}, should have a status={:?}", e.status());
///     },
///     Err(e) if e.is_timeout() => { println!("not enough time {e}"); },
///     Err(e) => { println!("some other error {e}"); },
///     Ok(_) => { println!("success, how boring"); },
/// }
///
/// fn example_function() -> Result<String, Error> {
///     // ... details omitted ...
///     # use google_cloud_gax::error::rpc::{Code, Status};
///     # Err(Error::service(Status::default().set_code(Code::NotFound).set_message("NOT FOUND")))
/// }
/// ```
#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
    source: Option<BoxError>,
}

impl Error {
    /// Creates an error with the information returned by Google Cloud services.
    ///
    /// # Example
    /// ```
    /// use google_cloud_gax::error::Error;
    /// use google_cloud_gax::error::rpc::{Code, Status};
    /// let status = Status::default().set_code(Code::NotFound).set_message("NOT FOUND");
    /// let error = Error::service(status.clone());
    /// assert_eq!(error.status(), Some(&status));
    /// ```
    pub fn service(status: Status) -> Self {
        let details = ServiceDetails {
            status,
            status_code: None,
            headers: None,
        };
        Self {
            kind: ErrorKind::Service(Box::new(details)),
            source: None,
        }
    }

    /// Creates an error representing a timeout.
    ///
    /// # Example
    /// ```
    /// use std::error::Error as _;
    /// use google_cloud_gax::error::Error;
    /// let error = Error::timeout("simulated timeout");
    /// assert!(error.is_timeout());
    /// assert!(error.source().is_some());
    /// ```
    pub fn timeout<T: Into<BoxError>>(source: T) -> Self {
        Self {
            kind: ErrorKind::Timeout,
            source: Some(source.into()),
        }
    }

    /// The request could not be completed before its deadline.
    ///
    /// This is always a client-side generated error. Note that the request may
    /// or may not have started, and it may or may not complete in the service.
    /// If the request mutates any state in the service, it may or may not be
    /// safe to attempt the request again.
    pub fn is_timeout(&self) -> bool {
        matches!(self.kind, ErrorKind::Timeout)
    }

    /// The [Status] payload associated with this error.
    ///
    /// # Examples
    /// ```
    /// use google_cloud_gax::error::{Error, rpc::{Code, Status}};
    /// let error = Error::service(Status::default().set_code(Code::NotFound));
    /// if let Some(status) = error.status() {
    ///     if status.code == Code::NotFound {
    ///         println!("cannot find the thing, more details in {:?}", status.details);
    ///     }
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
            ErrorKind::Service(d) => Some(&d.as_ref().status),
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
    ///
    /// [AIP-193]: https://google.aip.dev/193
    pub fn http_status_code(&self) -> Option<u16> {
        match &self.kind {
            ErrorKind::Transport(d) => d.as_ref().status_code,
            ErrorKind::Service(d) => d.as_ref().status_code,
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
    ///         println!("this can speed up troubleshooting for the Google Cloud Storage support team {id:?}");
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
    /// include information useful for troubleshooting in the response headers.
    /// Over gRPC this is called `metadata`, the Google Cloud client libraries
    /// for Rust normalize this to a [http::HeaderMap].
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
            ErrorKind::Transport(d) => d.as_ref().headers.as_ref(),
            ErrorKind::Service(d) => d.as_ref().headers.as_ref(),
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
    /// Sometimes the error may contain a payload that is useful for
    /// troubleshooting.
    ///
    /// Note that `http_status_code()`, `http_headers()`, `http_payload()`, and
    /// `status()` are represented as different fields, because they may be
    /// set in some errors but not others.
    pub fn http_payload(&self) -> Option<&bytes::Bytes> {
        match &self.kind {
            ErrorKind::Transport(d) => d.payload.as_ref(),
            _ => None,
        }
    }

    /// Not part of the public API, subject to change without notice.
    ///
    /// Create service errors including transport metadata.
    #[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
    pub fn service_with_http_metadata(
        status: Status,
        status_code: Option<u16>,
        headers: Option<http::HeaderMap>,
    ) -> Self {
        let details = ServiceDetails {
            status_code,
            headers,
            status,
        };
        let kind = ErrorKind::Service(Box::new(details));
        Self { kind, source: None }
    }

    /// Not part of the public API, subject to change without notice.
    ///
    /// Cannot find a valid HTTP binding to make the request.
    ///
    /// This indicates the request is missing required parameters, or the
    /// required parameters do not have a valid format.
    #[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
    pub fn binding<T: Into<BoxError>>(source: T) -> Self {
        Self {
            kind: ErrorKind::Binding,
            source: Some(source.into()),
        }
    }

    /// Not part of the public API, subject to change without notice.
    ///
    /// If true, the request was missing required parameters or the parameters
    /// did not match any of the expected formats.
    #[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
    pub fn is_binding(&self) -> bool {
        matches!(&self.kind, ErrorKind::Binding)
    }

    /// Not part of the public API, subject to change without notice.
    ///
    /// Cannot create the authentication headers.
    #[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
    pub fn authentication(source: CredentialsError) -> Self {
        Self {
            kind: ErrorKind::Authentication,
            source: Some(source.into()),
        }
    }

    /// Not part of the public API, subject to change without notice.
    ///
    /// Could not create the authentication headers before sending the request.
    #[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
    pub fn is_authentication(&self) -> bool {
        matches!(self.kind, ErrorKind::Authentication)
    }

    /// Not part of the public API, subject to change without notice.
    ///
    /// A problem reported by the transport layer.
    #[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
    pub fn http(status_code: u16, headers: HeaderMap, payload: bytes::Bytes) -> Self {
        let details = TransportDetails {
            status_code: Some(status_code),
            headers: Some(headers),
            payload: Some(payload),
        };
        let kind = ErrorKind::Transport(Box::new(details));
        Self { kind, source: None }
    }

    /// Not part of the public API, subject to change without notice.
    ///
    /// A problem in the transport layer without a full HTTP response.
    ///
    /// Examples include: a broken connection after the request is sent, or a
    /// any HTTP error that did not include a status code or other headers.
    #[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
    pub fn io<T: Into<BoxError>>(source: T) -> Self {
        let details = TransportDetails {
            status_code: None,
            headers: None,
            payload: None,
        };
        Self {
            kind: ErrorKind::Transport(Box::new(details)),
            source: Some(source.into()),
        }
    }

    /// Not part of the public API, subject to change without notice.
    ///
    /// A problem in the transport layer without a full HTTP response.
    ///
    /// Examples include read or write problems, and broken connections.
    #[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
    pub fn is_io(&self) -> bool {
        matches!(
        &self.kind,
        ErrorKind::Transport(d) if matches!(**d, TransportDetails {
            status_code: None,
            headers: None,
            payload: None,
            ..
        }))
    }

    /// Not part of the public API, subject to change without notice.
    ///
    /// A problem reported by the transport layer.
    #[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
    pub fn transport<T: Into<BoxError>>(headers: HeaderMap, source: T) -> Self {
        let details = TransportDetails {
            headers: Some(headers),
            status_code: None,
            payload: None,
        };
        Self {
            kind: ErrorKind::Transport(Box::new(details)),
            source: Some(source.into()),
        }
    }

    /// Not part of the public API, subject to change without notice.
    ///
    /// A problem in the transport layer.
    ///
    /// Examples include errors in a proxy, load balancer, or other network
    /// element generated before the service is able to send a full response.
    #[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
    pub fn is_transport(&self) -> bool {
        matches!(&self.kind, ErrorKind::Transport { .. })
    }

    // TODO(#2221) - remove once the migration is completed.
    #[doc(hidden)]
    pub fn serde<T: Into<BoxError>>(source: T) -> Self {
        Self {
            kind: ErrorKind::Serialization,
            source: Some(source.into()),
        }
    }

    /// Not part of the public API, subject to change without notice.
    ///
    /// A problem in serialization or deserialization.
    #[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
    pub fn is_serde(&self) -> bool {
        matches!(&self.kind, ErrorKind::Serialization)
    }

    // TODO(#2221) - remove once the migration is completed.
    #[doc(hidden)]
    pub fn other<T: Into<BoxError>>(source: T) -> Self {
        Self {
            kind: ErrorKind::Other,
            source: Some(source.into()),
        }
    }

    /// The error was generated before the RPC started and is transient.
    pub(crate) fn is_transient_and_before_rpc(&self) -> bool {
        if !matches!(&self.kind, ErrorKind::Authentication) {
            return false;
        }
        self.source
            .as_ref()
            .and_then(|e| e.downcast_ref::<CredentialsError>())
            .map(|e| e.is_transient())
            .unwrap_or(false)
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (&self.kind, &self.source) {
            (ErrorKind::Binding, Some(e)) => {
                write!(f, "cannot find a matching binding to send the request {e}")
            }
            (ErrorKind::Serialization, Some(e)) => write!(f, "cannot serialize the request {e}"),
            (ErrorKind::Authentication, Some(e)) => {
                write!(f, "cannot create the authentication headers {e}")
            }
            (ErrorKind::Timeout, Some(e)) => {
                write!(f, "the request exceeded the request deadline {e}")
            }
            (ErrorKind::Transport(details), _) => details.display(self.source(), f),
            (ErrorKind::Service(d), _) => {
                write!(
                    f,
                    "the service reports an error with code {} described as: {}",
                    d.status.code, d.status.message
                )
            }
            (ErrorKind::Other, Some(e)) => {
                write!(f, "an unclassified problem making a request: {e}")
            }
            (_, None) => unreachable!("no constructor allows this"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|e| e.as_ref() as &(dyn std::error::Error))
    }
}

/// The type of error held by an [Error] instance.
#[derive(Debug)]
enum ErrorKind {
    Binding,
    Serialization,
    Authentication,
    Timeout,
    Transport(Box<TransportDetails>),
    Service(Box<ServiceDetails>),
    /// A uncategorized error.
    Other,
}

#[derive(Debug)]
struct TransportDetails {
    status_code: Option<u16>,
    headers: Option<HeaderMap>,
    payload: Option<bytes::Bytes>,
}

impl TransportDetails {
    fn display(
        &self,
        source: Option<&(dyn StdError + 'static)>,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        match (source, &self) {
            (
                _,
                TransportDetails {
                    status_code: Some(code),
                    payload: Some(p),
                    ..
                },
            ) => {
                if let Ok(message) = std::str::from_utf8(p.as_ref()) {
                    write!(f, "the HTTP transport reports a [{code}] error: {message}")
                } else {
                    write!(f, "the HTTP transport reports a [{code}] error: {p:?}")
                }
            }
            (Some(source), _) => {
                write!(f, "the transport reports an error: {source}")
            }
            (None, _) => unreachable!("no Error constructor allows this"),
        }
    }
}

#[derive(Debug)]
struct ServiceDetails {
    status_code: Option<u16>,
    headers: Option<HeaderMap>,
    status: Status,
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::error::CredentialsError;
    use crate::error::rpc::Code;
    use std::error::Error as StdError;

    #[test]
    fn service() {
        let status = Status::default()
            .set_code(Code::NotFound)
            .set_message("NOT FOUND");
        let error = Error::service(status.clone());
        assert!(error.source().is_none(), "{error:?}");
        assert_eq!(error.status(), Some(&status));
        assert!(error.to_string().contains("NOT FOUND"), "{error}");
        assert!(error.to_string().contains(Code::NotFound.name()), "{error}");
        assert!(!error.is_transient_and_before_rpc(), "{error:?}");
    }

    #[test]
    fn timeout() {
        let source = wkt::TimestampError::OutOfRange;
        let error = Error::timeout(source);
        assert!(error.is_timeout(), "{error:?}");
        assert!(error.source().is_some(), "{error:?}");
        let got = error
            .source()
            .and_then(|e| e.downcast_ref::<wkt::TimestampError>());
        assert!(
            matches!(got, Some(wkt::TimestampError::OutOfRange)),
            "{error:?}"
        );
        let source = wkt::TimestampError::OutOfRange;
        assert!(error.to_string().contains(&source.to_string()), "{error}");
        assert!(!error.is_transient_and_before_rpc(), "{error:?}");

        assert!(error.http_headers().is_none(), "{error:?}");
        assert!(error.http_status_code().is_none(), "{error:?}");
        assert!(error.http_payload().is_none(), "{error:?}");
        assert!(error.status().is_none(), "{error:?}");
    }

    #[test]
    fn service_with_http_metadata() {
        let status = Status::default()
            .set_code(Code::NotFound)
            .set_message("NOT FOUND");
        let status_code = 404_u16;
        let headers = {
            let mut headers = http::HeaderMap::new();
            headers.insert(
                "content-type",
                http::HeaderValue::from_static("application/json"),
            );
            headers
        };
        let error = Error::service_with_http_metadata(
            status.clone(),
            Some(status_code),
            Some(headers.clone()),
        );
        assert_eq!(error.status(), Some(&status));
        assert!(error.to_string().contains("NOT FOUND"), "{error}");
        assert!(error.to_string().contains(Code::NotFound.name()), "{error}");
        assert_eq!(error.http_status_code(), Some(status_code));
        assert_eq!(error.http_headers(), Some(&headers));
        assert!(error.http_payload().is_none(), "{error:?}");
        assert!(!error.is_transient_and_before_rpc(), "{error:?}");
    }

    #[test]
    fn binding() {
        let source = wkt::TimestampError::OutOfRange;
        let error = Error::binding(source);
        assert!(error.is_binding(), "{error:?}");
        assert!(error.source().is_some(), "{error:?}");
        let got = error
            .source()
            .and_then(|e| e.downcast_ref::<wkt::TimestampError>());
        assert!(
            matches!(got, Some(wkt::TimestampError::OutOfRange)),
            "{error:?}"
        );
        let source = wkt::TimestampError::OutOfRange;
        assert!(error.to_string().contains(&source.to_string()), "{error}");
        assert!(!error.is_transient_and_before_rpc(), "{error:?}");

        assert!(error.status().is_none(), "{error:?}");
        assert!(error.http_status_code().is_none(), "{error:?}");
        assert!(error.http_headers().is_none(), "{error:?}");
        assert!(error.http_payload().is_none(), "{error:?}");
    }

    #[test]
    fn ser() {
        let source = wkt::TimestampError::OutOfRange;
        let error = Error::serde(source);
        assert!(error.is_serde(), "{error:?}");
        assert!(error.source().is_some(), "{error:?}");
        let got = error
            .source()
            .and_then(|e| e.downcast_ref::<wkt::TimestampError>());
        assert!(
            matches!(got, Some(wkt::TimestampError::OutOfRange)),
            "{error:?}"
        );
        let source = wkt::TimestampError::OutOfRange;
        assert!(error.to_string().contains(&source.to_string()), "{error}");
        assert!(!error.is_transient_and_before_rpc(), "{error:?}");
    }

    #[test]
    fn auth_transient() {
        let source = CredentialsError::from_msg(true, "test-message");
        let error = Error::authentication(source);
        assert!(error.is_authentication(), "{error:?}");
        assert!(error.source().is_some(), "{error:?}");
        let got = error
            .source()
            .and_then(|e| e.downcast_ref::<CredentialsError>());
        assert!(matches!(got, Some(c) if c.is_transient()), "{error:?}");
        assert!(error.to_string().contains("test-message"), "{error}");
        assert!(error.is_transient_and_before_rpc(), "{error:?}");
    }

    #[test]
    fn auth_not_transient() {
        let source = CredentialsError::from_msg(false, "test-message");
        let error = Error::authentication(source);
        assert!(error.is_authentication(), "{error:?}");
        assert!(error.source().is_some(), "{error:?}");
        let got = error
            .source()
            .and_then(|e| e.downcast_ref::<CredentialsError>());
        assert!(matches!(got, Some(c) if !c.is_transient()), "{error:?}");
        assert!(error.to_string().contains("test-message"), "{error}");
        assert!(!error.is_transient_and_before_rpc(), "{error:?}");
    }

    #[test]
    fn http() {
        let status_code = 404_u16;
        let headers = {
            let mut headers = http::HeaderMap::new();
            headers.insert(
                "content-type",
                http::HeaderValue::from_static("application/json"),
            );
            headers
        };
        let payload = bytes::Bytes::from_static(b"NOT FOUND");
        let error = Error::http(status_code, headers.clone(), payload.clone());
        assert!(error.is_transport(), "{error:?}");
        assert!(!error.is_io(), "{error:?}");
        assert!(error.source().is_none(), "{error:?}");
        assert!(error.status().is_none(), "{error:?}");
        assert!(error.to_string().contains("NOT FOUND"), "{error}");
        assert!(error.to_string().contains("404"), "{error}");
        assert_eq!(error.http_status_code(), Some(status_code));
        assert_eq!(error.http_headers(), Some(&headers));
        assert_eq!(error.http_payload(), Some(&payload));
        assert!(!error.is_transient_and_before_rpc(), "{error:?}");
    }

    #[test]
    fn http_binary() {
        let status_code = 404_u16;
        let headers = {
            let mut headers = http::HeaderMap::new();
            headers.insert(
                "content-type",
                http::HeaderValue::from_static("application/json"),
            );
            headers
        };
        let payload = bytes::Bytes::from_static(&[0xFF, 0xFF]);
        let error = Error::http(status_code, headers.clone(), payload.clone());
        assert!(error.is_transport(), "{error:?}");
        assert!(!error.is_io(), "{error:?}");
        assert!(error.source().is_none(), "{error:?}");
        assert!(error.status().is_none(), "{error:?}");
        assert!(
            error.to_string().contains(&format! {"{payload:?}"}),
            "{error}"
        );
        assert!(error.to_string().contains("404"), "{error}");
        assert_eq!(error.http_status_code(), Some(status_code));
        assert_eq!(error.http_headers(), Some(&headers));
        assert_eq!(error.http_payload(), Some(&payload));
        assert!(!error.is_transient_and_before_rpc(), "{error:?}");
    }

    #[test]
    fn io() {
        let source = wkt::TimestampError::OutOfRange;
        let error = Error::io(source);
        assert!(error.is_transport(), "{error:?}");
        assert!(error.is_io(), "{error:?}");
        assert!(error.status().is_none(), "{error:?}");
        let got = error
            .source()
            .and_then(|e| e.downcast_ref::<wkt::TimestampError>());
        assert!(
            matches!(got, Some(wkt::TimestampError::OutOfRange)),
            "{error:?}"
        );
        let source = wkt::TimestampError::OutOfRange;
        assert!(error.to_string().contains(&source.to_string()), "{error}");
        assert!(!error.is_transient_and_before_rpc(), "{error:?}");
    }

    #[test]
    fn transport() {
        let headers = {
            let mut headers = http::HeaderMap::new();
            headers.insert(
                "content-type",
                http::HeaderValue::from_static("application/json"),
            );
            headers
        };
        let source = wkt::TimestampError::OutOfRange;
        let error = Error::transport(headers.clone(), source);
        assert!(error.is_transport(), "{error:?}");
        assert!(!error.is_io(), "{error:?}");
        assert!(error.status().is_none(), "{error:?}");
        let source = wkt::TimestampError::OutOfRange;
        assert!(error.to_string().contains(&source.to_string()), "{error}");
        assert!(error.http_status_code().is_none(), "{error:?}");
        assert_eq!(error.http_headers(), Some(&headers));
        assert!(error.http_payload().is_none(), "{error:?}");
        assert!(!error.is_transient_and_before_rpc(), "{error:?}");
    }
}
