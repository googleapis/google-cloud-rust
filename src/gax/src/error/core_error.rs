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
///         println!("service error {e}, debug using {:?}", e.status().unwrap());
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
    ///
    /// # Troubleshooting
    ///
    /// The most common cause of this problem is setting a timeout value that is
    /// based on the observed latency when the service is not under load.
    /// Consider increasing the timeout value to handle temporary latency
    /// increases too.
    ///
    /// It could also indicate a congestion in the network, a service outage, or
    /// a service that is under load and will take time to scale up.
    pub fn is_timeout(&self) -> bool {
        matches!(self.kind, ErrorKind::Timeout)
    }

    /// Creates an error representing an exhausted policy.
    ///
    /// # Example
    /// ```
    /// use std::error::Error as _;
    /// use google_cloud_gax::error::Error;
    /// let error = Error::exhausted("too many retry attempts");
    /// assert!(error.is_exhausted());
    /// assert!(error.source().is_some());
    /// ```
    pub fn exhausted<T: Into<BoxError>>(source: T) -> Self {
        Self {
            kind: ErrorKind::Exhausted,
            source: Some(source.into()),
        }
    }

    /// The request could not complete be before the retry policy expired.
    ///
    /// This is always a client-side generated error, but it may be the result
    /// of multiple errors received from the service.
    ///
    /// # Troubleshooting
    ///
    /// The most common cause of this problem is a transient problem that lasts
    /// longer than your retry policy. For example, your retry policy may
    /// effectively be exhausted after a few seconds, but some services may take
    /// minutes to recover.
    ///
    /// If your application can tolerate longer recovery times then extend the
    /// retry policy. Otherwise consider recovery at a higher level, such as
    /// seeking human intervention, switching the workload to a different
    /// location, failing the batch job and starting from a previous checkpoint,
    /// or even presenting an error to the application user.
    pub fn is_exhausted(&self) -> bool {
        matches!(self.kind, ErrorKind::Exhausted)
    }

    /// Not part of the public API, subject to change without notice.
    ///
    /// Creates an error representing a deserialization problem.
    ///
    /// Applications should have no need to use this function. The exception
    /// could be mocks, but this error is too rare to merit mocks. If you are
    /// writing a mock that extracts values from [wkt::Any], consider using
    /// `.expect()` calls instead.
    ///
    /// # Example
    /// ```
    /// use std::error::Error as _;
    /// use google_cloud_gax::error::Error;
    /// let error = Error::deser("simulated problem");
    /// assert!(error.is_deserialization());
    /// assert!(error.source().is_some());
    /// ```
    #[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
    pub fn deser<T: Into<BoxError>>(source: T) -> Self {
        Self {
            kind: ErrorKind::Deserialization,
            source: Some(source.into()),
        }
    }

    /// The response could not be deserialized.
    ///
    /// This is always a client-side generated error. Note that the request may
    /// or may not have started, and it may or may not complete in the service.
    /// If the request mutates any state in the service, it may or may not be
    /// safe to attempt the request again.
    ///
    /// # Troubleshooting
    ///
    /// The most common cause for deserialization problems are bugs in the
    /// client library and (rarely) bugs in the service.
    ///
    /// When using gRPC services, and if the response includes a [wkt::Any]
    /// field, the client library may not be able to handle unknown types within
    /// the `Any`. In all services we know of, this should not happen, but it is
    /// impossible to prepare the client library for breaking changes in the
    /// service. Upgrading to the latest version of the client library may be
    /// the only possible fix.
    ///
    /// Beyond this issue with `Any`, while the client libraries are designed to
    /// handle all valid responses, including unknown fields and unknown
    /// enumeration values, it is possible that the client library has a bug.
    /// Please [open an issue] if you run in to this problem. Include any
    /// instructions on how to reproduce the problem. If you cannot use, or
    /// prefer not to use, GitHub to discuss this problem, then contact
    /// [Google Cloud support].
    ///
    /// [open an issue]: https://github.com/googleapis/google-cloud-rust/issues/new/choose
    /// [Google Cloud support]: https://cloud.google.com/support
    pub fn is_deserialization(&self) -> bool {
        matches!(self.kind, ErrorKind::Deserialization)
    }

    /// Not part of the public API, subject to change without notice.
    ///
    /// Creates an error representing a serialization problem.
    ///
    /// Applications should have no need to use this function. The exception
    /// could be mocks, but this error is too rare to merit mocks. If you are
    /// writing a mock that stores values into [wkt::Any], consider using
    /// `.expect()` calls instead.
    ///
    /// # Example
    /// ```
    /// use std::error::Error as _;
    /// use google_cloud_gax::error::Error;
    /// let error = Error::ser("simulated problem");
    /// assert!(error.is_serialization());
    /// assert!(error.source().is_some());
    /// ```
    #[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
    pub fn ser<T: Into<BoxError>>(source: T) -> Self {
        Self {
            kind: ErrorKind::Serialization,
            source: Some(source.into()),
        }
    }

    /// The request could not be serialized.
    ///
    /// This is always a client-side generated error, generated before the
    /// request is made. This error is never transient: the serialization is
    /// deterministic (modulo out of memory conditions), and will fail on future
    /// attempts with the same input data.
    ///
    /// # Troubleshooting
    ///
    /// Most client libraries use HTTP and JSON as the transport, though some
    /// client libraries use gRPC for some, or all RPCs.
    ///
    /// The most common cause for serialization problems is using an unknown
    /// enum value name with a gRPC-based RPC. gRPC requires integer enum
    /// values, while JSON accepts both. The client libraries convert **known**
    /// enum value names to their integer representation, but unknown values
    /// cannot be sent over gRPC. Verify the enum value is valid, and if so:
    /// - try using an integer value instead of the enum name, or
    /// - upgrade the client library: newer versions should include the new
    ///   value.
    ///
    /// In all other cases please [open an issue]. While we do not expect these
    /// problems to be common, we would like to hear if they are so we can
    /// prevent them. If you cannot use a public issue tracker, contact
    /// [Google Cloud support].
    ///
    /// A less common cause for serialization problems may be an out of memory
    /// condition, or any other runtime error. Use `format!("{:?}", ...)` to
    /// examine the error as it should include the original problem.
    ///
    /// Finally, sending a [wkt::Any] with a gRPC-based client is unsupported.
    /// As of this writing, no client libraries sends `Any` via gRPC, but this
    /// could be a problem in the future.
    ///
    /// [open an issue]: https://github.com/googleapis/google-cloud-rust/issues/new/choose
    /// [Google Cloud support]: https://cloud.google.com/support
    pub fn is_serialization(&self) -> bool {
        matches!(self.kind, ErrorKind::Serialization)
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
    /// # Troubleshooting
    ///
    /// As this error type is typically created by the service, troubleshooting
    /// this problem typically involves reading the service documentation to
    /// root cause the problem.
    ///
    /// Some services include additional details about the error, sometimes
    /// including what fields are missing or have bad values in the
    /// [Status::details] vector. The `std::fmt::Debug` format will include
    /// such details.
    ///
    /// With that said, review the status [Code][crate::error::rpc::Code]
    /// documentation. The description of the status codes provides a good
    /// starting point.
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

    // TODO(#2316) - update the troubleshooting text.
    /// Not part of the public API, subject to change without notice.
    ///
    /// If true, the request was missing required parameters or the parameters
    /// did not match any of the expected formats.
    ///
    /// # Troubleshooting
    ///
    /// Typically this indicates a problem in the application. A required field
    /// in the request builder was not initialized or the format of the field
    /// does not match the expectations.
    ///
    /// We are working to improve the messages in these errors to make them
    /// self-explanatory, until bug [#2316] is fixed, please consult the service
    /// REST API documentation.
    ///
    /// [#2316]: https://github.com/googleapis/google-cloud-rust/issues/2316
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
    ///
    /// # Troubleshooting
    ///
    /// Typically this indicates a misconfigured authentication environment for
    /// your application. Very rarely, this may indicate a failure to contact
    /// the HTTP services used to create [access tokens].
    ///
    /// If you are using the default [Credentials], the
    /// [Authenticate for using client libraries] guide includes good
    /// information on how to set up your environment for authentication.
    ///
    /// If you have configured custom `Credentials`, consult the documentation
    /// for the specific credential type you used.
    ///
    /// [Credentials]: https://docs.rs/google-cloud-auth/latest/google_cloud_auth/credentials/struct.Credentials.html
    /// [Authenticate for using client libraries]: https://cloud.google.com/docs/authentication/client-libraries
    /// [access tokens]: https://cloud.google.com/docs/authentication/token-types
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
    ///
    /// # Troubleshooting
    ///
    /// This indicates a problem completing the request. This type of error is
    /// rare, but includes crashes and restarts on proxies and load balancers.
    /// It could indicate a bug in the client library, if it tried to use a
    /// stale connection that had been closed by the service.
    ///
    /// Most often, the solution is to use the right retry policy. This may
    /// involve changing your request to be idempotent, or configuring the
    /// policy to retry non-idempotent failures.
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
    ///
    /// # Troubleshooting
    ///
    /// This indicates that the request did not reach the service. Most commonly
    /// the problem are invalid or mismatched request parameters that route
    /// the request to the wrong backend.
    ///
    /// In this regard, this is similar to the [is_binding][Error::is_binding]
    /// errors, except that the client library was unable to detect the problem
    /// locally.
    ///
    /// An increasingly common cause for this error is trying to use regional
    /// resources (e.g. `projects/my-project/locations/us-central1/secrets/my-secret`)
    /// while using the default, non-regional endpoint. Some services require
    /// using regional endpoints (e.g.
    /// `https://secretmanager.us-central1.rep.googleapis.com`) to access such
    /// resources.
    #[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
    pub fn is_transport(&self) -> bool {
        matches!(&self.kind, ErrorKind::Transport { .. })
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
            (ErrorKind::Deserialization, Some(e)) => {
                write!(f, "cannot deserialize the response {e}")
            }
            (ErrorKind::Authentication, Some(e)) => {
                write!(f, "cannot create the authentication headers {e}")
            }
            (ErrorKind::Timeout, Some(e)) => {
                write!(f, "the request exceeded the request deadline {e}")
            }
            (ErrorKind::Exhausted, Some(e)) => {
                write!(f, "{e}")
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
    Deserialization,
    Authentication,
    Timeout,
    Exhausted,
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
    fn exhausted() {
        let source = wkt::TimestampError::OutOfRange;
        let error = Error::exhausted(source);
        assert!(error.is_exhausted(), "{error:?}");
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
    fn serialization() {
        let source = wkt::TimestampError::OutOfRange;
        let error = Error::deser(source);
        assert!(error.is_deserialization(), "{error:?}");
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
        let error = Error::ser(source);
        assert!(error.is_serialization(), "{error:?}");
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
