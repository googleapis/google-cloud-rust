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

use crate::error::rpc::Status;
use http::HeaderMap;

/// An error returned by a Google Cloud service.
///
/// Google Cloud services include detailed error information represented by a
/// [Status]. Depending on how the error is received, the error may have a HTTP
/// status code and/or a number of headers associated with them.
///
/// More information about the Google Cloud error model in [AIP-193].
///
/// [AIP-193]: https://google.aip.dev/193
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ServiceError {
    status: Status,
    http_status_code: Option<u16>,
    headers: Option<HeaderMap>,
}

impl ServiceError {
    /// Returns the underlying [Status].
    pub fn status(&self) -> &Status {
        &self.status
    }

    /// The status code, if any, associated with this error.
    ///
    /// Errors received via HTTP have a HTTP status code associated with them.
    /// Not all service errors are received via HTTP. Errors received via gRPC
    /// do not have a corresponding HTTP status code. Errors received as part
    /// of the *payload* of a successful response also have no associated status
    /// code.
    ///
    /// The latter is common in APIs that perform multiple updates, each one
    /// failing independepently
    pub fn http_status_code(&self) -> &Option<u16> {
        &self.http_status_code
    }

    pub fn headers(&self) -> &Option<HeaderMap> {
        &self.headers
    }
}

/// A builder for [ServiceError].
pub struct ServiceErrorBuilder {
    inner: ServiceError,
}

impl ServiceErrorBuilder {
    /// Creates a new builder to construct complex [ServiceError] instances.
    pub fn new<T>(v: T) -> Self
    where
        T: Into<ServiceError>,
    {
        Self { inner: v.into() }
    }

    /// Consumes the builder and returns the resulting error.
    pub fn build(self) -> ServiceError {
        self.inner
    }

    /// Sets the HTTP status code for this service error.
    ///
    /// Not all `ServiceError` instances contain a HTTP status code. Errors
    /// received as part of a response message (e.g. a long-running operation)
    /// do not have them.
    pub fn with_http_status_code<T: Into<u16>>(mut self, v: T) -> Self {
        self.inner.http_status_code = Some(v.into());
        self
    }

    /// Sets the headers for this error.
    ///
    /// The headers may be HTTP headers or (in the future) gRPC response
    /// metadata.
    ///
    /// Not all `ServiceError` instances contain headers. Errors received as
    /// part of a response message (e.g. a long-running operation) do not have
    /// them.
    pub fn with_headers<T>(mut self, v: T) -> Self
    where
        T: Into<HeaderMap>,
    {
        self.inner.headers = Some(v.into());
        self
    }
}

impl From<Status> for ServiceError {
    fn from(value: Status) -> Self {
        Self {
            status: value,
            http_status_code: None,
            headers: None,
        }
    }
}

impl From<rpc::model::Status> for ServiceError {
    fn from(value: rpc::model::Status) -> Self {
        Self::from(Status::from(value))
    }
}

impl std::fmt::Display for ServiceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "the service returned an error: {:?}", self.status)?;
        if let Some(c) = &self.http_status_code {
            write!(f, ", http_status_code={c}")?;
        }
        if let Some(h) = &self.headers {
            write!(f, ", headers=[")?;
            for (k, v) in h.iter().take(1) {
                write!(f, "{k}: {}", v.to_str().unwrap_or("[error]"))?;
            }
            for (k, v) in h.iter().skip(1) {
                write!(f, "{k}: {}", v.to_str().unwrap_or("[error]"))?;
            }
            write!(f, "]")?;
        }
        Ok(())
    }
}

impl std::error::Error for ServiceError {}

#[cfg(test)]
mod test {
    use super::*;
    use crate::error::rpc::Code;
    use http::{HeaderName, HeaderValue};

    fn source() -> rpc::model::Status {
        rpc::model::Status::default()
            .set_code(Code::Aborted as i32)
            .set_message("ABORTED")
    }

    #[test]
    fn from_rpc_status() {
        let error = ServiceErrorBuilder::new(source()).build();
        assert_eq!(error.status(), &Status::from(source()));
        assert_eq!(error.http_status_code(), &None);
        assert_eq!(error.headers(), &None);

        let got = format!("{error}");
        assert!(got.contains("code: Aborted"), "{error:?}");
        assert!(got.contains("ABORTED"), "{error:?}");
    }

    #[test]
    fn from_gax_status() {
        let error = ServiceErrorBuilder::new(Status::from(source())).build();
        assert_eq!(error.status(), &Status::from(source()));
        assert_eq!(error.http_status_code(), &None);
        assert_eq!(error.headers(), &None);

        let got = format!("{error}");
        assert!(got.contains("code: Aborted"), "{error:?}");
        assert!(got.contains("ABORTED"), "{error:?}");
    }

    #[test]
    fn with_http_status_code() {
        let error = ServiceErrorBuilder::new(source())
            .with_http_status_code(404_u16)
            .build();
        assert_eq!(error.status(), &Status::from(source()));
        assert_eq!(error.http_status_code(), &Some(404));
        assert_eq!(error.headers(), &None);

        let got = format!("{error}");
        assert!(got.contains("code: Aborted"), "{error:?}");
        assert!(got.contains("http_status_code=404"), "{error:?}");
    }

    #[test]
    fn with_empty() {
        let empty = HeaderMap::new();
        let error = ServiceErrorBuilder::new(source())
            .with_headers(empty)
            .build();
        assert_eq!(error.status(), &Status::from(source()));
        assert_eq!(error.http_status_code(), &None);
        let want = HeaderMap::new();
        assert_eq!(error.headers(), &Some(want));

        let got = format!("{error}");
        assert!(got.contains("code: Aborted"), "{error:?}");
        assert!(got.contains("ABORTED"), "{error:?}");
        assert!(got.contains("headers=[]"), "{error:?}");
    }

    #[test]
    fn with_one_header() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("application/json"));
        let error = ServiceErrorBuilder::new(source())
            .with_headers(headers)
            .build();
        assert_eq!(error.status(), &Status::from(source()));
        assert_eq!(error.http_status_code(), &None);
        let want = {
            let mut map = HeaderMap::new();
            map.insert("content-type", HeaderValue::from_static("application/json"));
            map
        };
        assert_eq!(error.headers(), &Some(want));

        let got = format!("{error}");
        assert!(got.contains("code: Aborted"), "{error:?}");
        assert!(got.contains("ABORTED"), "{error:?}");
        assert!(got.contains("headers=["), "{error:?}");
        assert!(got.contains("content-type: application/json"), "{error:?}");
    }

    #[test]
    fn with_headers() {
        let headers = HeaderMap::from_iter([
            (
                HeaderName::from_static("content-type"),
                HeaderValue::from_static("application/json"),
            ),
            (
                HeaderName::from_static("h0"),
                HeaderValue::from_static("v0"),
            ),
            (
                HeaderName::from_static("h1"),
                HeaderValue::from_static("v1"),
            ),
        ]);
        let error = ServiceErrorBuilder::new(source())
            .with_headers(headers)
            .build();
        assert_eq!(error.status(), &Status::from(source()));
        assert_eq!(error.http_status_code(), &None);
        let want = {
            let mut map = HeaderMap::new();
            map.insert("content-type", HeaderValue::from_static("application/json"));
            map.insert("h0", HeaderValue::from_static("v0"));
            map.insert("h1", HeaderValue::from_static("v1"));
            map
        };
        assert_eq!(error.headers(), &Some(want));

        let got = format!("{error}");
        assert!(got.contains("code: Aborted"), "{error:?}");
        assert!(got.contains("ABORTED"), "{error:?}");
        assert!(got.contains("headers=["), "{error:?}");
        assert!(got.contains("content-type: application/json"), "{error:?}");
        assert!(got.contains("h0: v0"), "{error:?}");
        assert!(got.contains("h1: v1"), "{error:?}");
    }
}
