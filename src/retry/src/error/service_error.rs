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
use std::collections::HashMap;

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
    headers: Option<HashMap<String, String>>,
}

impl ServiceError {
    /// Returns the underlying [Status].
    pub fn status(&self) -> &Status {
        &self.status
    }

    pub fn http_status_code(&self) -> &Option<u16> {
        &self.http_status_code
    }

    pub fn headers(&self) -> &Option<HashMap<String, String>> {
        &self.headers
    }

    /// Sets the HTTP status code for this service error.
    ///
    /// Not all `ServiceError` instances contain a HTTP status code. Errors
    /// received as part of a response message (e.g. a long-running operation)
    /// do not have them.
    pub fn with_http_status_code<T: Into<u16>>(mut self, v: T) -> Self {
        self.http_status_code = Some(v.into());
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
    pub fn with_headers<K, V, T>(mut self, v: T) -> Self
    where
        K: Into<String>,
        V: Into<String>,
        T: IntoIterator<Item = (K, V)>,
    {
        self.headers = Some(v.into_iter().map(|(k, v)| (k.into(), v.into())).collect());
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
                write!(f, "{k}: {v}")?;
            }
            for (k, v) in h.iter().skip(1) {
                write!(f, "{k}: {v}")?;
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

    fn source() -> rpc::model::Status {
        rpc::model::Status::default()
            .set_code(Code::Aborted as i32)
            .set_message("ABORTED")
    }

    #[test]
    fn from_rpc_status() {
        let error = ServiceError::from(source());
        assert_eq!(error.status(), &Status::from(source()));
        assert_eq!(error.http_status_code(), &None);
        assert_eq!(error.headers(), &None);

        let got = format!("{error}");
        assert!(
            got.contains(&format!("code: {}", Code::Aborted as i32)),
            "{error:?}"
        );
        assert!(got.contains("ABORTED"), "{error:?}");
    }

    #[test]
    fn from_gax_status() {
        let error = ServiceError::from(Status::from(source()));
        assert_eq!(error.status(), &Status::from(source()));
        assert_eq!(error.http_status_code(), &None);
        assert_eq!(error.headers(), &None);

        let got = format!("{error}");
        assert!(
            got.contains(&format!("code: {}", Code::Aborted as i32)),
            "{error:?}"
        );
        assert!(got.contains("ABORTED"), "{error:?}");
    }

    #[test]
    fn with_http_status_code() {
        let error = ServiceError::from(source()).with_http_status_code(404 as u16);
        assert_eq!(error.status(), &Status::from(source()));
        assert_eq!(error.http_status_code(), &Some(404));
        assert_eq!(error.headers(), &None);

        let got = format!("{error}");
        assert!(
            got.contains(&format!("code: {}", Code::Aborted as i32)),
            "{error:?}"
        );
        assert!(got.contains("ABORTED"), "{error:?}");
        assert!(got.contains("http_status_code=404"), "{error:?}");
    }

    #[test]
    fn with_empty() {
        let empty: [(&str, &str); 0] = [];
        let error = ServiceError::from(source()).with_headers(empty);
        assert_eq!(error.status(), &Status::from(source()));
        assert_eq!(error.http_status_code(), &None);
        let want = HashMap::new();
        assert_eq!(error.headers(), &Some(want));

        let got = format!("{error}");
        assert!(
            got.contains(&format!("code: {}", Code::Aborted as i32)),
            "{error:?}"
        );
        assert!(got.contains("ABORTED"), "{error:?}");
        assert!(got.contains("headers=[]"), "{error:?}");
    }

    #[test]
    fn with_one_headers() {
        let error =
            ServiceError::from(source()).with_headers([("content-type", "application/json")]);
        assert_eq!(error.status(), &Status::from(source()));
        assert_eq!(error.http_status_code(), &None);
        let want = {
            let mut map = HashMap::new();
            map.insert("content-type".to_string(), "application/json".to_string());
            map
        };
        assert_eq!(error.headers(), &Some(want));

        let got = format!("{error}");
        assert!(
            got.contains(&format!("code: {}", Code::Aborted as i32)),
            "{error:?}"
        );
        assert!(got.contains("ABORTED"), "{error:?}");
        assert!(got.contains("headers=["), "{error:?}");
        assert!(got.contains("content-type: application/json"), "{error:?}");
    }

    #[test]
    fn with_headers() {
        let error = ServiceError::from(source()).with_headers([
            ("content-type", "application/json"),
            ("h0", "v0"),
            ("h1", "v1"),
        ]);
        assert_eq!(error.status(), &Status::from(source()));
        assert_eq!(error.http_status_code(), &None);
        let want = {
            let mut map = HashMap::new();
            map.insert("content-type".to_string(), "application/json".to_string());
            map.insert("h0".to_string(), "v0".to_string());
            map.insert("h1".to_string(), "v1".to_string());
            map
        };
        assert_eq!(error.headers(), &Some(want));

        let got = format!("{error}");
        assert!(
            got.contains(&format!("code: {}", Code::Aborted as i32)),
            "{error:?}"
        );
        assert!(got.contains("ABORTED"), "{error:?}");
        assert!(got.contains("headers=["), "{error:?}");
        assert!(got.contains("content-type: application/json"), "{error:?}");
        assert!(got.contains("h0: v0"), "{error:?}");
        assert!(got.contains("h1: v1"), "{error:?}");
    }
}
