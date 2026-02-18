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

use google_cloud_gax::client_builder::Error as BuilderError;
#[cfg(test)]
use google_cloud_gax::error::Error;
use http::Uri;
use std::str::FromStr;

/// Calculate the host header given the endpoint and default endpoint.
///
/// Notably, locational and regional endpoints are detected and used as the
/// host. For VIPs and private networks, we need to use the default host.
#[cfg(any(test, feature = "_internal-http-client"))]
pub(crate) fn header(endpoint: Option<&str>, default_endpoint: &str) -> Result<String, HostError> {
    origin_and_header(endpoint, default_endpoint).map(|(_, header)| header)
}

/// Calculate the gRPC authority given the endpoint and default endpoint.
///
/// Notably, locational and regional endpoints are detected and used as the
/// host. For VIPs and private networks, we need to use the default host.
///
/// Tonic consumes the authority as a [http::Uri].
#[cfg(any(test, feature = "_internal-grpc-client"))]
pub(crate) fn origin(endpoint: Option<&str>, default_endpoint: &str) -> Result<Uri, HostError> {
    origin_and_header(endpoint, default_endpoint).map(|(origin, _)| origin)
}

fn origin_and_header(
    endpoint: Option<&str>,
    default_endpoint: &str,
) -> Result<(Uri, String), HostError> {
    let default_origin = Uri::from_str(default_endpoint).map_err(HostError::Uri)?;
    let default_host = default_origin
        .authority()
        .expect("missing authority in default endpoint")
        .host()
        .to_string();

    let Some(endpoint) = endpoint else {
        return Ok((default_origin, default_host));
    };
    let custom_origin = Uri::from_str(endpoint).map_err(HostError::Uri)?;
    let custom_host = custom_origin
        .authority()
        .ok_or_else(|| HostError::MissingAuthority(endpoint.to_string()))?
        .host()
        .to_string();
    let (Some(prefix), Some(service)) = (
        custom_host.strip_suffix(".googleapis.com"),
        default_host.strip_suffix(".googleapis.com"),
    ) else {
        return Ok((default_origin, default_host));
    };
    let parts: Vec<&str> = prefix.split(".").collect();
    match &parts[..] {
        // This is a regional endpoint. It should be used as the host.
        // `{service}.{region}.rep.googleapis.com`
        [s, _, "rep"] if *s == service => Ok((custom_origin, custom_host)),
        // This is a locational endpoint. It should be used as the host.
        // `{region}-{service}.googleapis.com`
        [location]
            if location
                .strip_suffix(service)
                .and_then(|s| s.strip_suffix("-"))
                .is_some_and(|s| !s.is_empty()) =>
        {
            Ok((custom_origin, custom_host))
        }
        _ => Ok((default_origin, default_host)),
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum HostError {
    #[error("one of the URIs was invalid {0}")]
    Uri(http::uri::InvalidUri),
    #[error("missing authority in endpoint: {0}")]
    MissingAuthority(String),
}

impl HostError {
    pub fn client_builder(self) -> BuilderError {
        match self {
            Self::Uri(e) => BuilderError::transport(e),
            Self::MissingAuthority(e) => BuilderError::transport(Self::error_message(e)),
        }
    }

    #[cfg(test)]
    pub fn gax(self) -> Error {
        match self {
            Self::Uri(e) => Error::io(e),
            Self::MissingAuthority(e) => Error::io(Self::error_message(e)),
        }
    }

    fn error_message(endpoint: String) -> String {
        format!("missing authority in endpoint: {endpoint}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error as _;
    use test_case::test_case;

    #[test_case("http://www.googleapis.com", "test.googleapis.com"; "global")]
    #[test_case("http://private.googleapis.com", "test.googleapis.com"; "VPC-SC private")]
    #[test_case("http://restricted.googleapis.com", "test.googleapis.com"; "VPC-SC restricted")]
    #[test_case("http://test-my-private-ep.p.googleapis.com", "test.googleapis.com"; "PSC custom endpoint")]
    #[test_case("https://us-central1-test.googleapis.com", "us-central1-test.googleapis.com"; "locational endpoint")]
    #[test_case("https://us-central1-wrong.gooogleapis.com", "test.googleapis.com"; "locational but with bad service")]
    #[test_case("https://us-central1test.gooogleapis.com", "test.googleapis.com"; "maybe locational with missing dash")]
    #[test_case("https://-test.gooogleapis.com", "test.googleapis.com"; "maybe locational with missing location")]
    #[test_case("https://test.us-central1.rep.googleapis.com", "test.us-central1.rep.googleapis.com"; "regional endpoint")]
    #[test_case("https://test.my-universe-domain.com", "test.googleapis.com"; "universe domain")]
    #[test_case("localhost:5678", "test.googleapis.com"; "emulator")]
    #[test_case("https://localhost:5678", "test.googleapis.com"; "emulator with scheme")]
    fn header_success(input: &str, want: &str) -> anyhow::Result<()> {
        let got = header(Some(input), "https://test.googleapis.com")?;
        assert_eq!(got, want, "input={input:?}");
        Ok(())
    }

    #[test_case("https://service.googleapis.com", "service.googleapis.com")]
    #[test_case("http://service.googleapis.com", "service.googleapis.com")]
    #[test_case("https://storage.googleapis.com/", "storage.googleapis.com")]
    #[test_case("http://storage.googleapis.com/", "storage.googleapis.com")]
    #[test_case("test.googleapis.com", "test.googleapis.com")]
    #[test_case("localhost:5678", "localhost"; "emulator")]
    #[test_case("https://localhost:5678", "localhost"; "emulator with scheme")]
    fn header_default(input: &str, want: &str) -> anyhow::Result<()> {
        let got = header(None, input)?;
        assert_eq!(got, want, "input={input:?}");
        Ok(())
    }

    #[test_case("http://www.googleapis.com", "https://test.googleapis.com"; "global")]
    #[test_case("http://private.googleapis.com", "https://test.googleapis.com"; "VPC-SC private")]
    #[test_case("http://restricted.googleapis.com", "https://test.googleapis.com"; "VPC-SC restricted")]
    #[test_case("http://test-my-private-ep.p.googleapis.com", "https://test.googleapis.com"; "PSC custom endpoint")]
    #[test_case("https://us-central1-test.googleapis.com", "https://us-central1-test.googleapis.com"; "locational endpoint")]
    #[test_case("https://us-central1-wrong.gooogleapis.com", "https://test.googleapis.com"; "locational but with bad service")]
    #[test_case("https://us-central1test.gooogleapis.com", "https://test.googleapis.com"; "maybe locational with missing dash")]
    #[test_case("https://-test.gooogleapis.com", "https://test.googleapis.com"; "maybe locational with missing location")]
    #[test_case("https://test.us-central1.rep.googleapis.com", "https://test.us-central1.rep.googleapis.com"; "regional endpoint")]
    #[test_case("https://test.my-universe-domain.com", "https://test.googleapis.com"; "universe domain")]
    #[test_case("localhost:5678", "https://test.googleapis.com"; "emulator")]
    #[test_case("http://localhost:5678", "https://test.googleapis.com"; "emulator with scheme")]
    fn origin_success(input: &str, want: &str) -> anyhow::Result<()> {
        let got = origin(Some(input), "https://test.googleapis.com")?;
        assert_eq!(got, want, "input={input:?}");
        Ok(())
    }

    #[test_case("https://service.googleapis.com", "https://service.googleapis.com")]
    #[test_case("http://service.googleapis.com", "http://service.googleapis.com")]
    #[test_case("https://storage.googleapis.com/", "https://storage.googleapis.com")]
    #[test_case("http://storage.googleapis.com/", "http://storage.googleapis.com")]
    #[test_case("test.googleapis.com", "test.googleapis.com")]
    #[test_case("https://localhost:5678", "https://localhost:5678")]
    #[test_case("http://localhost:5678", "http://localhost:5678")]
    fn origin_default(input: &str, want: &str) -> anyhow::Result<()> {
        let got = origin(None, input)?;
        assert_eq!(got, want, "input={input:?}");
        Ok(())
    }

    #[test]
    fn errors() {
        let got = origin_and_header(Some("https:///a/b/c"), "https://test.googleapis.com");
        assert!(matches!(got, Err(HostError::Uri(_))), "{got:?}");
        let got = origin_and_header(Some("/a/b/c"), "https://test.googleapis.com");
        assert!(
            matches!(got, Err(HostError::MissingAuthority(ref e)) if e == "/a/b/c"),
            "{got:?}"
        );
        let got = origin_and_header(None, "https:///");
        assert!(matches!(got, Err(HostError::Uri(_))), "{got:?}");
    }

    #[test]
    fn uri_as_builder() {
        let p = Uri::from_str("https:///a/b/c").unwrap_err();
        let got = HostError::Uri(p).client_builder();
        assert!(got.is_transport(), "{got:?}");
        let source = got.source();
        assert!(
            source
                .and_then(|e| e.downcast_ref::<http::uri::InvalidUri>())
                .is_some(),
            "{got:?}"
        );
    }

    #[test]
    fn uri_as_gax() {
        let p = Uri::from_str("https:///a/b/c").unwrap_err();
        let got = HostError::Uri(p).gax();
        assert!(got.is_io(), "{got:?}");
        let source = got.source();
        assert!(
            source
                .and_then(|e| e.downcast_ref::<http::uri::InvalidUri>())
                .is_some(),
            "{got:?}"
        );
    }

    #[test]
    fn missing_authority_as_builder() {
        let got = HostError::MissingAuthority("a".to_string()).client_builder();
        assert!(got.is_transport(), "{got:?}");
        let source = got.source();
        assert!(source.is_some(), "{got:?}");
    }

    #[test]
    fn missing_authority_as_gax() {
        let got = HostError::MissingAuthority("a".to_string()).gax();
        assert!(got.is_io(), "{got:?}");
        let source = got.source();
        assert!(source.is_some(), "{got:?}");
    }
}
