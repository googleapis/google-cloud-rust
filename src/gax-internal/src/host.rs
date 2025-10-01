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

use gax::client_builder::Error as BuilderError;
use http::Uri;
use std::str::FromStr;

/// Calculate the host based on the configured endpoint and default endpoint.
///
/// Notably, locational and regional endpoints are detected and used as the
/// host. For VIPs and private networks, we need to use the default host.
pub(crate) fn host_from_endpoint(
    endpoint: Option<&str>,
    default_endpoint: &str,
) -> gax::client_builder::Result<String> {
    origin_from_endpoint(endpoint, default_endpoint).and_then(|uri| {
        Ok(uri
            .authority()
            .ok_or_else(|| BuilderError::transport("missing authority in endpoint"))?
            .host()
            .to_string())
    })
}

/// Calculate the origin to use in gRPC requests based on the configured
/// endpoint and default endpoint.
///
/// Notably, locational and regional endpoints are detected and used as the
/// origin. For VIPs and private networks, we need to use the default origin.
pub(crate) fn origin_from_endpoint(
    endpoint: Option<&str>,
    default_endpoint: &str,
) -> gax::client_builder::Result<Uri> {
    let default_origin = Uri::from_str(default_endpoint).map_err(BuilderError::transport)?;
    let default_host = default_origin
        .authority()
        .expect("default endpoint is well-formed")
        .host()
        .to_string();

    if let Some(endpoint) = endpoint {
        let custom_origin = Uri::from_str(endpoint).map_err(BuilderError::transport)?;
        let custom_host = custom_origin
            .authority()
            .ok_or_else(|| BuilderError::transport("missing authority in endpoint"))?
            .host()
            .to_string();
        if let (Some(prefix), Some(service)) = (
            custom_host.strip_suffix(".googleapis.com"),
            default_host.strip_suffix(".googleapis.com"),
        ) {
            let parts: Vec<&str> = prefix.split(".").collect();
            if parts.len() == 3 && parts[0] == service && parts[2] == "rep" {
                // This is a regional endpoint. It should be used as the host.
                // `{service}.{region}.rep.googleapis.com`
                return Ok(custom_origin);
            }
            if parts.len() == 1 && parts[0].ends_with(&format!("-{service}")) {
                // This is a locational endpoint. It should be used as the host.
                // `{region}-{service}.googleapis.com`
                return Ok(custom_origin);
            }
        }
    }
    Ok(default_origin)
}

#[cfg(test)]
mod tests {
    use test_case::test_case;

    #[test_case(None, "test.googleapis.com"; "default")]
    #[test_case(Some("http://www.googleapis.com"), "test.googleapis.com"; "global")]
    #[test_case(Some("http://private.googleapis.com"), "test.googleapis.com"; "VPC-SC private")]
    #[test_case(Some("http://restricted.googleapis.com"), "test.googleapis.com"; "VPC-SC restricted")]
    #[test_case(Some("http://test-my-private-ep.p.googleapis.com"), "test.googleapis.com"; "PSC custom endpoint")]
    #[test_case(Some("https://us-central1-test.googleapis.com"), "us-central1-test.googleapis.com"; "locational endpoint")]
    #[test_case(Some("https://test.us-central1.rep.googleapis.com"), "test.us-central1.rep.googleapis.com"; "regional endpoint")]
    #[test_case(Some("https://test.my-universe-domain.com"), "test.googleapis.com"; "universe domain")]
    #[test_case(Some("localhost:5678"), "test.googleapis.com"; "emulator")]
    fn host_from_endpoint(
        custom_endpoint: Option<&str>,
        expected_host: &str,
    ) -> anyhow::Result<()> {
        let host = super::host_from_endpoint(custom_endpoint, "https://test.googleapis.com/")?;
        assert_eq!(host, expected_host);

        // Rarely, (I think only in GCS), does the default endpoint end without
        // a `/`. Make sure everything still works.
        let host = super::host_from_endpoint(custom_endpoint, "https://test.googleapis.com")?;
        assert_eq!(host, expected_host);

        Ok(())
    }

    #[test_case(None; "default")]
    #[test_case(Some("localhost:5678"); "custom")]
    fn host_from_endpoint_showcase(custom_endpoint: Option<&str>) -> anyhow::Result<()> {
        let host = super::host_from_endpoint(custom_endpoint, "https://localhost:7469/")?;
        assert_eq!(host, "localhost");
        Ok(())
    }

    #[test]
    fn host_from_endpoint_error() -> anyhow::Result<()> {
        let err = super::host_from_endpoint(
            Some("/bad/endpoint/no/host"),
            "https://test.googleapis.com/",
        );
        assert!(matches!(&err, Err(e) if e.is_transport()), "{err:?}");
        Ok(())
    }
}
