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

//! Telemetry header helpers.

/// Generated libraries create one static instance of this struct and use it
/// to lazy initialize (via [std::sync::LazyLock]) the x-goog-api-client header
/// value.
#[derive(Debug, PartialEq)]
pub struct XGoogApiClient {
    pub name: &'static str,
    pub library_type: &'static str,
    pub version: &'static str,
}

pub const GAPIC: &str = "gapic";
pub const GCCL: &str = "gccl";

mod build_info {
    // The file has been placed there by the build script.
    include!(concat!(env!("OUT_DIR"), "/build_env.rs"));

    pub(crate) const PKG_VERSION: &str = env!("CARGO_PKG_VERSION");
}

impl XGoogApiClient {
    /// Format the struct as needed for the `x-goog-api-client` header.
    pub fn rest_header_value(&self) -> String {
        // Strip out the initial "rustc " string from `RUSTC_VERSION`. If not
        // found, leave RUSTC_VERSION unchanged.
        let rustc_version = build_info::RUSTC_VERSION;
        let rustc_version = rustc_version
            .strip_prefix("rustc ")
            .unwrap_or(build_info::RUSTC_VERSION);

        // Capture the gax version too.
        let gax_version = build_info::PKG_VERSION;

        format!(
            "gl-rust/{rustc_version} gax/{gax_version} rest/{gax_version}-reqwest {}/{}",
            self.library_type, self.version
        )
    }

    /// Format the struct as needed for the `x-goog-api-client` header.
    pub fn grpc_header_value(&self) -> String {
        // Strip out the initial "rustc " string from `RUSTC_VERSION`. If not
        // found, leave RUSTC_VERSION unchanged.
        let rustc_version = build_info::RUSTC_VERSION;
        let rustc_version = rustc_version
            .strip_prefix("rustc ")
            .unwrap_or(build_info::RUSTC_VERSION);

        // Capture the gax version too.
        let gax_version = build_info::PKG_VERSION;

        format!(
            "gl-rust/{rustc_version} gax/{gax_version} grpc/{gax_version}-tonic {}/{}",
            self.library_type, self.version
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::collections::HashMap;

    fn breakdown(formatted: &str) -> HashMap<String, String> {
        formatted
            .split(" ")
            .filter_map(|v| v.find('/').map(|i| v.split_at(i)))
            .map(|(k, v)| (k, &v[1..]))
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn test_format_rest() {
        let header = XGoogApiClient {
            name: "unused",
            version: "1.2.3",
            library_type: GCCL,
        };
        let fields = breakdown(header.rest_header_value().as_str());

        let got = fields.get(GCCL).map(String::to_owned);
        assert_eq!(got.as_deref(), Some("1.2.3"));

        let got = fields.get("gax").map(String::to_owned);
        assert_eq!(got.as_deref(), Some(build_info::PKG_VERSION));

        let got = fields.get("grpc");
        assert!(got.is_none(), "{fields:?}");

        let got = fields.get("rest");
        assert!(got.is_some(), "{fields:?}");

        let got = fields.get("gl-rust").map(String::to_owned);
        let want = build_info::RUSTC_VERSION;
        assert!(
            got.as_ref()
                .map(|s| want.contains(s) && !s.is_empty())
                .unwrap_or(false),
            "mismatched rustc version {want} and {got:?}"
        );
    }

    #[test]
    fn test_format_grpc() {
        let header = XGoogApiClient {
            name: "unused",
            version: "1.2.3",
            library_type: GCCL,
        };
        let fields = breakdown(header.grpc_header_value().as_str());

        let got = fields.get(GCCL).map(String::to_owned);
        assert_eq!(got.as_deref(), Some("1.2.3"));

        let got = fields.get("gax").map(String::to_owned);
        assert_eq!(got.as_deref(), Some(build_info::PKG_VERSION));

        let got = fields.get("grpc");
        assert!(got.is_some(), "{fields:?}");

        let got = fields.get("rest");
        assert!(got.is_none(), "{fields:?}");

        let got = fields.get("gl-rust").map(String::to_owned);
        let want = build_info::RUSTC_VERSION;
        assert!(
            got.as_ref()
                .map(|s| want.contains(s) && !s.is_empty())
                .unwrap_or(false),
            "mismatched rustc version {want} and {got:?}"
        );
    }
}
