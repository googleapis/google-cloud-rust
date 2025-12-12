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

/// Re-export types from the `http` crate used in this module.
pub mod http {
    /// HTTP method for the signed URL.
    pub use http::Method;
}

/// Formatting style for signed URLs.
///
/// There are several equivalent formats for signed URLs, see the [resource path] docs for more information.
///
/// [resource path]: https://docs.cloud.google.com/storage/docs/authentication/canonical-requests#about-resource-path
#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
pub enum UrlStyle {
    /// Path style URL: `https://storage.googleapis.com/bucket/object`.
    ///
    /// This is the default style.
    #[default]
    PathStyle,

    /// Bucket bound hostname URL: `https://bucket-name/object`.
    ///
    /// This style is used when you have a CNAME alias for your bucket.
    BucketBoundHostname,

    /// Virtual hosted style URL: `https://bucket.storage.googleapis.com/object`.
    VirtualHostedStyle,
}
