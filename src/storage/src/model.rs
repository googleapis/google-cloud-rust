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

//! The messages and enums that are part of this client library.

// Re-export all generated types
pub use crate::control::model::*;

// Custom types used in the hand-crafted code. We do not expect this name to
// conflict with generated types, that would require a `RequestHelpers` message
// with nested enums or messages. If we ever get a conflict, we would configure
// sidekick to rename the generated types.
pub mod request_helpers;

/// Define types related to the `v1` JSON protocol.
///
/// The client library uses the JSON protocol in its implementation. For the
/// most, the types are mapped to the v2 protocol (which is gRPC and Protobuf
/// based). Where no accurate mapping to a type in v2 is possible, the client
/// library returns a type in this module.
///
/// We anticipate no breaking changes when switching to the v2 protocol.
pub mod v1 {
    /// ObjectHighlights contains select metadata from a [crate::model::Object].
    #[derive(Clone, Debug, PartialEq)]
    #[non_exhaustive]
    pub struct ObjectHighlights {
        /// The content generation of this object. Used for object versioning.
        pub generation: i64,

        /// The version of the metadata for this generation of this
        /// object. Used for preconditions and for detecting changes in metadata. A
        /// metageneration number is only meaningful in the context of a particular
        /// generation of a particular object.
        pub metageneration: i64,

        /// Content-Length of the object data in bytes, matching [RFC 7230 §3.3.2].
        ///
        /// [rfc 7230 §3.3.2]: https://tools.ietf.org/html/rfc7230#section-3.3.2
        pub size: i64,

        /// Content-Encoding of the object data, matching [RFC 7231 §3.1.2.2].
        ///
        /// [rfc 7231 §3.1.2.2]: https://tools.ietf.org/html/rfc7231#section-3.1.2.2
        pub content_encoding: String,

        /// Hashes for the data part of this object. The checksums of the complete
        /// object regardless of data range. If the object is read in full, the
        /// client should compute one of these checksums over the read object and
        /// compare it against the value provided here.
        pub checksums: std::option::Option<crate::model::ObjectChecksums>,

        /// Storage class of the object.
        pub storage_class: String,

        /// Content-Language of the object data, matching [RFC 7231 §3.1.3.2].
        ///
        /// [rfc 7231 §3.1.3.2]: https://tools.ietf.org/html/rfc7231#section-3.1.3.2
        pub content_language: String,

        /// Content-Type of the object data, matching [RFC 7231 §3.1.1.5]. If an
        /// object is stored without a Content-Type, it is served as
        /// `application/octet-stream`.
        ///
        /// [rfc 7231 §3.1.1.5]: https://tools.ietf.org/html/rfc7231#section-3.1.1.5
        pub content_type: String,

        /// Content-Disposition of the object data, matching [RFC 6266].
        ///
        /// [rfc 6266]: https://tools.ietf.org/html/rfc6266
        pub content_disposition: String,

        /// The etag of the object.
        pub etag: String,
    }
}
