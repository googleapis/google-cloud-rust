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

//! Implementation details for Google Cloud clients.
//!
//! All the types, traits, and functions defined in this crate are **not**
//! intended for general use. This crate will remain unstable for the
//! foreseeable future, even if used in the implementation for stable client
//! libraries. We (the Google Cloud Client Libraries for Rust team) control
//! both and will change both if needed.
//!
//! The types, traits, and functions defined in this crate are undocumented.
//! This is intentional, as they are not intended for general use and will be
//! changed without notice.

#[doc(hidden)]
pub mod path_parameter;

#[doc(hidden)]
pub mod query_parameter;

#[doc(hidden)]
pub mod http;

// TODO(#1539) - remove these once all the generated clients use http::
#[doc(hidden)]
pub use http::NoBody;
#[doc(hidden)]
pub use http::ReqwestClient;
