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

//! Well-known-types for Google Cloud APIs.
//!
//! Google Cloud APIs use a number of well-known types. These typically have
//! custom JSON encoding, and may provide conversion functions to and from
//! native or commonly used Rust types.

#![cfg_attr(docsrs, feature(doc_cfg))]

mod any;
pub use crate::any::*;
mod duration;
pub use crate::duration::*;
mod empty;
pub use crate::empty::*;
mod field_mask;
pub use crate::field_mask::*;
// The generated code contains (and uses) deprecated code.
#[allow(deprecated)]
mod generated;
#[cfg_attr(not(feature = "_internal-semver"), doc(hidden))]
pub mod internal;
pub use crate::generated::*;
mod timestamp;
pub use crate::timestamp::*;
mod rstruct;
pub use crate::rstruct::*;
mod wrappers;
pub use crate::wrappers::*;
pub mod message;
