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

//! This module contains common implementation details for generated code.
//!
//! It is not part of the public API of this crate. Types and functions in this
//! module may be changed or removed without notice. Applications should not use
//! any types or functions contained within.

mod aip151;
mod discovery;
pub use aip151::{
    Operation, new_poller, new_unit_metadata_poller, new_unit_poller, new_unit_response_poller,
};

pub use discovery::new_discovery_poller;
