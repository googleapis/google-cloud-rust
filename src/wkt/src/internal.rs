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

//! Implementation details provided by the `google-cloud-sdk` crate.
//!
//! These types are intended for developers of the Google Cloud client libraries
//! for Rust. They are undocumented and may change at any time.

#[macro_use]
mod visitor_32;
mod int32;
pub use int32::I32;
mod uint32;
pub use uint32::U32;

#[macro_use]
mod visitor_64;
mod int64;
pub use int64::I64;
mod uint64;
pub use uint64::U64;

mod value;
pub use value::OptionalValue;

#[macro_use]
mod visitor_float;
mod float32;
pub use float32::F32;
mod float64;
pub use float64::F64;

// For skipping serialization of default values of bool/numeric types.
pub fn is_default<T>(t: &T) -> bool
where
    T: Default + PartialEq,
{
    *t == T::default()
}

mod enums;
pub use enums::*;
