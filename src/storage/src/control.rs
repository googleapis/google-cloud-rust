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

pub mod builder {
    pub use crate::generated::gapic::builder::storage_control::*;
    pub use crate::generated::gapic_control::builder::storage_control::*;
}
pub mod model {
    pub use crate::generated::gapic::model::*;
    pub use crate::generated::gapic_control::model::*;
}
pub mod client;
/// Traits to mock the clients in this library.
///
/// Application developers may need to mock the clients in this library to test
/// how their application works with different (and sometimes hard to trigger)
/// client and service behavior. Such test can define mocks implementing the
/// trait(s) defined in this module, initialize the client with an instance of
/// this mock in their tests, and verify their application responds as expected.
pub use generated::stub;

mod convert;
mod status;

mod generated;
