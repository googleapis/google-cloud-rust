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

#[allow(dead_code)]
// TODO(#1813) - fix the broken link to `[here]`.
#[allow(rustdoc::broken_intra_doc_links)]
pub(crate) mod generated;

pub mod builder {
    pub use crate::control::generated::gapic::builder::storage_control::*;
    pub use crate::control::generated::gapic_control::builder::storage_control::*;
}
pub mod model {
    pub use crate::control::generated::gapic::model::*;
    pub use crate::control::generated::gapic_control::model::*;
}
pub mod client;
pub mod stub;

mod convert;
