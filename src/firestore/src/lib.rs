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

pub use gax::error::Error;
pub use gax::Result;
pub(crate) mod generated;

pub use generated::gapic::model;

pub(crate) mod google {
    pub mod firestore {
        #[allow(clippy::enum_variant_names)]
        pub mod v1 {
            include!("generated/protos/firestore/google.firestore.v1.rs");
            include!("generated/convert/firestore/convert.rs");
        }
    }
    pub mod rpc {
        include!("generated/protos/rpc/google.rpc.rs");
        include!("generated/convert/rpc/convert.rs");
    }
    pub mod r#type {
        // TODO(#1414) - decide if we want to generate this as its own directory.
        include!("generated/protos/firestore/google.r#type.rs");
        include!("generated/convert/type/convert.rs");
    }
}

mod convert;
pub mod status;
