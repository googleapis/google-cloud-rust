// Copyright 2026 Google LLC
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

// no-prefer-dynamic
#![crate_type = "lib"]
#![allow(dead_code)]

// Stand-in for `google_cloud_gax_internal`: defines the traits and some impls
// for foreign types.
pub mod prost {
    pub type Result<T> = std::result::Result<T, ()>;

    pub trait ToProto<T>: Sized {
        type Output;
        fn to_proto(self) -> Result<Self::Output>;
    }

    pub trait FromProto<T>: Sized {
        fn cnv(self) -> Result<T>;
    }

    impl ToProto<String> for String {
        type Output = String;
        fn to_proto(self) -> Result<String> {
            Ok(self)
        }
    }
    impl FromProto<String> for String {
        fn cnv(self) -> Result<String> {
            Ok(self)
        }
    }
    impl ToProto<i32> for i32 {
        type Output = i32;
        fn to_proto(self) -> Result<i32> {
            Ok(self)
        }
    }

    // Calls inside the defining crate (not compared by the test harness; the
    // defining-crate case is covered by `ui_defining_crate`).
    pub fn inside(s: String) {
        let _ = s.to_proto();
    }
}

/// A type that is foreign to the downstream crate (think `wkt::Timestamp`).
pub struct Foreign;
pub struct ForeignProto;

impl prost::ToProto<ForeignProto> for Foreign {
    type Output = ForeignProto;
    fn to_proto(self) -> prost::Result<ForeignProto> {
        Ok(ForeignProto)
    }
}
