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

use prost::{Enumeration, Message};

/// Serialized `FileDescriptorSet` containing `test.SingerInfo` and `test.Genre`.
///
/// Generated with `protoc` from the following definition (`singer.proto`):
/// ```text
/// syntax = "proto3";
/// package test;
///
/// message SingerInfo {
///   int64 singer_id = 1;
///   string nationality = 2;
/// }
///
/// enum Genre {
///   GENRE_UNSPECIFIED = 0;
///   POP = 1;
///   ROCK = 2;
///   JAZZ = 3;
/// }
/// ```
/// Generated via:
/// `protoc --include_imports --descriptor_set_out=descriptors.pb singer.proto`
pub const DESCRIPTOR_BYTES: &[u8] = &[
    10, 166, 1, 10, 12, 115, 105, 110, 103, 101, 114, 46, 112, 114, 111, 116, 111, 18, 4, 116, 101,
    115, 116, 34, 75, 10, 10, 83, 105, 110, 103, 101, 114, 73, 110, 102, 111, 18, 27, 10, 9, 115,
    105, 110, 103, 101, 114, 95, 105, 100, 24, 1, 32, 1, 40, 3, 82, 8, 115, 105, 110, 103, 101,
    114, 73, 100, 18, 32, 10, 11, 110, 97, 116, 105, 111, 110, 97, 108, 105, 116, 121, 24, 2, 32,
    1, 40, 9, 82, 11, 110, 97, 116, 105, 111, 110, 97, 108, 105, 116, 121, 42, 59, 10, 5, 71, 101,
    110, 114, 101, 18, 21, 10, 17, 71, 69, 78, 82, 69, 95, 85, 78, 83, 80, 69, 67, 73, 70, 73, 69,
    68, 16, 0, 18, 7, 10, 3, 80, 79, 80, 16, 1, 18, 8, 10, 4, 82, 79, 67, 75, 16, 2, 18, 8, 10, 4,
    74, 65, 90, 90, 16, 3, 98, 6, 112, 114, 111, 116, 111, 51,
];

/// Protobuf message corresponding to `test.SingerInfo`.
#[derive(Clone, PartialEq, Message)]
pub struct SingerInfo {
    #[prost(int64, tag = "1")]
    pub singer_id: i64,
    #[prost(string, tag = "2")]
    pub nationality: String,
}

/// Protobuf enumeration corresponding to `test.Genre`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Enumeration)]
#[repr(i32)]
pub enum Genre {
    GenreUnspecified = 0,
    Pop = 1,
    Rock = 2,
    Jazz = 3,
}
