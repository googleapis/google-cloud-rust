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

pub(crate) trait RequestParameter {}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("cannot format as request parameter {0:?}")]
    Format(Box<dyn std::error::Error + Send + Sync>),
}

impl RequestParameter for i32 {}
impl RequestParameter for i64 {}
impl RequestParameter for u32 {}
impl RequestParameter for u64 {}
impl RequestParameter for f32 {}
impl RequestParameter for f64 {}
impl RequestParameter for String {}
impl RequestParameter for bool {}
impl RequestParameter for bytes::Bytes {}
impl RequestParameter for wkt::Duration {}
impl RequestParameter for wkt::FieldMask {}
impl RequestParameter for wkt::Timestamp {}
