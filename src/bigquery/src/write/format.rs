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

mod arrow;
mod proto;

/// The data format accepted by a writer.
///
/// This trait is sealed and cannot be implemented for types outside this crate.
pub trait DataFormat: sealed::DataFormat {
    type Rows;
}

pub(super) mod sealed {
    use crate::model::AppendRowsRequest;

    pub trait DataFormat {
        fn make_request(&self, write_stream: &str, rows: Self::Rows) -> AppendRowsRequest
        where
            Self: super::DataFormat;
    }
}

pub use arrow::Arrow;
pub(crate) use proto::Proto;
