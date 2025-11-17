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

use crate::error::ReadError;
use crate::google::storage::v2::ReadRange as ProtoRange;
use crate::model_ext::RequestedRange;

type ReadResult<T> = std::result::Result<T, ReadError>;

/// Additional functions to use `RequestedRange` in pending requests.
///
/// [PendingRange][super::pending_range::PendingRange] is initialized with
/// the requested ranges. While these are normalized when the first response
/// arrives, they must be usable to resume connections.
impl RequestedRange {
    pub fn as_proto(&self, id: i64) -> ProtoRange {
        match self {
            Self::Offset(o) => ProtoRange {
                read_id: id,
                read_offset: *o as i64,
                ..ProtoRange::default()
            },
            Self::Tail(o) => ProtoRange {
                read_id: id,
                read_offset: -(*o as i64),
                ..ProtoRange::default()
            },
            Self::Segment { offset, limit } => ProtoRange {
                read_id: id,
                read_offset: *offset as i64,
                read_length: *limit as i64,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model_ext::ReadRange;
    use test_case::test_case;

    #[test_case(ReadRange::all().0, ProtoRange::default())]
    #[test_case(ReadRange::offset(100).0, ProtoRange { read_offset: 100, ..ProtoRange::default()})]
    #[test_case(ReadRange::head(100).0, ProtoRange { read_offset: 0, read_length: 100, ..ProtoRange::default()})]
    #[test_case(ReadRange::tail(100).0, ProtoRange { read_offset: -100, ..ProtoRange::default()})]
    #[test_case(ReadRange::segment(100, 50).0, ProtoRange { read_offset: 100, read_length: 50, ..ProtoRange::default()})]
    fn as_proto(input: RequestedRange, want: ProtoRange) {
        let mut want = want;
        want.read_id = 123456;
        let got = input.as_proto(123456);
        assert_eq!(got, want);
    }
}
