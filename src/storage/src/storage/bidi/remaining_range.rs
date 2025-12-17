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

use super::normalized_range::NormalizedRange;
use crate::error::ReadError;
use crate::google::storage::v2::ReadRange as ProtoRange;
use crate::model_ext::RequestedRange;

type ReadResult<T> = std::result::Result<T, ReadError>;

/// Tracks the remaining range.
///
/// [ActiveRead][super::active_read::ActiveRead] is initialized with the
/// requested range. The range is normalized when the first response arrives.
/// Both the normalized and initial ranges must be usable to resume connections.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RemainingRange {
    Requested(RequestedRange),
    Normalized(NormalizedRange),
}

impl RemainingRange {
    pub fn update(&mut self, response: ProtoRange) -> ReadResult<()> {
        match self {
            Self::Normalized(segment) => segment.update(response)?,
            Self::Requested(range) => {
                let mut segment = Self::normalize(*range, response)?;
                segment.update(response)?;
                *self = Self::Normalized(segment);
            }
        };
        Ok(())
    }

    fn normalize(current: RequestedRange, response: ProtoRange) -> ReadResult<NormalizedRange> {
        match current {
            RequestedRange::Tail(_) => NormalizedRange::new(response.read_offset),

            RequestedRange::Offset(offset) if response.read_offset as u64 != offset => Err(
                ReadError::bidi_out_of_order(offset as i64, response.read_offset),
            ),
            RequestedRange::Offset(_) => NormalizedRange::new(response.read_offset),

            RequestedRange::Segment { limit, .. }
                if response.read_length as u64 > limit && limit != 0 =>
            {
                Err(ReadError::LongRead {
                    got: response.read_length as u64,
                    expected: limit,
                })
            }
            RequestedRange::Segment { offset, .. } if response.read_offset as u64 != offset => Err(
                ReadError::bidi_out_of_order(offset as i64, response.read_offset),
            ),
            RequestedRange::Segment { limit: 0_u64, .. } => {
                NormalizedRange::new(response.read_offset)
            }
            RequestedRange::Segment { limit, .. } => NormalizedRange::new(response.read_offset)?
                .with_length(limit.clamp(0, i64::MAX as u64) as i64),
        }
    }

    pub fn as_proto(&self, id: i64) -> ProtoRange {
        match self {
            Self::Requested(r) => r.as_proto(id),
            Self::Normalized(s) => s.as_proto(id),
        }
    }

    pub fn handle_empty(&self, end: bool) -> ReadResult<()> {
        match self {
            Self::Normalized(s) => s.handle_empty(end),
            Self::Requested(_) => unreachable!("always called after update()"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::proto_range;
    use super::*;
    use crate::model_ext::ReadRange;
    use test_case::test_case;

    #[test_case(ReadRange::all(), proto_range(0, 100), proto_range(100, 0))]
    #[test_case(ReadRange::offset(1000), proto_range(1000, 100), proto_range(1100, 0))]
    #[test_case(ReadRange::tail(1000), proto_range(2000, 100), proto_range(2100, 0))]
    #[test_case(ReadRange::head(1000), proto_range(0, 100), proto_range(100, 900))]
    #[test_case(
        ReadRange::segment(1000, 2000),
        proto_range(1000, 100),
        proto_range(1100, 1900)
    )]
    #[test_case(
        ReadRange::segment(1000, 0),
        proto_range(1000, 100),
        proto_range(1100, 0)
    )]
    fn initial_update(
        input: ReadRange,
        update: ProtoRange,
        want: ProtoRange,
    ) -> anyhow::Result<()> {
        let mut remaining = RemainingRange::Requested(input.0);
        remaining.update(update)?;
        assert_eq!(remaining.as_proto(0), want, "{remaining:?}");
        Ok(())
    }

    #[test_case(NormalizedRange::new(100).unwrap(), proto_range(100, 25), proto_range(125, 0))]
    #[test_case(NormalizedRange::new(100).unwrap().with_length(200).unwrap(), proto_range(100, 25), proto_range(125, 175))]
    fn following_updates(
        input: NormalizedRange,
        update: ProtoRange,
        want: ProtoRange,
    ) -> anyhow::Result<()> {
        let mut remaining = RemainingRange::Normalized(input);
        remaining.update(update)?;
        assert_eq!(remaining.as_proto(0), want, "{remaining:?}");
        Ok(())
    }

    #[test]
    fn initial_update_errors() {
        let mut remaining = RemainingRange::Requested(ReadRange::offset(100).0);
        let result = remaining.update(proto_range(200, 25));
        assert!(
            matches!(result, Err(ReadError::InvalidBidiStreamingReadResponse(_))),
            "{result:?}"
        );

        let mut remaining = RemainingRange::Requested(ReadRange::segment(100, 200).0);
        let result = remaining.update(proto_range(200, 25));
        assert!(
            matches!(result, Err(ReadError::InvalidBidiStreamingReadResponse(_))),
            "{result:?}"
        );

        let mut remaining = RemainingRange::Requested(ReadRange::segment(100, 200).0);
        let result = remaining.update(proto_range(100, 400));
        assert!(
            matches!(
                result,
                Err(ReadError::LongRead { got, expected }) if got == 400 && expected == 200
            ),
            "{result:?}"
        );
    }

    #[test]
    fn following_update_errors() -> anyhow::Result<()> {
        let mut remaining = RemainingRange::Normalized(NormalizedRange::new(100)?);
        let result = remaining.update(proto_range(200, 25));
        assert!(
            matches!(result, Err(ReadError::InvalidBidiStreamingReadResponse(_))),
            "{result:?}"
        );

        let mut remaining =
            RemainingRange::Normalized(NormalizedRange::new(100)?.with_length(100)?);
        let result = remaining.update(proto_range(100, 200));
        assert!(
            matches!(
                result,
                Err(ReadError::LongRead { got, expected }) if got == 200 && expected == 100
            ),
            "{result:?}"
        );
        Ok(())
    }

    #[test_case(ReadRange::all(), proto_range(0, 0))]
    #[test_case(ReadRange::tail(100), proto_range(-100, 0))]
    #[test_case(ReadRange::offset(100), proto_range(100, 0))]
    #[test_case(ReadRange::head(100), proto_range(0, 100))]
    #[test_case(ReadRange::segment(100, 200), proto_range(100, 200))]
    fn as_proto_requested(input: ReadRange, want: ProtoRange) {
        let got = RemainingRange::Requested(input.0).as_proto(0);
        assert_eq!(got, want);
    }

    #[test]
    fn handle_empty() -> anyhow::Result<()> {
        let normalized = NormalizedRange::new(100)?.with_length(50)?;
        let remaining = RemainingRange::Normalized(normalized);
        let result = remaining.handle_empty(false);
        assert!(result.is_ok(), "{result:?}");
        let result = remaining.handle_empty(true);
        assert!(matches!(result, Err(ReadError::ShortRead(_))), "{result:?}");

        let normalized = NormalizedRange::new(100)?;
        let remaining = RemainingRange::Normalized(normalized);
        let result = remaining.handle_empty(false);
        assert!(result.is_ok(), "{result:?}");
        let result = remaining.handle_empty(true);
        assert!(result.is_ok(), "{result:?}");
        Ok(())
    }
}
