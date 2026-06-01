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

use crate::model::transaction_options::read_only::TimestampBound as ReadOnlyTimestampBound;
use std::fmt::Debug;

/// Use a timestamp bound to specify how to choose a timestamp at which a query should read data.
///
/// # Example
/// ```
/// # use google_cloud_spanner::{Spanner, TimestampBound};
/// # async fn test_doc() -> Result<(), google_cloud_spanner::Error> {
/// let client = Spanner::builder().build().await.unwrap();
/// let db = client.database_client("projects/p/instances/i/databases/d").build().await.unwrap();
///
/// let tx = db.single_use().with_timestamp_bound(TimestampBound::strong()).build();
/// # Ok(())
/// # }
/// ```
///
/// See <https://cloud.google.com/spanner/docs/timestamp-bounds> for more information.
#[derive(Clone, Debug)]
pub struct TimestampBound(pub(crate) ReadOnlyTimestampBound);

impl TimestampBound {
    /// Returns a strong timestamp bound. Strong reads are guaranteed to see the
    /// effects of all transactions that have committed before the start of the read.
    ///
    /// See [timestamp_bound_strong] for more information.
    ///
    /// [timestamp_bound_strong]: https://docs.cloud.google.com/spanner/docs/timestamp-bounds#strong
    pub fn strong() -> Self {
        Self(ReadOnlyTimestampBound::Strong(true))
    }

    /// Returns a timestamp bound for an exact timestamp. The data will be read as it was at the given timestamp.
    ///
    /// See [timestamp_bound_exact_staleness] for more information.
    ///
    /// [timestamp_bound_exact_staleness]: https://docs.cloud.google.com/spanner/docs/timestamp-bounds#exact_staleness
    pub fn read_timestamp<T>(timestamp: T) -> Self
    where
        T: TryInto<wkt::Timestamp>,
        T::Error: Debug,
    {
        Self::try_read_timestamp(timestamp).expect("timestamp out of range")
    }

    /// Returns a timestamp bound for an exact timestamp, returning an error if the timestamp is out of range.
    ///
    /// See [timestamp_bound_exact_staleness] for more information.
    ///
    /// [timestamp_bound_exact_staleness]: https://docs.cloud.google.com/spanner/docs/timestamp-bounds#exact_staleness
    pub fn try_read_timestamp<T>(timestamp: T) -> Result<Self, T::Error>
    where
        T: TryInto<wkt::Timestamp>,
    {
        let timestamp = timestamp.try_into()?;
        Ok(Self(ReadOnlyTimestampBound::ReadTimestamp(Box::new(
            timestamp,
        ))))
    }

    /// Returns a timestamp bound for a minimum read timestamp. The data will be read as it was at the
    /// given timestamp or later.
    ///
    /// See [timestamp_bound_bounded_staleness] for more information.
    ///
    /// [timestamp_bound_bounded_staleness]: https://docs.cloud.google.com/spanner/docs/timestamp-bounds#bounded_staleness
    pub fn min_read_timestamp<T>(timestamp: T) -> Self
    where
        T: TryInto<wkt::Timestamp>,
        T::Error: Debug,
    {
        Self::try_min_read_timestamp(timestamp).expect("timestamp out of range")
    }

    /// Returns a timestamp bound for a minimum read timestamp, returning an error if the timestamp is out of range.
    ///
    /// See [timestamp_bound_bounded_staleness] for more information.
    ///
    /// [timestamp_bound_bounded_staleness]: https://docs.cloud.google.com/spanner/docs/timestamp-bounds#bounded_staleness
    pub fn try_min_read_timestamp<T>(timestamp: T) -> Result<Self, T::Error>
    where
        T: TryInto<wkt::Timestamp>,
    {
        let timestamp = timestamp.try_into()?;
        Ok(Self(ReadOnlyTimestampBound::MinReadTimestamp(Box::new(
            timestamp,
        ))))
    }

    /// Returns a timestamp bound for an exact staleness. The data will be read as it was at the given timestamp
    /// calculated by the current server time minus the given duration.
    ///
    /// See [timestamp_bound_exact_staleness] for more information.
    ///
    /// [timestamp_bound_exact_staleness]: https://docs.cloud.google.com/spanner/docs/timestamp-bounds#exact_staleness
    pub fn exact_staleness<T>(duration: T) -> Self
    where
        T: TryInto<wkt::Duration>,
        T::Error: Debug,
    {
        Self::try_exact_staleness(duration).expect("duration out of range")
    }

    /// Returns a timestamp bound for an exact staleness, returning an error if the duration is out of range.
    ///
    /// See [timestamp_bound_exact_staleness] for more information.
    ///
    /// [timestamp_bound_exact_staleness]: https://docs.cloud.google.com/spanner/docs/timestamp-bounds#exact_staleness
    pub fn try_exact_staleness<T>(duration: T) -> Result<Self, T::Error>
    where
        T: TryInto<wkt::Duration>,
    {
        let duration = duration.try_into()?;
        Ok(Self(ReadOnlyTimestampBound::ExactStaleness(Box::new(
            duration,
        ))))
    }

    /// Returns a timestamp bound for a maximum staleness. The data will be read as it was at the
    /// current server time minus the given duration or later.
    ///
    /// See [timestamp_bound_bounded_staleness] for more information.
    ///
    /// [timestamp_bound_bounded_staleness]: https://docs.cloud.google.com/spanner/docs/timestamp-bounds#bounded_staleness
    pub fn max_staleness<T>(duration: T) -> Self
    where
        T: TryInto<wkt::Duration>,
        T::Error: Debug,
    {
        Self::try_max_staleness(duration).expect("duration out of range")
    }

    /// Returns a timestamp bound for a maximum staleness, returning an error if the duration is out of range.
    ///
    /// See [timestamp_bound_bounded_staleness] for more information.
    ///
    /// [timestamp_bound_bounded_staleness]: https://docs.cloud.google.com/spanner/docs/timestamp-bounds#bounded_staleness
    pub fn try_max_staleness<T>(duration: T) -> Result<Self, T::Error>
    where
        T: TryInto<wkt::Duration>,
    {
        let duration = duration.try_into()?;
        Ok(Self(ReadOnlyTimestampBound::MaxStaleness(Box::new(
            duration,
        ))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use time::macros::datetime;

    #[test]
    fn test_auto_traits() {
        static_assertions::assert_impl_all!(TimestampBound: Clone, Debug, Send, Sync);
    }

    #[test]
    fn test_strong() {
        let bound = TimestampBound::strong();
        assert!(matches!(bound.0, ReadOnlyTimestampBound::Strong(true)));
    }

    #[test]
    fn test_read_timestamp_methods() {
        let ts = datetime!(2026-03-09 18:00:00 UTC);

        // 1. OffsetDateTime
        let read = TimestampBound::read_timestamp(ts);
        assert!(matches!(
            read.0,
            ReadOnlyTimestampBound::ReadTimestamp(ref t) if t.seconds() == ts.unix_timestamp() && t.nanos() == ts.nanosecond() as i32
        ));

        let try_read = TimestampBound::try_read_timestamp(ts).expect("valid OffsetDateTime");
        assert!(matches!(
            try_read.0,
            ReadOnlyTimestampBound::ReadTimestamp(ref t) if t.seconds() == ts.unix_timestamp() && t.nanos() == ts.nanosecond() as i32
        ));

        // 2. wkt::Timestamp
        let wkt_ts = wkt::Timestamp::try_from(ts).expect("valid wkt timestamp");
        let read = TimestampBound::read_timestamp(wkt_ts);
        assert!(matches!(
            read.0,
            ReadOnlyTimestampBound::ReadTimestamp(ref t) if t.seconds() == ts.unix_timestamp() && t.nanos() == ts.nanosecond() as i32
        ));

        let try_read = TimestampBound::try_read_timestamp(wkt_ts).expect("valid wkt timestamp");
        assert!(matches!(
            try_read.0,
            ReadOnlyTimestampBound::ReadTimestamp(ref t) if t.seconds() == ts.unix_timestamp() && t.nanos() == ts.nanosecond() as i32
        ));

        // 3. SystemTime
        let system_time = std::time::SystemTime::from(ts);
        let read = TimestampBound::read_timestamp(system_time);
        assert!(matches!(
            read.0,
            ReadOnlyTimestampBound::ReadTimestamp(ref t) if t.seconds() == ts.unix_timestamp() && t.nanos() == ts.nanosecond() as i32
        ));

        let try_read = TimestampBound::try_read_timestamp(system_time).expect("valid SystemTime");
        assert!(matches!(
            try_read.0,
            ReadOnlyTimestampBound::ReadTimestamp(ref t) if t.seconds() == ts.unix_timestamp() && t.nanos() == ts.nanosecond() as i32
        ));
    }

    #[test]
    fn test_min_read_timestamp_methods() {
        let ts = datetime!(2026-03-09 18:00:00 UTC);

        // 1. OffsetDateTime
        let min_read = TimestampBound::min_read_timestamp(ts);
        assert!(matches!(
            min_read.0,
            ReadOnlyTimestampBound::MinReadTimestamp(ref t) if t.seconds() == ts.unix_timestamp() && t.nanos() == ts.nanosecond() as i32
        ));

        let try_min_read =
            TimestampBound::try_min_read_timestamp(ts).expect("valid OffsetDateTime");
        assert!(matches!(
            try_min_read.0,
            ReadOnlyTimestampBound::MinReadTimestamp(ref t) if t.seconds() == ts.unix_timestamp() && t.nanos() == ts.nanosecond() as i32
        ));

        // 2. wkt::Timestamp
        let wkt_ts = wkt::Timestamp::try_from(ts).expect("valid wkt timestamp");
        let min_read = TimestampBound::min_read_timestamp(wkt_ts);
        assert!(matches!(
            min_read.0,
            ReadOnlyTimestampBound::MinReadTimestamp(ref t) if t.seconds() == ts.unix_timestamp() && t.nanos() == ts.nanosecond() as i32
        ));

        let try_min_read =
            TimestampBound::try_min_read_timestamp(wkt_ts).expect("valid wkt timestamp");
        assert!(matches!(
            try_min_read.0,
            ReadOnlyTimestampBound::MinReadTimestamp(ref t) if t.seconds() == ts.unix_timestamp() && t.nanos() == ts.nanosecond() as i32
        ));

        // 3. SystemTime
        let system_time = std::time::SystemTime::from(ts);
        let min_read = TimestampBound::min_read_timestamp(system_time);
        assert!(matches!(
            min_read.0,
            ReadOnlyTimestampBound::MinReadTimestamp(ref t) if t.seconds() == ts.unix_timestamp() && t.nanos() == ts.nanosecond() as i32
        ));

        let try_min_read =
            TimestampBound::try_min_read_timestamp(system_time).expect("valid SystemTime");
        assert!(matches!(
            try_min_read.0,
            ReadOnlyTimestampBound::MinReadTimestamp(ref t) if t.seconds() == ts.unix_timestamp() && t.nanos() == ts.nanosecond() as i32
        ));
    }

    #[test]
    fn test_exact_staleness_methods() {
        let d = Duration::from_secs(60);

        // 1. std::time::Duration
        let exact = TimestampBound::exact_staleness(d);
        assert!(matches!(
            exact.0,
            ReadOnlyTimestampBound::ExactStaleness(ref t) if t.seconds() == 60 && t.nanos() == 0
        ));

        let try_exact = TimestampBound::try_exact_staleness(d).expect("valid std::time::Duration");
        assert!(matches!(
            try_exact.0,
            ReadOnlyTimestampBound::ExactStaleness(ref t) if t.seconds() == 60 && t.nanos() == 0
        ));

        // 2. wkt::Duration
        let wkt_d = wkt::Duration::try_from(d).expect("valid wkt duration");
        let exact = TimestampBound::exact_staleness(wkt_d);
        assert!(matches!(
            exact.0,
            ReadOnlyTimestampBound::ExactStaleness(ref t) if t.seconds() == 60 && t.nanos() == 0
        ));

        let try_exact = TimestampBound::try_exact_staleness(wkt_d).expect("valid wkt::Duration");
        assert!(matches!(
            try_exact.0,
            ReadOnlyTimestampBound::ExactStaleness(ref t) if t.seconds() == 60 && t.nanos() == 0
        ));
    }

    #[test]
    fn test_max_staleness_methods() {
        let d = Duration::from_secs(120);

        // 1. std::time::Duration
        let max = TimestampBound::max_staleness(d);
        assert!(matches!(
            max.0,
            ReadOnlyTimestampBound::MaxStaleness(ref t) if t.seconds() == 120 && t.nanos() == 0
        ));

        let try_max = TimestampBound::try_max_staleness(d).expect("valid std::time::Duration");
        assert!(matches!(
            try_max.0,
            ReadOnlyTimestampBound::MaxStaleness(ref t) if t.seconds() == 120 && t.nanos() == 0
        ));

        // 2. wkt::Duration
        let wkt_d = wkt::Duration::try_from(d).expect("valid wkt duration");
        let max = TimestampBound::max_staleness(wkt_d);
        assert!(matches!(
            max.0,
            ReadOnlyTimestampBound::MaxStaleness(ref t) if t.seconds() == 120 && t.nanos() == 0
        ));

        let try_max = TimestampBound::try_max_staleness(wkt_d).expect("valid wkt::Duration");
        assert!(matches!(
            try_max.0,
            ReadOnlyTimestampBound::MaxStaleness(ref t) if t.seconds() == 120 && t.nanos() == 0
        ));
    }
}
