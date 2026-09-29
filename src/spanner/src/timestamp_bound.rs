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
use std::time::Duration as StdDuration;
use wkt::Duration;
use wkt::Timestamp;

/// Use a timestamp bound to specify how to choose a timestamp at which a query should read data.
///
/// # Example
/// ```
/// # use google_cloud_spanner::client::Spanner;
/// # use google_cloud_spanner::transaction::TimestampBound;
/// # async fn test_doc() -> Result<(), google_cloud_spanner::Error> {
/// let client = Spanner::builder().build().await.unwrap();
/// let db = client.database_client("projects/p/instances/i/databases/d").build().await.unwrap();
///
/// let tx = db.single_use().set_timestamp_bound(TimestampBound::strong()).build();
/// # Ok(())
/// # }
/// ```
///
/// See <https://cloud.google.com/spanner/docs/timestamp-bounds> for more information.
#[derive(Clone, Debug, PartialEq)]
pub struct TimestampBound(pub(crate) ReadOnlyTimestampBound);

impl Default for TimestampBound {
    fn default() -> Self {
        Self::strong()
    }
}

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
    /// For fallible conversion from types such as RFC 3339 strings, [`std::time::SystemTime`],
    /// or [`time::OffsetDateTime`], use [`try_read_timestamp`](Self::try_read_timestamp).
    ///
    /// See [timestamp_bound_read_timestamp] for more information.
    ///
    /// [timestamp_bound_read_timestamp]: https://docs.cloud.google.com/spanner/docs/timestamp-bounds#read_timestamp
    pub fn read_timestamp(timestamp: Timestamp) -> Self {
        Self(ReadOnlyTimestampBound::ReadTimestamp(Box::new(timestamp)))
    }

    /// Returns a timestamp bound for an exact timestamp, returning an error if the timestamp is out of range.
    ///
    /// See [timestamp_bound_read_timestamp] for more information.
    ///
    /// [timestamp_bound_read_timestamp]: https://docs.cloud.google.com/spanner/docs/timestamp-bounds#read_timestamp
    pub fn try_read_timestamp<T>(timestamp: T) -> Result<Self, T::Error>
    where
        T: TryInto<Timestamp>,
    {
        let timestamp = timestamp.try_into()?;
        Ok(Self::read_timestamp(timestamp))
    }

    /// Returns a timestamp bound for a minimum read timestamp. The data will be read as it was at the
    /// given timestamp or later.
    ///
    /// For fallible conversion from types such as RFC 3339 strings, [`std::time::SystemTime`],
    /// or [`time::OffsetDateTime`], use [`try_min_read_timestamp`](Self::try_min_read_timestamp).
    ///
    /// See [timestamp_bound_bounded_staleness] for more information.
    ///
    /// [timestamp_bound_bounded_staleness]: https://docs.cloud.google.com/spanner/docs/timestamp-bounds#bounded_staleness
    pub fn min_read_timestamp(timestamp: Timestamp) -> Self {
        Self(ReadOnlyTimestampBound::MinReadTimestamp(Box::new(
            timestamp,
        )))
    }

    /// Returns a timestamp bound for a minimum read timestamp, returning an error if the timestamp is out of range.
    ///
    /// See [timestamp_bound_bounded_staleness] for more information.
    ///
    /// [timestamp_bound_bounded_staleness]: https://docs.cloud.google.com/spanner/docs/timestamp-bounds#bounded_staleness
    pub fn try_min_read_timestamp<T>(timestamp: T) -> Result<Self, T::Error>
    where
        T: TryInto<Timestamp>,
    {
        let timestamp = timestamp.try_into()?;
        Ok(Self::min_read_timestamp(timestamp))
    }

    /// Returns a timestamp bound for an exact staleness. The data will be read as it was at the given timestamp
    /// calculated by the current server time minus the given duration.
    ///
    /// For fallible conversion from types such as [`wkt::Duration`], use
    /// [`try_exact_staleness`](Self::try_exact_staleness).
    ///
    /// See [timestamp_bound_exact_staleness] for more information.
    ///
    /// [timestamp_bound_exact_staleness]: https://docs.cloud.google.com/spanner/docs/timestamp-bounds#exact_staleness
    pub fn exact_staleness(duration: StdDuration) -> Self {
        Self(ReadOnlyTimestampBound::ExactStaleness(Box::new(
            to_clamped_duration(duration),
        )))
    }

    /// Returns a timestamp bound for an exact staleness, returning an error if the duration is out of range.
    ///
    /// See [timestamp_bound_exact_staleness] for more information.
    ///
    /// [timestamp_bound_exact_staleness]: https://docs.cloud.google.com/spanner/docs/timestamp-bounds#exact_staleness
    pub fn try_exact_staleness<T>(duration: T) -> Result<Self, T::Error>
    where
        T: TryInto<Duration>,
    {
        let duration = duration.try_into()?;
        Ok(Self(ReadOnlyTimestampBound::ExactStaleness(Box::new(
            duration,
        ))))
    }

    /// Returns a timestamp bound for a maximum staleness. The data will be read as it was at the
    /// current server time minus the given duration or later.
    ///
    /// For fallible conversion from types such as [`wkt::Duration`], use
    /// [`try_max_staleness`](Self::try_max_staleness).
    ///
    /// See [timestamp_bound_bounded_staleness] for more information.
    ///
    /// [timestamp_bound_bounded_staleness]: https://docs.cloud.google.com/spanner/docs/timestamp-bounds#bounded_staleness
    pub fn max_staleness(duration: StdDuration) -> Self {
        Self(ReadOnlyTimestampBound::MaxStaleness(Box::new(
            to_clamped_duration(duration),
        )))
    }

    /// Returns a timestamp bound for a maximum staleness, returning an error if the duration is out of range.
    ///
    /// See [timestamp_bound_bounded_staleness] for more information.
    ///
    /// [timestamp_bound_bounded_staleness]: https://docs.cloud.google.com/spanner/docs/timestamp-bounds#bounded_staleness
    pub fn try_max_staleness<T>(duration: T) -> Result<Self, T::Error>
    where
        T: TryInto<Duration>,
    {
        let duration = duration.try_into()?;
        Ok(Self(ReadOnlyTimestampBound::MaxStaleness(Box::new(
            duration,
        ))))
    }
}

fn to_clamped_duration(duration: StdDuration) -> Duration {
    let seconds = i64::try_from(duration.as_secs()).unwrap_or(i64::MAX);
    let nanos = duration.subsec_nanos() as i32;
    Duration::clamp(seconds, nanos)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt::Debug;
    use std::time::Duration as StdDuration;
    use time::macros::datetime;

    #[test]
    fn test_auto_traits() {
        static_assertions::assert_impl_all!(TimestampBound: Clone, Debug, Default, PartialEq, Send, Sync);
    }

    #[test]
    fn test_strong() {
        let bound = TimestampBound::strong();
        assert!(matches!(bound.0, ReadOnlyTimestampBound::Strong(true)));
        assert_eq!(bound, TimestampBound::default());
    }

    #[test]
    fn test_read_timestamp_methods() {
        let ts = datetime!(2026-03-09 18:00:00 UTC);

        // 1. OffsetDateTime
        let try_read = TimestampBound::try_read_timestamp(ts).expect("valid OffsetDateTime");
        assert!(matches!(
            try_read.0,
            ReadOnlyTimestampBound::ReadTimestamp(ref t) if t.seconds() == ts.unix_timestamp() && t.nanos() == ts.nanosecond() as i32
        ));

        // 2. wkt::Timestamp
        let wkt_ts = Timestamp::try_from(ts).expect("valid wkt timestamp");
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
        let try_min_read =
            TimestampBound::try_min_read_timestamp(ts).expect("valid OffsetDateTime");
        assert!(matches!(
            try_min_read.0,
            ReadOnlyTimestampBound::MinReadTimestamp(ref t) if t.seconds() == ts.unix_timestamp() && t.nanos() == ts.nanosecond() as i32
        ));

        // 2. wkt::Timestamp
        let wkt_ts = Timestamp::try_from(ts).expect("valid wkt timestamp");
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
        let try_min_read =
            TimestampBound::try_min_read_timestamp(system_time).expect("valid SystemTime");
        assert!(matches!(
            try_min_read.0,
            ReadOnlyTimestampBound::MinReadTimestamp(ref t) if t.seconds() == ts.unix_timestamp() && t.nanos() == ts.nanosecond() as i32
        ));
    }

    #[test]
    fn test_exact_staleness_methods() {
        let d = StdDuration::from_secs(60);

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
        let wkt_d = Duration::clamp(60, 0);
        let try_exact = TimestampBound::try_exact_staleness(wkt_d).expect("valid wkt::Duration");
        assert!(matches!(
            try_exact.0,
            ReadOnlyTimestampBound::ExactStaleness(ref t) if t.seconds() == 60 && t.nanos() == 0
        ));
    }

    #[test]
    fn test_max_staleness_methods() {
        let d = StdDuration::from_secs(120);

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
        let wkt_d = Duration::clamp(120, 0);
        let try_max = TimestampBound::try_max_staleness(wkt_d).expect("valid wkt::Duration");
        assert!(matches!(
            try_max.0,
            ReadOnlyTimestampBound::MaxStaleness(ref t) if t.seconds() == 120 && t.nanos() == 0
        ));
    }

    #[test]
    fn test_out_of_range() {
        let out_of_range_time = std::time::SystemTime::UNIX_EPOCH
            .checked_sub(StdDuration::from_secs(100_000_000_000))
            .expect("valid SystemTime");
        assert!(
            TimestampBound::try_read_timestamp(out_of_range_time).is_err(),
            "expected try_read_timestamp to fail for out-of-range timestamp"
        );
        assert!(
            TimestampBound::try_min_read_timestamp(out_of_range_time).is_err(),
            "expected try_min_read_timestamp to fail for out-of-range timestamp"
        );

        let out_of_range_duration = StdDuration::from_secs(u64::MAX);
        assert!(
            TimestampBound::try_exact_staleness(out_of_range_duration).is_err(),
            "expected try_exact_staleness to fail for out-of-range duration"
        );
        assert!(
            TimestampBound::try_max_staleness(out_of_range_duration).is_err(),
            "expected try_max_staleness to fail for out-of-range duration"
        );

        let clamped_exact = TimestampBound::exact_staleness(out_of_range_duration);
        assert!(
            matches!(
                clamped_exact.0,
                ReadOnlyTimestampBound::ExactStaleness(ref d)
                    if d.seconds() == Duration::MAX_SECONDS && d.nanos() == 0
            ),
            "expected exact_staleness to clamp to MAX_SECONDS"
        );
        let clamped_max = TimestampBound::max_staleness(out_of_range_duration);
        assert!(
            matches!(
                clamped_max.0,
                ReadOnlyTimestampBound::MaxStaleness(ref d)
                    if d.seconds() == Duration::MAX_SECONDS && d.nanos() == 0
            ),
            "expected max_staleness to clamp to MAX_SECONDS"
        );
    }
}
