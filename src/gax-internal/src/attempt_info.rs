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

//! Defines [AttemptInfo]: the information for an HTTP attempt.

use std::time::Duration;

/// The information for an HTTP attempt.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct AttemptInfo {
    /// The time allowed for this HTTP attempt.
    ///
    /// When set to `None` there is no limit on the attempt. Typically
    /// this limit is set by the retry loop, and needs to be combined
    /// with any per-attempt timeout set by the request options.
    pub remaining_time: Option<Duration>,

    /// The number of prior attempts.
    pub attempt_count: u32,
}

impl AttemptInfo {
    /// Creates a new instance.
    ///
    /// # Example:
    /// ```
    /// # use google_cloud_gax_internal::attempt_info::AttemptInfo;
    /// let info = AttemptInfo::new(42);
    /// assert_eq!(info.attempt_count, 42);
    /// assert!(info.remaining_time.is_none());
    /// ```
    pub fn new(attempt_count: u32) -> Self {
        Self {
            attempt_count,
            remaining_time: None,
        }
    }

    /// Sets the remaining time.
    ///
    /// # Example:
    /// ```
    /// # use google_cloud_gax_internal::attempt_info::AttemptInfo;
    /// use std::time::Duration;
    /// let info = AttemptInfo::new(42).set_remaining_time(Duration::from_secs(60));
    /// assert_eq!(info.attempt_count, 42);
    /// assert_eq!(info.remaining_time, Some(Duration::from_secs(60)));
    /// ```
    pub fn set_remaining_time<V>(mut self, v: V) -> Self
    where
        V: Into<Duration>,
    {
        self.remaining_time = Some(v.into());
        self
    }

    /// Sets or clears the remaining time.
    ///
    /// # Example:
    /// ```
    /// # use google_cloud_gax_internal::attempt_info::AttemptInfo;
    /// use std::time::Duration;
    /// let info = AttemptInfo::new(42)
    ///     .set_remaining_time(Duration::from_secs(60))
    ///     .set_or_clear_remaining_time(None);
    /// assert_eq!(info.attempt_count, 42);
    /// assert!(info.remaining_time.is_none());
    /// ```
    pub fn set_or_clear_remaining_time<V>(mut self, v: V) -> Self
    where
        V: Into<Option<Duration>>,
    {
        self.remaining_time = v.into();
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new() {
        let got = AttemptInfo::new(42);
        assert_eq!(got.attempt_count, 42);
        assert!(got.remaining_time.is_none(), "{got:?}");
    }

    #[test]
    fn remaining_time() {
        let got = AttemptInfo::new(42).set_remaining_time(Duration::from_secs(60));
        assert_eq!(got.attempt_count, 42);
        assert_eq!(got.remaining_time, Some(Duration::from_secs(60)));
        let got = got.set_or_clear_remaining_time(None);
        assert!(got.remaining_time.is_none(), "{got:?}");
        let got = got.set_or_clear_remaining_time(Some(Duration::from_millis(123)));
        assert_eq!(
            got.remaining_time,
            Some(Duration::from_millis(123)),
            "{got:?}"
        );
    }
}
