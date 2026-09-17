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

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Result status of an individual query execution sample.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SampleStatus {
    Ok,
    Error,
    Timeout,
}

impl SampleStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Ok => "OK",
            Self::Error => "ERR",
            Self::Timeout => "TIMEOUT",
        }
    }
}

/// A recorded sample of a single query execution.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Sample {
    pub task_id: usize,
    pub iteration: u64,
    pub start_offset_micros: u64,
    pub send_duration_micros: u64,
    pub poll_duration_micros: u64,
    pub read_duration_micros: u64,
    pub total_duration_micros: u64,
    pub rows_count: usize,
    pub bytes_processed: i64,
    pub cache_hit: bool,
    pub initial_job_id: String,
    pub final_job_id: String,
    pub retry_detected: bool,
    pub status: SampleStatus,
    pub error_message: String,
}

impl Sample {
    pub const HEADER: &'static str = concat!(
        "Task,Iteration,StartOffsetMicros,SendDurationMicros,PollDurationMicros,",
        "ReadDurationMicros,TotalDurationMicros,RowsCount,BytesProcessed,CacheHit,",
        "InitialJobId,FinalJobId,RetryDetected,Status,ErrorMessage"
    );

    /// Creates a sample for an iteration that has just started.
    ///
    /// The status defaults to [`SampleStatus::Error`] so that an execution path
    /// which returns early without recording an outcome is never mistaken for a
    /// success. Each phase fills in its own fields as it completes.
    pub fn new(task_id: usize, iteration: u64, start_offset_micros: u64) -> Self {
        Self {
            task_id,
            iteration,
            start_offset_micros,
            send_duration_micros: 0,
            poll_duration_micros: 0,
            read_duration_micros: 0,
            total_duration_micros: 0,
            rows_count: 0,
            bytes_processed: 0,
            cache_hit: false,
            initial_job_id: String::new(),
            final_job_id: String::new(),
            retry_detected: false,
            status: SampleStatus::Error,
            error_message: String::new(),
        }
    }

    /// Returns the job ID to display in reports, preferring the final one.
    pub fn display_job_id(&self) -> &str {
        display_job_id(&self.initial_job_id, &self.final_job_id)
    }

    /// Returns the offset from the start of the test run, in seconds.
    pub fn start_offset_secs(&self) -> f64 {
        self.start_offset_micros as f64 / 1_000_000.0
    }

    pub fn to_csv_row(&self) -> String {
        let clean_err = self.error_message.replace(',', ";").replace('\n', " ");
        let initial_job_id = if self.initial_job_id.is_empty() {
            UNKNOWN_JOB_ID
        } else {
            &self.initial_job_id
        };
        let final_job_id = if self.final_job_id.is_empty() {
            UNKNOWN_JOB_ID
        } else {
            &self.final_job_id
        };
        format!(
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            self.task_id,
            self.iteration,
            self.start_offset_micros,
            self.send_duration_micros,
            self.poll_duration_micros,
            self.read_duration_micros,
            self.total_duration_micros,
            self.rows_count,
            self.bytes_processed,
            self.cache_hit,
            initial_job_id,
            final_job_id,
            self.retry_detected,
            self.status.as_str(),
            clean_err,
        )
    }

    pub fn total_duration(&self) -> Duration {
        Duration::from_micros(self.total_duration_micros)
    }

    pub fn send_duration(&self) -> Duration {
        Duration::from_micros(self.send_duration_micros)
    }

    pub fn poll_duration(&self) -> Duration {
        Duration::from_micros(self.poll_duration_micros)
    }

    pub fn read_duration(&self) -> Duration {
        Duration::from_micros(self.read_duration_micros)
    }
}

/// Placeholder used in reports when a job ID was never assigned.
pub const UNKNOWN_JOB_ID: &str = "N/A";

/// Converts a duration to whole microseconds, saturating at [`u64::MAX`].
pub fn as_micros_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

/// Returns the most specific job ID available, preferring the final one.
pub fn display_job_id<'a>(initial: &'a str, final_id: &'a str) -> &'a str {
    [final_id, initial]
        .into_iter()
        .find(|id| !id.is_empty())
        .unwrap_or(UNKNOWN_JOB_ID)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sample_csv_serialization() {
        let sample = Sample {
            send_duration_micros: 20_000,
            poll_duration_micros: 30_000,
            read_duration_micros: 50_000,
            total_duration_micros: 100_000,
            rows_count: 500,
            bytes_processed: 1024,
            initial_job_id: "job_init_123".to_string(),
            final_job_id: "job_retry_456".to_string(),
            retry_detected: true,
            status: SampleStatus::Ok,
            ..Sample::new(1, 42, 100_000)
        };

        let row = sample.to_csv_row();
        assert!(row.contains("1,42,100000,20000,30000,50000,100000,500,1024,false,job_init_123,job_retry_456,true,OK,"));
    }

    #[test]
    fn test_new_sample_defaults_to_error() {
        // Guards against an early return being reported as a success.
        assert_eq!(Sample::new(0, 0, 0).status, SampleStatus::Error);
    }
}
