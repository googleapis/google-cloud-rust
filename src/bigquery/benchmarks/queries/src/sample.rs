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
    pub start_offset_micros: u128,
    pub send_duration_micros: u128,
    pub poll_duration_micros: u128,
    pub read_duration_micros: u128,
    pub total_duration_micros: u128,
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

    pub fn to_csv_row(&self) -> String {
        let clean_err = self.error_message.replace(',', ";").replace('\n', " ");
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
            if self.initial_job_id.is_empty() {
                "N/A"
            } else {
                &self.initial_job_id
            },
            if self.final_job_id.is_empty() {
                "N/A"
            } else {
                &self.final_job_id
            },
            self.retry_detected,
            self.status.as_str(),
            clean_err,
        )
    }

    pub fn total_duration(&self) -> Duration {
        Duration::from_micros(self.total_duration_micros as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sample_csv_serialization() {
        let sample = Sample {
            task_id: 1,
            iteration: 42,
            start_offset_micros: 100_000,
            send_duration_micros: 20_000,
            poll_duration_micros: 30_000,
            read_duration_micros: 50_000,
            total_duration_micros: 100_000,
            rows_count: 500,
            bytes_processed: 1024,
            cache_hit: false,
            initial_job_id: "job_init_123".to_string(),
            final_job_id: "job_retry_456".to_string(),
            retry_detected: true,
            status: SampleStatus::Ok,
            error_message: String::new(),
        };

        let row = sample.to_csv_row();
        assert!(row.contains("1,42,100000,20000,30000,50000,100000,500,1024,false,job_init_123,job_retry_456,true,OK,"));
    }
}
