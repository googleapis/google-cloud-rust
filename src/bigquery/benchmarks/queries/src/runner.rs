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

use crate::args::Args;
use crate::metrics::OtelMetrics;
use crate::sample::{Sample, SampleStatus, as_micros_u64};
use crate::scenarios::Scenario;
use google_cloud_bigquery::client::BigQuery;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::Sender;
use tracing::Instrument;

/// Manages and executes benchmark queries for a single worker task.
pub struct TaskRunner<'a> {
    pub task_id: usize,
    pub test_start: Instant,
    pub client: &'a BigQuery,
    pub scenario: &'a Scenario,
    pub args: &'a Args,
    pub tx: &'a Sender<Sample>,
    pub metrics: &'a OtelMetrics,
}

impl TaskRunner<'_> {
    /// Executes the task loop until iterations or duration limit is reached.
    pub async fn run(&self) -> anyhow::Result<()> {
        if self.args.rampup_period > Duration::ZERO {
            tokio::time::sleep(self.args.rampup_period * self.task_id as u32).await;
        }

        let mut iteration = 0_u64;

        loop {
            if let Some(max_iter) = self.args.iterations
                && iteration >= max_iter
            {
                break;
            }

            if let Some(max_duration) = self.args.duration
                && self.test_start.elapsed() >= max_duration
            {
                break;
            }

            let iter_start = Instant::now();
            let start_offset_micros = as_micros_u64(self.test_start.elapsed());

            let iteration_span = tracing::info_span!(
                "bigquery.query_benchmark.iteration",
                task_id = self.task_id,
                iteration,
                scenario = %self.scenario.name
            );

            let sample_fut = self
                .execute_iteration(iteration, start_offset_micros, iter_start)
                .instrument(iteration_span);

            let sample = match tokio::time::timeout(self.args.query_timeout, sample_fut).await {
                Ok(sample) => sample,
                Err(_) => {
                    tracing::error!(
                        task_id = self.task_id,
                        iteration,
                        "Query iteration timed out after {:?}",
                        self.args.query_timeout
                    );
                    Sample {
                        total_duration_micros: as_micros_u64(iter_start.elapsed()),
                        status: SampleStatus::Timeout,
                        error_message: format!(
                            "Query iteration timed out after {:?}",
                            self.args.query_timeout
                        ),
                        ..Sample::new(self.task_id, iteration, start_offset_micros)
                    }
                }
            };

            // Every execution path funnels through here, so no outcome can go
            // unrecorded.
            self.metrics.record_sample(&sample);
            let _ = self.tx.send(sample).await;
            iteration += 1;
        }

        Ok(())
    }

    /// Runs one query end to end, returning a sample describing the outcome.
    ///
    /// The sample starts out marked as an error and is upgraded to
    /// [`SampleStatus::Ok`] only once every phase has succeeded.
    async fn execute_iteration(
        &self,
        iteration: u64,
        start_offset_micros: u64,
        iter_start: Instant,
    ) -> Sample {
        let mut sample = Sample::new(self.task_id, iteration, start_offset_micros);

        let mut query_builder = self
            .client
            .query(&self.scenario.sql)
            .set_location(&self.args.location)
            .set_use_query_cache(self.args.use_query_cache);

        if let Some(max_results) = self.args.max_results {
            query_builder = query_builder.set_max_results(max_results);
        }
        if let Some(project_id) = &self.args.project_id {
            query_builder = query_builder.with_project_id(project_id);
        }

        // Step 1: Execute Query::send()
        let send_start = Instant::now();
        let send_span = tracing::info_span!("bigquery.send", task_id = self.task_id, iteration);
        let send_result = query_builder.send().instrument(send_span).await;
        sample.send_duration_micros = as_micros_u64(send_start.elapsed());

        let query_handle = match send_result {
            Ok(handle) => handle,
            Err(err) => {
                tracing::error!(
                    task_id = self.task_id,
                    iteration,
                    "Query::send failed: {err:?}"
                );
                sample.total_duration_micros = as_micros_u64(iter_start.elapsed());
                sample.error_message = format!("Query::send: {err:#}");
                return sample;
            }
        };

        // Capture initial job_id if present
        sample.initial_job_id = query_handle
            .metadata()
            .job_reference
            .as_ref()
            .map(|r| r.job_id.clone())
            .unwrap_or_default();

        // Step 2: Execute Query::until_done()
        let poll_start = Instant::now();
        let poll_span = tracing::info_span!(
            "bigquery.until_done",
            task_id = self.task_id,
            iteration,
            initial_job_id = %sample.initial_job_id
        );
        let done_result = query_handle.until_done().instrument(poll_span).await;
        sample.poll_duration_micros = as_micros_u64(poll_start.elapsed());

        let complete_query = match done_result {
            Ok(complete) => complete,
            Err(err) => {
                tracing::error!(
                    task_id = self.task_id,
                    iteration,
                    initial_job_id = %sample.initial_job_id,
                    "Query::until_done failed: {err:?}"
                );
                sample.total_duration_micros = as_micros_u64(iter_start.elapsed());
                sample.error_message = format!("Query::until_done: {err:#}");
                return sample;
            }
        };

        // Capture final job_id
        let metadata = complete_query.metadata();
        sample.final_job_id = metadata
            .job_reference
            .as_ref()
            .map(|r| r.job_id.clone())
            .unwrap_or_default();
        sample.bytes_processed = metadata.total_bytes_processed.unwrap_or(0);
        sample.cache_hit = metadata.cache_hit.unwrap_or(false);

        // Step 3: Detect if under-the-hood job retry occurred
        sample.retry_detected = !sample.initial_job_id.is_empty()
            && !sample.final_job_id.is_empty()
            && sample.initial_job_id != sample.final_job_id;

        if sample.retry_detected {
            tracing::warn!(
                task_id = self.task_id,
                iteration,
                initial_job_id = %sample.initial_job_id,
                final_job_id = %sample.final_job_id,
                "Query job retry detected under the hood (job_id mutated)!"
            );
        }

        // Step 4: Stream and read result rows if enabled
        let read_start = Instant::now();
        let mut read_error = None;

        if self.args.read_results {
            let read_span =
                tracing::info_span!("bigquery.read_rows", task_id = self.task_id, iteration);
            async {
                let mut rows = complete_query.read();
                while let Some(row_result) = rows.next().await {
                    match row_result {
                        Ok(_) => {
                            sample.rows_count += 1;
                        }
                        Err(err) => {
                            tracing::error!(
                                task_id = self.task_id,
                                iteration,
                                "Error streaming rows: {err:?}"
                            );
                            read_error = Some(format!("CompleteQuery::read: {err:#}"));
                            break;
                        }
                    }
                }
            }
            .instrument(read_span)
            .await;
        }

        sample.read_duration_micros = as_micros_u64(read_start.elapsed());
        sample.total_duration_micros = as_micros_u64(iter_start.elapsed());

        match read_error {
            Some(err_msg) => sample.error_message = err_msg,
            None => sample.status = SampleStatus::Ok,
        }

        sample
    }
}
