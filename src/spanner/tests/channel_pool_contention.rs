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

use gaxi::grpc::tonic::Response;
use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
use google_cloud_spanner::channel_pool::{DynamicChannelPoolConfig, StaticChannelPoolConfig};
use google_cloud_spanner::client::{DatabaseClient, Spanner, SpannerBuilderExt};
use google_cloud_spanner::statement::Statement;
use google_cloud_spanner::transaction::{ReadWriteTransaction, TimestampBound};
use google_cloud_test_macros::tokio_test_no_panics;
use prost_types::Value as ProtoValue;
use prost_types::value::Kind as ProtoValueKind;
use rand::random_range;
use serial_test::serial;
use spanner_grpc_mock::google::spanner::v1 as mock_v1;
use spanner_grpc_mock::google::spanner::v1::struct_type::Field;
use spanner_grpc_mock::google::spanner::v1::{StructType, Type, TypeCode};
use spanner_grpc_mock::{MockSpanner, start};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Semaphore, mpsc};
use tokio::task::JoinSet;
use tokio::time::sleep;

/// Simulates GFE and SpanFE connection-level queuing and processing contention.
///
/// For each physical TCP connection (identified by client socket address), allows at most
/// `max_parallel_per_channel` concurrent requests to execute in parallel. Any excess requests
/// are held in an asynchronous FIFO queue until an active request completes.
#[derive(Clone, Debug)]
pub(crate) struct ChannelContentionManager {
    channels: Arc<Mutex<HashMap<SocketAddr, Arc<Semaphore>>>>,
    max_parallel_per_channel: usize,
    base_latency_min_micros: u64,
    base_latency_noise_max_micros: u64,
    total_queued_requests: Arc<AtomicUsize>,
    total_completed_requests: Arc<AtomicUsize>,
}

impl ChannelContentionManager {
    pub(crate) fn new(
        max_parallel_per_channel: usize,
        base_latency_min_micros: u64,
        base_latency_noise_max_micros: u64,
    ) -> Self {
        Self {
            channels: Arc::new(Mutex::new(HashMap::new())),
            max_parallel_per_channel,
            base_latency_min_micros,
            base_latency_noise_max_micros,
            total_queued_requests: Arc::new(AtomicUsize::new(0)),
            total_completed_requests: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Acquires a slot for execution on the connection, waiting in the FIFO queue if all
    /// parallel slots are currently occupied. Simulates execution latency with random noise.
    pub(crate) async fn execute_with_queue(&self, client_address: SocketAddr) {
        let semaphore = {
            let mut registry = self.channels.lock().await;
            registry
                .entry(client_address)
                .or_insert_with(|| Arc::new(Semaphore::new(self.max_parallel_per_channel)))
                .clone()
        };

        // If all permits are currently in use, the request waits in the semaphore's FIFO queue.
        if semaphore.available_permits() == 0 {
            self.total_queued_requests.fetch_add(1, Ordering::Relaxed);
        }

        let _permit = semaphore
            .acquire()
            .await
            .expect("semaphore must never be closed");

        let noise_micros = if self.base_latency_noise_max_micros > 0 {
            random_range(0..=self.base_latency_noise_max_micros)
        } else {
            0
        };
        let execution_delay = Duration::from_micros(self.base_latency_min_micros + noise_micros);
        sleep(execution_delay).await;

        self.total_completed_requests
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn total_queued_count(&self) -> usize {
        self.total_queued_requests.load(Ordering::Relaxed)
    }

    pub(crate) fn total_completed_count(&self) -> usize {
        self.total_completed_requests.load(Ordering::Relaxed)
    }
}

async fn start_contention_server(
    contention_manager: ChannelContentionManager,
) -> (String, tokio::task::JoinHandle<()>) {
    let mut mock = MockSpanner::new();

    mock.expect_create_session().returning(|_| {
        Ok(Response::new(mock_v1::Session {
            name:
                "projects/test-project/instances/test-instance/databases/test-database/sessions/s1"
                    .to_string(),
            multiplexed: true,
            ..Default::default()
        }))
    });

    let contention_manager_clone = contention_manager.clone();
    mock.expect_execute_streaming_sql()
        .returning(move |request| {
            let client_address = request
                .remote_addr()
                .expect("remote client address must be present on TCP transport");
            let is_begin = request
                .get_ref()
                .transaction
                .as_ref()
                .and_then(|selector| selector.selector.as_ref())
                .is_some_and(|selector| {
                    matches!(selector, mock_v1::transaction_selector::Selector::Begin(_))
                });
            let transaction_metadata = if is_begin {
                Some(mock_v1::Transaction {
                    id: vec![1, 2, 3],
                    ..Default::default()
                })
            } else {
                None
            };

            let result_set = mock_v1::PartialResultSet {
                metadata: Some(mock_v1::ResultSetMetadata {
                    row_type: Some(StructType {
                        fields: vec![
                            Field {
                                name: "id".to_string(),
                                r#type: Some(Type {
                                    code: TypeCode::Int64 as i32,
                                    ..Default::default()
                                }),
                            },
                            Field {
                                name: "value".to_string(),
                                r#type: Some(Type {
                                    code: TypeCode::String as i32,
                                    ..Default::default()
                                }),
                            },
                        ],
                    }),
                    transaction: transaction_metadata,
                    undeclared_parameters: None,
                }),
                values: vec![
                    ProtoValue {
                        kind: Some(ProtoValueKind::StringValue("1".to_string())),
                    },
                    ProtoValue {
                        kind: Some(ProtoValueKind::StringValue("test-value".to_string())),
                    },
                ],
                chunked_value: false,
                resume_token: vec![1, 2, 3],
                stats: None,
                precommit_token: None,
                cache_update: None,
                last: true,
            };

            let (transmitter, receiver) = mpsc::channel(1);
            let contention_manager = contention_manager_clone.clone();
            tokio::spawn(async move {
                contention_manager.execute_with_queue(client_address).await;
                let _ = transmitter.send(Ok(result_set)).await;
            });
            Ok(Response::new(receiver))
        });

    let contention_manager_clone = contention_manager.clone();
    mock.expect_execute_sql().returning(move |request| {
        let client_address = request
            .remote_addr()
            .expect("remote client address must be present on TCP transport");
        let contention_manager = contention_manager_clone.clone();
        tokio::spawn(async move {
            contention_manager.execute_with_queue(client_address).await;
        });
        Ok(Response::new(mock_v1::ResultSet {
            metadata: Some(mock_v1::ResultSetMetadata {
                row_type: Some(StructType { fields: vec![] }),
                ..Default::default()
            }),
            stats: Some(mock_v1::ResultSetStats {
                row_count: Some(mock_v1::result_set_stats::RowCount::RowCountExact(1)),
                ..Default::default()
            }),
            ..Default::default()
        }))
    });

    let contention_manager_clone = contention_manager.clone();
    mock.expect_begin_transaction().returning(move |request| {
        let client_address = request
            .remote_addr()
            .expect("remote client address must be present on TCP transport");
        let contention_manager = contention_manager_clone.clone();
        tokio::spawn(async move {
            contention_manager.execute_with_queue(client_address).await;
        });
        Ok(Response::new(mock_v1::Transaction {
            id: vec![1, 2, 3],
            ..Default::default()
        }))
    });

    let contention_manager_clone = contention_manager.clone();
    mock.expect_commit().returning(move |request| {
        let client_address = request
            .remote_addr()
            .expect("remote client address must be present on TCP transport");
        let contention_manager = contention_manager_clone.clone();
        tokio::spawn(async move {
            contention_manager.execute_with_queue(client_address).await;
        });
        Ok(Response::new(mock_v1::CommitResponse {
            commit_timestamp: Some(prost_types::Timestamp {
                seconds: 12345,
                nanos: 0,
            }),
            ..Default::default()
        }))
    });

    mock.expect_rollback().returning(|_| Ok(Response::new(())));

    start("127.0.0.1:0", mock)
        .await
        .expect("mock server must start")
}

#[tokio_test_no_panics]
#[serial]
#[ignore]
async fn dynamic_channel_pool_scales_up_and_reduces_contention() {
    // 1. Mock server setup: 4 parallel requests max per channel connection.
    // Base latency: 1.5ms + random noise in [0.0ms, 0.5ms].
    let contention_manager = ChannelContentionManager::new(4, 1500, 500);
    let (address, _server) = start_contention_server(contention_manager.clone()).await;

    // 2. Client setup with DynamicChannelPoolConfig:
    // Initial channels: 4, Max channels: 32, scale_up_cooldown: 10ms, doubling upon saturation.
    let dynamic_config = DynamicChannelPoolConfig::new()
        .with_initial_channels(4)
        .with_min_channels(4)
        .with_max_channels(32)
        .with_min_rpc_per_channel(2.0)
        .with_max_rpc_per_channel(8.0)
        .with_scale_up_cooldown(Duration::from_millis(10))
        .with_max_scale_up_percent(100);

    let spanner = Spanner::builder()
        .with_endpoint(address)
        .with_credentials(Anonymous::new().build())
        .with_channel_pool(dynamic_config)
        .build()
        .await
        .expect("client build must succeed");

    let database_client = spanner
        .database_client("projects/test-project/instances/test-instance/databases/test-database")
        .build()
        .await
        .expect("database_client build must succeed");

    assert_eq!(
        spanner.active_channel_count(),
        4,
        "initial channel count must be 4"
    );

    // 3. Workload generation: 48 concurrent worker tasks generating continuous load.
    // On 4 initial channels, 48 requests creates 12 concurrent requests per channel,
    // which exceeds max_parallel_per_channel (4) and max_rpc_per_channel (8.0),
    // driving saturation and triggering automatic scale-up.
    let total_workers = 48;
    let is_stopped = Arc::new(AtomicBool::new(false));
    let mut join_set = JoinSet::new();

    for _ in 0..total_workers {
        let client_clone = database_client.clone();
        let stopped = is_stopped.clone();
        join_set.spawn(async move {
            let mut latencies = Vec::new();
            while !stopped.load(Ordering::Relaxed) {
                let start_time = Instant::now();
                let statement = Statement::builder("SELECT 1").build();
                let transaction = client_clone
                    .single_use()
                    .set_timestamp_bound(TimestampBound::exact_staleness(Duration::from_secs(15)))
                    .build();
                let mut result_set = transaction
                    .execute_query(statement)
                    .await
                    .expect("execute_query must succeed");
                while let Some(row) = result_set
                    .next()
                    .await
                    .transpose()
                    .expect("row must succeed")
                {
                    let _: i64 = row.get(0_usize);
                }
                latencies.push(start_time.elapsed());
            }
            latencies
        });
    }

    // Monitor pool scaling with a deadline.
    let scale_deadline = Instant::now() + Duration::from_secs(5);
    let mut final_channels;
    loop {
        final_channels = spanner.active_channel_count();
        if final_channels > 4 || Instant::now() >= scale_deadline {
            break;
        }
        sleep(Duration::from_millis(10)).await;
    }

    // Stop all initial burst worker tasks and collect their latencies.
    is_stopped.store(true, Ordering::Relaxed);
    let mut burst_latencies = Vec::new();
    while let Some(join_result) = join_set.join_next().await {
        let worker_latencies = join_result.expect("worker task must succeed");
        burst_latencies.extend(worker_latencies);
    }

    // 4. Phase 2: With the scaled-up pool (>=8 channels), run 16 concurrent workers
    // (2 requests per channel on average). Because concurrency per channel (2) <= 4 (server limit),
    // queuing is completely eliminated and latency returns to base execution time (1.5ms - 2.0ms).
    let post_scale_workers = 16;
    let mut post_scale_latencies = Vec::new();
    for _ in 0..10 {
        let mut wave_set = JoinSet::new();
        for _ in 0..post_scale_workers {
            let client_clone = database_client.clone();
            wave_set.spawn(async move {
                let start_time = Instant::now();
                let statement = Statement::builder("SELECT 1").build();
                let transaction = client_clone
                    .single_use()
                    .set_timestamp_bound(TimestampBound::exact_staleness(Duration::from_secs(15)))
                    .build();
                let mut result_set = transaction
                    .execute_query(statement)
                    .await
                    .expect("execute_query must succeed");
                while let Some(row) = result_set
                    .next()
                    .await
                    .transpose()
                    .expect("row must succeed")
                {
                    let _: i64 = row.get(0_usize);
                }
                start_time.elapsed()
            });
        }
        while let Some(join_result) = wave_set.join_next().await {
            post_scale_latencies.push(join_result.expect("worker task must succeed"));
        }
    }

    // 5. Assertions:
    // A. The contention manager must have experienced queuing during the initial burst.
    assert!(
        contention_manager.total_queued_count() > 0,
        "contention manager should have observed queued requests during initial burst"
    );

    // B. Total requests completed must be positive.
    assert!(
        contention_manager.total_completed_count() > 0,
        "requests should have completed"
    );

    // C. The dynamic channel pool must have scaled up from 4 channels.
    assert!(
        final_channels > 4,
        "dynamic channel pool must scale up beyond initial 4 channels under contention, got {final_channels}"
    );

    // D. Post-scale p50 latency must be significantly relieved compared to the contended phase.
    post_scale_latencies.sort();
    let post_p50 = post_scale_latencies[post_scale_latencies.len() / 2];
    let post_p95 = post_scale_latencies[(post_scale_latencies.len() * 95) / 100];
    let post_p99 = post_scale_latencies[(post_scale_latencies.len() * 99) / 100];

    burst_latencies.sort();
    let burst_p50 = burst_latencies[burst_latencies.len() / 2];
    let burst_p95 = burst_latencies[(burst_latencies.len() * 95) / 100];
    let burst_p99 = burst_latencies[(burst_latencies.len() * 99) / 100];

    eprintln!(
        "\n=======================================================\n[Dynamic Channel Pool Contention Benchmark Result]\nChannels scaled: 4 -> {final_channels}\nServer Queue Limit: 4 parallel requests per channel\nBase Latency: 1.5ms + [0.0ms, 0.5ms] noise\n-------------------------------------------------------\nInitial Contended Burst (4 channels, 48 concurrent workers):\n  Requests: {}\n  Queued on server: {}\n  p50 latency: {:?}\n  p95 latency: {:?}\n  p99 latency: {:?}\n-------------------------------------------------------\nPost Scale-Up Distribution ({final_channels} channels, {post_scale_workers} concurrent workers):\n  Requests: {}\n  p50 latency: {:?}\n  p95 latency: {:?}\n  p99 latency: {:?}\n=======================================================",
        burst_latencies.len(),
        contention_manager.total_queued_count(),
        burst_p50,
        burst_p95,
        burst_p99,
        post_scale_latencies.len(),
        post_p50,
        post_p95,
        post_p99
    );

    assert!(
        post_p50 < burst_p50,
        "post-scale p50 latency ({post_p50:?}) should be lower than contended burst p50 ({burst_p50:?})"
    );
}

#[tokio_test_no_panics]
#[serial]
#[ignore]
async fn static_channel_pool_remains_at_fixed_size_under_contention() {
    let contention_manager = ChannelContentionManager::new(4, 1500, 500);
    let (address, _server) = start_contention_server(contention_manager.clone()).await;

    let static_config = StaticChannelPoolConfig::new(4);
    let spanner = Spanner::builder()
        .with_endpoint(address)
        .with_credentials(Anonymous::new().build())
        .with_channel_pool(static_config)
        .build()
        .await
        .expect("client build must succeed");

    let database_client = spanner
        .database_client("projects/test-project/instances/test-instance/databases/test-database")
        .build()
        .await
        .expect("database_client build must succeed");

    assert_eq!(
        spanner.active_channel_count(),
        4,
        "static pool must have 4 channels"
    );

    let total_workers = 16;
    let requests_per_worker = 4;
    let mut join_set = JoinSet::new();

    for _ in 0..total_workers {
        let client_clone = database_client.clone();
        join_set.spawn(async move {
            for _ in 0..requests_per_worker {
                let start_time = Instant::now();
                let statement = Statement::builder("SELECT 1").build();
                let transaction = client_clone
                    .single_use()
                    .set_timestamp_bound(TimestampBound::exact_staleness(Duration::from_secs(15)))
                    .build();
                let mut result_set = transaction
                    .execute_query(statement)
                    .await
                    .expect("execute_query must succeed");
                while let Some(row) = result_set
                    .next()
                    .await
                    .transpose()
                    .expect("row must succeed")
                {
                    let _: i64 = row.get(0_usize);
                }
                let _ = start_time.elapsed();
            }
        });
    }

    while let Some(join_result) = join_set.join_next().await {
        join_result.expect("worker task must succeed");
    }

    // Static pool must strictly remain at 4 channels
    assert_eq!(
        spanner.active_channel_count(),
        4,
        "static channel pool must remain fixed at 4 channels"
    );
}

#[derive(Debug, Clone)]
struct BenchmarkRunMetrics {
    total_operations: usize,
    queued_operations: usize,
    mean_latency: Duration,
    p50_latency: Duration,
    p90_latency: Duration,
    p95_latency: Duration,
    p99_latency: Duration,
    operations_over_15ms: usize,
    operations_over_15ms_percent: f64,
}

impl BenchmarkRunMetrics {
    fn calculate(queued_operations: usize, mut latencies: Vec<Duration>) -> Self {
        latencies.sort();
        let total_operations = latencies.len();
        let total_duration: Duration = latencies.iter().sum();
        let mean_latency = if total_operations > 0 {
            total_duration / total_operations as u32
        } else {
            Duration::ZERO
        };
        let p50_latency = if total_operations > 0 {
            latencies[total_operations / 2]
        } else {
            Duration::ZERO
        };
        let p90_latency = if total_operations > 0 {
            latencies[(total_operations * 90) / 100]
        } else {
            Duration::ZERO
        };
        let p95_latency = if total_operations > 0 {
            latencies[(total_operations * 95) / 100]
        } else {
            Duration::ZERO
        };
        let p99_latency = if total_operations > 0 {
            latencies[(total_operations * 99) / 100]
        } else {
            Duration::ZERO
        };
        let operations_over_15ms = latencies
            .iter()
            .filter(|&&lat| lat >= Duration::from_millis(15))
            .count();
        let operations_over_15ms_percent = if total_operations > 0 {
            (operations_over_15ms as f64 / total_operations as f64) * 100.0
        } else {
            0.0
        };

        Self {
            total_operations,
            queued_operations,
            mean_latency,
            p50_latency,
            p90_latency,
            p95_latency,
            p99_latency,
            operations_over_15ms,
            operations_over_15ms_percent,
        }
    }
}

async fn warm_up_database_client(database_client: &DatabaseClient) {
    let statement = Statement::builder("SELECT id, value FROM test WHERE id = @id")
        .add_param("id", 1i64)
        .build();
    let transaction = database_client
        .single_use()
        .set_timestamp_bound(TimestampBound::exact_staleness(Duration::from_secs(15)))
        .build();
    let mut result_set = transaction
        .execute_query(statement)
        .await
        .expect("warm_up execute_query must succeed");
    while let Some(row) = result_set
        .next()
        .await
        .transpose()
        .expect("warm_up row must succeed")
    {
        let _: i64 = row.get(0_usize);
        let _: String = row.get(1_usize);
    }
}

async fn run_database_client_point_select_workload(
    database_client: &DatabaseClient,
    worker_count: usize,
    duration: Duration,
) -> Vec<Duration> {
    let is_stopped = Arc::new(AtomicBool::new(false));
    let mut join_set = JoinSet::new();

    for _ in 0..worker_count {
        let client_clone = database_client.clone();
        let stopped = is_stopped.clone();
        join_set.spawn(async move {
            let mut worker_latencies = Vec::new();
            while !stopped.load(Ordering::Relaxed) {
                let start_time = Instant::now();
                let statement = Statement::builder("SELECT id, value FROM test WHERE id = @id")
                    .add_param("id", 1i64)
                    .build();
                let transaction = client_clone
                    .single_use()
                    .set_timestamp_bound(TimestampBound::exact_staleness(Duration::from_secs(15)))
                    .build();
                let mut result_set = transaction
                    .execute_query(statement)
                    .await
                    .expect("execute_query must succeed");
                while let Some(row) = result_set
                    .next()
                    .await
                    .transpose()
                    .expect("row must succeed")
                {
                    let _: i64 = row.get(0_usize);
                    let _: String = row.get(1_usize);
                }
                worker_latencies.push(start_time.elapsed());
            }
            worker_latencies
        });
    }

    sleep(duration).await;
    is_stopped.store(true, Ordering::Relaxed);

    let mut all_latencies = Vec::new();
    while let Some(join_result) = join_set.join_next().await {
        let worker_latencies = join_result.expect("worker task must succeed");
        all_latencies.extend(worker_latencies);
    }
    all_latencies
}

#[tokio_test_no_panics]
#[serial]
#[ignore]
async fn replication_spiky_workload_database_client_under_default_dynamic_pool() {
    let contention_manager = ChannelContentionManager::new(2, 1500, 500);
    let (address, _server) = start_contention_server(contention_manager.clone()).await;

    let dynamic_config = DynamicChannelPoolConfig::new()
        .with_initial_channels(4)
        .with_min_channels(4)
        .with_max_channels(256)
        .with_min_rpc_per_channel(1.0)
        .with_max_rpc_per_channel(2.0)
        .with_error_penalty_step(1)
        .with_scale_up_cooldown(Duration::from_secs(4))
        .with_max_scale_up_percent(100);

    let spanner = Spanner::builder()
        .with_endpoint(address)
        .with_credentials(Anonymous::new().build())
        .with_channel_pool(dynamic_config)
        .build()
        .await
        .expect("spanner client build must succeed");

    let database_client = spanner
        .database_client("projects/test-project/instances/test-instance/databases/test-database")
        .build()
        .await
        .expect("database_client build must succeed");

    assert_eq!(
        spanner.active_channel_count(),
        4,
        "initial dynamic channel pool count must be 4"
    );

    warm_up_database_client(&database_client).await;

    // Phase 1: Calm/Normal State (2 concurrent workers for 300ms)
    let calm_latencies =
        run_database_client_point_select_workload(&database_client, 2, Duration::from_millis(300))
            .await;

    let calm_queued_count = contention_manager.total_queued_count();
    let calm_metrics = BenchmarkRunMetrics::calculate(calm_queued_count, calm_latencies);

    assert_eq!(
        calm_queued_count, 0,
        "calm phase must not cause server queuing"
    );
    assert!(
        calm_metrics.p50_latency < Duration::from_millis(6),
        "calm p50 latency must be near baseline (<6ms)"
    );

    // Phase 2: Spiky Burst State (12 concurrent workers for 800ms)
    // With 4 channels and 2 permits per channel, 12 workers creates 3 requests per channel (> 2 permits).
    let burst_start_queued = contention_manager.total_queued_count();
    let burst_latencies =
        run_database_client_point_select_workload(&database_client, 12, Duration::from_millis(800))
            .await;

    let burst_queued_count = contention_manager.total_queued_count() - burst_start_queued;
    let final_channels = spanner.active_channel_count();
    let burst_metrics = BenchmarkRunMetrics::calculate(burst_queued_count, burst_latencies);

    eprintln!(
        "\n===============================================================================\n[Replication 1: Dynamic Pool Under DatabaseClient Point-Select Spiky Load]\nChannels: 4 -> {final_channels} (Scales to 8 at onset, then blocked by 4s cooldown!)\nServer Permit Limit: 2 parallel requests per connection\nBase Latency: 1.5ms + [0.0ms, 0.5ms] noise\n-------------------------------------------------------------------------------\nNormal State (2 workers, 300ms):\n  Operations: {}\n  Server Queued: {}\n  p50 latency: {:?}\n  p95 latency: {:?}\nBurst State (12 workers, 800ms):\n  Operations: {}\n  Server Queued: {}\n  Mean latency: {:?}\n  p50 latency: {:?}\n  p90 latency: {:?}\n  p95 latency: {:?}\n  p99 latency: {:?}\n  Operations > 15ms: {} ({:.2}%)\n===============================================================================",
        calm_metrics.total_operations,
        calm_metrics.queued_operations,
        calm_metrics.p50_latency,
        calm_metrics.p95_latency,
        burst_metrics.total_operations,
        burst_metrics.queued_operations,
        burst_metrics.mean_latency,
        burst_metrics.p50_latency,
        burst_metrics.p90_latency,
        burst_metrics.p95_latency,
        burst_metrics.p99_latency,
        burst_metrics.operations_over_15ms,
        burst_metrics.operations_over_15ms_percent
    );

    // ROOT CAUSE 2 VERIFICATION:
    // With DatabaseClient now wired to dynamic channel pool, the pool doubles from 4 to 8 at burst onset,
    // but the 4-second scale-up cooldown prevents scaling any further (e.g. to 16, 32, 64) during the 800ms burst.
    assert_eq!(
        final_channels, 8,
        "Dynamic pool must scale from 4 to 8 at onset, but remain throttled at 8 due to 4s cooldown"
    );

    assert!(
        burst_queued_count > 0,
        "Burst must produce server-side queuing on the 4 overloaded connections"
    );
    assert!(
        burst_metrics.p95_latency > Duration::from_millis(5),
        "p95 latency must be degraded due to queuing behind the 4 channels"
    );
}

#[tokio_test_no_panics]
#[serial]
#[ignore]
async fn replication_spiky_workload_database_client_under_64_channel_static_pool() {
    let contention_manager = ChannelContentionManager::new(2, 1500, 500);
    let (address, _server) = start_contention_server(contention_manager.clone()).await;

    // Static 64 channels (matching benchmark Configuration C).
    let static_config = StaticChannelPoolConfig::new(64);
    let spanner = Spanner::builder()
        .with_endpoint(address)
        .with_credentials(Anonymous::new().build())
        .with_channel_pool(static_config)
        .build()
        .await
        .expect("spanner client build must succeed");

    let database_client = spanner
        .database_client("projects/test-project/instances/test-instance/databases/test-database")
        .build()
        .await
        .expect("database_client build must succeed");

    assert_eq!(
        spanner.active_channel_count(),
        64,
        "static 64-channel pool must have 64 channels"
    );

    warm_up_database_client(&database_client).await;

    let calm_latencies =
        run_database_client_point_select_workload(&database_client, 2, Duration::from_millis(300))
            .await;
    let calm_queued_count = contention_manager.total_queued_count();
    let calm_metrics = BenchmarkRunMetrics::calculate(calm_queued_count, calm_latencies);

    let burst_start_queued = contention_manager.total_queued_count();
    let burst_latencies =
        run_database_client_point_select_workload(&database_client, 12, Duration::from_millis(800))
            .await;

    let burst_queued_count = contention_manager.total_queued_count() - burst_start_queued;
    let burst_metrics = BenchmarkRunMetrics::calculate(burst_queued_count, burst_latencies);

    eprintln!(
        "\n===============================================================================\n[Replication 2: Static 64 Channels Under DatabaseClient Point-Select Spiky Load]\nChannels: 64 (Fixed)\nServer Permit Limit: 2 parallel requests per connection\nBase Latency: 1.5ms + [0.0ms, 0.5ms] noise\n-------------------------------------------------------------------------------\nNormal State (2 workers, 300ms):\n  Operations: {}\n  Server Queued: {}\n  p50 latency: {:?}\nBurst State (12 workers, 800ms):\n  Operations: {}\n  Server Queued: {}\n  Mean latency: {:?}\n  p50 latency: {:?}\n  p90 latency: {:?}\n  p95 latency: {:?}\n  p99 latency: {:?}\n  Operations > 15ms: {} ({:.2}%)\n===============================================================================",
        calm_metrics.total_operations,
        calm_metrics.queued_operations,
        calm_metrics.p50_latency,
        burst_metrics.total_operations,
        burst_metrics.queued_operations,
        burst_metrics.mean_latency,
        burst_metrics.p50_latency,
        burst_metrics.p90_latency,
        burst_metrics.p95_latency,
        burst_metrics.p99_latency,
        burst_metrics.operations_over_15ms,
        burst_metrics.operations_over_15ms_percent
    );

    // With 64 channels, 12 concurrent workers distribute across separate channels (<= 1 req/channel).
    // Server-side queuing is completely zero, exactly reproducing Configuration C in the benchmark!
    assert_eq!(
        burst_queued_count, 0,
        "64 channels must experience zero server queuing during the 12-worker burst"
    );
    assert!(
        burst_metrics.p95_latency < Duration::from_millis(50),
        "P95 latency with 64 channels must remain bounded in debug mode (<50ms)"
    );
}

#[tokio_test_no_panics]
#[serial]
#[ignore]
async fn replication_dynamic_pool_4s_cooldown_bottleneck_under_short_burst() {
    let contention_manager = ChannelContentionManager::new(2, 1500, 500);
    let (address, _server) = start_contention_server(contention_manager.clone()).await;

    let dynamic_config = DynamicChannelPoolConfig::new()
        .with_initial_channels(4)
        .with_min_channels(4)
        .with_max_channels(64)
        .with_min_rpc_per_channel(1.0)
        .with_max_rpc_per_channel(2.0)
        .with_error_penalty_step(1)
        .with_scale_up_cooldown(Duration::from_secs(4))
        .with_max_scale_up_percent(100);

    let spanner = Spanner::builder()
        .with_endpoint(address)
        .with_credentials(Anonymous::new().build())
        .with_channel_pool(dynamic_config)
        .build()
        .await
        .expect("spanner build must succeed");

    let database_client = spanner
        .database_client("projects/test-project/instances/test-instance/databases/test-database")
        .build()
        .await
        .expect("database_client build must succeed");

    assert_eq!(
        spanner.active_channel_count(),
        4,
        "initial channels must be 4"
    );

    let total_workers = 16;
    let is_stopped = Arc::new(AtomicBool::new(false));
    let mut join_set = JoinSet::new();

    for _ in 0..total_workers {
        let client_clone = database_client.clone();
        let stopped = is_stopped.clone();
        join_set.spawn(async move {
            let mut latencies = Vec::new();
            while !stopped.load(Ordering::Relaxed) {
                let start_time = Instant::now();
                let statement = Statement::builder("SELECT 1").build();
                let transaction = client_clone
                    .single_use()
                    .set_timestamp_bound(TimestampBound::exact_staleness(Duration::from_secs(15)))
                    .build();
                let mut result_set = transaction
                    .execute_query(statement)
                    .await
                    .expect("execute_query must succeed");
                while let Some(row) = result_set
                    .next()
                    .await
                    .transpose()
                    .expect("row must succeed")
                {
                    let _: i64 = row.get(0_usize);
                }
                latencies.push(start_time.elapsed());
            }
            latencies
        });
    }

    // Run burst for 800ms (shorter than the 4s cooldown)
    sleep(Duration::from_millis(800)).await;
    is_stopped.store(true, Ordering::Relaxed);

    let mut burst_latencies = Vec::new();
    while let Some(join_result) = join_set.join_next().await {
        burst_latencies.extend(join_result.expect("worker task must succeed"));
    }

    let final_channels = spanner.active_channel_count();
    let metrics =
        BenchmarkRunMetrics::calculate(contention_manager.total_queued_count(), burst_latencies);

    eprintln!(
        "\n===============================================================================\n[Replication 3: Direct Dynamic Pool With 4s Cooldown Under 800ms Burst]\nChannels: 4 -> {final_channels} (Doubled once at t=0, then throttled by 4s cooldown!)\nServer Permit Limit: 2 parallel requests per connection\nBase Latency: 1.5ms + [0.0ms, 0.5ms] noise\n-------------------------------------------------------------------------------\nBurst State (16 workers, 800ms):\n  Operations: {}\n  Server Queued: {}\n  Mean latency: {:?}\n  p50 latency: {:?}\n  p95 latency: {:?}\n  p99 latency: {:?}\n  Operations > 15ms: {} ({:.2}%)\n===============================================================================",
        metrics.total_operations,
        metrics.queued_operations,
        metrics.mean_latency,
        metrics.p50_latency,
        metrics.p95_latency,
        metrics.p99_latency,
        metrics.operations_over_15ms,
        metrics.operations_over_15ms_percent
    );

    // At t=0, the pool detected saturation and scaled 4 -> 8.
    // But because scale_up_cooldown is 4 seconds, during the entire 800ms burst,
    // it was legally prohibited from scaling further!
    // So it was stuck at 8 channels, where 16 workers create 2 req/channel.
    assert_eq!(
        final_channels, 8,
        "Pool must scale from 4 to 8 at burst onset, but remain throttled at 8 due to 4s cooldown"
    );
}

#[tokio_test_no_panics]
#[serial]
#[ignore]
async fn replication_dynamic_pool_fast_cooldown_recovers_under_burst() {
    let contention_manager = ChannelContentionManager::new(2, 1500, 500);
    let (address, _server) = start_contention_server(contention_manager.clone()).await;

    // Dynamic pool with responsive 50ms cooldown and 100% geometric scale-up.
    let dynamic_config = DynamicChannelPoolConfig::new()
        .with_initial_channels(4)
        .with_min_channels(4)
        .with_max_channels(64)
        .with_min_rpc_per_channel(1.0)
        .with_max_rpc_per_channel(2.0)
        .with_error_penalty_step(1)
        .with_scale_up_cooldown(Duration::from_millis(50))
        .with_max_scale_up_percent(100);

    let spanner = Spanner::builder()
        .with_endpoint(address)
        .with_credentials(Anonymous::new().build())
        .with_channel_pool(dynamic_config)
        .build()
        .await
        .expect("spanner build must succeed");

    let database_client = spanner
        .database_client("projects/test-project/instances/test-instance/databases/test-database")
        .build()
        .await
        .expect("database_client build must succeed");

    assert_eq!(
        spanner.active_channel_count(),
        4,
        "initial channels must be 4"
    );

    let total_workers = 24;
    let is_stopped = Arc::new(AtomicBool::new(false));
    let mut join_set = JoinSet::new();

    for _ in 0..total_workers {
        let client_clone = database_client.clone();
        let stopped = is_stopped.clone();
        join_set.spawn(async move {
            let mut latencies = Vec::new();
            while !stopped.load(Ordering::Relaxed) {
                let start_time = Instant::now();
                let statement = Statement::builder("SELECT 1").build();
                let transaction = client_clone
                    .single_use()
                    .set_timestamp_bound(TimestampBound::exact_staleness(Duration::from_secs(15)))
                    .build();
                let mut result_set = transaction
                    .execute_query(statement)
                    .await
                    .expect("execute_query must succeed");
                while let Some(row) = result_set
                    .next()
                    .await
                    .transpose()
                    .expect("row must succeed")
                {
                    let _: i64 = row.get(0_usize);
                }
                latencies.push(start_time.elapsed());
            }
            latencies
        });
    }

    // Run burst for 800ms (ample time for 50ms cooldowns: 4 -> 8 -> 16 -> 32)
    sleep(Duration::from_millis(800)).await;
    is_stopped.store(true, Ordering::Relaxed);

    let mut burst_latencies = Vec::new();
    while let Some(join_result) = join_set.join_next().await {
        burst_latencies.extend(join_result.expect("worker task must succeed"));
    }

    let final_channels = spanner.active_channel_count();
    let metrics =
        BenchmarkRunMetrics::calculate(contention_manager.total_queued_count(), burst_latencies);

    eprintln!(
        "\n===============================================================================\n[Replication 4: Dynamic Pool With Responsive 50ms Cooldown Under 800ms Burst]\nChannels: 4 -> {final_channels} (Scaled multiple steps rapidly!)\nServer Permit Limit: 2 parallel requests per connection\nBase Latency: 1.5ms + [0.0ms, 0.5ms] noise\n-------------------------------------------------------------------------------\nBurst State (24 workers, 800ms):\n  Operations: {}\n  Server Queued: {}\n  Mean latency: {:?}\n  p50 latency: {:?}\n  p95 latency: {:?}\n  p99 latency: {:?}\n  Operations > 15ms: {} ({:.2}%)\n===============================================================================",
        metrics.total_operations,
        metrics.queued_operations,
        metrics.mean_latency,
        metrics.p50_latency,
        metrics.p95_latency,
        metrics.p99_latency,
        metrics.operations_over_15ms,
        metrics.operations_over_15ms_percent
    );

    // With a 50ms cooldown and 24 workers, the pool scales beyond 8 channels to at least 16 channels.
    assert!(
        final_channels >= 16,
        "Dynamic pool with 50ms cooldown must scale up to at least 16 channels, got {final_channels}"
    );
}

#[tokio_test_no_panics]
#[serial]
#[ignore]
async fn replication_database_client_scales_dynamically_with_fast_cooldown() {
    let contention_manager = ChannelContentionManager::new(2, 1500, 500);
    let (address, _server) = start_contention_server(contention_manager.clone()).await;

    let dynamic_config = DynamicChannelPoolConfig::new()
        .with_initial_channels(4)
        .with_min_channels(4)
        .with_max_channels(64)
        .with_min_rpc_per_channel(1.0)
        .with_max_rpc_per_channel(2.0)
        .with_error_penalty_step(1)
        .with_scale_up_cooldown(Duration::from_millis(50))
        .with_max_scale_up_percent(100);

    let spanner = Spanner::builder()
        .with_endpoint(address)
        .with_credentials(Anonymous::new().build())
        .with_channel_pool(dynamic_config)
        .build()
        .await
        .expect("spanner client build must succeed");

    let database_client = spanner
        .database_client("projects/test-project/instances/test-instance/databases/test-database")
        .build()
        .await
        .expect("database_client build must succeed");

    warm_up_database_client(&database_client).await;

    assert_eq!(
        spanner.active_channel_count(),
        4,
        "initial dynamic channel pool count must be 4"
    );

    // Run burst using DatabaseClient with 24 concurrent workers for 800ms
    let burst_latencies =
        run_database_client_point_select_workload(&database_client, 24, Duration::from_millis(800))
            .await;

    let final_channels = spanner.active_channel_count();
    let queued_count = contention_manager.total_queued_count();
    let burst_metrics = BenchmarkRunMetrics::calculate(queued_count, burst_latencies);

    eprintln!(
        "\n===============================================================================\n[Replication 5: DatabaseClient Under Dynamic Pool With 50ms Cooldown]\nChannels: 4 -> {final_channels} (DatabaseClient successfully drives dynamic scaling!)\nServer Permit Limit: 2 parallel requests per connection\nBase Latency: 1.5ms + [0.0ms, 0.5ms] noise\n-------------------------------------------------------------------------------\nBurst State (24 workers, 800ms):\n  Operations: {}\n  Server Queued: {}\n  Mean latency: {:?}\n  p50 latency: {:?}\n  p90 latency: {:?}\n  p95 latency: {:?}\n  p99 latency: {:?}\n  Operations > 15ms: {} ({:.2}%)\n===============================================================================",
        burst_metrics.total_operations,
        burst_metrics.queued_operations,
        burst_metrics.mean_latency,
        burst_metrics.p50_latency,
        burst_metrics.p90_latency,
        burst_metrics.p95_latency,
        burst_metrics.p99_latency,
        burst_metrics.operations_over_15ms,
        burst_metrics.operations_over_15ms_percent
    );

    assert!(
        final_channels >= 16,
        "DatabaseClient queries must drive dynamic channel pool scaling beyond 8 to at least 16 channels, got {final_channels}"
    );
}

async fn warm_up_select_update(database_client: &DatabaseClient) {
    let runner = database_client
        .read_write_transaction()
        .build()
        .await
        .expect("warm_up build read_write_transaction");

    runner
        .run(|transaction: ReadWriteTransaction| async move {
            let statement = Statement::builder("SELECT id FROM test WHERE id = @id")
                .add_param("id", 1i64)
                .build();
            let mut result_set = transaction.execute_query(statement).await?;
            let _ = result_set.next().await;
            drop(result_set);

            let update_statement =
                Statement::builder("UPDATE test SET value = @value WHERE id = @id")
                    .add_param("value", "warmup-value")
                    .add_param("id", 1i64)
                    .build();
            transaction.execute_update(update_statement).await?;
            Ok(())
        })
        .await
        .expect("warm_up select_update must succeed");
}

async fn run_database_client_select_update_workload(
    database_client: &DatabaseClient,
    worker_count: usize,
    duration: Duration,
) -> Vec<Duration> {
    let is_stopped = Arc::new(AtomicBool::new(false));
    let mut join_set = JoinSet::new();

    for _ in 0..worker_count {
        let client_clone = database_client.clone();
        let stopped = is_stopped.clone();
        join_set.spawn(async move {
            let mut worker_latencies = Vec::new();
            while !stopped.load(Ordering::Relaxed) {
                let start_time = Instant::now();
                let runner = client_clone
                    .read_write_transaction()
                    .build()
                    .await
                    .expect("build read_write_transaction runner");

                runner
                    .run(|transaction: ReadWriteTransaction| async move {
                        let random_id = random_range(1i64..=1_000_000i64);
                        let select_statement =
                            Statement::builder("SELECT id FROM test WHERE id = @id")
                                .add_param("id", random_id)
                                .build();

                        let mut result_set = transaction.execute_query(select_statement).await?;
                        let exists = result_set.next().await.transpose()?.is_some();
                        drop(result_set);

                        if exists {
                            let update_statement =
                                Statement::builder("UPDATE test SET value = @value WHERE id = @id")
                                    .add_param("value", "updated-test-value")
                                    .add_param("id", random_id)
                                    .build();
                            transaction.execute_update(update_statement).await?;
                        } else {
                            let insert_statement = Statement::builder(
                                "INSERT INTO test (id, value) VALUES (@id, @value)",
                            )
                            .add_param("id", random_id)
                            .add_param("value", "inserted-test-value")
                            .build();
                            transaction.execute_update(insert_statement).await?;
                        }

                        Ok(())
                    })
                    .await
                    .expect("select_update transaction must succeed");

                worker_latencies.push(start_time.elapsed());
            }
            worker_latencies
        });
    }

    sleep(duration).await;
    is_stopped.store(true, Ordering::Relaxed);

    let mut all_latencies = Vec::new();
    while let Some(join_result) = join_set.join_next().await {
        let worker_latencies = join_result.expect("worker task must succeed");
        all_latencies.extend(worker_latencies);
    }
    all_latencies
}

#[tokio_test_no_panics]
#[serial]
#[ignore]
async fn replication_select_update_database_client_under_default_dynamic_pool() {
    let contention_manager = ChannelContentionManager::new(2, 1500, 500);
    let (address, _server) = start_contention_server(contention_manager.clone()).await;

    let dynamic_config = DynamicChannelPoolConfig::new()
        .with_initial_channels(4)
        .with_min_channels(4)
        .with_max_channels(256)
        .with_min_rpc_per_channel(1.0)
        .with_max_rpc_per_channel(2.0)
        .with_error_penalty_step(1)
        .with_scale_up_cooldown(Duration::from_secs(4))
        .with_max_scale_up_percent(100);

    let spanner = Spanner::builder()
        .with_endpoint(address)
        .with_credentials(Anonymous::new().build())
        .with_channel_pool(dynamic_config)
        .build()
        .await
        .expect("spanner client build must succeed");

    let database_client = spanner
        .database_client("projects/test-project/instances/test-instance/databases/test-database")
        .build()
        .await
        .expect("database_client build must succeed");

    warm_up_select_update(&database_client).await;

    assert_eq!(
        spanner.active_channel_count(),
        4,
        "initial dynamic channel pool count must be 4"
    );

    // Run burst of select-then-update transactions with 24 concurrent workers for 800ms
    let burst_latencies = run_database_client_select_update_workload(
        &database_client,
        24,
        Duration::from_millis(800),
    )
    .await;

    let final_channels = spanner.active_channel_count();
    let queued_count = contention_manager.total_queued_count();
    let burst_metrics = BenchmarkRunMetrics::calculate(queued_count, burst_latencies);

    eprintln!(
        "\n===============================================================================\n[Replication 6: Select-Then-Update DatabaseClient Under Default Dynamic Pool (4s Cooldown)]\nChannels: 4 -> {final_channels} (Default 4s cooldown limits scale-up during 800ms burst)\nServer Permit Limit: 2 parallel requests per connection\nBase Latency: 1.5ms + [0.0ms, 0.5ms] noise per RPC (3 RPCs per txn: select, update, commit)\n-------------------------------------------------------------------------------\nBurst State (24 workers, 800ms):\n  Transactions: {}\n  Server Queued Requests: {}\n  Mean latency: {:?}\n  p50 latency: {:?}\n  p90 latency: {:?}\n  p95 latency: {:?}\n  p99 latency: {:?}\n  Transactions > 15ms: {} ({:.2}%)\n===============================================================================",
        burst_metrics.total_operations,
        burst_metrics.queued_operations,
        burst_metrics.mean_latency,
        burst_metrics.p50_latency,
        burst_metrics.p90_latency,
        burst_metrics.p95_latency,
        burst_metrics.p99_latency,
        burst_metrics.operations_over_15ms,
        burst_metrics.operations_over_15ms_percent
    );

    assert!(
        burst_metrics.total_operations > 0,
        "Expected at least one select-update transaction to execute"
    );
}

#[tokio_test_no_panics]
#[serial]
#[ignore]
async fn replication_select_update_database_client_under_64_channel_static_pool() {
    let contention_manager = ChannelContentionManager::new(2, 1500, 500);
    let (address, _server) = start_contention_server(contention_manager.clone()).await;

    let static_config = StaticChannelPoolConfig::new(64);

    let spanner = Spanner::builder()
        .with_endpoint(address)
        .with_credentials(Anonymous::new().build())
        .with_channel_pool(static_config)
        .build()
        .await
        .expect("spanner client build must succeed");

    let database_client = spanner
        .database_client("projects/test-project/instances/test-instance/databases/test-database")
        .build()
        .await
        .expect("database_client build must succeed");

    warm_up_select_update(&database_client).await;

    assert_eq!(
        spanner.active_channel_count(),
        64,
        "static pool must have 64 channels"
    );

    // Run burst of select-then-update transactions with 24 concurrent workers for 800ms
    let burst_latencies = run_database_client_select_update_workload(
        &database_client,
        24,
        Duration::from_millis(800),
    )
    .await;

    let queued_count = contention_manager.total_queued_count();
    let burst_metrics = BenchmarkRunMetrics::calculate(queued_count, burst_latencies);

    eprintln!(
        "\n===============================================================================\n[Replication 7: Select-Then-Update DatabaseClient Under Static 64-Channel Pool]\nChannels: 64 (Static, abundant channels distribute 24 transactions with minimal queuing)\nServer Permit Limit: 2 parallel requests per connection\nBase Latency: 1.5ms + [0.0ms, 0.5ms] noise per RPC (3 RPCs per txn: select, update, commit)\n-------------------------------------------------------------------------------\nBurst State (24 workers, 800ms):\n  Transactions: {}\n  Server Queued Requests: {}\n  Mean latency: {:?}\n  p50 latency: {:?}\n  p90 latency: {:?}\n  p95 latency: {:?}\n  p99 latency: {:?}\n  Transactions > 15ms: {} ({:.2}%)\n===============================================================================",
        burst_metrics.total_operations,
        burst_metrics.queued_operations,
        burst_metrics.mean_latency,
        burst_metrics.p50_latency,
        burst_metrics.p90_latency,
        burst_metrics.p95_latency,
        burst_metrics.p99_latency,
        burst_metrics.operations_over_15ms,
        burst_metrics.operations_over_15ms_percent
    );

    assert!(
        burst_metrics.total_operations > 0,
        "Expected at least one select-update transaction to execute"
    );
}

#[tokio_test_no_panics]
#[serial]
#[ignore]
async fn replication_select_update_database_client_scales_dynamically_with_fast_cooldown() {
    let contention_manager = ChannelContentionManager::new(2, 1500, 500);
    let (address, _server) = start_contention_server(contention_manager.clone()).await;

    let dynamic_config = DynamicChannelPoolConfig::new()
        .with_initial_channels(4)
        .with_min_channels(4)
        .with_max_channels(64)
        .with_min_rpc_per_channel(1.0)
        .with_max_rpc_per_channel(2.0)
        .with_error_penalty_step(1)
        .with_scale_up_cooldown(Duration::from_millis(50))
        .with_max_scale_up_percent(100);

    let spanner = Spanner::builder()
        .with_endpoint(address)
        .with_credentials(Anonymous::new().build())
        .with_channel_pool(dynamic_config)
        .build()
        .await
        .expect("spanner client build must succeed");

    let database_client = spanner
        .database_client("projects/test-project/instances/test-instance/databases/test-database")
        .build()
        .await
        .expect("database_client build must succeed");

    warm_up_select_update(&database_client).await;

    assert_eq!(
        spanner.active_channel_count(),
        4,
        "initial dynamic channel pool count must be 4"
    );

    // Run burst of select-then-update transactions with 24 concurrent workers for 800ms
    let burst_latencies = run_database_client_select_update_workload(
        &database_client,
        24,
        Duration::from_millis(800),
    )
    .await;

    let final_channels = spanner.active_channel_count();
    let queued_count = contention_manager.total_queued_count();
    let burst_metrics = BenchmarkRunMetrics::calculate(queued_count, burst_latencies);

    eprintln!(
        "\n===============================================================================\n[Replication 8: Select-Then-Update DatabaseClient Under Dynamic Pool With 50ms Cooldown]\nChannels: 4 -> {final_channels} (Select-Update transactions successfully drive dynamic scaling!)\nServer Permit Limit: 2 parallel requests per connection\nBase Latency: 1.5ms + [0.0ms, 0.5ms] noise per RPC (3 RPCs per txn: select, update, commit)\n-------------------------------------------------------------------------------\nBurst State (24 workers, 800ms):\n  Transactions: {}\n  Server Queued Requests: {}\n  Mean latency: {:?}\n  p50 latency: {:?}\n  p90 latency: {:?}\n  p95 latency: {:?}\n  p99 latency: {:?}\n  Transactions > 15ms: {} ({:.2}%)\n===============================================================================",
        burst_metrics.total_operations,
        burst_metrics.queued_operations,
        burst_metrics.mean_latency,
        burst_metrics.p50_latency,
        burst_metrics.p90_latency,
        burst_metrics.p95_latency,
        burst_metrics.p99_latency,
        burst_metrics.operations_over_15ms,
        burst_metrics.operations_over_15ms_percent
    );

    assert!(
        final_channels >= 16,
        "Select-Update transactions must drive dynamic channel pool scaling beyond 8 to at least 16 channels, got {final_channels}"
    );
}

#[tokio_test_no_panics]
#[serial]
#[ignore]
async fn replication_select_update_database_client_2min_spiky_default_dynamic_pool() {
    let contention_manager = ChannelContentionManager::new(2, 1500, 500);
    let (address, _server) = start_contention_server(contention_manager.clone()).await;

    // Default dynamic pool configuration with 4s scale_up_cooldown
    let dynamic_config = DynamicChannelPoolConfig::new()
        .with_initial_channels(4)
        .with_min_channels(4)
        .with_max_channels(64)
        .with_min_rpc_per_channel(1.0)
        .with_max_rpc_per_channel(2.0)
        .with_error_penalty_step(1)
        .with_scale_up_cooldown(Duration::from_secs(4))
        .with_max_scale_up_percent(100);

    let spanner = Spanner::builder()
        .with_endpoint(address)
        .with_credentials(Anonymous::new().build())
        .with_channel_pool(dynamic_config)
        .build()
        .await
        .expect("spanner client build must succeed");

    let database_client = spanner
        .database_client("projects/test-project/instances/test-instance/databases/test-database")
        .build()
        .await
        .expect("database_client build must succeed");

    warm_up_select_update(&database_client).await;

    assert_eq!(
        spanner.active_channel_count(),
        4,
        "initial dynamic channel pool count must be 4"
    );

    // 2-Minute Spiky Workload: 6 Cycles of [10s calm (2 workers), 10s burst (24 workers)]
    let mut all_calm_latencies = Vec::new();
    let mut all_burst_latencies = Vec::new();
    let mut steady_state_burst_latencies = Vec::new();

    eprintln!("\n===============================================================================");
    eprintln!("[Replication 9: 2-Minute Spiky Select-Then-Update Workload (Default 4s Cooldown)]");
    eprintln!("Load Pattern: 6 Cycles of [10s Calm (2 workers) + 10s Burst (24 workers)] = 120s");
    eprintln!("Dynamic Pool: Initial 4, Min 4, Max 64, 4s Scale-Up Cooldown, 100% Geometric Scale");
    eprintln!(
        "Server Limits: 2 permits per channel, 1.5ms base + [0.0, 0.5ms] jitter (3 RPCs/txn)"
    );
    eprintln!("===============================================================================");

    for cycle in 1..=6 {
        // --- Calm Phase (10s, 2 workers) ---
        let calm_queued_start = contention_manager.total_queued_count();
        let calm_latencies = run_database_client_select_update_workload(
            &database_client,
            2,
            Duration::from_secs(10),
        )
        .await;
        let calm_queued = contention_manager.total_queued_count() - calm_queued_start;
        let calm_channels = spanner.active_channel_count();
        let calm_metrics = BenchmarkRunMetrics::calculate(calm_queued, calm_latencies.clone());
        all_calm_latencies.extend(calm_latencies);

        eprintln!(
            "Cycle {} Calm  (10s, 2 workers):  Txns: {:>5} | Queued: {:>4} | Channels: {:>2} | p50: {:>8.2?} | p95: {:>8.2?}",
            cycle,
            calm_metrics.total_operations,
            calm_metrics.queued_operations,
            calm_channels,
            calm_metrics.p50_latency,
            calm_metrics.p95_latency,
        );

        // --- Burst Phase (10s, 24 workers) ---
        let burst_queued_start = contention_manager.total_queued_count();
        let burst_latencies = run_database_client_select_update_workload(
            &database_client,
            24,
            Duration::from_secs(10),
        )
        .await;
        let burst_queued = contention_manager.total_queued_count() - burst_queued_start;
        let burst_channels = spanner.active_channel_count();
        let burst_metrics = BenchmarkRunMetrics::calculate(burst_queued, burst_latencies.clone());
        all_burst_latencies.extend(burst_latencies.clone());

        if cycle > 1 {
            steady_state_burst_latencies.extend(burst_latencies);
        }

        eprintln!(
            "Cycle {} Burst (10s, 24 workers): Txns: {:>5} | Queued: {:>4} | Channels: {:>2} | p50: {:>8.2?} | p90: {:>8.2?} | p95: {:>8.2?} | p99: {:>8.2?} | >15ms: {:>5.2}%",
            cycle,
            burst_metrics.total_operations,
            burst_metrics.queued_operations,
            burst_channels,
            burst_metrics.p50_latency,
            burst_metrics.p90_latency,
            burst_metrics.p95_latency,
            burst_metrics.p99_latency,
            burst_metrics.operations_over_15ms_percent,
        );
    }

    let final_channels = spanner.active_channel_count();
    let total_queued = contention_manager.total_queued_count();
    let overall_burst_metrics = BenchmarkRunMetrics::calculate(total_queued, all_burst_latencies);
    let steady_burst_metrics = BenchmarkRunMetrics::calculate(0, steady_state_burst_latencies);

    eprintln!("\n-------------------------------------------------------------------------------");
    eprintln!("[2-Minute Aggregate Summary: Dynamic Pool (4s Cooldown)]");
    eprintln!("Final Channels: {}", final_channels);
    eprintln!(
        "Total Server Queued Requests across 2 minutes: {}",
        total_queued
    );
    eprintln!("\nSteady-State Bursts (Cycles 2-6 Combined, Channels = 64):");
    eprintln!(
        "  Transactions:       {}",
        steady_burst_metrics.total_operations
    );
    eprintln!(
        "  Mean latency:       {:?}",
        steady_burst_metrics.mean_latency
    );
    eprintln!(
        "  p50 latency:        {:?}",
        steady_burst_metrics.p50_latency
    );
    eprintln!(
        "  p90 latency:        {:?}",
        steady_burst_metrics.p90_latency
    );
    eprintln!(
        "  p95 latency:        {:?}",
        steady_burst_metrics.p95_latency
    );
    eprintln!(
        "  p99 latency:        {:?}",
        steady_burst_metrics.p99_latency
    );
    eprintln!(
        "  Transactions >15ms: {} ({:.2}%)",
        steady_burst_metrics.operations_over_15ms,
        steady_burst_metrics.operations_over_15ms_percent
    );
    eprintln!("\nOverall All Bursts (Cycles 1-6 Combined, including Cold Ramp-up):");
    eprintln!(
        "  Transactions:       {}",
        overall_burst_metrics.total_operations
    );
    eprintln!(
        "  Mean latency:       {:?}",
        overall_burst_metrics.mean_latency
    );
    eprintln!(
        "  p50 latency:        {:?}",
        overall_burst_metrics.p50_latency
    );
    eprintln!(
        "  p90 latency:        {:?}",
        overall_burst_metrics.p90_latency
    );
    eprintln!(
        "  p95 latency:        {:?}",
        overall_burst_metrics.p95_latency
    );
    eprintln!(
        "  p99 latency:        {:?}",
        overall_burst_metrics.p99_latency
    );
    eprintln!(
        "  Transactions >15ms: {} ({:.2}%)",
        overall_burst_metrics.operations_over_15ms,
        overall_burst_metrics.operations_over_15ms_percent
    );
    eprintln!("===============================================================================");

    assert!(
        final_channels >= 32,
        "Pool must scale to at least 32 channels during 2-minute spiky workload with 10s bursts, got {final_channels}"
    );
}

#[tokio_test_no_panics]
#[serial]
#[ignore]
async fn replication_select_update_database_client_2min_spiky_64_channel_static_pool() {
    let contention_manager = ChannelContentionManager::new(2, 1500, 500);
    let (address, _server) = start_contention_server(contention_manager.clone()).await;

    let static_config = StaticChannelPoolConfig::new(64);

    let spanner = Spanner::builder()
        .with_endpoint(address)
        .with_credentials(Anonymous::new().build())
        .with_channel_pool(static_config)
        .build()
        .await
        .expect("spanner client build must succeed");

    let database_client = spanner
        .database_client("projects/test-project/instances/test-instance/databases/test-database")
        .build()
        .await
        .expect("database_client build must succeed");

    warm_up_select_update(&database_client).await;

    assert_eq!(
        spanner.active_channel_count(),
        64,
        "static channel pool count must be 64"
    );

    // 2-Minute Spiky Workload: 6 Cycles of [10s calm (2 workers), 10s burst (24 workers)]
    let mut all_burst_latencies = Vec::new();

    eprintln!("\n===============================================================================");
    eprintln!("[Replication 10: 2-Minute Spiky Select-Then-Update Workload (Static 64 Channels)]");
    eprintln!("Load Pattern: 6 Cycles of [10s Calm (2 workers) + 10s Burst (24 workers)] = 120s");
    eprintln!("Static Pool: 64 Channels fixed (abundant channels)");
    eprintln!(
        "Server Limits: 2 permits per channel, 1.5ms base + [0.0, 0.5ms] jitter (3 RPCs/txn)"
    );
    eprintln!("===============================================================================");

    for cycle in 1..=6 {
        // --- Calm Phase (10s, 2 workers) ---
        let calm_queued_start = contention_manager.total_queued_count();
        let calm_latencies = run_database_client_select_update_workload(
            &database_client,
            2,
            Duration::from_secs(10),
        )
        .await;
        let calm_queued = contention_manager.total_queued_count() - calm_queued_start;
        let calm_metrics = BenchmarkRunMetrics::calculate(calm_queued, calm_latencies);

        eprintln!(
            "Cycle {} Calm  (10s, 2 workers):  Txns: {:>5} | Queued: {:>4} | Channels: 64 | p50: {:>8.2?} | p95: {:>8.2?}",
            cycle,
            calm_metrics.total_operations,
            calm_metrics.queued_operations,
            calm_metrics.p50_latency,
            calm_metrics.p95_latency,
        );

        // --- Burst Phase (10s, 24 workers) ---
        let burst_queued_start = contention_manager.total_queued_count();
        let burst_latencies = run_database_client_select_update_workload(
            &database_client,
            24,
            Duration::from_secs(10),
        )
        .await;
        let burst_queued = contention_manager.total_queued_count() - burst_queued_start;
        let burst_metrics = BenchmarkRunMetrics::calculate(burst_queued, burst_latencies.clone());
        all_burst_latencies.extend(burst_latencies);

        eprintln!(
            "Cycle {} Burst (10s, 24 workers): Txns: {:>5} | Queued: {:>4} | Channels: 64 | p50: {:>8.2?} | p90: {:>8.2?} | p95: {:>8.2?} | p99: {:>8.2?} | >15ms: {:>5.2}%",
            cycle,
            burst_metrics.total_operations,
            burst_metrics.queued_operations,
            burst_metrics.p50_latency,
            burst_metrics.p90_latency,
            burst_metrics.p95_latency,
            burst_metrics.p99_latency,
            burst_metrics.operations_over_15ms_percent,
        );
    }

    let total_queued = contention_manager.total_queued_count();
    let overall_burst_metrics = BenchmarkRunMetrics::calculate(total_queued, all_burst_latencies);

    eprintln!("\n-------------------------------------------------------------------------------");
    eprintln!("[Static 64-Channel Pool 2-Minute Aggregate Summary]");
    eprintln!("Channels: 64");
    eprintln!(
        "Total Server Queued Requests across 2 minutes: {}",
        total_queued
    );
    eprintln!("All Bursts Combined:");
    eprintln!(
        "  Transactions:       {}",
        overall_burst_metrics.total_operations
    );
    eprintln!(
        "  Mean latency:       {:?}",
        overall_burst_metrics.mean_latency
    );
    eprintln!(
        "  p50 latency:        {:?}",
        overall_burst_metrics.p50_latency
    );
    eprintln!(
        "  p90 latency:        {:?}",
        overall_burst_metrics.p90_latency
    );
    eprintln!(
        "  p95 latency:        {:?}",
        overall_burst_metrics.p95_latency
    );
    eprintln!(
        "  p99 latency:        {:?}",
        overall_burst_metrics.p99_latency
    );
    eprintln!(
        "  Transactions >15ms: {} ({:.2}%)",
        overall_burst_metrics.operations_over_15ms,
        overall_burst_metrics.operations_over_15ms_percent
    );
    assert_eq!(
        spanner.active_channel_count(),
        64,
        "static channel pool count must remain 64"
    );
    eprintln!("===============================================================================");
}
