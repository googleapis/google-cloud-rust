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

use gax::{
    options::RequestOptionsBuilder,
    retry_policy::{Aip194Strict, RetryPolicyExt},
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

const TOO_MANY_ERRORS: u64 = 100;
static ERROR_COUNT: AtomicU64 = AtomicU64::new(0);
static SUCCESS_COUNT: AtomicU64 = AtomicU64::new(0);
static UPDATE_COUNT: AtomicU64 = AtomicU64::new(0);

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() {
    if let Err(e) = start_workers().await {
        report_error(e, "main");
    }
}

/// Continuously perform RPCs to a given secret.
async fn worker(
    client: sm::client::SecretManagerService,
    secret: String,
    task_id: usize,
    total_workers: usize,
) -> anyhow::Result<()> {
    // First create a secret version, to ensure the loop will succeed.
    let mut version = update_secret(&client, &secret).await?;

    // We want to create a new version every minute on average. That will
    // keep this test well below the quota:
    //   https://cloud.google.com/secret-manager/quotas
    let add_version_period = Duration::from_secs(60).mul_f32(total_workers as f32);
    let report_period = Duration::from_secs(10);

    let mut last_add_version = Instant::now();
    let mut last_report = Instant::now();
    let mut error_count = 0;
    let mut success_count = 0;
    let mut update_count = 0;
    loop {
        if Instant::now() >= last_add_version + add_version_period {
            version = update_secret(&client, &secret).await?;
            last_add_version = Instant::now();
            update_count += 1;
        }
        if Instant::now() >= last_report + report_period {
            SUCCESS_COUNT.fetch_add(success_count, Ordering::SeqCst);
            ERROR_COUNT.fetch_add(error_count, Ordering::SeqCst);
            UPDATE_COUNT.fetch_add(update_count, Ordering::SeqCst);
            if task_id == 0 {
                report_info(
                    format!(
                        "success_count={}, error_count={}, version_count={}, current_success_count={}, current_error_count={}, total_workers={}",
                        SUCCESS_COUNT.load(Ordering::Relaxed),
                        ERROR_COUNT.load(Ordering::Relaxed),
                        UPDATE_COUNT.load(Ordering::Relaxed),
                        success_count,
                        error_count,
                        total_workers
                    ),
                    "task[0]",
                );
            }
            last_report = Instant::now();
            error_count = 0;
            success_count = 0;
            update_count = 0;
        }
        // We want to average about 80,000 requests per minute. That will keep
        // the test well below the quota:
        //   https://cloud.google.com/secret-manager/quotas
        let wait = tokio::time::Instant::now()
            + Duration::from_secs(60)
                .mul_f32(total_workers as f32)
                .div_f32(80_000_f32);
        let access = client
            .access_secret_version()
            .set_name(&version.name)
            .with_attempt_timeout(Duration::from_secs(1))
            .send()
            .await;
        match access {
            Ok(_) => {
                success_count += 1;
            }
            Err(e) => {
                error_count += 1;
                if error_count > TOO_MANY_ERRORS && success_count == 0 {
                    return Err(e.into());
                }
            }
        };
        tokio::time::sleep_until(wait).await;
    }
}

async fn update_secret(
    client: &sm::client::SecretManagerService,
    secret: &str,
) -> anyhow::Result<sm::model::SecretVersion> {
    use gax::paginator::ItemPaginator;
    use sm::model::secret_version::State;
    // To keep things tidy, remove any existing versions.
    let mut items = client.list_secret_versions().set_parent(secret).by_item();
    while let Some(version) = items.next().await {
        let version = version?;
        if version.state == State::Destroyed {
            continue;
        }
        client
            .destroy_secret_version()
            .set_name(&version.name)
            .send()
            .await?;
    }

    let data = "The quick brown fox jumps over the lazy dog".as_bytes();
    let checksum = crc32c::crc32c(data);
    let version = client
        .add_secret_version()
        .set_parent(secret)
        .set_payload(
            sm::model::SecretPayload::new()
                .set_data(data)
                .set_data_crc32c(checksum as i64),
        )
        .send()
        .await?;
    Ok(version)
}

async fn start_workers() -> anyhow::Result<()> {
    let project_id = std::env::var("PROJECT_ID")?;
    let client = sm::client::SecretManagerService::builder()
        .with_retry_policy(
            Aip194Strict
                .with_time_limit(Duration::from_secs(15))
                .with_attempt_limit(5),
        )
        .build()
        .await?;

    // Get the list of secrets available for the endurance test.
    let endurance_secrets = get_endurance_secrets(&client, &project_id).await?;
    let total_workers = endurance_secrets.len();
    assert_ne!(total_workers, 0);
    let workers = endurance_secrets
        .into_iter()
        .enumerate()
        .map(|(id, secret)| {
            tokio::spawn({
                let client = client.clone();
                async move {
                    let task = format!("worker[{secret}]");
                    worker(client.clone(), secret, id, total_workers)
                        .await
                        .map_err(|e| report_error(e, &task))
                }
            })
        })
        .collect::<Vec<_>>();

    let _ = futures::future::join_all(workers).await;

    Ok(())
}

async fn get_endurance_secrets(
    client: &sm::client::SecretManagerService,
    project_id: &str,
) -> anyhow::Result<Vec<String>> {
    use gax::paginator::ItemPaginator;
    let mut secrets = Vec::new();
    let mut items = client
        .list_secrets()
        .set_parent(format!("projects/{project_id}"))
        .by_item();
    while let Some(secret) = items.next().await {
        let secret = secret?;
        if secret.labels.contains_key("endurance-test") {
            secrets.push(secret.name);
        }
    }
    if secrets.is_empty() {
        return Err(anyhow::Error::msg(format!(
            "no secrets with the `endurance-test` label found in {project_id}"
        )));
    }
    Ok(secrets)
}

fn report_error(error: anyhow::Error, task: &str) {
    let structured = serde_json::json!({
        "severity": "error",
        "labels": {
            "application": env!("CARGO_PKG_NAME"),
            "version":     env!("CARGO_PKG_VERSION"),
            "task": task,
        },
        "message": format!("{error}"),
    });
    eprintln!("{structured}");
}

fn report_info(msg: String, task: &str) {
    let structured = serde_json::json!({
        "severity": "info",
        "labels": {
            "application": env!("CARGO_PKG_NAME"),
            "version":     env!("CARGO_PKG_VERSION"),
            "task": task,
        },
        "message": msg,
    });
    println!("{structured}");
}
