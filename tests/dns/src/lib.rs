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

use google_cloud_dns_v1::{client::ManagedZones, model::ManagedZone};
use google_cloud_gax::paginator::ItemPaginator as _;
use rand::Rng;
use std::time::Duration;

mod random_chars;
use random_chars::RandomChars;

const MAX_STALE: Duration = Duration::from_secs(48 * 3600);

pub async fn run_all() -> anyhow::Result<()> {
    let _guard = enable_tracing();
    let project = std::env::var("GOOGLE_CLOUD_PROJECT")?;
    let builder = ManagedZones::builder();
    #[cfg(feature = "log-integration-tests")]
    let builder = builder.with_tracing();
    let client = builder.build().await?;
    cleanup_stale_zones(&client, &project).await?;

    let zone_id = random_zone_id();
    let zone = client
        .create()
        .set_project(&project)
        .set_body(
            ManagedZone::new()
                .set_name(&zone_id)
                .set_description(
                    "Test managed zone created by the google-cloud-rust integration tests",
                )
                .set_dns_name(format!("{project}.internal."))
                .set_labels([("integration-test", "true")]),
        )
        .send()
        .await?;
    tracing::info!("successfully created zone: {zone:?}");

    let get = client
        .get_iam_policy()
        .set_resource(format!("projects/{project}/managedZones/{zone_id}"))
        .send()
        .await?;
    tracing::info!("successfully fetched zone IAM policy: {get:?}");

    client
        .delete()
        .set_project(&project)
        .set_managed_zone(zone_id)
        .send()
        .await?;
    tracing::info!("successfully deleted the zone");

    Ok(())
}

async fn cleanup_stale_zones(client: &ManagedZones, project: &str) -> anyhow::Result<()> {
    use chrono::Utc;
    let deadline = Utc::now() - MAX_STALE;
    let deadline = deadline.to_rfc3339();

    let mut stale = Vec::new();
    let mut items = client.list().set_project(project).by_item();
    while let Some(zone) = items.next().await.transpose()? {
        if zone
            .labels
            .get("integration-test")
            .is_none_or(|v| v != "true")
        {
            continue;
        }
        if zone.creation_time.is_some_and(|c| c <= deadline) {
            stale.push(zone.name.unwrap());
        }
    }
    let stale = stale
        .into_iter()
        .map(|name| client.delete().set_managed_zone(name).send());
    let result = futures::future::join_all(stale).await;
    result
        .into_iter()
        .filter(|r| r.is_err())
        .for_each(|r| tracing::error!("error deleting zone: {r:?}"));
    Ok(())
}

fn random_zone_id() -> String {
    const ZONE_ID_LENGTH: usize = 63;
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";

    let distr = RandomChars::new(CHARSET);
    const PREFIX: &str = "rust-sdk-testing-";
    let id: String = rand::rng()
        .sample_iter(distr)
        .take(ZONE_ID_LENGTH - PREFIX.len())
        .map(char::from)
        .collect();
    format!("{PREFIX}{id}")
}

fn enable_tracing() -> tracing::dispatcher::DefaultGuard {
    use tracing_subscriber::fmt::format::FmtSpan;

    let subscriber = tracing_subscriber::fmt()
        .with_level(true)
        .with_thread_ids(true)
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .with_writer(std::io::stderr)
        .with_max_level(tracing::Level::INFO)
        .finish();

    tracing::subscriber::set_default(subscriber)
}
