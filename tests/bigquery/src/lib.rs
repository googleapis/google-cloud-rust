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

use anyhow::Result;
use futures::stream::StreamExt;
use google_cloud_bigquery::client::BigQuery;
use google_cloud_bigquery::model::QueryReference;
use google_cloud_bigquery_v2::client::{DatasetService, JobService};
use google_cloud_bigquery_v2::model::{
    Dataset, DatasetReference, Job, JobConfiguration, JobConfigurationQuery, JobReference,
};
use google_cloud_gax::{error::rpc::Code, paginator::ItemPaginator};
use google_cloud_test_utils::runtime_config::project_id;
use google_cloud_type::model::Decimal;
use rand::{RngExt, distr::Alphanumeric};
use rust_decimal::Decimal as RustDecimal;

const INSTANCE_LABEL: &str = "rust-sdk-integration-test";

pub async fn dataset_admin() -> Result<()> {
    let project_id = project_id()?;
    let client = DatasetService::builder().with_tracing().build().await?;
    cleanup_stale_datasets(&client, &project_id).await?;

    let dataset_id = random_dataset_id();

    println!("CREATING DATASET WITH ID: {dataset_id}");

    let create = client
        .insert_dataset()
        .set_project_id(&project_id)
        .set_dataset(
            Dataset::new()
                .set_dataset_reference(DatasetReference::new().set_dataset_id(&dataset_id))
                .set_labels([(INSTANCE_LABEL, "true")]),
        )
        .send()
        .await?;
    println!("CREATE DATASET = {create:?}");

    assert!(create.dataset_reference.is_some(), "{create:?}");

    let list = client
        .list_datasets()
        .set_project_id(&project_id)
        .set_filter(format!("labels.{INSTANCE_LABEL}"))
        .by_item()
        .into_stream();
    let items = list.collect::<Vec<_>>().await;
    println!("LIST DATASET = {} entries", items.len());

    assert!(
        items
            .iter()
            .any(|v| v.as_ref().unwrap().id.contains(&dataset_id))
    );

    client
        .delete_dataset()
        .set_project_id(&project_id)
        .set_dataset_id(&dataset_id)
        .set_delete_contents(true)
        .send()
        .await?;
    println!("DELETE DATASET");

    Ok(())
}

async fn cleanup_stale_datasets(client: &DatasetService, project_id: &str) -> Result<()> {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    let stale_deadline = SystemTime::now().duration_since(UNIX_EPOCH)?;
    let stale_deadline = stale_deadline - Duration::from_secs(48 * 60 * 60);
    let stale_deadline = stale_deadline.as_millis() as i64;

    let list = client
        .list_datasets()
        .set_project_id(project_id)
        .set_filter(format!("labels.{INSTANCE_LABEL}"))
        .by_item()
        .into_stream();
    let datasets = list.collect::<Vec<_>>().await;

    let pending_all_datasets = datasets
        .iter()
        .filter_map(|v| match v {
            Ok(v) => {
                if let Some(dataset_id) = extract_dataset_id(project_id, &v.id) {
                    return Some(
                        client
                            .get_dataset()
                            .set_project_id(project_id)
                            .set_dataset_id(dataset_id)
                            .send(),
                    );
                }
                None
            }
            Err(_) => None,
        })
        .collect::<Vec<_>>();

    let stale_datasets = futures::future::join_all(pending_all_datasets)
        .await
        .into_iter()
        .filter_map(|r| match r {
            Ok(dataset) => Some(dataset),
            Err(e) if e.status().is_some_and(|s| s.code == Code::NotFound) => None,
            Err(_) => panic!("expected a successful get_dataset()"),
        })
        .filter_map(|dataset| {
            if dataset
                .labels
                .get(INSTANCE_LABEL)
                .is_some_and(|v| v == "true")
                && dataset.creation_time < stale_deadline
            {
                return Some(dataset);
            }
            None
        })
        .collect::<Vec<_>>();

    println!("found {} stale datasets", stale_datasets.len());

    let pending_deletion: Vec<_> = stale_datasets
        .into_iter()
        .filter_map(|ds| {
            if let Some(dataset_id) = extract_dataset_id(project_id, &ds.id) {
                return Some(
                    client
                        .delete_dataset()
                        .set_project_id(project_id)
                        .set_dataset_id(dataset_id)
                        .set_delete_contents(true)
                        .send(),
                );
            }
            None
        })
        .collect();

    futures::future::join_all(pending_deletion).await;

    Ok(())
}

fn random_dataset_id() -> String {
    let rand_suffix = random_id_suffix();
    format!("rust_bq_test_dataset_{rand_suffix}")
}

fn random_job_id() -> String {
    let rand_suffix = random_id_suffix();
    format!("rust_bq_test_job_{rand_suffix}")
}

fn random_id_suffix() -> String {
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(8)
        .map(char::from)
        .collect()
}

fn extract_dataset_id(project_id: &str, id: &str) -> Option<String> {
    id.strip_prefix(format!("{project_id}:").as_str())
        .map(|v| v.to_string())
}

pub async fn job_service() -> Result<()> {
    let project_id = project_id()?;
    let client = JobService::builder().with_tracing().build().await?;
    cleanup_stale_jobs(&client, &project_id).await?;

    let job_id = random_job_id();
    println!("CREATING JOB WITH ID: {job_id}");

    let query = "SELECT 1 as one";
    let job = client
        .insert_job()
        .set_project_id(&project_id)
        .set_job(
            Job::new()
                .set_job_reference(JobReference::new().set_job_id(&job_id))
                .set_configuration(
                    JobConfiguration::new()
                        .set_labels([(INSTANCE_LABEL, "true")])
                        .set_query(JobConfigurationQuery::new().set_query(query)),
                ),
        )
        .send()
        .await?;
    println!("CREATE JOB = {job:?}");

    assert!(job.job_reference.is_some(), "{job:?}");

    let list = client
        .list_jobs()
        .set_project_id(&project_id)
        .by_item()
        .into_stream();
    let items = list.collect::<Vec<_>>().await;
    println!("LIST JOBS = {} entries", items.len());

    assert!(
        items
            .iter()
            .any(|v| v.as_ref().unwrap().id.contains(&job_id))
    );

    Ok(())
}

pub async fn query_client() -> Result<()> {
    let project_id = project_id()?;
    let bq = BigQuery::builder().build().await?;

    let query = bq
        .query("SELECT 1 as one")
        .with_project_id(project_id)
        .set_labels(vec![(INSTANCE_LABEL, "true")])
        .run()
        .await?;

    // BigQuery client sets JobCreationMode::JobCreationOptional by default
    let query_ref = query.query_reference();
    let QueryReference::Stateless { ref query_id } = query_ref else {
        anyhow::bail!("expected a stateless query reference, got {query_ref:?}");
    };

    assert!(!query_id.is_empty(), "{query_ref:?}");

    let complete_query = query.until_done().await?;

    assert_eq!(complete_query.metadata().total_rows, Some(1));

    let mut iter = complete_query.read();
    let row = iter.next().await.expect("should return first row")?;
    assert_eq!(row.get::<i64, _>("one"), 1);
    assert!(iter.next().await.is_none(), "{iter:?}");

    Ok(())
}

#[derive(google_cloud_bigquery::FromRow, Debug, PartialEq)]
struct UserData {
    name: String,
    age: i64,
    height: f64,
    active: bool,
    numbers: Vec<i64>,
    created_at: wkt::Timestamp,
    birth_date: google_cloud_type::model::Date,
    daily_alarm: google_cloud_type::model::TimeOfDay,
    event_time: google_cloud_type::model::DateTime,
    date_range: google_cloud_bigquery::Range<google_cloud_type::model::Date>,
    timestamp_range: google_cloud_bigquery::Range<wkt::Timestamp>,
    nullable_name: Option<String>,
    nullable_age: Option<i64>,
}

pub async fn query_client_datatypes() -> Result<()> {
    let project_id = project_id()?;
    let bq = BigQuery::builder().build().await?;

    let query = bq
        .query(
            "SELECT \
                 'John Doe' AS name, \
                 30 AS age, \
                 1.85 AS height, \
                 true AS active, \
                 ARRAY[1, 2, 3] AS numbers, \
                 TIMESTAMP '2026-05-28 15:30:00 UTC' AS created_at, \
                 DATE '2026-05-28' AS birth_date, \
                 TIME '15:30:00' AS daily_alarm, \
                 DATETIME '2026-05-28 15:30:00' AS event_time, \
                 RANGE(DATE '2026-05-28', DATE '2026-05-29') AS date_range, \
                 RANGE(TIMESTAMP '2026-05-28 15:30:00 UTC', NULL) AS timestamp_range, \
                 CAST(NULL AS STRING) AS nullable_name, \
                 CAST(NULL AS INT64) AS nullable_age",
        )
        .with_project_id(project_id)
        .set_labels(vec![(INSTANCE_LABEL, "true")])
        .run()
        .await?;

    let complete_query = query.until_done().await?;
    assert_eq!(complete_query.metadata().total_rows, Some(1));

    let mut iter = complete_query.read();
    let row = iter.next().await.expect("row must exist")?;

    let expected = UserData {
        name: "John Doe".to_string(),
        age: 30,
        height: 1.85,
        active: true,
        numbers: vec![1, 2, 3],
        created_at: wkt::Timestamp::new(1779982200, 0).unwrap(),
        birth_date: google_cloud_type::model::Date::new()
            .set_year(2026)
            .set_month(5)
            .set_day(28),
        daily_alarm: google_cloud_type::model::TimeOfDay::new()
            .set_hours(15)
            .set_minutes(30)
            .set_seconds(0)
            .set_nanos(0),
        event_time: google_cloud_type::model::DateTime::new()
            .set_year(2026)
            .set_month(5)
            .set_day(28)
            .set_hours(15)
            .set_minutes(30)
            .set_seconds(0)
            .set_nanos(0),
        date_range: google_cloud_bigquery::Range {
            start: Some(
                google_cloud_type::model::Date::new()
                    .set_year(2026)
                    .set_month(5)
                    .set_day(28),
            ),
            end: Some(
                google_cloud_type::model::Date::new()
                    .set_year(2026)
                    .set_month(5)
                    .set_day(29),
            ),
        },
        timestamp_range: google_cloud_bigquery::Range {
            start: Some(wkt::Timestamp::new(1779982200, 0).unwrap()),
            end: None,
        },
        nullable_name: None,
        nullable_age: None,
    };

    assert_eq!(row.get::<String, _>("name"), expected.name);
    assert_eq!(row.get::<i64, _>("age"), expected.age);
    assert_eq!(row.get::<f64, _>("height"), expected.height);
    assert_eq!(row.get::<bool, _>("active"), expected.active);
    assert_eq!(row.get::<Vec<i64>, _>("numbers"), expected.numbers);
    assert_eq!(
        row.get::<wkt::Timestamp, _>("created_at"),
        expected.created_at
    );
    assert_eq!(
        row.get::<google_cloud_type::model::Date, _>("birth_date"),
        expected.birth_date
    );
    assert_eq!(
        row.get::<google_cloud_type::model::TimeOfDay, _>("daily_alarm"),
        expected.daily_alarm
    );
    assert_eq!(
        row.get::<google_cloud_type::model::DateTime, _>("event_time"),
        expected.event_time
    );
    assert_eq!(
        row.get::<google_cloud_bigquery::Range<google_cloud_type::model::Date>, _>("date_range"),
        expected.date_range
    );
    assert_eq!(
        row.get::<google_cloud_bigquery::Range<wkt::Timestamp>, _>("timestamp_range"),
        expected.timestamp_range
    );
    assert_eq!(
        row.get::<Option<String>, _>("nullable_name"),
        expected.nullable_name
    );
    assert_eq!(
        row.get::<Option<i64>, _>("nullable_age"),
        expected.nullable_age
    );

    let data: UserData = row.try_into()?;
    assert_eq!(data, expected);

    assert!(iter.next().await.is_none());

    Ok(())
}

pub async fn query_client_numeric_limits() -> Result<()> {
    let project_id = project_id()?;
    let bq = BigQuery::builder().build().await?;

    let query = bq
        .query(
            "SELECT \
                 CAST('99999999999999999999999999999.999999999' AS NUMERIC) AS max_numeric, \
                 CAST('99999999999999999999999999999999999999.99999999999999999999999999999999999999' AS BIGNUMERIC) AS max_bignumeric, \
                 CAST('123.123456789' AS NUMERIC) AS standard_numeric, \
                 CAST('1234567890.1234567890' AS BIGNUMERIC) AS standard_bignumeric",
        )
        .with_project_id(project_id)
        .set_labels(vec![(INSTANCE_LABEL, "true")])
        .run()
        .await?;

    let complete_query = query.until_done().await?;
    assert_eq!(complete_query.metadata().total_rows, Some(1));

    let mut iter = complete_query.read();
    let row = iter.next().await.expect("row must exist")?;

    // Verify google_cloud_type::model::Decimal preserves values for NUMERIC (38 digits) and BIGNUMERIC (76 digits).
    assert_eq!(
        row.get::<Decimal, _>("max_numeric"),
        Decimal::new().set_value("99999999999999999999999999999.999999999")
    );
    assert_eq!(
        row.get::<Decimal, _>("max_bignumeric"),
        Decimal::new().set_value(
            "99999999999999999999999999999999999999.99999999999999999999999999999999999999"
        )
    );

    // Verify rust_decimal handles numbers within its 96-bit bounds (around 28 digits)
    // and errors on out-of-range values.
    assert_eq!(
        row.get::<RustDecimal, _>("standard_numeric"),
        "123.123456789".parse().expect("valid decimal")
    );
    assert_eq!(
        row.get::<RustDecimal, _>("standard_bignumeric"),
        "1234567890.1234567890".parse().expect("valid decimal")
    );
    assert!(row.try_get::<RustDecimal, _>("max_numeric").is_err());
    assert!(row.try_get::<RustDecimal, _>("max_bignumeric").is_err());

    assert!(iter.next().await.is_none());

    Ok(())
}

pub async fn query_client_multi_page() -> Result<()> {
    let project_id = project_id()?;
    let bq = BigQuery::builder().build().await?;

    let query = bq
        .query("SELECT * FROM UNNEST(GENERATE_ARRAY(1, 10000)) AS val")
        .set_use_legacy_sql(false)
        .set_max_results(1000_u32)
        .with_project_id(project_id)
        .set_labels(vec![(INSTANCE_LABEL, "true")])
        .run()
        .await?;

    let complete_query = query.until_done().await?;

    assert_eq!(complete_query.metadata().total_rows, Some(10000));

    let mut iter = complete_query.read().set_max_rows_buffered(1000);
    let mut count = 0;
    while let Some(_row) = iter.next().await.transpose()? {
        count += 1;
    }
    assert_eq!(count, 10000);

    Ok(())
}

pub async fn query_client_job() -> Result<()> {
    let project_id = project_id()?;
    let bq = BigQuery::builder().build().await?;

    let query = bq
        .query("SELECT 2 as two")
        .set_use_legacy_sql(false)
        .set_priority("INTERACTIVE") // force job path
        .with_project_id(project_id)
        .set_labels(vec![(INSTANCE_LABEL, "true")])
        .run()
        .await?;

    let complete_query = query.until_done().await?;

    assert_eq!(complete_query.metadata().total_rows, Some(1));

    let mut iter = complete_query.read();
    let row = iter.next().await.expect("should return first row")?;
    assert_eq!(row.get::<i64, _>("two"), 2);
    assert!(iter.next().await.is_none(), "{iter:?}");

    Ok(())
}

async fn cleanup_stale_jobs(client: &JobService, project_id: &str) -> Result<()> {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    let stale_deadline = SystemTime::now().duration_since(UNIX_EPOCH)?;
    let stale_deadline = stale_deadline - Duration::from_secs(48 * 60 * 60);
    let stale_deadline = stale_deadline.as_millis() as u64;

    let list = client
        .list_jobs()
        .set_project_id(project_id)
        .set_max_creation_time(stale_deadline)
        .by_item()
        .into_stream();
    let items = list.collect::<Vec<_>>().await;
    println!("LIST JOBS = {} entries", items.len());

    let pending_all_stale_jobs = items
        .iter()
        .filter_map(|v| match v {
            Ok(v) => {
                if let Some(job_reference) = &v.job_reference {
                    return Some(
                        client
                            .get_job()
                            .set_project_id(project_id)
                            .set_job_id(&job_reference.job_id)
                            .send(),
                    );
                }
                None
            }
            Err(_) => None,
        })
        .collect::<Vec<_>>();

    let pending_deletion = futures::future::join_all(pending_all_stale_jobs)
        .await
        .into_iter()
        .filter_map(|r| match r {
            Ok(r) => {
                let job_reference = r.job_reference?;
                if r.configuration
                    .is_some_and(|c| c.labels.get(INSTANCE_LABEL).is_some_and(|v| v == "true"))
                    && r.status.is_some_and(|s| s.state == "DONE")
                {
                    return Some(
                        client
                            .delete_job()
                            .set_project_id(project_id)
                            .set_job_id(&job_reference.job_id)
                            .send(),
                    );
                }
                None
            }
            Err(_) => None,
        })
        .collect::<Vec<_>>();

    println!("found {} stale test jobs", pending_deletion.len());

    futures::future::join_all(pending_deletion).await;
    Ok(())
}
