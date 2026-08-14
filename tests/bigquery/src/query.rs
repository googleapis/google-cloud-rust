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

use super::INSTANCE_LABEL;
use anyhow::Result;
use google_cloud_bigquery::client::BigQuery;
use google_cloud_bigquery::datatypes::{Interval, Range};
use google_cloud_bigquery::{FromRow, FromSql};
use google_cloud_test_utils::runtime_config::project_id;
use google_cloud_type::model::Decimal;
use rust_decimal::Decimal as RustDecimal;

pub async fn query_client() -> Result<()> {
    let project_id = project_id()?;
    let bq = BigQuery::builder().build().await?;

    let query = bq
        .query("SELECT 1 as one")
        .with_project_id(project_id)
        .set_labels(vec![(INSTANCE_LABEL, "true")])
        .send()
        .await?;

    // BigQuery client sets JobCreationMode::JobCreationOptional by default
    let metadata = query.metadata();
    let query_id = &metadata.query_id;
    assert!(!query_id.is_empty(), "expected non-empty query_id");

    let complete_query = query.until_done().await?;

    assert_eq!(complete_query.metadata().total_rows, Some(1));

    let mut iter = complete_query.read();
    let row = iter.next().await.expect("should return first row")?;
    assert_eq!(row.get::<i64, _>("one"), 1);
    assert!(iter.next().await.is_none(), "{iter:?}");

    Ok(())
}

#[derive(FromRow, Debug, PartialEq)]
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
    date_range: Range<google_cloud_type::model::Date>,
    timestamp_range: Range<wkt::Timestamp>,
    nullable_name: Option<String>,
    nullable_age: Option<i64>,
    raw_bytes: Vec<u8>,
    payload_bytes: bytes::Bytes,
    nullable_bytes: Option<Vec<u8>>,
    interval_val: Interval,
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
                 CAST(NULL AS INT64) AS nullable_age, \
                 B'hello world' AS raw_bytes, \
                 B'payload in bytes' AS payload_bytes, \
                 CAST(NULL AS BYTES) AS nullable_bytes, \
                 INTERVAL '1 2:30:45.123456' DAY TO SECOND AS interval_val",
        )
        .with_project_id(project_id)
        .set_labels(vec![(INSTANCE_LABEL, "true")])
        .until_done()
        .await?;

    assert_eq!(query.metadata().total_rows, Some(1));

    let mut iter = query.read();
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
        date_range: Range {
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
        timestamp_range: Range {
            start: Some(wkt::Timestamp::new(1779982200, 0).unwrap()),
            end: None,
        },
        nullable_name: None,
        nullable_age: None,
        raw_bytes: b"hello world".to_vec(),
        payload_bytes: bytes::Bytes::from_static(b"payload in bytes"),
        nullable_bytes: None,
        interval_val: Interval {
            years: 0,
            months: 0,
            days: 1,
            hours: 2,
            minutes: 30,
            seconds: 45,
            nanos: 123_456_000,
        },
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
        row.get::<Range<google_cloud_type::model::Date>, _>("date_range"),
        expected.date_range
    );
    assert_eq!(
        row.get::<Range<wkt::Timestamp>, _>("timestamp_range"),
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
    assert_eq!(row.get::<Vec<u8>, _>("raw_bytes"), expected.raw_bytes);
    assert_eq!(
        row.get::<bytes::Bytes, _>("payload_bytes"),
        expected.payload_bytes
    );
    assert_eq!(
        row.get::<Option<Vec<u8>>, _>("nullable_bytes"),
        expected.nullable_bytes
    );
    assert_eq!(
        row.get::<Interval, _>("interval_val"),
        expected.interval_val
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
        .until_done()
        .await?;

    assert_eq!(query.metadata().total_rows, Some(1));

    let mut iter = query.read();
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
        .set_max_results(1000_u32)
        .with_project_id(project_id)
        .set_labels(vec![(INSTANCE_LABEL, "true")])
        .until_done()
        .await?;

    assert_eq!(query.metadata().total_rows, Some(10000));

    let mut iter = query.read();
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
        .set_priority("INTERACTIVE") // force job path
        .with_project_id(project_id.clone())
        .set_labels(vec![(INSTANCE_LABEL, "true")])
        .until_done()
        .await?;

    assert_eq!(query.metadata().total_rows, Some(1));

    // fetch full job metadata
    let job = query.get_job()?.send().await?;

    let job_ref = job
        .job_reference
        .as_ref()
        .expect("job should have job_reference");
    assert_eq!(job_ref.project_id, project_id);
    assert!(!job_ref.job_id.is_empty(), "{job_ref:?}");
    assert!(job.status.is_some(), "{job:?}");
    assert!(job.statistics.is_some(), "{job:?}");

    let config_query = job
        .configuration
        .as_ref()
        .and_then(|c| c.query.as_ref())
        .map(|q| q.query.as_str())
        .expect("job should have configuration.query");
    assert_eq!(config_query, "SELECT 2 as two");

    // read the results
    let mut iter = query.read();
    let row = iter.next().await.expect("should return first row")?;
    assert_eq!(row.get::<i64, _>("two"), 2);
    assert!(iter.next().await.is_none(), "{iter:?}");

    Ok(())
}

#[derive(FromRow, FromSql, Debug, PartialEq)]
pub(crate) struct UserRecord {
    pub(crate) name: String,
    pub(crate) age: i64,
}

#[derive(FromSql, Debug, PartialEq)]
struct UserProfile {
    name: String,
    age: i64,
    birth_date: google_cloud_type::model::Date,
}

#[derive(FromRow, Debug, PartialEq)]
struct RowData {
    user: UserRecord,
    numbers: Vec<i64>,
    users: Vec<UserRecord>,
    profile: UserProfile,
}

pub async fn query_client_nested_types() -> Result<()> {
    let project_id = project_id()?;
    let bq = google_cloud_bigquery::client::BigQuery::builder()
        .build()
        .await?;

    println!("STARTING NESTED TYPES INTEGRATION TEST");
    let sql = "SELECT \
                 STRUCT('Alice' AS name, 25 AS age) AS user, \
                 ARRAY[1, 2, 3] AS numbers, \
                 ARRAY[STRUCT('Bob' AS name, 28 AS age), STRUCT('Charlie' AS name, 31 AS age)] AS users, \
                 STRUCT('Dave' AS name, 40 AS age, DATE '1986-05-28' AS birth_date) AS profile";

    let query = bq
        .query(sql)
        .with_project_id(project_id)
        .set_labels(vec![(INSTANCE_LABEL, "true")])
        .until_done()
        .await?;

    let mut rows = query.read();

    let row = rows.next().await.expect("row must exist")?;

    // Deserialize the entire row as user defined struct
    let data: RowData = row.try_into()?;

    // verify nested struct
    assert_eq!(data.user.name, "Alice");
    assert_eq!(data.user.age, 25);

    // verify repeated basic type (ARRAY)
    assert_eq!(data.numbers, vec![1, 2, 3]);

    // verify repeated struct
    let bob = UserRecord {
        name: "Bob".to_string(),
        age: 28,
    };
    let charlie = UserRecord {
        name: "Charlie".to_string(),
        age: 31,
    };
    assert_eq!(data.users, [bob, charlie]);

    // verify user-defined struct with BQ-specific date field
    assert_eq!(data.profile.name, "Dave");
    assert_eq!(data.profile.age, 40);
    assert_eq!(data.profile.birth_date.year, 1986);
    assert_eq!(data.profile.birth_date.month, 5);
    assert_eq!(data.profile.birth_date.day, 28);

    Ok(())
}
