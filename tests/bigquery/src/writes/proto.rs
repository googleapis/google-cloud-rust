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

//! Tests for rows written with `#[derive(ToRow)]`.
//!
//! Each test creates a table, writes rows to its default stream in the
//! `Proto` data format, and reads them back with a query.

use anyhow::Result;
use bigquery_samples::INSTANCE_LABEL;
use bytes::Bytes;
use google_cloud_bigquery::client::{BigQuery, Write};
use google_cloud_bigquery::datatypes::{Interval, Range};
use google_cloud_bigquery::error::RowError;
use google_cloud_bigquery::model::ProtoRows;
use google_cloud_bigquery::query::{FromRow, FromSql, Row};
use google_cloud_bigquery::write::ToRow;
use google_cloud_bigquery_v2::client::TableService;
use google_cloud_bigquery_v2::model::table_field_schema::FieldElementType;
use google_cloud_bigquery_v2::model::{TableFieldSchema, TableSchema};
use google_cloud_type::model::{Date, DateTime, Decimal, TimeOfDay};
use rust_decimal::Decimal as RustDecimal;
use serde_json::json;

/// The column that orders the rows in each table.
const ID: &str = "id";

// The BigQuery column types, and the mode for `ARRAY` columns.
const BIGNUMERIC: &str = "BIGNUMERIC";
const BOOLEAN: &str = "BOOLEAN";
const BYTES: &str = "BYTES";
const DATE: &str = "DATE";
const DATETIME: &str = "DATETIME";
const FLOAT: &str = "FLOAT";
const GEOGRAPHY: &str = "GEOGRAPHY";
const INTEGER: &str = "INTEGER";
const INTERVAL: &str = "INTERVAL";
const JSON: &str = "JSON";
const NUMERIC: &str = "NUMERIC";
const RANGE: &str = "RANGE";
const RECORD: &str = "RECORD";
const REPEATED: &str = "REPEATED";
const STRING: &str = "STRING";
const TIME: &str = "TIME";
const TIMESTAMP: &str = "TIMESTAMP";

/// 2025-05-16T09:46:12Z, in seconds since the Unix epoch.
const MAY_16_2025_SECONDS: i64 = 1_747_388_772;

/// 0001-01-01T00:00:00Z, the earliest `TIMESTAMP`, in seconds since the Unix
/// epoch.
const MIN_TIMESTAMP_SECONDS: i64 = -62_135_596_800;

/// The number of seconds in a day.
const SECONDS_PER_DAY: i64 = 86_400;

/// The number of nanoseconds in a microsecond.
const NANOS_PER_MICRO: i32 = 1_000;

/// The clients and the dataset that the tests share.
pub struct Fixture {
    project_id: String,
    dataset_id: String,
    tables: TableService,
    writer: Write,
    reader: BigQuery,
}

impl Fixture {
    /// Creates the clients, for tables in `dataset_id`.
    pub async fn new(project_id: &str, dataset_id: &str) -> Result<Self> {
        Ok(Self {
            project_id: project_id.to_string(),
            dataset_id: dataset_id.to_string(),
            tables: TableService::builder().with_tracing().build().await?,
            writer: Write::builder().build().await?,
            reader: BigQuery::builder().build().await?,
        })
    }

    /// Creates the table `table_id`, with the given columns.
    async fn create_table(&self, table_id: &str, columns: Vec<TableFieldSchema>) -> Result<()> {
        let schema = TableSchema::new().set_fields(columns);
        bigquery_samples::create_table(
            &self.tables,
            &self.project_id,
            &self.dataset_id,
            table_id,
            schema,
        )
        .await?;
        Ok(())
    }

    /// Returns the resource name of `table_id`, for the Write API.
    fn table_path(&self, table_id: &str) -> String {
        format!(
            "projects/{}/datasets/{}/tables/{table_id}",
            self.project_id, self.dataset_id
        )
    }

    /// Returns the name of `table_id`, for a query.
    fn table_sql(&self, table_id: &str) -> String {
        format!("`{}.{}.{table_id}`", self.project_id, self.dataset_id)
    }

    /// Writes `rows` to the default stream of `table_id`, in one append.
    async fn write<T: ToRow>(&self, table_id: &str, rows: &[T]) -> Result<()> {
        let writer = self
            .writer
            .open_default_stream(self.table_path(table_id))
            .build_proto(T::schema())
            .await?;
        let serialized_rows = rows.iter().map(T::to_row).collect::<Result<Vec<_>, _>>()?;
        let _ = writer
            .append(ProtoRows::new().set_serialized_rows(serialized_rows))
            .send()
            .await?;
        Ok(())
    }

    /// Reads the rows in `table_id`, ordered by the [ID] column.
    async fn read<T>(&self, table_id: &str) -> Result<Vec<T>>
    where
        T: TryFrom<Row, Error = RowError>,
    {
        let sql = format!("SELECT * FROM {} ORDER BY {ID}", self.table_sql(table_id));
        self.query(sql).await
    }

    /// Runs `sql`, and converts each row in the results.
    async fn query<T>(&self, sql: String) -> Result<Vec<T>>
    where
        T: TryFrom<Row, Error = RowError>,
    {
        let mut rows = self
            .reader
            .query(sql)
            .with_project_id(self.project_id.as_str())
            .set_labels(vec![(INSTANCE_LABEL, "true")])
            .until_done()
            .await?
            .read();
        let mut results = Vec::new();
        while let Some(row) = rows.next().await {
            results.push(T::try_from(row?)?);
        }
        Ok(results)
    }
}

/// The table for [basic].
const BASIC_TABLE: &str = "proto_basic";

/// The number of rows in each append, in [basic].
const ROWS_PER_APPEND: usize = 2;

/// The row from the `ToRow` documentation, with an [ID] column.
#[derive(Debug, FromRow, PartialEq, ToRow)]
struct Basic {
    id: i64,
    name: String,
    age: Option<i64>,
}

/// Writes rows in two appends, as the `ToRow` documentation does.
pub async fn basic(fixture: &Fixture) -> Result<()> {
    fixture
        .create_table(
            BASIC_TABLE,
            vec![
                column(ID, INTEGER),
                column("name", STRING),
                column("age", INTEGER),
            ],
        )
        .await?;

    let writer = fixture
        .writer
        .open_default_stream(fixture.table_path(BASIC_TABLE))
        .build_proto(Basic::schema())
        .await?;
    let rows = [
        Basic {
            id: 1,
            name: "alice".to_string(),
            age: Some(30),
        },
        // `None` writes `NULL` to the `age` column.
        Basic {
            id: 2,
            name: "bob".to_string(),
            age: None,
        },
        Basic {
            id: 3,
            name: "carol".to_string(),
            age: Some(41),
        },
    ];
    for batch in rows.chunks(ROWS_PER_APPEND) {
        let serialized_rows = batch
            .iter()
            .map(Basic::to_row)
            .collect::<Result<Vec<_>, _>>()?;
        let _ = writer
            .append(ProtoRows::new().set_serialized_rows(serialized_rows))
            .send()
            .await?;
    }

    let got: Vec<Basic> = fixture.read(BASIC_TABLE).await?;
    assert_eq!(got, rows);
    Ok(())
}

/// The table for [datatypes].
const DATATYPES_TABLE: &str = "proto_datatypes";

/// 2026-05-28T15:30:00Z, in seconds since the Unix epoch.
const MAY_28_2026_SECONDS: i64 = 1_779_982_200;

/// A row with a column for each BigQuery data type.
///
/// Apart from `id`, the fields up to `json_val` have the same names, types,
/// and values as `UserData` in `query.rs`, which `query_client_datatypes`
/// reads with a query. The fields after it have the types that `UserData`
/// doesn't: `NUMERIC`, `BIGNUMERIC`, `GEOGRAPHY`, `STRUCT`, and
/// `RANGE<DATETIME>`.
#[derive(Debug, FromRow, PartialEq, ToRow)]
struct UserData {
    id: i64,
    name: String,
    age: i64,
    height: f64,
    active: bool,
    numbers: Vec<i64>,
    created_at: wkt::Timestamp,
    birth_date: Date,
    daily_alarm: TimeOfDay,
    event_time: DateTime,
    date_range: Range<Date>,
    timestamp_range: Range<wkt::Timestamp>,
    nullable_name: Option<String>,
    nullable_age: Option<i64>,
    raw_bytes: Vec<u8>,
    payload_bytes: Bytes,
    nullable_bytes: Option<Vec<u8>>,
    interval_val: Interval,
    json_val: wkt::Struct,
    balance: RustDecimal,
    big_balance: Decimal,
    location: String,
    home: Address,
    datetime_range: Range<DateTime>,
}

/// Writes a row with a column for each BigQuery data type, and reads it back.
///
/// This is the write side of `query_client_datatypes` in `query.rs`.
pub async fn datatypes(fixture: &Fixture) -> Result<()> {
    fixture
        .create_table(
            DATATYPES_TABLE,
            vec![
                column(ID, INTEGER),
                column("name", STRING),
                column("age", INTEGER),
                column("height", FLOAT),
                column("active", BOOLEAN),
                repeated(column("numbers", INTEGER)),
                column("created_at", TIMESTAMP),
                column("birth_date", DATE),
                column("daily_alarm", TIME),
                column("event_time", DATETIME),
                range("date_range", DATE),
                range("timestamp_range", TIMESTAMP),
                column("nullable_name", STRING),
                column("nullable_age", INTEGER),
                column("raw_bytes", BYTES),
                column("payload_bytes", BYTES),
                column("nullable_bytes", BYTES),
                column("interval_val", INTERVAL),
                column("json_val", JSON),
                column("balance", NUMERIC),
                column("big_balance", BIGNUMERIC),
                column("location", GEOGRAPHY),
                record("home", address_fields()),
                range("datetime_range", DATETIME),
            ],
        )
        .await?;

    let birth_date = date(2026, 5, 28);
    let next_day = date(2026, 5, 29);
    let daily_alarm = time(15, 30, 0, 0);
    let event_time = datetime(&birth_date, &daily_alarm);
    let next_event = datetime(&next_day, &daily_alarm);
    let created_at = wkt::Timestamp::new(MAY_28_2026_SECONDS, 0)?;
    let rows = [UserData {
        id: 1,
        // The values that `query_client_datatypes` reads.
        name: "John Doe".to_string(),
        age: 30,
        height: 1.85,
        active: true,
        numbers: vec![1, 2, 3],
        created_at,
        birth_date: birth_date.clone(),
        daily_alarm,
        event_time: event_time.clone(),
        date_range: Range::new().set_start(birth_date).set_end(next_day),
        timestamp_range: Range::new().set_start(created_at),
        nullable_name: None,
        nullable_age: None,
        raw_bytes: b"hello world".to_vec(),
        payload_bytes: Bytes::from_static(b"payload in bytes"),
        nullable_bytes: None,
        interval_val: Interval::new()
            .set_days(1)
            .set_hours(2)
            .set_minutes(30)
            .set_seconds(45)
            .set_nanos(123_456 * NANOS_PER_MICRO),
        json_val: object(json!({"role": "admin", "level": 5}))?,
        // The types that `query_client_datatypes` doesn't read.
        balance: "1234.56".parse()?,
        // More digits before the decimal point than `NUMERIC` keeps.
        big_balance: Decimal::new().set_value("123456789012345678901234567890.123456789"),
        location: "POINT(-122 37)".to_string(),
        home: Address {
            city: Some("Mountain View".to_string()),
            zip: Some("94043".to_string()),
        },
        datetime_range: Range::new().set_start(event_time).set_end(next_event),
    }];
    fixture.write(DATATYPES_TABLE, &rows).await?;

    let got: Vec<UserData> = fixture.read(DATATYPES_TABLE).await?;
    assert_eq!(got, rows);
    Ok(())
}

/// The table for [scalars].
const SCALARS_TABLE: &str = "proto_scalars";

/// A row with a column for each scalar type.
#[derive(Clone, Debug, FromRow, PartialEq, ToRow)]
struct Scalars {
    id: i64,
    boolean: bool,
    int32: i32,
    int64: i64,
    float32: f32,
    float64: f64,
    numeric: RustDecimal,
    bignumeric: Decimal,
    string: String,
    vec_u8: Vec<u8>,
    bytes: Bytes,
    date: Date,
    datetime: DateTime,
    time: TimeOfDay,
    timestamp: wkt::Timestamp,
    json: wkt::Struct,
}

/// Writes each scalar type: typical values, zero values, and the smallest or
/// largest values.
pub async fn scalars(fixture: &Fixture) -> Result<()> {
    fixture
        .create_table(
            SCALARS_TABLE,
            vec![
                column(ID, INTEGER),
                column("boolean", BOOLEAN),
                column("int32", INTEGER),
                column("int64", INTEGER),
                column("float32", FLOAT),
                column("float64", FLOAT),
                column("numeric", NUMERIC),
                column("bignumeric", BIGNUMERIC),
                column("string", STRING),
                column("vec_u8", BYTES),
                column("bytes", BYTES),
                column("date", DATE),
                column("datetime", DATETIME),
                column("time", TIME),
                column("timestamp", TIMESTAMP),
                column("json", JSON),
            ],
        )
        .await?;

    let rows = [
        Scalars {
            id: 1,
            boolean: true,
            // Negative values take 10 bytes.
            int32: -42,
            int64: 1_234_567_890_123,
            float32: 1.5,
            float64: -0.25,
            numeric: "123.45".parse()?,
            // More decimal places than `NUMERIC` keeps.
            bignumeric: Decimal::new().set_value("1234567890.0987654321"),
            string: "hello world".to_string(),
            vec_u8: vec![0x00, 0x7F, 0xFF],
            bytes: Bytes::from_static(b"payload"),
            date: date(2025, 5, 16),
            datetime: datetime(&date(2025, 5, 16), &time(9, 46, 12, 123_456)),
            time: time(9, 46, 12, 123_456),
            timestamp: wkt::Timestamp::new(MAY_16_2025_SECONDS, 123_456 * NANOS_PER_MICRO)?,
            json: object(json!({"name": "alice", "tags": ["a", "b"], "count": 5}))?,
        },
        // BigQuery stores these values, not `NULL`. A `NULL` would not convert
        // back to these types.
        Scalars {
            id: 2,
            boolean: false,
            int32: 0,
            int64: 0,
            float32: 0.0,
            float64: 0.0,
            numeric: RustDecimal::ZERO,
            bignumeric: Decimal::new().set_value("0"),
            string: String::new(),
            vec_u8: Vec::new(),
            bytes: Bytes::new(),
            date: date(1970, 1, 1),
            datetime: datetime(&date(1970, 1, 1), &time(0, 0, 0, 0)),
            time: time(0, 0, 0, 0),
            timestamp: wkt::Timestamp::default(),
            json: wkt::Struct::new(),
        },
        Scalars {
            id: 3,
            boolean: true,
            int32: i32::MIN,
            int64: i64::MIN,
            float32: f32::MAX,
            float64: 1e-300,
            numeric: "-9999999999999999999.999999999".parse()?,
            bignumeric: Decimal::new().set_value(
                "-12345678901234567890123456789012345678.12345678901234567890123456789012345678",
            ),
            // The emoji takes 4 bytes in UTF-8, and the JSON query results
            // escape the quotes and the backslash.
            string: r#"emoji 🚀, "quotes", \backslash, and !@#$%^&*()"#.to_string(),
            // These take more than 127 bytes, so their lengths take 2 bytes.
            vec_u8: (0..=u8::MAX).collect(),
            bytes: Bytes::from(vec![0xAB; 300]),
            date: date(1, 1, 1),
            datetime: datetime(&date(9999, 12, 31), &time(23, 59, 59, 999_999)),
            time: time(23, 59, 59, 999_999),
            timestamp: wkt::Timestamp::new(MIN_TIMESTAMP_SECONDS, 0)?,
            json: object(json!({
                "text": r#"emoji 🚀, "quotes", \backslash, and !@#$%^&*()"#,
                "list": [1, [2, 3], {"key": null}],
                "nested": {"deep": {"deeper": true}},
            }))?,
        },
    ];
    fixture.write(SCALARS_TABLE, &rows).await?;

    let got: Vec<Scalars> = fixture.read(SCALARS_TABLE).await?;
    assert_eq!(got, rows);
    Ok(())
}

/// The table for [json].
const JSON_TABLE: &str = "proto_json";

/// A row with a `JSON` column, written from a `wkt::Value`.
#[derive(ToRow)]
struct JsonValue {
    id: i64,
    value: Option<wkt::Value>,
}

/// A row from [JSON_TABLE], with the `JSON` column as text.
///
/// `FromSql` does not parse `JSON` text into a `wkt::Value`, so the test
/// compares the text.
#[derive(Debug, FromRow, PartialEq)]
struct JsonText {
    id: i64,
    value: Option<String>,
}

/// Writes `wkt::Value` values, including the JSON value `null`.
pub async fn json(fixture: &Fixture) -> Result<()> {
    fixture
        .create_table(JSON_TABLE, vec![column(ID, INTEGER), column("value", JSON)])
        .await?;

    let rows = [
        JsonValue {
            id: 1,
            value: Some(json!({"b": [1, "two"], "a": true})),
        },
        JsonValue {
            id: 2,
            value: Some(json!("text")),
        },
        // The JSON value `null`, which is not a SQL `NULL`.
        JsonValue {
            id: 3,
            value: Some(wkt::Value::Null),
        },
        // A SQL `NULL`.
        JsonValue { id: 4, value: None },
    ];
    fixture.write(JSON_TABLE, &rows).await?;

    let got: Vec<JsonText> = fixture.read(JSON_TABLE).await?;
    // BigQuery sorts the keys in JSON objects, and removes the spaces.
    let want = [
        JsonText {
            id: 1,
            value: Some(r#"{"a":true,"b":[1,"two"]}"#.to_string()),
        },
        JsonText {
            id: 2,
            value: Some(r#""text""#.to_string()),
        },
        JsonText {
            id: 3,
            value: Some("null".to_string()),
        },
        JsonText { id: 4, value: None },
    ];
    assert_eq!(got, want);
    Ok(())
}

/// The table for [intervals].
const INTERVALS_TABLE: &str = "proto_intervals";

/// The most years in a BigQuery `INTERVAL`, positive or negative.
const MAX_INTERVAL_YEARS: i32 = 10_000;

/// The most days in a BigQuery `INTERVAL`, positive or negative.
const MAX_INTERVAL_DAYS: i32 = 3_660_000;

/// The most hours in a BigQuery `INTERVAL`, positive or negative.
const MAX_INTERVAL_HOURS: i32 = 87_840_000;

/// A row with `INTERVAL` columns.
#[derive(Clone, Debug, FromRow, PartialEq, ToRow)]
struct Intervals {
    id: i64,
    interval: Interval,
    intervals: Vec<Interval>,
}

/// Writes intervals with mixed signs, with microseconds, and at the limits of
/// a BigQuery `INTERVAL`.
pub async fn intervals(fixture: &Fixture) -> Result<()> {
    fixture
        .create_table(
            INTERVALS_TABLE,
            vec![
                column(ID, INTEGER),
                column("interval", INTERVAL),
                repeated(column("intervals", INTERVAL)),
            ],
        )
        .await?;

    let typical = Intervals {
        id: 1,
        // `1-2 3 4:5:6.123456`
        interval: Interval::new()
            .set_years(1)
            .set_months(2)
            .set_days(3)
            .set_hours(4)
            .set_minutes(5)
            .set_seconds(6)
            .set_nanos(123_456 * NANOS_PER_MICRO),
        intervals: vec![
            // Each part has its own sign: `0-8 -20 17:0:0`.
            Interval::new().set_months(8).set_days(-20).set_hours(17),
            // Negative parts under a year and under an hour: `-0-2 0 -0:30:10`.
            Interval::new()
                .set_months(-2)
                .set_minutes(-30)
                .set_seconds(-10),
        ],
    };
    let zero = Intervals {
        id: 2,
        interval: Interval::new(),
        intervals: Vec::new(),
    };
    let longest = Intervals {
        id: 3,
        interval: Interval::new()
            .set_years(MAX_INTERVAL_YEARS)
            .set_days(MAX_INTERVAL_DAYS)
            .set_hours(MAX_INTERVAL_HOURS),
        intervals: vec![
            Interval::new()
                .set_years(-MAX_INTERVAL_YEARS)
                .set_days(-MAX_INTERVAL_DAYS)
                .set_hours(-MAX_INTERVAL_HOURS),
        ],
    };
    // Fields that BigQuery combines, and nanoseconds that it does not keep.
    let uncombined = Intervals {
        id: 4,
        interval: Interval::new()
            .set_months(14)
            .set_minutes(90)
            .set_nanos(1_999),
        intervals: Vec::new(),
    };
    fixture
        .write(
            INTERVALS_TABLE,
            &[
                typical.clone(),
                zero.clone(),
                longest.clone(),
                uncombined.clone(),
            ],
        )
        .await?;

    let got: Vec<Intervals> = fixture.read(INTERVALS_TABLE).await?;
    // 14 months are 1 year and 2 months, 90 minutes are 1 hour and 30
    // minutes, and BigQuery keeps whole microseconds.
    let combined = Intervals {
        interval: Interval::new()
            .set_years(1)
            .set_months(2)
            .set_hours(1)
            .set_minutes(30)
            .set_nanos(NANOS_PER_MICRO),
        ..uncombined
    };
    assert_eq!(got, [typical, zero, longest, combined]);
    Ok(())
}

/// The table for [geography].
const GEOGRAPHY_TABLE: &str = "proto_geography";

/// A row with a `GEOGRAPHY` column, written from text.
#[derive(Debug, FromRow, PartialEq, ToRow)]
struct Place {
    id: i64,
    location: Option<String>,
}

/// Writes `GEOGRAPHY` values in the WKT and GeoJSON formats, and a `NULL`.
pub async fn geography(fixture: &Fixture) -> Result<()> {
    fixture
        .create_table(
            GEOGRAPHY_TABLE,
            vec![column(ID, INTEGER), column("location", GEOGRAPHY)],
        )
        .await?;

    let rows = [
        Place {
            id: 1,
            location: Some("POINT(1 2)".to_string()),
        },
        Place {
            id: 2,
            location: Some(r#"{"type": "Point", "coordinates": [3, 4]}"#.to_string()),
        },
        Place {
            id: 3,
            location: None,
        },
    ];
    fixture.write(GEOGRAPHY_TABLE, &rows).await?;

    let got: Vec<Place> = fixture.read(GEOGRAPHY_TABLE).await?;
    // BigQuery returns `GEOGRAPHY` values in the WKT format.
    let want = [
        Place {
            id: 1,
            location: Some("POINT(1 2)".to_string()),
        },
        Place {
            id: 2,
            location: Some("POINT(3 4)".to_string()),
        },
        Place {
            id: 3,
            location: None,
        },
    ];
    assert_eq!(got, want);
    Ok(())
}

/// The table for [nulls].
const NULLS_TABLE: &str = "proto_nulls";

/// A row with `NULL`able columns.
#[derive(Clone, Debug, Default, FromRow, PartialEq, ToRow)]
struct Nullable {
    id: i64,
    string: Option<String>,
    int64: Option<i64>,
    float64: Option<f64>,
    boolean: Option<bool>,
    bytes: Option<Vec<u8>>,
    date: Option<Date>,
    timestamp: Option<wkt::Timestamp>,
    numeric: Option<RustDecimal>,
    json: Option<wkt::Struct>,
    tags: Option<Vec<String>>,
}

/// Writes `Some` and `None` values.
pub async fn nulls(fixture: &Fixture) -> Result<()> {
    fixture
        .create_table(
            NULLS_TABLE,
            vec![
                column(ID, INTEGER),
                column("string", STRING),
                column("int64", INTEGER),
                column("float64", FLOAT),
                column("boolean", BOOLEAN),
                column("bytes", BYTES),
                column("date", DATE),
                column("timestamp", TIMESTAMP),
                column("numeric", NUMERIC),
                column("json", JSON),
                repeated(column("tags", STRING)),
            ],
        )
        .await?;

    let present = Nullable {
        id: 1,
        string: Some("alice".to_string()),
        int64: Some(42),
        float64: Some(0.5),
        boolean: Some(false),
        bytes: Some(vec![1, 2, 3]),
        date: Some(date(2025, 5, 16)),
        timestamp: Some(wkt::Timestamp::new(MAY_16_2025_SECONDS, 0)?),
        numeric: Some("1.5".parse()?),
        json: Some(object(json!({"a": 1}))?),
        tags: Some(vec!["x".to_string(), "y".to_string()]),
    };
    let missing = Nullable {
        id: 2,
        ..Nullable::default()
    };
    fixture
        .write(NULLS_TABLE, &[present.clone(), missing.clone()])
        .await?;

    let got: Vec<Nullable> = fixture.read(NULLS_TABLE).await?;
    // Arrays cannot be `NULL`, so `None` writes an empty array.
    let missing = Nullable {
        tags: Some(Vec::new()),
        ..missing
    };
    assert_eq!(got, [present, missing]);
    Ok(())
}

/// The table for [arrays].
const ARRAYS_TABLE: &str = "proto_arrays";

/// A row with `ARRAY` columns.
#[derive(Clone, Debug, Default, FromRow, PartialEq, ToRow)]
struct Arrays {
    id: i64,
    strings: Vec<String>,
    int64s: Vec<i64>,
    float64s: Vec<f64>,
    booleans: Vec<bool>,
    blobs: Vec<Vec<u8>>,
    dates: Vec<Date>,
    timestamps: Vec<wkt::Timestamp>,
    numerics: Vec<RustDecimal>,
    documents: Vec<wkt::Struct>,
}

/// Writes arrays, including empty arrays.
pub async fn arrays(fixture: &Fixture) -> Result<()> {
    fixture
        .create_table(
            ARRAYS_TABLE,
            vec![
                column(ID, INTEGER),
                repeated(column("strings", STRING)),
                repeated(column("int64s", INTEGER)),
                repeated(column("float64s", FLOAT)),
                repeated(column("booleans", BOOLEAN)),
                repeated(column("blobs", BYTES)),
                repeated(column("dates", DATE)),
                repeated(column("timestamps", TIMESTAMP)),
                repeated(column("numerics", NUMERIC)),
                repeated(column("documents", JSON)),
            ],
        )
        .await?;

    let rows = [
        // Each array has a value such as `0` or `""`, which BigQuery keeps.
        Arrays {
            id: 1,
            strings: vec!["a".to_string(), String::new()],
            int64s: vec![42, 0, -1],
            float64s: vec![1.5, 0.0],
            booleans: vec![true, false],
            blobs: vec![vec![1, 2], Vec::new()],
            dates: vec![date(2025, 5, 16), date(1970, 1, 1)],
            timestamps: vec![
                wkt::Timestamp::new(MAY_16_2025_SECONDS, 0)?,
                wkt::Timestamp::default(),
            ],
            numerics: vec!["1.5".parse()?, RustDecimal::ZERO],
            documents: vec![object(json!({"a": 1}))?, wkt::Struct::new()],
        },
        // Empty arrays.
        Arrays {
            id: 2,
            ..Arrays::default()
        },
    ];
    fixture.write(ARRAYS_TABLE, &rows).await?;

    let got: Vec<Arrays> = fixture.read(ARRAYS_TABLE).await?;
    assert_eq!(got, rows);
    Ok(())
}

/// The table for [nested].
const NESTED_TABLE: &str = "proto_nested";

/// A `STRUCT<city STRING, zip STRING>` column.
#[derive(Clone, Debug, FromSql, PartialEq, ToRow)]
struct Address {
    city: Option<String>,
    zip: Option<String>,
}

/// A `STRUCT` column with a `STRUCT` field.
#[derive(Clone, Debug, FromSql, PartialEq, ToRow)]
struct Contact {
    email: String,
    address: Option<Address>,
}

/// Types with the same names as types in the parent module.
mod billing {
    use google_cloud_bigquery::query::FromSql;
    use google_cloud_bigquery::write::ToRow;

    /// A billing address.
    ///
    /// It has the same name as [super::Address], but different fields, so its
    /// message type is `Address_2`.
    #[derive(Clone, Debug, FromSql, PartialEq, ToRow)]
    pub struct Address {
        pub street: String,
        pub country: String,
    }
}

/// A row with `STRUCT` columns.
#[derive(Clone, Debug, FromRow, PartialEq, ToRow)]
struct Customer {
    id: i64,
    address: Option<Address>,
    previous_addresses: Vec<Address>,
    contact: Contact,
    billing: billing::Address,
}

/// Returns the fields of an [Address] column.
fn address_fields() -> Vec<TableFieldSchema> {
    vec![column("city", STRING), column("zip", STRING)]
}

/// Writes nested structs, `NULL` structs, and structs with only `NULL` fields.
pub async fn nested(fixture: &Fixture) -> Result<()> {
    fixture
        .create_table(
            NESTED_TABLE,
            vec![
                column(ID, INTEGER),
                record("address", address_fields()),
                repeated(record("previous_addresses", address_fields())),
                record(
                    "contact",
                    vec![column("email", STRING), record("address", address_fields())],
                ),
                record(
                    "billing",
                    vec![column("street", STRING), column("country", STRING)],
                ),
            ],
        )
        .await?;

    let paris = Address {
        city: Some("Paris".to_string()),
        zip: Some("75001".to_string()),
    };
    let lyon = Address {
        city: Some("Lyon".to_string()),
        zip: None,
    };
    // A struct with only `NULL` fields. It is written as an empty message, and
    // it is not a `NULL` struct.
    let unknown = Address {
        city: None,
        zip: None,
    };
    let billing = billing::Address {
        street: "1 Rue de Rivoli".to_string(),
        country: "FR".to_string(),
    };
    let rows = [
        Customer {
            id: 1,
            address: Some(paris.clone()),
            previous_addresses: vec![lyon.clone(), paris],
            contact: Contact {
                email: "alice@example.com".to_string(),
                address: Some(lyon),
            },
            billing: billing.clone(),
        },
        // `NULL` structs, and an empty array of structs.
        Customer {
            id: 2,
            address: None,
            previous_addresses: Vec::new(),
            contact: Contact {
                email: "bob@example.com".to_string(),
                address: None,
            },
            billing,
        },
        // Structs with only `NULL` fields, or only empty strings.
        Customer {
            id: 3,
            address: Some(unknown.clone()),
            previous_addresses: vec![unknown.clone()],
            contact: Contact {
                email: String::new(),
                address: Some(unknown),
            },
            billing: billing::Address {
                street: String::new(),
                country: String::new(),
            },
        },
    ];
    fixture.write(NESTED_TABLE, &rows).await?;

    let got: Vec<Customer> = fixture.read(NESTED_TABLE).await?;
    assert_eq!(got, rows);
    Ok(())
}

/// The table for [ranges].
const RANGES_TABLE: &str = "proto_ranges";

/// A row with `RANGE` columns.
#[derive(Clone, Debug, FromRow, PartialEq, ToRow)]
struct Ranges {
    id: i64,
    date_range: Range<Date>,
    datetime_range: Range<DateTime>,
    timestamp_range: Option<Range<wkt::Timestamp>>,
}

/// Writes bounded, unbounded, and `NULL` ranges.
pub async fn ranges(fixture: &Fixture) -> Result<()> {
    fixture
        .create_table(
            RANGES_TABLE,
            vec![
                column(ID, INTEGER),
                range("date_range", DATE),
                range("datetime_range", DATETIME),
                range("timestamp_range", TIMESTAMP),
            ],
        )
        .await?;

    let check_in = date(2025, 5, 16);
    let check_out = date(2025, 5, 18);
    let arrival = datetime(&check_in, &time(9, 46, 12, 123_456));
    let departure = datetime(&check_out, &time(12, 0, 0, 0));
    let first = wkt::Timestamp::new(MAY_16_2025_SECONDS, 0)?;
    let last = wkt::Timestamp::new(MAY_16_2025_SECONDS + SECONDS_PER_DAY, 0)?;
    let rows = [
        Ranges {
            id: 1,
            date_range: Range::new().set_start(check_in.clone()).set_end(check_out),
            datetime_range: Range::new().set_start(arrival).set_end(departure.clone()),
            timestamp_range: Some(Range::new().set_start(first).set_end(last)),
        },
        // Ranges without an end or a start, and a `NULL` range.
        Ranges {
            id: 2,
            date_range: Range::new().set_start(check_in),
            datetime_range: Range::new().set_end(departure),
            timestamp_range: None,
        },
        // Ranges without a start and an end. They are not `NULL`.
        Ranges {
            id: 3,
            date_range: Range::new(),
            datetime_range: Range::new(),
            timestamp_range: Some(Range::new()),
        },
    ];
    fixture.write(RANGES_TABLE, &rows).await?;

    let got: Vec<Ranges> = fixture.read(RANGES_TABLE).await?;
    assert_eq!(got, rows);
    Ok(())
}

/// The table for [repeated_ranges].
const REPEATED_RANGES_TABLE: &str = "proto_repeated_ranges";

/// A row with an `ARRAY<RANGE<DATE>>` column.
#[derive(Clone, Debug, FromRow, PartialEq, ToRow)]
struct Stays {
    id: i64,
    stays: Vec<Range<Date>>,
}

/// Writes an array of ranges.
pub async fn repeated_ranges(fixture: &Fixture) -> Result<()> {
    fixture
        .create_table(
            REPEATED_RANGES_TABLE,
            vec![column(ID, INTEGER), repeated(range("stays", DATE))],
        )
        .await?;

    let rows = [
        Stays {
            id: 1,
            stays: vec![
                Range::new()
                    .set_start(date(2025, 5, 16))
                    .set_end(date(2025, 5, 18)),
                Range::new().set_start(date(2025, 6, 1)),
            ],
        },
        Stays {
            id: 2,
            stays: Vec::new(),
        },
    ];
    fixture.write(REPEATED_RANGES_TABLE, &rows).await?;

    let got: Vec<Stays> = fixture.read(REPEATED_RANGES_TABLE).await?;
    assert_eq!(got, rows);
    Ok(())
}

/// The table for [depth].
const DEPTH_TABLE: &str = "proto_depth";

/// The most levels of nested `STRUCT` columns that BigQuery supports.
///
/// BigQuery limits the depth of a schema to 15, and counts every part of a
/// field path, such as `a.b.c`. The innermost field is not a `STRUCT`, so at
/// most 14 parts can be.
const MAX_STRUCT_DEPTH: usize = 14;

/// The name of the field in [Nest].
const CHILD: &str = "child";

/// The column in [DEPTH_TABLE] that holds an `INT64` value.
const DEEP_INT: &str = "deep_int";

/// The column in [DEPTH_TABLE] that holds a `RANGE<DATE>` value.
const DEEP_RANGE: &str = "deep_range";

/// A struct with one field, to nest structs many levels deep.
#[derive(ToRow)]
struct Nest<T> {
    child: T,
}

/// A field of this type nests 7 levels of `STRUCT` columns.
type Nest7<T> = Nest<Nest<Nest<Nest<Nest<Nest<Nest<T>>>>>>>;

/// A field of this type nests [MAX_STRUCT_DEPTH] levels of `STRUCT` columns.
type Nest14<T> = Nest7<Nest7<T>>;

/// Returns `child`, nested in 1 struct.
fn nest<T>(child: T) -> Nest<T> {
    Nest { child }
}

/// Returns `child`, nested in 7 structs.
fn nest7<T>(child: T) -> Nest7<T> {
    nest(nest(nest(nest(nest(nest(nest(child)))))))
}

/// Returns `child`, nested in [MAX_STRUCT_DEPTH] structs.
fn nest14<T>(child: T) -> Nest14<T> {
    nest7(nest7(child))
}

/// A row with columns that nest [MAX_STRUCT_DEPTH] levels of `STRUCT` columns.
#[derive(ToRow)]
struct Deep {
    id: i64,
    deep_int: Nest14<i64>,
    // A `RANGE` column is not a `STRUCT` column, so this is 14 levels too.
    deep_range: Nest14<Range<Date>>,
}

/// The innermost values of a row in [DEPTH_TABLE].
#[derive(Debug, FromRow, PartialEq)]
struct DeepLeaves {
    id: i64,
    int_leaf: i64,
    range_leaf: Range<Date>,
}

/// Returns a column that nests `leaf` in [MAX_STRUCT_DEPTH] levels of
/// `RECORD` columns, for a `Nest14<T>` field.
fn nested_column(name: &str, leaf: TableFieldSchema) -> TableFieldSchema {
    // `leaf` is the field of the innermost `RECORD`.
    let inner = (1..MAX_STRUCT_DEPTH).fold(leaf, |field, _| record(CHILD, vec![field]));
    record(name, vec![inner])
}

/// Returns the path to the innermost field of a column from [nested_column].
fn leaf_path(name: &str) -> String {
    std::iter::once(name)
        .chain(std::iter::repeat_n(CHILD, MAX_STRUCT_DEPTH))
        .collect::<Vec<_>>()
        .join(".")
}

/// Writes structs nested as deep as BigQuery allows, with a `RANGE` inside the
/// innermost one.
pub async fn depth(fixture: &Fixture) -> Result<()> {
    fixture
        .create_table(
            DEPTH_TABLE,
            vec![
                column(ID, INTEGER),
                nested_column(DEEP_INT, column(CHILD, INTEGER)),
                nested_column(DEEP_RANGE, range(CHILD, DATE)),
            ],
        )
        .await?;

    let stay = Range::new()
        .set_start(date(2025, 5, 16))
        .set_end(date(2025, 5, 18));
    let row = Deep {
        id: 1,
        deep_int: nest14(42),
        deep_range: nest14(stay.clone()),
    };
    fixture.write(DEPTH_TABLE, &[row]).await?;

    let sql = format!(
        "SELECT {ID}, {} AS int_leaf, {} AS range_leaf FROM {} ORDER BY {ID}",
        leaf_path(DEEP_INT),
        leaf_path(DEEP_RANGE),
        fixture.table_sql(DEPTH_TABLE)
    );
    let got: Vec<DeepLeaves> = fixture.query(sql).await?;
    let want = DeepLeaves {
        id: 1,
        int_leaf: 42,
        range_leaf: stay,
    };
    assert_eq!(got, [want]);
    Ok(())
}

/// Returns a column of the given type.
fn column(name: &str, column_type: &str) -> TableFieldSchema {
    TableFieldSchema::new().set_name(name).set_type(column_type)
}

/// Returns `column` as an `ARRAY` column, for a `Vec<T>` field.
fn repeated(column: TableFieldSchema) -> TableFieldSchema {
    column.set_mode(REPEATED)
}

/// Returns a `RECORD` column, for a struct field.
fn record(name: &str, fields: Vec<TableFieldSchema>) -> TableFieldSchema {
    column(name, RECORD).set_fields(fields)
}

/// Returns a `RANGE` column, for a `Range<T>` field.
fn range(name: &str, element_type: &str) -> TableFieldSchema {
    column(name, RANGE).set_range_element_type(FieldElementType::new().set_type(element_type))
}

/// Returns the date `year`-`month`-`day`.
fn date(year: i32, month: i32, day: i32) -> Date {
    Date::new().set_year(year).set_month(month).set_day(day)
}

/// Returns the time `hours`:`minutes`:`seconds`.`micros`.
fn time(hours: i32, minutes: i32, seconds: i32, micros: i32) -> TimeOfDay {
    TimeOfDay::new()
        .set_hours(hours)
        .set_minutes(minutes)
        .set_seconds(seconds)
        .set_nanos(micros * NANOS_PER_MICRO)
}

/// Returns the civil date and time for `date` and `time`.
fn datetime(date: &Date, time: &TimeOfDay) -> DateTime {
    DateTime::new()
        .set_year(date.year)
        .set_month(date.month)
        .set_day(date.day)
        .set_hours(time.hours)
        .set_minutes(time.minutes)
        .set_seconds(time.seconds)
        .set_nanos(time.nanos)
}

/// Returns the JSON object in `value`.
fn object(value: wkt::Value) -> Result<wkt::Struct> {
    Ok(serde_json::from_value(value)?)
}
