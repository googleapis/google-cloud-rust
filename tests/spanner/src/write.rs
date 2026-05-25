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

use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use google_cloud_spanner::Decimal;
use google_cloud_spanner::client::Kind;
use google_cloud_spanner::client::{DatabaseClient, Mutation, Statement, Value};
use prost_types::value::Kind as ProtoKind;
use prost_types::{ListValue, Value as ProtoValue};
use std::str::FromStr;

fn string_val(s: &str) -> ProtoValue {
    ProtoValue {
        kind: Some(ProtoKind::StringValue(s.to_string())),
    }
}

fn number_val(f: f64) -> ProtoValue {
    ProtoValue {
        kind: Some(ProtoKind::NumberValue(f)),
    }
}

fn bool_val(b: bool) -> ProtoValue {
    ProtoValue {
        kind: Some(ProtoKind::BoolValue(b)),
    }
}

fn array_val(values: Vec<ProtoValue>) -> ProtoValue {
    ProtoValue {
        kind: Some(ProtoKind::ListValue(ListValue { values })),
    }
}

fn null_val() -> ProtoValue {
    ProtoValue {
        kind: Some(ProtoKind::NullValue(0)),
    }
}

pub enum WriteMethod {
    WriteAtLeastOnce,
    Write,
}

pub async fn write_only_transaction(db_client: &DatabaseClient) -> anyhow::Result<()> {
    write_internal(
        db_client,
        WriteMethod::WriteAtLeastOnce,
        /* offset = */ 0,
    )
    .await
}

pub async fn write(db_client: &DatabaseClient) -> anyhow::Result<()> {
    write_internal(db_client, WriteMethod::Write, /* offset = */ 2).await
}

async fn write_internal(
    db_client: &DatabaseClient,
    method: WriteMethod,
    offset: i64,
) -> anyhow::Result<()> {
    let id1 = format!(
        "write1-{}-{}",
        offset,
        google_cloud_test_utils::resource_names::LowercaseAlphanumeric.random_string(10)
    );
    let id2 = format!(
        "write2-{}-{}",
        offset,
        google_cloud_test_utils::resource_names::LowercaseAlphanumeric.random_string(10)
    );

    // Write 1 row with values, 1 row with explicit nulls.
    let m1 = Mutation::new_insert_or_update_builder("AllTypes")
        .set("Id")
        .to(&id1)
        .set("ColBool")
        .to(&true)
        .set("ColInt64")
        .to(&100_i64)
        .set("ColFloat32")
        .to(&1.0_f32)
        .set("ColFloat64")
        .to(&1.0_f64)
        .set("ColNumeric")
        .to(&"1.0".to_string())
        .set("ColString")
        .to(&"hello".to_string())
        .set("ColBytes")
        .to(&vec![1_u8, 2_u8, 3_u8])
        .set("ColDate")
        .to(&"2026-03-09".to_string())
        .set("ColTimestamp")
        .to(&"2026-03-09T16:20:00Z".to_string())
        .set("ColJson")
        .to(&"{\"value\": 1}".to_string())
        .set("ColArrayBool")
        .to(&array_val(vec![
            bool_val(true),
            bool_val(false),
            null_val(),
        ]))
        .set("ColArrayInt64")
        .to(&array_val(vec![
            string_val("1"),
            string_val("2"),
            null_val(),
        ]))
        .set("ColArrayFloat32")
        .to(&array_val(vec![
            number_val(1.0),
            number_val(2.0),
            null_val(),
        ]))
        .set("ColArrayFloat64")
        .to(&array_val(vec![
            number_val(1.0),
            number_val(2.0),
            null_val(),
        ]))
        .set("ColArrayNumeric")
        .to(&array_val(vec![
            string_val("1.0"),
            string_val("2.0"),
            null_val(),
        ]))
        .set("ColArrayString")
        .to(&array_val(vec![
            string_val("hello"),
            string_val("world"),
            null_val(),
        ]))
        .set("ColArrayBytes")
        .to(&array_val(vec![
            string_val(&BASE64_STANDARD.encode([1_u8, 2_u8])),
            string_val(&BASE64_STANDARD.encode([3_u8])),
            null_val(),
        ]))
        .set("ColArrayDate")
        .to(&array_val(vec![string_val("2026-03-09"), null_val()]))
        .set("ColArrayTimestamp")
        .to(&array_val(vec![
            string_val("2026-03-09T16:20:00Z"),
            null_val(),
        ]))
        .set("ColArrayJson")
        .to(&array_val(vec![string_val("{\"value\": 1}"), null_val()]))
        .build();

    let m2 = Mutation::new_insert_or_update_builder("AllTypes")
        .set("Id")
        .to(&id2)
        .set("ColBool")
        .to::<Option<bool>>(&None)
        .set("ColInt64")
        .to::<Option<i64>>(&None)
        .set("ColFloat32")
        .to::<Option<f32>>(&None)
        .set("ColFloat64")
        .to::<Option<f64>>(&None)
        .set("ColNumeric")
        .to::<Option<String>>(&None)
        .set("ColString")
        .to::<Option<String>>(&None)
        .set("ColBytes")
        .to::<Option<Vec<u8>>>(&None)
        .set("ColDate")
        .to::<Option<String>>(&None)
        .set("ColTimestamp")
        .to::<Option<String>>(&None)
        .set("ColJson")
        .to::<Option<String>>(&None)
        .set("ColArrayBool")
        .to::<Option<ProtoValue>>(&None)
        .set("ColArrayInt64")
        .to::<Option<ProtoValue>>(&None)
        .set("ColArrayFloat32")
        .to::<Option<ProtoValue>>(&None)
        .set("ColArrayFloat64")
        .to::<Option<ProtoValue>>(&None)
        .set("ColArrayNumeric")
        .to::<Option<ProtoValue>>(&None)
        .set("ColArrayString")
        .to::<Option<ProtoValue>>(&None)
        .set("ColArrayBytes")
        .to::<Option<ProtoValue>>(&None)
        .set("ColArrayDate")
        .to::<Option<ProtoValue>>(&None)
        .set("ColArrayTimestamp")
        .to::<Option<ProtoValue>>(&None)
        .set("ColArrayJson")
        .to::<Option<ProtoValue>>(&None)
        .build();

    let write_tx = db_client
        .write_only_transaction()
        .with_transaction_tag("write-only-tag")
        .build();
    let commit_ts = match method {
        WriteMethod::WriteAtLeastOnce => write_tx.write_at_least_once(vec![m1, m2]).await?,
        WriteMethod::Write => write_tx.write(vec![m1, m2]).await?,
    };
    assert!(
        commit_ts
            .commit_timestamp
            .expect("commit timestamp is unexpectedly missing")
            .seconds()
            > 0,
        "Commit timestamp must be positive"
    );

    // Read it back to verify
    let read_tx = db_client.single_use().build();
    let stmt = Statement::builder(format!(
        "SELECT * FROM AllTypes WHERE Id IN ('{}', '{}') ORDER BY Id",
        id1, id2
    ))
    .build();
    let mut rs = read_tx.execute_query(stmt).await?;

    let mut rows = Vec::new();
    while let Some(row) = rs.next().await {
        rows.push(row?);
    }
    assert_eq!(rows.len(), 2, "Expected precisely 2 rows inserted/updated");

    // Verify row 1 (100)
    let row1 = &rows[0];

    let id: String = row1.get("Id");
    assert_eq!(id, id1);

    let col_bool: bool = row1.get("ColBool");
    assert!(col_bool);

    let col_int64: i64 = row1.get("ColInt64");
    assert_eq!(col_int64, 100);

    let col_float32: f32 = row1.get("ColFloat32");
    assert_eq!(col_float32, 1.0_f32);

    let col_float64: f64 = row1.get("ColFloat64");
    assert_eq!(col_float64, 1.0_f64);

    let col_numeric: String = row1.get("ColNumeric");
    assert_eq!(col_numeric, "1");

    let col_string: String = row1.get("ColString");
    assert_eq!(col_string, "hello");

    let col_bytes: Vec<u8> = row1.get("ColBytes");
    assert_eq!(col_bytes, vec![1, 2, 3]);

    let col_date: String = row1.get("ColDate");
    assert_eq!(col_date, "2026-03-09");

    let col_timestamp: String = row1.get("ColTimestamp");
    assert_eq!(col_timestamp, "2026-03-09T16:20:00Z");

    let col_json: String = row1.get("ColJson");
    assert_eq!(col_json, "{\"value\":1}");

    // TODO: We should implement FromValue and ToValue for specific array types.
    // For now, we fallback to extracting the raw Value to verify the array types.
    let arr_bool: Value = row1.get("ColArrayBool");
    assert_eq!(arr_bool.as_list().len(), 3); // ArrayBool
    assert!(
        arr_bool
            .as_list()
            .get(0)
            .expect("expected ArrayBool element at index 0")
            .as_bool()
    );
    assert!(
        !arr_bool
            .as_list()
            .get(1)
            .expect("expected ArrayBool element at index 1")
            .as_bool()
    );
    assert_eq!(
        arr_bool
            .as_list()
            .get(2)
            .expect("expected ArrayBool element at index 2")
            .kind(),
        Kind::Null
    );

    let arr_int64: Value = row1.get("ColArrayInt64");
    assert_eq!(arr_int64.as_list().len(), 3); // ArrayInt64
    assert_eq!(
        arr_int64
            .as_list()
            .get(0)
            .expect("expected ArrayInt64 element at index 0")
            .as_string(),
        "1"
    );
    assert_eq!(
        arr_int64
            .as_list()
            .get(1)
            .expect("expected ArrayInt64 element at index 1")
            .as_string(),
        "2"
    );
    assert_eq!(
        arr_int64
            .as_list()
            .get(2)
            .expect("expected ArrayInt64 element at index 2")
            .kind(),
        Kind::Null
    );

    let arr_float32: Value = row1.get("ColArrayFloat32");
    assert_eq!(arr_float32.as_list().len(), 3); // ArrayFloat32 mapped to f64
    assert_eq!(
        arr_float32
            .as_list()
            .get(0)
            .expect("expected ArrayFloat32 element at index 0")
            .as_f64(),
        1.0
    );
    assert_eq!(
        arr_float32
            .as_list()
            .get(1)
            .expect("expected ArrayFloat32 element at index 1")
            .as_f64(),
        2.0
    );
    assert_eq!(
        arr_float32
            .as_list()
            .get(2)
            .expect("expected ArrayFloat32 element at index 2")
            .kind(),
        Kind::Null
    );

    let arr_float64: Value = row1.get("ColArrayFloat64");
    assert_eq!(arr_float64.as_list().len(), 3); // ArrayFloat64
    assert_eq!(
        arr_float64
            .as_list()
            .get(0)
            .expect("expected ArrayFloat64 element at index 0")
            .as_f64(),
        1.0
    );
    assert_eq!(
        arr_float64
            .as_list()
            .get(1)
            .expect("expected ArrayFloat64 element at index 1")
            .as_f64(),
        2.0
    );
    assert_eq!(
        arr_float64
            .as_list()
            .get(2)
            .expect("expected ArrayFloat64 element at index 2")
            .kind(),
        Kind::Null
    );

    let arr_numeric: Value = row1.get("ColArrayNumeric");
    assert_eq!(arr_numeric.as_list().len(), 3); // ArrayNumeric
    assert_eq!(
        arr_numeric
            .as_list()
            .get(0)
            .expect("expected ArrayNumeric element at index 0")
            .as_string(),
        "1"
    );
    assert_eq!(
        arr_numeric
            .as_list()
            .get(1)
            .expect("expected ArrayNumeric element at index 1")
            .as_string(),
        "2"
    );
    assert_eq!(
        arr_numeric
            .as_list()
            .get(2)
            .expect("expected ArrayNumeric element at index 2")
            .kind(),
        Kind::Null
    );

    let arr_string: Value = row1.get("ColArrayString");
    assert_eq!(arr_string.as_list().len(), 3); // ArrayString
    assert_eq!(
        arr_string
            .as_list()
            .get(0)
            .expect("expected ArrayString element at index 0")
            .as_string(),
        "hello"
    );
    assert_eq!(
        arr_string
            .as_list()
            .get(1)
            .expect("expected ArrayString element at index 1")
            .as_string(),
        "world"
    );
    assert_eq!(
        arr_string
            .as_list()
            .get(2)
            .expect("expected ArrayString element at index 2")
            .kind(),
        Kind::Null
    );

    let arr_bytes: Value = row1.get("ColArrayBytes");
    assert_eq!(arr_bytes.as_list().len(), 3); // ArrayBytes (base64 returned from Spanner REST)
    assert!(
        arr_bytes
            .as_list()
            .get(0)
            .expect("expected ArrayBytes element at index 0")
            .try_as_string()
            .is_some()
    );
    assert!(
        arr_bytes
            .as_list()
            .get(1)
            .expect("expected ArrayBytes element at index 1")
            .try_as_string()
            .is_some()
    );
    assert_eq!(
        arr_bytes
            .as_list()
            .get(2)
            .expect("expected ArrayBytes element at index 2")
            .kind(),
        Kind::Null
    );

    let arr_date: Value = row1.get("ColArrayDate");
    assert_eq!(arr_date.as_list().len(), 2); // ArrayDate
    assert_eq!(
        arr_date
            .as_list()
            .get(0)
            .expect("expected ArrayDate element at index 0")
            .as_string(),
        "2026-03-09"
    );
    assert_eq!(
        arr_date
            .as_list()
            .get(1)
            .expect("expected ArrayDate element at index 1")
            .kind(),
        Kind::Null
    );

    let arr_timestamp: Value = row1.get("ColArrayTimestamp");
    assert_eq!(arr_timestamp.as_list().len(), 2); // ArrayTimestamp
    assert_eq!(
        arr_timestamp
            .as_list()
            .get(0)
            .expect("expected ArrayTimestamp element at index 0")
            .as_string(),
        "2026-03-09T16:20:00Z"
    );
    assert_eq!(
        arr_timestamp
            .as_list()
            .get(1)
            .expect("expected ArrayTimestamp element at index 1")
            .kind(),
        Kind::Null
    );

    let arr_json: Value = row1.get("ColArrayJson");
    assert_eq!(arr_json.as_list().len(), 2); // ArrayJson
    assert_eq!(
        arr_json
            .as_list()
            .get(0)
            .expect("expected ArrayJson element at index 0")
            .as_string(),
        "{\"value\":1}"
    );
    assert_eq!(
        arr_json
            .as_list()
            .get(1)
            .expect("expected ArrayJson element at index 1")
            .kind(),
        Kind::Null
    );

    // Verify row 2 (200) - explicitly NULL fields
    let row2 = &rows[1];
    let row2_id: String = row2.get("Id");
    assert_eq!(row2_id, id2);

    let metadata = rs
        .metadata()
        .expect("result set metadata is unexpectedly missing");
    let column_count = metadata.column_names().len();
    assert_eq!(row2.raw_values().len(), column_count);
    for i in 1..column_count {
        assert!(
            row2.is_null(i),
            "Column {} must be null",
            metadata.column_names()[i]
        );
    }

    Ok(())
}

pub async fn all_data_types_roundtrip(db_client: &DatabaseClient) -> anyhow::Result<()> {
    let run_id = google_cloud_test_utils::resource_names::LowercaseAlphanumeric.random_string(10);
    let id = format!("all-types-{}", run_id);

    // Native values
    let val_bool = true;
    let val_int64 = 42_i64;
    let val_float32 = 1.23_f32;
    let val_float64 = 4.56_f64;
    let val_numeric = Decimal::from_str("12345.6789").expect("valid Decimal");
    let val_string = "Hello Spanner".to_string();
    let val_bytes = vec![1_u8, 2_u8, 3_u8, 4_u8];
    let val_date = time::Date::from_calendar_date(2026, time::Month::May, 25).expect("valid Date");
    let val_timestamp = time::OffsetDateTime::now_utc();
    let val_json = "{\"name\": \"John Doe\", \"age\": 30}".to_string();
    let val_uuid = "f81d4fae-7dec-11d0-a765-00a0c91e6bf6".to_string();

    // Arrays containing NULL (None) elements
    let val_array_bool = vec![Some(true), None, Some(true)];
    let val_array_int64 = vec![Some(1_i64), None, Some(3_i64)];
    let val_array_float32 = vec![Some(1.1_f32), None];
    let val_array_float64 = vec![Some(3.3_f64), None];
    let val_array_numeric = vec![Some(Decimal::from_str("1.1").expect("valid Decimal")), None];
    let val_array_string = vec![Some("A".to_string()), None];
    let val_array_bytes = vec![Some(vec![1_u8]), None];
    let val_array_date = vec![Some(val_date), None];
    let val_array_timestamp = vec![Some(val_timestamp), None];
    let val_array_json = vec![Some(val_json.clone()), None];
    let val_array_uuid = vec![Some(val_uuid.clone()), None];

    // 1. Write Row 1 (Full Values & Nullable Array Elements)
    let mutation1 = Mutation::new_insert_or_update_builder("AllTypes")
        .set("Id")
        .to(&id)
        .set("ColBool")
        .to(&val_bool)
        .set("ColInt64")
        .to(&val_int64)
        .set("ColFloat32")
        .to(&val_float32)
        .set("ColFloat64")
        .to(&val_float64)
        .set("ColNumeric")
        .to(&val_numeric)
        .set("ColString")
        .to(&val_string)
        .set("ColBytes")
        .to(&val_bytes)
        .set("ColDate")
        .to(&val_date)
        .set("ColTimestamp")
        .to(&val_timestamp)
        .set("ColJson")
        .to(&val_json)
        .set("ColUuid")
        .to(&val_uuid)
        .set("ColArrayBool")
        .to(&val_array_bool)
        .set("ColArrayInt64")
        .to(&val_array_int64)
        .set("ColArrayFloat32")
        .to(&val_array_float32)
        .set("ColArrayFloat64")
        .to(&val_array_float64)
        .set("ColArrayNumeric")
        .to(&val_array_numeric)
        .set("ColArrayString")
        .to(&val_array_string)
        .set("ColArrayBytes")
        .to(&val_array_bytes)
        .set("ColArrayDate")
        .to(&val_array_date)
        .set("ColArrayTimestamp")
        .to(&val_array_timestamp)
        .set("ColArrayJson")
        .to(&val_array_json)
        .set("ColArrayUuid")
        .to(&val_array_uuid)
        .build();

    // 2. Write Row 2 (Strictly NULL Values E2E)
    let id_null = format!("all-types-null-{}", run_id);
    let mutation2 = Mutation::new_insert_or_update_builder("AllTypes")
        .set("Id")
        .to(&id_null)
        .set("ColBool")
        .to::<Option<bool>>(&None)
        .set("ColInt64")
        .to::<Option<i64>>(&None)
        .set("ColFloat32")
        .to::<Option<f32>>(&None)
        .set("ColFloat64")
        .to::<Option<f64>>(&None)
        .set("ColNumeric")
        .to::<Option<Decimal>>(&None)
        .set("ColString")
        .to::<Option<String>>(&None)
        .set("ColBytes")
        .to::<Option<Vec<u8>>>(&None)
        .set("ColDate")
        .to::<Option<time::Date>>(&None)
        .set("ColTimestamp")
        .to::<Option<time::OffsetDateTime>>(&None)
        .set("ColJson")
        .to::<Option<String>>(&None)
        .set("ColUuid")
        .to::<Option<String>>(&None)
        .set("ColArrayBool")
        .to::<Option<Vec<Option<bool>>>>(&None)
        .set("ColArrayInt64")
        .to::<Option<Vec<Option<i64>>>>(&None)
        .set("ColArrayFloat32")
        .to::<Option<Vec<Option<f32>>>>(&None)
        .set("ColArrayFloat64")
        .to::<Option<Vec<Option<f64>>>>(&None)
        .set("ColArrayNumeric")
        .to::<Option<Vec<Option<Decimal>>>>(&None)
        .set("ColArrayString")
        .to::<Option<Vec<Option<String>>>>(&None)
        .set("ColArrayBytes")
        .to::<Option<Vec<Option<Vec<u8>>>>>(&None)
        .set("ColArrayDate")
        .to::<Option<Vec<Option<time::Date>>>>(&None)
        .set("ColArrayTimestamp")
        .to::<Option<Vec<Option<time::OffsetDateTime>>>>(&None)
        .set("ColArrayJson")
        .to::<Option<Vec<Option<String>>>>(&None)
        .set("ColArrayUuid")
        .to::<Option<Vec<Option<String>>>>(&None)
        .build();

    let write_tx = db_client.write_only_transaction().build();
    write_tx
        .write_at_least_once(vec![mutation1, mutation2])
        .await?;

    // 3. Read back Row 1 natively and assert
    let read_tx = db_client.single_use().build();
    let mut result_set = read_tx
        .execute_read(
            google_cloud_spanner::client::ReadRequest::builder(
                "AllTypes",
                vec![
                    "ColBool",
                    "ColInt64",
                    "ColFloat32",
                    "ColFloat64",
                    "ColNumeric",
                    "ColString",
                    "ColBytes",
                    "ColDate",
                    "ColTimestamp",
                    "ColJson",
                    "ColUuid",
                    "ColArrayBool",
                    "ColArrayInt64",
                    "ColArrayFloat32",
                    "ColArrayFloat64",
                    "ColArrayNumeric",
                    "ColArrayString",
                    "ColArrayBytes",
                    "ColArrayDate",
                    "ColArrayTimestamp",
                    "ColArrayJson",
                    "ColArrayUuid",
                ],
            )
            .with_keys(
                google_cloud_spanner::client::KeySet::builder()
                    .add_key(google_cloud_spanner::key![id])
                    .build(),
            )
            .build(),
        )
        .await?;

    let row = result_set
        .next()
        .await
        .transpose()?
        .expect("Expected to find inserted row");

    // Row 1 Assertions
    assert_eq!(row.get::<bool, _>("ColBool"), val_bool, "ColBool mismatch");
    assert_eq!(
        row.get::<i64, _>("ColInt64"),
        val_int64,
        "ColInt64 mismatch"
    );
    assert_eq!(
        row.get::<f32, _>("ColFloat32"),
        val_float32,
        "ColFloat32 mismatch"
    );
    assert_eq!(
        row.get::<f64, _>("ColFloat64"),
        val_float64,
        "ColFloat64 mismatch"
    );
    assert_eq!(
        row.get::<Decimal, _>("ColNumeric"),
        val_numeric,
        "ColNumeric mismatch"
    );
    assert_eq!(
        row.get::<String, _>("ColString"),
        val_string,
        "ColString mismatch"
    );
    assert_eq!(
        row.get::<Vec<u8>, _>("ColBytes"),
        val_bytes,
        "ColBytes mismatch"
    );
    assert_eq!(
        row.get::<time::Date, _>("ColDate"),
        val_date,
        "ColDate mismatch"
    );

    // Timestamp comparison: Spanner stores timestamp at microsecond precision E2E
    let read_timestamp: time::OffsetDateTime = row.get("ColTimestamp");
    assert_eq!(
        read_timestamp.unix_timestamp_nanos() / 1000,
        val_timestamp.unix_timestamp_nanos() / 1000,
        "ColTimestamp mismatch"
    );

    let read_json_str: String = row.get("ColJson");
    let read_json: serde_json::Value =
        serde_json::from_str(&read_json_str).expect("valid read JSON");
    let expected_json: serde_json::Value =
        serde_json::from_str(&val_json).expect("valid expected JSON");
    assert_eq!(read_json, expected_json, "ColJson mismatch");
    assert_eq!(
        row.get::<String, _>("ColUuid"),
        val_uuid,
        "ColUuid mismatch"
    );

    assert_eq!(
        row.get::<Vec<Option<bool>>, _>("ColArrayBool"),
        val_array_bool,
        "ColArrayBool mismatch"
    );
    assert_eq!(
        row.get::<Vec<Option<i64>>, _>("ColArrayInt64"),
        val_array_int64,
        "ColArrayInt64 mismatch"
    );
    assert_eq!(
        row.get::<Vec<Option<f32>>, _>("ColArrayFloat32"),
        val_array_float32,
        "ColArrayFloat32 mismatch"
    );
    assert_eq!(
        row.get::<Vec<Option<f64>>, _>("ColArrayFloat64"),
        val_array_float64,
        "ColArrayFloat64 mismatch"
    );
    assert_eq!(
        row.get::<Vec<Option<Decimal>>, _>("ColArrayNumeric"),
        val_array_numeric,
        "ColArrayNumeric mismatch"
    );
    assert_eq!(
        row.get::<Vec<Option<String>>, _>("ColArrayString"),
        val_array_string,
        "ColArrayString mismatch"
    );
    assert_eq!(
        row.get::<Vec<Option<Vec<u8>>>, _>("ColArrayBytes"),
        val_array_bytes,
        "ColArrayBytes mismatch"
    );
    assert_eq!(
        row.get::<Vec<Option<time::Date>>, _>("ColArrayDate"),
        val_array_date,
        "ColArrayDate mismatch"
    );

    let read_array_timestamp: Vec<Option<time::OffsetDateTime>> = row.get("ColArrayTimestamp");
    assert_eq!(read_array_timestamp.len(), 2);
    assert_eq!(
        read_array_timestamp[0]
            .expect("Expected non-null timestamp element")
            .unix_timestamp_nanos()
            / 1000,
        val_array_timestamp[0]
            .expect("Expected non-null timestamp value")
            .unix_timestamp_nanos()
            / 1000,
        "ColArrayTimestamp mismatch"
    );
    assert!(read_array_timestamp[1].is_none());

    let read_array_json_str: Vec<Option<String>> = row.get("ColArrayJson");
    assert_eq!(read_array_json_str.len(), 2);
    let read_array_json: serde_json::Value = serde_json::from_str(
        read_array_json_str[0]
            .as_ref()
            .expect("Expected non-null JSON element"),
    )
    .expect("valid read Array JSON");
    let expected_array_json: serde_json::Value = serde_json::from_str(
        val_array_json[0]
            .as_ref()
            .expect("Expected non-null JSON value"),
    )
    .expect("valid expected Array JSON");
    assert_eq!(
        read_array_json, expected_array_json,
        "ColArrayJson mismatch"
    );
    assert!(read_array_json_str[1].is_none());

    assert_eq!(
        row.get::<Vec<Option<String>>, _>("ColArrayUuid"),
        val_array_uuid,
        "ColArrayUuid mismatch"
    );

    // 4. Read back Row 2 (NULL values) and assert E2E
    let read_tx_null = db_client.single_use().build();
    let mut result_set_null = read_tx_null
        .execute_read(
            google_cloud_spanner::client::ReadRequest::builder(
                "AllTypes",
                vec![
                    "ColBool",
                    "ColInt64",
                    "ColFloat32",
                    "ColFloat64",
                    "ColNumeric",
                    "ColString",
                    "ColBytes",
                    "ColDate",
                    "ColTimestamp",
                    "ColJson",
                    "ColUuid",
                    "ColArrayBool",
                    "ColArrayInt64",
                    "ColArrayFloat32",
                    "ColArrayFloat64",
                    "ColArrayNumeric",
                    "ColArrayString",
                    "ColArrayBytes",
                    "ColArrayDate",
                    "ColArrayTimestamp",
                    "ColArrayJson",
                    "ColArrayUuid",
                ],
            )
            .with_keys(
                google_cloud_spanner::client::KeySet::builder()
                    .add_key(google_cloud_spanner::key![id_null])
                    .build(),
            )
            .build(),
        )
        .await?;

    let row_null = result_set_null
        .next()
        .await
        .transpose()?
        .expect("Expected to find null row");
    assert!(
        row_null.get::<Option<bool>, _>("ColBool").is_none(),
        "Expected ColBool to be NULL"
    );
    assert!(
        row_null.get::<Option<i64>, _>("ColInt64").is_none(),
        "Expected ColInt64 to be NULL"
    );
    assert!(
        row_null.get::<Option<f32>, _>("ColFloat32").is_none(),
        "Expected ColFloat32 to be NULL"
    );
    assert!(
        row_null.get::<Option<f64>, _>("ColFloat64").is_none(),
        "Expected ColFloat64 to be NULL"
    );
    assert!(
        row_null.get::<Option<Decimal>, _>("ColNumeric").is_none(),
        "Expected ColNumeric to be NULL"
    );
    assert!(
        row_null.get::<Option<String>, _>("ColString").is_none(),
        "Expected ColString to be NULL"
    );
    assert!(
        row_null.get::<Option<Vec<u8>>, _>("ColBytes").is_none(),
        "Expected ColBytes to be NULL"
    );
    assert!(
        row_null.get::<Option<time::Date>, _>("ColDate").is_none(),
        "Expected ColDate to be NULL"
    );
    assert!(
        row_null
            .get::<Option<time::OffsetDateTime>, _>("ColTimestamp")
            .is_none(),
        "Expected ColTimestamp to be NULL"
    );
    assert!(
        row_null.get::<Option<String>, _>("ColJson").is_none(),
        "Expected ColJson to be NULL"
    );
    assert!(
        row_null.get::<Option<String>, _>("ColUuid").is_none(),
        "Expected ColUuid to be NULL"
    );
    assert!(
        row_null
            .get::<Option<Vec<Option<bool>>>, _>("ColArrayBool")
            .is_none(),
        "Expected ColArrayBool to be NULL"
    );
    assert!(
        row_null
            .get::<Option<Vec<Option<i64>>>, _>("ColArrayInt64")
            .is_none(),
        "Expected ColArrayInt64 to be NULL"
    );
    assert!(
        row_null
            .get::<Option<Vec<Option<f32>>>, _>("ColArrayFloat32")
            .is_none(),
        "Expected ColArrayFloat32 to be NULL"
    );
    assert!(
        row_null
            .get::<Option<Vec<Option<f64>>>, _>("ColArrayFloat64")
            .is_none(),
        "Expected ColArrayFloat64 to be NULL"
    );
    assert!(
        row_null
            .get::<Option<Vec<Option<Decimal>>>, _>("ColArrayNumeric")
            .is_none(),
        "Expected ColArrayNumeric to be NULL"
    );
    assert!(
        row_null
            .get::<Option<Vec<Option<String>>>, _>("ColArrayString")
            .is_none(),
        "Expected ColArrayString to be NULL"
    );
    assert!(
        row_null
            .get::<Option<Vec<Option<Vec<u8>>>>, _>("ColArrayBytes")
            .is_none(),
        "Expected ColArrayBytes to be NULL"
    );
    assert!(
        row_null
            .get::<Option<Vec<Option<time::Date>>>, _>("ColArrayDate")
            .is_none(),
        "Expected ColArrayDate to be NULL"
    );
    assert!(
        row_null
            .get::<Option<Vec<Option<time::OffsetDateTime>>>, _>("ColArrayTimestamp")
            .is_none(),
        "Expected ColArrayTimestamp to be NULL"
    );
    assert!(
        row_null
            .get::<Option<Vec<Option<String>>>, _>("ColArrayJson")
            .is_none(),
        "Expected ColArrayJson to be NULL"
    );
    assert!(
        row_null
            .get::<Option<Vec<Option<String>>>, _>("ColArrayUuid")
            .is_none(),
        "Expected ColArrayUuid to be NULL"
    );

    Ok(())
}

pub async fn all_data_types_parameter_binding(db_client: &DatabaseClient) -> anyhow::Result<()> {
    let run_id = google_cloud_test_utils::resource_names::LowercaseAlphanumeric.random_string(10);
    let id = format!("all-types-params-{}", run_id);

    let val_bool = false;
    let val_int64 = 123456_i64;
    let val_float32 = 9.87_f32;
    let val_float64 = 6.54_f64;
    let val_numeric = Decimal::from_str("99999.9999").expect("valid Decimal");
    let val_string = "Params Testing".to_string();
    let val_bytes = vec![9_u8, 8_u8, 7_u8];
    let val_date = time::Date::from_calendar_date(2026, time::Month::May, 25).expect("valid Date");

    // Truncate timestamp to microsecond precision to avoid nanosecond-to-microsecond rounding flakiness
    let val_timestamp = {
        let now = time::OffsetDateTime::now_utc();
        let microsecond_nanos = (now.nanosecond() / 1000) * 1000;
        now.replace_nanosecond(microsecond_nanos)
            .expect("valid truncated timestamp")
    };

    let val_json = "{\"active\": true}".to_string();
    let val_uuid = "f81d4fae-7dec-11d0-a765-00a0c91e6bf6".to_string();

    // Array parameter values
    let val_array_bool = vec![false, true];
    let val_array_int64 = vec![100_i64, 200_i64];
    let val_array_float32 = vec![4.5_f32, 5.6_f32];
    let val_array_float64 = vec![7.8_f64, 8.9_f64];
    let val_array_numeric = vec![Decimal::from_str("12.34").expect("valid Decimal")];
    let val_array_string = vec!["ParamStr".to_string()];
    let val_array_bytes = vec![vec![4_u8, 5_u8]];
    let val_array_date = vec![val_date];
    let val_array_timestamp = vec![val_timestamp];
    let val_array_json = vec![val_json.clone()];
    let val_array_uuid = vec![val_uuid.clone()];

    // Write the row
    let mutation = Mutation::new_insert_or_update_builder("AllTypes")
        .set("Id")
        .to(&id)
        .set("ColBool")
        .to(&val_bool)
        .set("ColInt64")
        .to(&val_int64)
        .set("ColFloat32")
        .to(&val_float32)
        .set("ColFloat64")
        .to(&val_float64)
        .set("ColNumeric")
        .to(&val_numeric)
        .set("ColString")
        .to(&val_string)
        .set("ColBytes")
        .to(&val_bytes)
        .set("ColDate")
        .to(&val_date)
        .set("ColTimestamp")
        .to(&val_timestamp)
        .set("ColJson")
        .to(&val_json)
        .set("ColUuid")
        .to(&val_uuid)
        .set("ColArrayBool")
        .to(&val_array_bool)
        .set("ColArrayInt64")
        .to(&val_array_int64)
        .set("ColArrayFloat32")
        .to(&val_array_float32)
        .set("ColArrayFloat64")
        .to(&val_array_float64)
        .set("ColArrayNumeric")
        .to(&val_array_numeric)
        .set("ColArrayString")
        .to(&val_array_string)
        .set("ColArrayBytes")
        .to(&val_array_bytes)
        .set("ColArrayDate")
        .to(&val_array_date)
        .set("ColArrayTimestamp")
        .to(&val_array_timestamp)
        .set("ColArrayJson")
        .to(&val_array_json)
        .set("ColArrayUuid")
        .to(&val_array_uuid)
        .build();

    let write_tx = db_client.write_only_transaction().build();
    write_tx.write_at_least_once(vec![mutation]).await?;

    let read_tx = db_client.single_use().build();
    let stmt = Statement::builder(
        "SELECT Id, \
        CAST(@array_bool AS ARRAY<BOOL>), \
        CAST(@array_int64 AS ARRAY<INT64>), \
        CAST(@array_float32 AS ARRAY<FLOAT32>), \
        CAST(@array_float64 AS ARRAY<FLOAT64>), \
        CAST(@array_numeric AS ARRAY<NUMERIC>), \
        CAST(@array_string AS ARRAY<STRING>), \
        CAST(@array_bytes AS ARRAY<BYTES>), \
        CAST(@array_date AS ARRAY<DATE>), \
        CAST(@array_timestamp AS ARRAY<TIMESTAMP>), \
        CAST(@array_json AS ARRAY<JSON>), \
        CAST(@array_uuid AS ARRAY<UUID>) \
        FROM AllTypes WHERE \
        Id = @id AND ColBool = @bool AND ColInt64 = @int64 AND ColFloat32 = @float32 AND \
        ColFloat64 = @float64 AND ColNumeric = @numeric AND ColString = @string AND \
        ColBytes = @bytes AND ColDate = @date AND ColTimestamp = @timestamp AND ColUuid = @uuid",
    )
    .add_param("id", &id)
    .add_param("bool", &val_bool)
    .add_param("int64", &val_int64)
    .add_param("float32", &val_float32)
    .add_param("float64", &val_float64)
    .add_param("numeric", &val_numeric)
    .add_param("string", &val_string)
    .add_param("bytes", &val_bytes)
    .add_param("date", &val_date)
    .add_param("timestamp", &val_timestamp)
    .add_param("uuid", &val_uuid)
    .add_param("array_bool", &val_array_bool)
    .add_param("array_int64", &val_array_int64)
    .add_param("array_float32", &val_array_float32)
    .add_param("array_float64", &val_array_float64)
    .add_param("array_numeric", &val_array_numeric)
    .add_param("array_string", &val_array_string)
    .add_param("array_bytes", &val_array_bytes)
    .add_param("array_date", &val_array_date)
    .add_param("array_timestamp", &val_array_timestamp)
    .add_param("array_json", &val_array_json)
    .add_param("array_uuid", &val_array_uuid)
    .build();

    let mut result_set = read_tx.execute_query(stmt).await?;
    let row = result_set
        .next()
        .await
        .transpose()?
        .expect("Expected to find row matching parameter bindings");
    let returned_id: String = row.get(0);
    assert_eq!(returned_id, id, "Row ID mismatch");

    // Assertions on returned array parameters
    assert_eq!(
        row.get::<Vec<bool>, _>(1),
        val_array_bool,
        "array_bool mismatch"
    );
    assert_eq!(
        row.get::<Vec<i64>, _>(2),
        val_array_int64,
        "array_int64 mismatch"
    );
    assert_eq!(
        row.get::<Vec<f32>, _>(3),
        val_array_float32,
        "array_float32 mismatch"
    );
    assert_eq!(
        row.get::<Vec<f64>, _>(4),
        val_array_float64,
        "array_float64 mismatch"
    );
    assert_eq!(
        row.get::<Vec<Decimal>, _>(5),
        val_array_numeric,
        "array_numeric mismatch"
    );
    assert_eq!(
        row.get::<Vec<String>, _>(6),
        val_array_string,
        "array_string mismatch"
    );
    assert_eq!(
        row.get::<Vec<Vec<u8>>, _>(7),
        val_array_bytes,
        "array_bytes mismatch"
    );
    assert_eq!(
        row.get::<Vec<time::Date>, _>(8),
        val_array_date,
        "array_date mismatch"
    );

    let read_array_timestamp: Vec<time::OffsetDateTime> = row.get(9);
    assert_eq!(read_array_timestamp.len(), 1);
    assert_eq!(
        read_array_timestamp[0].unix_timestamp_nanos() / 1000,
        val_array_timestamp[0].unix_timestamp_nanos() / 1000,
        "array_timestamp mismatch"
    );

    let read_array_json_str: Vec<String> = row.get(10);
    assert_eq!(read_array_json_str.len(), 1);
    let read_array_json: serde_json::Value =
        serde_json::from_str(&read_array_json_str[0]).expect("valid read Array JSON");
    let expected_array_json: serde_json::Value =
        serde_json::from_str(&val_array_json[0]).expect("valid expected Array JSON");
    assert_eq!(read_array_json, expected_array_json, "array_json mismatch");

    assert_eq!(
        row.get::<Vec<String>, _>(11),
        val_array_uuid,
        "array_uuid mismatch"
    );

    Ok(())
}

pub async fn interval_parameter_binding(db_client: &DatabaseClient) -> anyhow::Result<()> {
    let read_tx = db_client.single_use().build();

    // TODO(#5734): Add native ToValue / FromValue implementations for the Spanner INTERVAL
    // data type (potentially mapping to a dedicated Interval struct or standard Duration types),
    // and update this test to bind and extract it natively.
    // We send the interval value as a plain string representing 30 days
    let interval_str = "0-0 30 0:0:0".to_string();

    let stmt = Statement::builder(
        "WITH intervals AS ( \
            SELECT MAKE_INTERVAL(day => 30) AS val \
        ) \
        SELECT val FROM intervals WHERE val = @my_param",
    )
    .add_param("my_param", &interval_str)
    .build();

    let mut result_set = read_tx.execute_query(stmt).await?;
    let row = result_set
        .next()
        .await
        .transpose()?
        .expect("Expected to find row matching INTERVAL CTE");

    let returned_interval: String = row.get(0);
    assert!(
        returned_interval.contains("30"),
        "Expected returned interval to contain '30', got: {}",
        returned_interval
    );

    Ok(())
}
