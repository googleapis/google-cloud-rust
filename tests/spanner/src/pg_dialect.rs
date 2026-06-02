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

use anyhow::Result;
use google_cloud_spanner::Decimal;
use google_cloud_spanner::client::DatabaseClient;
use google_cloud_spanner::mutation::Mutation;
use google_cloud_spanner::statement::Statement;
use google_cloud_test_utils::resource_names::LowercaseAlphanumeric;
use std::str::FromStr;

pub async fn pg_dialect_types_roundtrip(db_client: &DatabaseClient) -> Result<()> {
    let run_id = LowercaseAlphanumeric.random_string(10);
    let id = format!("pg-types-{}", run_id);

    // Native scalar values
    let val_bool = true;
    let val_int64 = 99999_i64;
    let val_float32 = 1.23_f32;
    let val_float64 = 123.456_f64;
    let val_numeric = Decimal::from_str("98765.4321").expect("valid Decimal");
    let val_string = "PostgreSQL Dialect Testing".to_string();
    let val_bytes = vec![10_u8, 20_u8, 30_u8];
    let val_date = time::Date::from_calendar_date(2026, time::Month::May, 25).expect("valid Date");

    // Truncate timestamp to microsecond precision to avoid flakiness
    let val_timestamp = {
        let now = time::OffsetDateTime::now_utc();
        let microsecond_nanos = (now.nanosecond() / 1000) * 1000;
        now.replace_nanosecond(microsecond_nanos)
            .expect("valid truncated timestamp")
    };

    let val_json = "{\"status\": \"pg-active\"}".to_string();
    let val_uuid = "f81d4fae-7dec-11d0-a765-00a0c91e6bf6".to_string();

    // PG dialect arrays containing NULL (None) elements
    let val_array_bool = vec![Some(true), None, Some(true)];
    let val_array_int64 = vec![Some(10_i64), None, Some(30_i64)];
    let val_array_float32 = vec![Some(1.1_f32), None, Some(2.2_f32)];
    let val_array_float64 = vec![Some(3.3_f64), None, Some(4.4_f64)];
    let val_array_numeric = vec![Some(Decimal::from_str("1.1").expect("valid Decimal")), None];
    let val_array_string = vec![Some("A".to_string()), None, Some("B".to_string())];
    let val_array_bytes = vec![Some(vec![1_u8]), None];
    let val_array_date = vec![Some(val_date), None];
    let val_array_timestamp = vec![Some(val_timestamp), None];
    let val_array_json = vec![Some(val_json.clone()), None];
    let val_array_uuid = vec![Some(val_uuid.clone()), None];

    // 1. Write row via PG dialect Mutation
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

    // 2. Query using positional parameter bindings ($1 -> p1)
    let read_tx = db_client.single_use().build();
    let stmt = Statement::builder(
        "SELECT Id, ColBool, ColInt64, ColFloat32, ColFloat64, ColNumeric, ColString, ColBytes, ColDate, ColTimestamp, ColJson, ColUuid, \
         ColArrayBool, ColArrayInt64, ColArrayFloat32, ColArrayFloat64, ColArrayNumeric, ColArrayString, ColArrayBytes, ColArrayDate, ColArrayTimestamp, ColArrayJson, ColArrayUuid \
         FROM AllTypes WHERE Id = $1"
    )
    .add_param("p1", &id)
    .build();

    let mut result_set = read_tx.execute_query(stmt).await?;
    let row = result_set
        .next()
        .await
        .transpose()?
        .expect("Expected to find inserted PG row");

    // 3. Assertions using positional indices (immune to PG case-folding lowercasing!)
    assert_eq!(row.get::<String, _>(0), id, "Id mismatch");
    assert_eq!(row.get::<bool, _>(1), val_bool, "ColBool mismatch");
    assert_eq!(row.get::<i64, _>(2), val_int64, "ColInt64 mismatch");
    assert_eq!(row.get::<f32, _>(3), val_float32, "ColFloat32 mismatch");
    assert_eq!(row.get::<f64, _>(4), val_float64, "ColFloat64 mismatch");
    assert_eq!(row.get::<Decimal, _>(5), val_numeric, "ColNumeric mismatch");
    assert_eq!(row.get::<String, _>(6), val_string, "ColString mismatch");
    assert_eq!(row.get::<Vec<u8>, _>(7), val_bytes, "ColBytes mismatch");
    assert_eq!(row.get::<time::Date, _>(8), val_date, "ColDate mismatch");

    let read_timestamp: time::OffsetDateTime = row.get(9);
    assert_eq!(
        read_timestamp.unix_timestamp_nanos() / 1000,
        val_timestamp.unix_timestamp_nanos() / 1000,
        "ColTimestamp mismatch"
    );

    let read_json_str: String = row.get(10);
    let read_json: serde_json::Value =
        serde_json::from_str(&read_json_str).expect("valid read JSON");
    let expected_json: serde_json::Value =
        serde_json::from_str(&val_json).expect("valid expected JSON");
    assert_eq!(read_json, expected_json, "ColJson mismatch");
    assert_eq!(row.get::<String, _>(11), val_uuid, "ColUuid mismatch");

    // Array assertions using positional indices
    assert_eq!(
        row.get::<Vec<Option<bool>>, _>(12),
        val_array_bool,
        "ColArrayBool mismatch"
    );
    assert_eq!(
        row.get::<Vec<Option<i64>>, _>(13),
        val_array_int64,
        "ColArrayInt64 mismatch"
    );
    assert_eq!(
        row.get::<Vec<Option<f32>>, _>(14),
        val_array_float32,
        "ColArrayFloat32 mismatch"
    );
    assert_eq!(
        row.get::<Vec<Option<f64>>, _>(15),
        val_array_float64,
        "ColArrayFloat64 mismatch"
    );
    assert_eq!(
        row.get::<Vec<Option<Decimal>>, _>(16),
        val_array_numeric,
        "ColArrayNumeric mismatch"
    );
    assert_eq!(
        row.get::<Vec<Option<String>>, _>(17),
        val_array_string,
        "ColArrayString mismatch"
    );
    assert_eq!(
        row.get::<Vec<Option<Vec<u8>>>, _>(18),
        val_array_bytes,
        "ColArrayBytes mismatch"
    );
    assert_eq!(
        row.get::<Vec<Option<time::Date>>, _>(19),
        val_array_date,
        "ColArrayDate mismatch"
    );

    let read_array_timestamp: Vec<Option<time::OffsetDateTime>> = row.get(20);
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
    assert!(
        read_array_timestamp[1].is_none(),
        "ColArrayTimestamp element 1 must be None (NULL)"
    );

    let read_array_json_str: Vec<Option<String>> = row.get(21);
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
    assert!(
        read_array_json_str[1].is_none(),
        "ColArrayJson element 1 must be None (NULL)"
    );

    assert_eq!(
        row.get::<Vec<Option<String>>, _>(22),
        val_array_uuid,
        "ColArrayUuid mismatch"
    );

    Ok(())
}
