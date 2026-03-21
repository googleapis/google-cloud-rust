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
use google_cloud_spanner::client::Kind;
use google_cloud_spanner::client::{DatabaseClient, Mutation, Statement, Value};
use prost_types::value::Kind as ProtoKind;
use prost_types::{ListValue, Value as ProtoValue};

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

pub async fn write_only_transaction(db_client: &DatabaseClient) -> anyhow::Result<()> {
    // Write 1 row with values, 1 row with explicit nulls.
    let m1 = Mutation::new_insert_or_update_builder("AllTypes")
        .set("Id")
        .to(&100_i64)
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
            string_val(&BASE64_STANDARD.encode(&[1_u8, 2_u8])),
            string_val(&BASE64_STANDARD.encode(&[3_u8])),
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
        .to(&200_i64)
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

    let write_tx = db_client.write_only_transaction().build();
    let commit_ts = write_tx.write(vec![m1, m2]).await?;
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
    let stmt =
        Statement::builder("SELECT * FROM AllTypes WHERE Id IN (100, 200) ORDER BY Id").build();
    let mut rs = read_tx.execute_query(stmt).await?;

    let mut rows = Vec::new();
    while let Some(row) = rs.next().await.transpose()? {
        rows.push(row);
    }
    assert_eq!(rows.len(), 2, "Expected precisely 2 rows inserted/updated");

    // Verify row 1 (100)
    let row1 = &rows[0];

    let id: i64 = row1.get("Id");
    assert_eq!(id, 100);

    let col_bool: bool = row1.get("ColBool");
    assert_eq!(col_bool, true);

    let col_int64: i64 = row1.get("ColInt64");
    assert_eq!(col_int64, 100);

    let col_float32: f32 = row1.get("ColFloat32");
    assert_eq!(col_float32, 1.0_f32);

    let col_float64: f64 = row1.get("ColFloat64");
    assert_eq!(col_float64, 1.0_f64);

    let col_numeric: String = row1.get("ColNumeric");
    assert_eq!(col_numeric, "1.0");

    let col_string: String = row1.get("ColString");
    assert_eq!(col_string, "hello");

    let col_bytes: Vec<u8> = row1.get("ColBytes");
    assert_eq!(col_bytes, vec![1, 2, 3]);

    let col_date: String = row1.get("ColDate");
    assert_eq!(col_date, "2026-03-09");

    let col_timestamp: String = row1.get("ColTimestamp");
    assert_eq!(col_timestamp, "2026-03-09T16:20:00Z");

    let col_json: String = row1.get("ColJson");
    assert_eq!(col_json, "{\"value\": 1}");

    // TODO: We should implement FromValue and ToValue for specific array types.
    // For now, we fallback to extracting the raw Value to verify the array types.
    let arr_bool: Value = row1.get("ColArrayBool");
    assert_eq!(arr_bool.as_list().len(), 3); // ArrayBool
    assert_eq!(
        arr_bool
            .as_list()
            .get(0)
            .expect("expected ArrayBool element at index 0")
            .as_bool(),
        true
    );
    assert_eq!(
        arr_bool
            .as_list()
            .get(1)
            .expect("expected ArrayBool element at index 1")
            .as_bool(),
        false
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
        "1.0"
    );
    assert_eq!(
        arr_numeric
            .as_list()
            .get(1)
            .expect("expected ArrayNumeric element at index 1")
            .as_string(),
        "2.0"
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
        "{\"value\": 1}"
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
    let row2_id: i64 = row2.get("Id");
    assert_eq!(row2_id, 200);

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
