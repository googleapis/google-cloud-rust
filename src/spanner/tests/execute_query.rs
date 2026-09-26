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
use google_cloud_auth::credentials::anonymous::Builder as AnonymousCredentialsBuilder;
use google_cloud_spanner::client::Spanner;
use google_cloud_spanner::statement::Statement;
use google_cloud_test_macros::tokio_test_no_panics;
use prost_types::value::Kind;
use prost_types::{ListValue, Value as ProstValue};
use spanner_grpc_mock::MockSpanner;
use spanner_grpc_mock::google::spanner::v1::struct_type::Field;
use spanner_grpc_mock::google::spanner::v1::{
    self as mock_v1, PartialResultSet, ResultSetMetadata, Session, StructType, Type, TypeCode,
};
use spanner_grpc_mock::start;
use tokio::sync::mpsc::channel;

#[tokio_test_no_panics]
async fn test_execute_query() -> anyhow::Result<()> {
    // Set up a MockSpanner server
    let mut mock = MockSpanner::new();
    mock.expect_create_session().once().returning(|_| {
        Ok(gaxi::grpc::tonic::Response::new(mock_v1::Session {
            name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
            ..Default::default()
        }))
    });

    mock.expect_execute_streaming_sql().once().returning(|req| {
        let req = req.into_inner();
        assert_eq!(
            req.session,
            "projects/p/instances/i/databases/d/sessions/123"
        );
        assert_eq!(req.sql, "SELECT 1");

        let result_set = mock_v1::PartialResultSet {
            metadata: Some(mock_v1::ResultSetMetadata {
                row_type: Some(mock_v1::StructType {
                    fields: vec![spanner_grpc_mock::google::spanner::v1::struct_type::Field {
                        name: "column1".to_string(),
                        r#type: Some(spanner_grpc_mock::google::spanner::v1::Type {
                            code: spanner_grpc_mock::google::spanner::v1::TypeCode::String as i32,
                            array_element_type: None,
                            struct_type: None,
                            type_annotation: 0,
                            proto_type_fqn: "".to_string(),
                        }),
                    }],
                }),
                transaction: None,
                undeclared_parameters: None,
            }),
            values: vec![prost_types::Value {
                kind: Some(prost_types::value::Kind::StringValue("1".to_string())),
            }],
            chunked_value: false,
            resume_token: vec![],
            stats: None,
            precommit_token: None,
            cache_update: None,
            last: true,
        };
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tx.try_send(Ok(result_set)).expect("always succeeds");
        Ok(gaxi::grpc::tonic::Response::from(rx))
    });

    let (address, _server) = start("0.0.0.0:0", mock)
        .await
        .expect("Failed to start mock server");

    let spanner = Spanner::builder()
        .with_endpoint(address)
        .with_credentials(google_cloud_auth::credentials::anonymous::Builder::new().build())
        .build()
        .await
        .expect("Failed to build client");

    let db_client = spanner
        .database_client("projects/p/instances/i/databases/d")
        .build()
        .await
        .expect("Failed to create DatabaseClient");

    // Test the builder and execution flow
    let tx = db_client.single_use().build();
    let stmt = Statement::builder("SELECT 1").build();

    let mut rs = tx.execute_query(stmt).await?;
    let row = rs.next().await.expect("has row").expect("has valid row");

    // Assert 1 row, 1 column with value "1"
    let val: &str = row.raw_values()[0].as_string();
    assert_eq!(val, "1");

    assert!(rs.next().await.is_none());

    Ok(())
}

#[tokio_test_no_panics]
async fn execute_query_with_non_finite_float_params() -> anyhow::Result<()> {
    let mut mock = MockSpanner::new();
    mock.expect_create_session().once().returning(|_| {
        Ok(Response::new(Session {
            name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
            ..Default::default()
        }))
    });

    mock.expect_execute_streaming_sql()
        .once()
        .returning(|request| {
            let request = request.into_inner();
            let params = request.params.expect("request must have params");
            let fields = params.fields;

            let test_cases = [
                ("f64_nan", "NaN"),
                ("f64_inf", "Infinity"),
                ("f64_neginf", "-Infinity"),
                ("f32_nan", "NaN"),
                ("f32_inf", "Infinity"),
                ("f32_neginf", "-Infinity"),
            ];

            for (param_name, expected_string) in test_cases {
                let field = fields.get(param_name).expect("parameter must exist");
                assert_eq!(
                    field.kind,
                    Some(Kind::StringValue(expected_string.to_string())),
                    "{param_name} parameter must be transmitted as StringValue('{expected_string}')"
                );
            }

            let f64_array_param = fields
                .get("f64_array")
                .expect("f64_array parameter must exist");
            if let Some(Kind::ListValue(list)) = &f64_array_param.kind {
                assert_eq!(list.values.len(), 3);
                assert_eq!(
                    list.values[0].kind,
                    Some(Kind::StringValue("NaN".to_string()))
                );
                assert_eq!(
                    list.values[1].kind,
                    Some(Kind::StringValue("Infinity".to_string()))
                );
                assert_eq!(
                    list.values[2].kind,
                    Some(Kind::StringValue("-Infinity".to_string()))
                );
            } else {
                panic!("f64_array parameter must be ListValue");
            }

            let f32_array_param = fields
                .get("f32_array")
                .expect("f32_array parameter must exist");
            if let Some(Kind::ListValue(list)) = &f32_array_param.kind {
                assert_eq!(list.values.len(), 3);
                assert_eq!(
                    list.values[0].kind,
                    Some(Kind::StringValue("NaN".to_string()))
                );
                assert_eq!(
                    list.values[1].kind,
                    Some(Kind::StringValue("Infinity".to_string()))
                );
                assert_eq!(
                    list.values[2].kind,
                    Some(Kind::StringValue("-Infinity".to_string()))
                );
            } else {
                panic!("f32_array parameter must be ListValue");
            }

            let result_fields = vec![
                Field {
                    name: "f64_nan".to_string(),
                    r#type: Some(Type {
                        code: TypeCode::Float64 as i32,
                        array_element_type: None,
                        struct_type: None,
                        type_annotation: 0,
                        proto_type_fqn: "".to_string(),
                    }),
                },
                Field {
                    name: "f64_inf".to_string(),
                    r#type: Some(Type {
                        code: TypeCode::Float64 as i32,
                        array_element_type: None,
                        struct_type: None,
                        type_annotation: 0,
                        proto_type_fqn: "".to_string(),
                    }),
                },
                Field {
                    name: "f64_neginf".to_string(),
                    r#type: Some(Type {
                        code: TypeCode::Float64 as i32,
                        array_element_type: None,
                        struct_type: None,
                        type_annotation: 0,
                        proto_type_fqn: "".to_string(),
                    }),
                },
                Field {
                    name: "f32_nan".to_string(),
                    r#type: Some(Type {
                        code: TypeCode::Float32 as i32,
                        array_element_type: None,
                        struct_type: None,
                        type_annotation: 0,
                        proto_type_fqn: "".to_string(),
                    }),
                },
                Field {
                    name: "f32_inf".to_string(),
                    r#type: Some(Type {
                        code: TypeCode::Float32 as i32,
                        array_element_type: None,
                        struct_type: None,
                        type_annotation: 0,
                        proto_type_fqn: "".to_string(),
                    }),
                },
                Field {
                    name: "f32_neginf".to_string(),
                    r#type: Some(Type {
                        code: TypeCode::Float32 as i32,
                        array_element_type: None,
                        struct_type: None,
                        type_annotation: 0,
                        proto_type_fqn: "".to_string(),
                    }),
                },
                Field {
                    name: "f64_array".to_string(),
                    r#type: Some(Type {
                        code: TypeCode::Array as i32,
                        array_element_type: Some(Box::new(Type {
                            code: TypeCode::Float64 as i32,
                            array_element_type: None,
                            struct_type: None,
                            type_annotation: 0,
                            proto_type_fqn: "".to_string(),
                        })),
                        struct_type: None,
                        type_annotation: 0,
                        proto_type_fqn: "".to_string(),
                    }),
                },
                Field {
                    name: "f32_array".to_string(),
                    r#type: Some(Type {
                        code: TypeCode::Array as i32,
                        array_element_type: Some(Box::new(Type {
                            code: TypeCode::Float32 as i32,
                            array_element_type: None,
                            struct_type: None,
                            type_annotation: 0,
                            proto_type_fqn: "".to_string(),
                        })),
                        struct_type: None,
                        type_annotation: 0,
                        proto_type_fqn: "".to_string(),
                    }),
                },
            ];

            let row_values = vec![
                ProstValue {
                    kind: Some(Kind::StringValue("NaN".to_string())),
                },
                ProstValue {
                    kind: Some(Kind::StringValue("Infinity".to_string())),
                },
                ProstValue {
                    kind: Some(Kind::StringValue("-Infinity".to_string())),
                },
                ProstValue {
                    kind: Some(Kind::StringValue("NaN".to_string())),
                },
                ProstValue {
                    kind: Some(Kind::StringValue("Infinity".to_string())),
                },
                ProstValue {
                    kind: Some(Kind::StringValue("-Infinity".to_string())),
                },
                ProstValue {
                    kind: Some(Kind::ListValue(ListValue {
                        values: vec![
                            ProstValue {
                                kind: Some(Kind::StringValue("NaN".to_string())),
                            },
                            ProstValue {
                                kind: Some(Kind::StringValue("Infinity".to_string())),
                            },
                            ProstValue {
                                kind: Some(Kind::StringValue("-Infinity".to_string())),
                            },
                        ],
                    })),
                },
                ProstValue {
                    kind: Some(Kind::ListValue(ListValue {
                        values: vec![
                            ProstValue {
                                kind: Some(Kind::StringValue("NaN".to_string())),
                            },
                            ProstValue {
                                kind: Some(Kind::StringValue("Infinity".to_string())),
                            },
                            ProstValue {
                                kind: Some(Kind::StringValue("-Infinity".to_string())),
                            },
                        ],
                    })),
                },
            ];

            let result_set = PartialResultSet {
                metadata: Some(ResultSetMetadata {
                    row_type: Some(StructType {
                        fields: result_fields,
                    }),
                    transaction: None,
                    undeclared_parameters: None,
                }),
                values: row_values,
                chunked_value: false,
                resume_token: vec![],
                stats: None,
                precommit_token: None,
                cache_update: None,
                last: true,
            };
            let (tx, rx) = channel(1);
            tx.try_send(Ok(result_set))
                .expect("sending mock result set succeeds");
            Ok(Response::from(rx))
        });

    let (address, _server) = start("0.0.0.0:0", mock).await?;

    let spanner = Spanner::builder()
        .with_endpoint(address)
        .with_credentials(AnonymousCredentialsBuilder::new().build())
        .build()
        .await?;

    let database_client = spanner
        .database_client("projects/p/instances/i/databases/d")
        .build()
        .await?;

    let transaction = database_client.single_use().build();
    let statement = Statement::builder(
        "SELECT @f64_nan, @f64_inf, @f64_neginf, @f32_nan, @f32_inf, @f32_neginf, @f64_array, @f32_array",
    )
    .add_param("f64_nan", f64::NAN)
    .add_param("f64_inf", f64::INFINITY)
    .add_param("f64_neginf", f64::NEG_INFINITY)
    .add_param("f32_nan", f32::NAN)
    .add_param("f32_inf", f32::INFINITY)
    .add_param("f32_neginf", f32::NEG_INFINITY)
    .add_param("f64_array", vec![f64::NAN, f64::INFINITY, f64::NEG_INFINITY])
    .add_param("f32_array", vec![f32::NAN, f32::INFINITY, f32::NEG_INFINITY])
    .build();

    let mut result_set = transaction.execute_query(statement).await?;
    let row = result_set
        .next()
        .await
        .ok_or_else(|| anyhow::anyhow!("result set should yield at least one row"))??;

    let f64_nan_val: f64 = row.try_get("f64_nan")?;
    assert!(f64_nan_val.is_nan(), "expected NaN for f64_nan");

    let f64_inf_val: f64 = row.try_get("f64_inf")?;
    assert_eq!(f64_inf_val, f64::INFINITY, "expected Infinity for f64_inf");

    let f64_neginf_val: f64 = row.try_get("f64_neginf")?;
    assert_eq!(
        f64_neginf_val,
        f64::NEG_INFINITY,
        "expected -Infinity for f64_neginf"
    );

    let f32_nan_val: f32 = row.try_get("f32_nan")?;
    assert!(f32_nan_val.is_nan(), "expected NaN for f32_nan");

    let f32_inf_val: f32 = row.try_get("f32_inf")?;
    assert_eq!(f32_inf_val, f32::INFINITY, "expected Infinity for f32_inf");

    let f32_neginf_val: f32 = row.try_get("f32_neginf")?;
    assert_eq!(
        f32_neginf_val,
        f32::NEG_INFINITY,
        "expected -Infinity for f32_neginf"
    );

    let f64_array_val: Vec<f64> = row.try_get("f64_array")?;
    assert_eq!(f64_array_val.len(), 3, "expected 3 elements in f64_array");
    assert!(f64_array_val[0].is_nan(), "expected NaN for f64_array[0]");
    assert_eq!(
        f64_array_val[1],
        f64::INFINITY,
        "expected Infinity for f64_array[1]"
    );
    assert_eq!(
        f64_array_val[2],
        f64::NEG_INFINITY,
        "expected -Infinity for f64_array[2]"
    );

    let f32_array_val: Vec<f32> = row.try_get("f32_array")?;
    assert_eq!(f32_array_val.len(), 3, "expected 3 elements in f32_array");
    assert!(f32_array_val[0].is_nan(), "expected NaN for f32_array[0]");
    assert_eq!(
        f32_array_val[1],
        f32::INFINITY,
        "expected Infinity for f32_array[1]"
    );
    assert_eq!(
        f32_array_val[2],
        f32::NEG_INFINITY,
        "expected -Infinity for f32_array[2]"
    );

    assert!(
        result_set.next().await.is_none(),
        "result set should yield exactly one row"
    );

    Ok(())
}
