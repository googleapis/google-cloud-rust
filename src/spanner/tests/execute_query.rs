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
use gaxi::grpc::tonic::Response;
use google_cloud_auth::credentials::anonymous::Builder as AnonymousCredentialsBuilder;
use google_cloud_spanner::client::Spanner;
use google_cloud_spanner::statement::Statement;
use google_cloud_spanner::types;
use google_cloud_test_macros::tokio_test_no_panics;
use prost::{Enumeration, Message};
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
    let val = row.raw_values()[0].as_str();
    assert_eq!(val, Some("1"));

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

    let f64_nan_val: f64 = row.get("f64_nan")?;
    assert!(f64_nan_val.is_nan(), "expected NaN for f64_nan");

    let f64_inf_val: f64 = row.get("f64_inf")?;
    assert_eq!(f64_inf_val, f64::INFINITY, "expected Infinity for f64_inf");

    let f64_neginf_val: f64 = row.get("f64_neginf")?;
    assert_eq!(
        f64_neginf_val,
        f64::NEG_INFINITY,
        "expected -Infinity for f64_neginf"
    );

    let f32_nan_val: f32 = row.get("f32_nan")?;
    assert!(f32_nan_val.is_nan(), "expected NaN for f32_nan");

    let f32_inf_val: f32 = row.get("f32_inf")?;
    assert_eq!(f32_inf_val, f32::INFINITY, "expected Infinity for f32_inf");

    let f32_neginf_val: f32 = row.get("f32_neginf")?;
    assert_eq!(
        f32_neginf_val,
        f32::NEG_INFINITY,
        "expected -Infinity for f32_neginf"
    );

    let f64_array_val: Vec<f64> = row.get("f64_array")?;
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

    let f32_array_val: Vec<f32> = row.get("f32_array")?;
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

#[derive(Clone, PartialEq, Message)]
struct CustomerAddress {
    #[prost(string, tag = "1")]
    street: String,
    #[prost(string, tag = "2")]
    city: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Enumeration)]
#[repr(i32)]
enum OrderStatus {
    Unspecified = 0,
    Pending = 1,
    Shipped = 2,
    Delivered = 3,
}

#[tokio_test_no_panics]
async fn execute_query_with_proto_and_proto_enum() -> anyhow::Result<()> {
    let mut mock = MockSpanner::new();
    mock.expect_create_session().once().returning(|_| {
        Ok(Response::new(Session {
            name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
            ..Default::default()
        }))
    });

    let sample_address = CustomerAddress {
        street: "1600 Amphitheatre Pkwy".to_string(),
        city: "Mountain View".to_string(),
    };
    let proto_payload = sample_address.encode_to_vec();
    let proto_base64 = BASE64_STANDARD.encode(&proto_payload);

    let expected_proto_fqn = "examples.shipping.Address";
    let expected_enum_fqn = "examples.shipping.OrderStatus";

    mock.expect_execute_streaming_sql()
        .once()
        .returning(move |request| {
            let request = request.into_inner();
            let param_types = request.param_types;

            // Verify typed parameters received by the mock server
            let proto_param_type = param_types
                .get("addr")
                .expect("request must contain addr param type");
            assert_eq!(
                proto_param_type.code,
                TypeCode::Proto as i32,
                "expected Proto type code for addr param"
            );
            assert_eq!(
                proto_param_type.proto_type_fqn, expected_proto_fqn,
                "expected proto_type_fqn for addr param"
            );

            let enum_param_type = param_types
                .get("status")
                .expect("request must contain status param type");
            assert_eq!(
                enum_param_type.code,
                TypeCode::Enum as i32,
                "expected Enum type code for status param"
            );
            assert_eq!(
                enum_param_type.proto_type_fqn, expected_enum_fqn,
                "expected proto_type_fqn for status param"
            );

            let result_set = PartialResultSet {
                metadata: Some(ResultSetMetadata {
                    row_type: Some(StructType {
                        fields: vec![
                            Field {
                                name: "Address".to_string(),
                                r#type: Some(Type {
                                    code: TypeCode::Proto as i32,
                                    array_element_type: None,
                                    struct_type: None,
                                    type_annotation: 0,
                                    proto_type_fqn: expected_proto_fqn.to_string(),
                                }),
                            },
                            Field {
                                name: "Status".to_string(),
                                r#type: Some(Type {
                                    code: TypeCode::Enum as i32,
                                    array_element_type: None,
                                    struct_type: None,
                                    type_annotation: 0,
                                    proto_type_fqn: expected_enum_fqn.to_string(),
                                }),
                            },
                            Field {
                                name: "NullAddress".to_string(),
                                r#type: Some(Type {
                                    code: TypeCode::Proto as i32,
                                    array_element_type: None,
                                    struct_type: None,
                                    type_annotation: 0,
                                    proto_type_fqn: expected_proto_fqn.to_string(),
                                }),
                            },
                            Field {
                                name: "NullStatus".to_string(),
                                r#type: Some(Type {
                                    code: TypeCode::Enum as i32,
                                    array_element_type: None,
                                    struct_type: None,
                                    type_annotation: 0,
                                    proto_type_fqn: expected_enum_fqn.to_string(),
                                }),
                            },
                            Field {
                                name: "AddressList".to_string(),
                                r#type: Some(Type {
                                    code: TypeCode::Array as i32,
                                    array_element_type: Some(Box::new(Type {
                                        code: TypeCode::Proto as i32,
                                        array_element_type: None,
                                        struct_type: None,
                                        type_annotation: 0,
                                        proto_type_fqn: expected_proto_fqn.to_string(),
                                    })),
                                    struct_type: None,
                                    type_annotation: 0,
                                    proto_type_fqn: String::new(),
                                }),
                            },
                            Field {
                                name: "StatusList".to_string(),
                                r#type: Some(Type {
                                    code: TypeCode::Array as i32,
                                    array_element_type: Some(Box::new(Type {
                                        code: TypeCode::Enum as i32,
                                        array_element_type: None,
                                        struct_type: None,
                                        type_annotation: 0,
                                        proto_type_fqn: expected_enum_fqn.to_string(),
                                    })),
                                    struct_type: None,
                                    type_annotation: 0,
                                    proto_type_fqn: String::new(),
                                }),
                            },
                        ],
                    }),
                    transaction: None,
                    undeclared_parameters: None,
                }),
                values: vec![
                    ProstValue {
                        kind: Some(Kind::StringValue(proto_base64.clone())),
                    },
                    ProstValue {
                        kind: Some(Kind::StringValue("2".to_string())),
                    },
                    ProstValue {
                        kind: Some(Kind::NullValue(0)),
                    },
                    ProstValue {
                        kind: Some(Kind::NullValue(0)),
                    },
                    ProstValue {
                        kind: Some(Kind::ListValue(ListValue {
                            values: vec![ProstValue {
                                kind: Some(Kind::StringValue(proto_base64.clone())),
                            }],
                        })),
                    },
                    ProstValue {
                        kind: Some(Kind::ListValue(ListValue {
                            values: vec![ProstValue {
                                kind: Some(Kind::StringValue("1".to_string())),
                            }],
                        })),
                    },
                ],
                chunked_value: false,
                resume_token: vec![],
                stats: None,
                precommit_token: None,
                cache_update: None,
                last: true,
            };

            let (sender, receiver) = channel(1);
            sender
                .try_send(Ok(result_set))
                .expect("channel send must succeed");
            Ok(Response::from(receiver))
        });

    let (address, _server) = start("0.0.0.0:0", mock)
        .await
        .expect("Failed to start mock server");

    let spanner = Spanner::builder()
        .with_endpoint(address)
        .with_credentials(AnonymousCredentialsBuilder::new().build())
        .build()
        .await
        .expect("Failed to build client");

    let database_client = spanner
        .database_client("projects/p/instances/i/databases/d")
        .build()
        .await
        .expect("Failed to create DatabaseClient");

    let transaction = database_client.single_use().build();
    let statement = Statement::builder(
        "SELECT Address, Status, NullAddress, NullStatus, AddressList, StatusList FROM Orders WHERE Address = @addr AND Status = @status",
    )
    .add_typed_param(
        "addr",
        sample_address.encode_to_vec(),
        types::proto("examples.shipping.Address"),
    )
    .add_typed_param(
        "status",
        OrderStatus::Shipped as i32 as i64,
        types::proto_enum("examples.shipping.OrderStatus"),
    )
    .build();

    let mut result_set = transaction.execute_query(statement).await?;
    let row = result_set
        .next()
        .await
        .expect("stream must yield first item")
        .expect("row must be valid");

    let address_bytes: Vec<u8> = row.get("Address")?;
    let address = CustomerAddress::decode(address_bytes.as_slice())
        .expect("decode CustomerAddress from row bytes");
    assert_eq!(address, sample_address, "expected address proto message");

    let status_raw: i64 = row.get("Status")?;
    let status =
        OrderStatus::try_from(status_raw as i32).expect("convert status i64 to OrderStatus enum");
    assert_eq!(status, OrderStatus::Shipped, "expected status enum Shipped");

    let null_address: Option<Vec<u8>> = row.get("NullAddress")?;
    assert_eq!(null_address, None, "expected None for NullAddress");

    let null_status: Option<i64> = row.get("NullStatus")?;
    assert_eq!(null_status, None, "expected None for NullStatus");

    let address_list_bytes: Vec<Vec<u8>> = row.get("AddressList")?;
    let address_list: Vec<CustomerAddress> = address_list_bytes
        .iter()
        .map(|bytes| {
            CustomerAddress::decode(bytes.as_slice())
                .expect("decode CustomerAddress from list element")
        })
        .collect();
    assert_eq!(
        address_list,
        vec![sample_address.clone()],
        "expected address list items"
    );

    let status_list_raw: Vec<i64> = row.get("StatusList")?;
    let status_list: Vec<OrderStatus> = status_list_raw
        .iter()
        .map(|&val| {
            OrderStatus::try_from(val as i32).expect("convert status_list i64 to OrderStatus enum")
        })
        .collect();
    assert_eq!(
        status_list,
        vec![OrderStatus::Pending],
        "expected status list items"
    );

    assert!(
        result_set.next().await.is_none(),
        "result set should yield exactly one row"
    );

    Ok(())
}
