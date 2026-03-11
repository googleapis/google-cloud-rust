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

use google_cloud_spanner::client::Spanner;
use google_cloud_spanner::client::Statement;
use spanner_grpc_mock::MockSpanner;
use spanner_grpc_mock::google::spanner::v1 as mock_v1;
use spanner_grpc_mock::start;

#[tokio::test]
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
        Ok(gaxi::grpc::tonic::Response::new(Box::pin(
            tokio_stream::iter(vec![Ok(result_set)]),
        )))
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
    let stmt = Statement::new("SELECT 1");

    let mut rs = tx.execute_query(stmt).await?;
    let row = rs.next().await.expect("has row").expect("has valid row");

    // Assert 1 row, 1 column with value "1"
    let val: &str = row.raw_values()[0].as_string();
    assert_eq!(val, "1");

    assert!(rs.next().await.is_none());

    Ok(())
}
