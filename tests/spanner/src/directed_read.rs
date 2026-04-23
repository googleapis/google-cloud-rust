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

use google_cloud_spanner::client::{DatabaseClient, KeySet, ReadRequest};
use google_cloud_spanner::model::DirectedReadOptions;
use google_cloud_spanner::model::directed_read_options::{IncludeReplicas, Replicas};

pub async fn read_only_with_directed_read(db_client: &DatabaseClient) -> anyhow::Result<()> {
    let mut dro = DirectedReadOptions::default();
    let mut include = IncludeReplicas::default();
    include.auto_failover_disabled = true;
    dro.replicas = Some(Replicas::IncludeReplicas(Box::new(include)));

    let read = ReadRequest::builder("AllTypes", vec!["Id"])
        .with_keys(KeySet::all())
        .with_directed_read_options(dro)
        .build();

    let mut result_set = db_client
        .single_use()
        .build()
        .execute_read(read)
        .await
        .expect("Failed to execute read with DirectedReadOptions in RO transaction");

    // We don't need to check rows, just that the call succeeded.
    let _ = result_set.next().await;
    Ok(())
}

pub async fn read_write_with_directed_read_error(db_client: &DatabaseClient) -> anyhow::Result<()> {
    let mut dro = DirectedReadOptions::default();
    let mut include = IncludeReplicas::default();
    include.auto_failover_disabled = true;
    dro.replicas = Some(Replicas::IncludeReplicas(Box::new(include)));

    let read = ReadRequest::builder("AllTypes", vec!["Id"])
        .with_keys(KeySet::all())
        .with_directed_read_options(dro)
        .build();

    // Read-write transaction runner
    let runner = db_client.read_write_transaction().build().await?;

    let result: google_cloud_spanner::Result<()> = runner
        .run(async |tx| {
            let read = read.clone();
            let mut rs = tx.execute_read(read).await?;
            let _ = rs.next().await;
            Ok(())
        })
        .await;

    assert!(
        result.is_err(),
        "Expected read-write transaction with DirectedReadOptions to fail"
    );

    let err = result.unwrap_err();
    let err_str = format!("{:?}", err);
    // The proto documentation states that an INVALID_ARGUMENT error should be returned,
    // but the emulator returns FailedPrecondition with a specific message.
    assert!(
        err_str.contains("FailedPrecondition")
            || err_str.contains("Directed reads can only be performed in a read-only transaction"),
        "Expected FailedPrecondition error, got: {}",
        err_str
    );

    Ok(())
}
