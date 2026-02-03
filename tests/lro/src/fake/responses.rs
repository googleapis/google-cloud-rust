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
use google_cloud_longrunning as longrunning;
use google_cloud_workflows_v1::model::{OperationMetadata, Workflow};
use httptest::http::StatusCode;

pub fn success<N, R>(name: N, resource: R) -> Result<(StatusCode, String)>
where
    N: std::fmt::Display,
    R: Into<String>,
{
    let resource = Workflow::new().set_name(resource);
    let metadata = OperationMetadata::new().set_target("percent=100");
    let metadata = wkt::Any::from_msg(&metadata)?;
    let result =
        longrunning::model::operation::Result::Response(wkt::Any::from_msg(&resource)?.into());
    let operation = longrunning::model::Operation::default()
        .set_name(format!("projects/p/locations/l/operations/{name}"))
        .set_metadata(metadata)
        .set_done(true)
        .set_result(result);
    let payload = serde_json::to_string(&operation)?;
    Ok((StatusCode::OK, payload))
}

pub fn pending<N>(name: N, percent: u32) -> Result<(StatusCode, String)>
where
    N: std::fmt::Display,
{
    let metadata = OperationMetadata::new().set_target(format!("percent={percent}"));
    let metadata = wkt::Any::from_msg(&metadata)?;
    let operation = longrunning::model::Operation::default()
        .set_name(format!("projects/p/locations/l/operations/{name}"))
        .set_metadata(metadata);
    let payload = serde_json::to_string(&operation)?;
    Ok((StatusCode::OK, payload))
}

pub fn operation_error<N>(name: N) -> Result<(StatusCode, String)>
where
    N: std::fmt::Display,
{
    let error = rpc::model::Status::default()
        .set_code(gax::error::rpc::Code::AlreadyExists as i32)
        .set_message("The resource  already exists");
    let result = longrunning::model::operation::Result::Error(Box::new(error));
    let operation = longrunning::model::Operation::default()
        .set_name(format!("projects/p/locations/l/operations/{name}"))
        .set_done(true)
        .set_result(result);
    let payload = serde_json::to_string(&operation)?;
    Ok((StatusCode::OK, payload))
}
