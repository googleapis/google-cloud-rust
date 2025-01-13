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

use super::fake_library::model;
use axum::http::StatusCode;
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub fn success(
    name: impl Into<String>,
    resource: impl Into<String>,
) -> Result<(StatusCode, String)> {
    let resource = model::Resource {
        name: resource.into(),
    };
    let metadata = model::CreateResourceMetadata { percent: 100 };
    let metadata = wkt::Any::try_from(&metadata)?;
    let result = longrunning::model::operation::Result::Response(wkt::Any::try_from(&resource)?);
    let operation = longrunning::model::Operation::default()
        .set_name(name)
        .set_metadata(metadata)
        .set_done(true)
        .set_result(result);
    let payload = serde_json::to_string(&operation)?;
    Ok((StatusCode::OK, payload))
}

pub fn pending(name: impl Into<String>, percent: u32) -> Result<(StatusCode, String)> {
    let metadata = model::CreateResourceMetadata { percent };
    let metadata = wkt::Any::try_from(&metadata)?;
    let operation = longrunning::model::Operation::default()
        .set_name(name)
        .set_metadata(metadata);
    let payload = serde_json::to_string(&operation)?;
    Ok((StatusCode::OK, payload))
}

pub fn operation_error(name: impl Into<String>) -> Result<(StatusCode, String)> {
    let error = rpc::model::Status::default()
        .set_code(gax::error::rpc::Code::AlreadyExists as i32)
        .set_message(format!("The resource  already exists"));
    let result = longrunning::model::operation::Result::Response(wkt::Any::try_from(&error)?);
    let operation = longrunning::model::Operation::default()
        .set_name(name)
        .set_done(true)
        .set_result(result);
    let payload = serde_json::to_string(&operation)?;
    Ok((StatusCode::OK, payload))
}
