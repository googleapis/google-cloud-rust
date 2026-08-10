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

use crate::Error;
use crate::error::{AppendError, AppendResult};
use crate::generated::gapic_storage::model::append_rows_response::Response;
use crate::model::{AppendRowsResponse, TableSchema};

/// The return type of an `append()` operation.
#[derive(Clone, Debug, Default, PartialEq)]
#[non_exhaustive]
pub struct AppendResponse {
    /// The row offset at which the last append occurred. The offset will not be
    /// set if appending using default streams.
    pub offset: Option<i64>,

    /// If set, the service reports that the table schema has changed.
    ///
    /// Note that this notification is best effort. Changing a table schema can
    /// take several minutes to propagate on the server side.
    ///
    /// The client library does not use this information to modify any internal
    /// state. It only forwards the notification to the application, which
    /// should react accordingly (if necessary).
    pub updated_schema: Option<TableSchema>,
}

pub(crate) fn to_result(resp: AppendRowsResponse) -> AppendResult<AppendResponse> {
    if !resp.row_errors.is_empty() {
        return Err(AppendError::RowErrors(resp.row_errors));
    }

    let offset = match resp.response {
        None => None,
        Some(Response::AppendResult(r)) => r.offset,
        Some(Response::Error(s)) => {
            return Err(Error::service((*s).into()).into());
        }
    };
    Ok(AppendResponse {
        offset,
        updated_schema: resp.updated_schema,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::RowError;
    use crate::model::append_rows_response::AppendResult;
    use crate::model::row_error::RowErrorCode;
    use google_cloud_gax::error::rpc::Code;
    use google_cloud_rpc::model::Status as RpcStatus;

    fn schema() -> TableSchema {
        TableSchema::new()
    }

    fn row_error(index: i64) -> RowError {
        RowError::new()
            .set_index(index)
            .set_code(RowErrorCode::FieldsError)
            .set_message("fail")
    }

    #[test]
    fn success() -> anyhow::Result<()> {
        let resp = AppendRowsResponse::new()
            .set_append_result(AppendResult::new().set_offset(42))
            .set_updated_schema(schema());

        let res = to_result(resp)?;
        assert_eq!(res.offset, Some(42));
        assert_eq!(res.updated_schema, Some(schema()));
        Ok(())
    }

    #[test]
    fn rpc_error() {
        let resp = AppendRowsResponse::new().set_error(
            RpcStatus::new()
                .set_code(Code::InvalidArgument as i32)
                .set_message("fail"),
        );

        let err = to_result(resp).expect_err("should error");
        let AppendError::Rpc { source } = err else {
            panic!("Expected AppendError::Rpc, got {:?}", err);
        };
        let status = source.status().expect("status should be set");
        assert_eq!(status.code, Code::InvalidArgument);
        assert_eq!(status.message, "fail");
    }

    #[test]
    fn row_errors() {
        let resp = AppendRowsResponse::new().set_row_errors(vec![row_error(1), row_error(2)]);

        let err = to_result(resp).expect_err("should error");
        let AppendError::RowErrors(errors) = err else {
            panic!("Expected AppendError::RowErrors, got {:?}", err);
        };
        assert_eq!(errors, vec![row_error(1), row_error(2)]);
    }

    #[test]
    fn unset_response() -> anyhow::Result<()> {
        let resp = AppendRowsResponse::new().set_updated_schema(schema());
        let res = to_result(resp)?;
        assert_eq!(res.offset, None);
        assert_eq!(res.updated_schema, Some(schema()));
        Ok(())
    }
}
