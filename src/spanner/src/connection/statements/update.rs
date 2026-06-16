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

use super::StatementStatus;
use crate::Error;
use crate::connection::Connection;
use crate::statement::Statement;

pub(crate) struct UpdateStatement {
    pub(crate) has_returning: bool,
}

impl UpdateStatement {
    pub(crate) async fn execute(
        self,
        conn: &mut Connection,
        statement: &mut Statement,
    ) -> Result<StatementStatus, Error> {
        conn.execute_query_or_update(statement.clone(), true, self.has_returning)
            .await
            .map(StatementStatus::Done)
    }
}
