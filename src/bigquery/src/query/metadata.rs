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

use google_cloud_bigquery_v2::model::{
    DmlStats, ErrorProto, GetQueryResultsResponse, Job, JobConfiguration, JobCreationReason,
    JobReference, JobStatistics, JobStatus, QueryResponse, SessionInfo, TableSchema,
};

/// Metadata associated with the creation of a BigQuery query job.
///
/// Depending on how the query was initiated, this metadata originates from either a `jobs.query`
/// response or a `jobs.insert` job resource.
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum QueryCreationMetadata {
    /// Metadata originating from a `jobs.query` API call.
    JobsQuery(QueryResponse),
    /// Metadata originating from a `jobs.insert` API call.
    JobsInsert(Job),
}

impl QueryCreationMetadata {
    /// Returns the resource type of the response.
    pub fn kind(&self) -> &str {
        match self {
            Self::JobsQuery(res) => &res.kind,
            Self::JobsInsert(res) => &res.kind,
        }
    }

    /// Returns the reference to the BigQuery job created to run the query.
    pub fn job_reference(&self) -> Option<&JobReference> {
        match self {
            Self::JobsQuery(res) => res.job_reference.as_ref(),
            Self::JobsInsert(res) => res.job_reference.as_ref(),
        }
    }

    /// Returns the reason why the BigQuery job was created.
    pub fn job_creation_reason(&self) -> Option<&JobCreationReason> {
        match self {
            Self::JobsQuery(res) => res.job_creation_reason.as_ref(),
            Self::JobsInsert(res) => res.job_creation_reason.as_ref(),
        }
    }

    /// Returns the schema of the query results.
    ///
    /// This field only exists in the `JobsQuery` branch.
    pub fn schema(&self) -> Option<&TableSchema> {
        match self {
            Self::JobsQuery(res) => res.schema.as_ref(),
            Self::JobsInsert(_) => None,
        }
    }

    /// Returns the auto-generated ID for the query.
    ///
    /// This field only exists in the `JobsQuery` branch.
    pub fn query_id(&self) -> Option<&str> {
        match self {
            Self::JobsQuery(res) => Some(&res.query_id),
            Self::JobsInsert(_) => None,
        }
    }

    /// Returns the geographic location of the query.
    ///
    /// This field only exists in the `JobsQuery` branch.
    pub fn location(&self) -> Option<&str> {
        match self {
            Self::JobsQuery(res) => Some(&res.location),
            Self::JobsInsert(_) => None,
        }
    }

    /// Returns the total number of rows in the complete query result set.
    ///
    /// This field only exists in the `JobsQuery` branch.
    pub fn total_rows(&self) -> Option<u64> {
        match self {
            Self::JobsQuery(res) => res.total_rows,
            Self::JobsInsert(_) => None,
        }
    }

    /// Returns the token used for paging results.
    ///
    /// This field only exists in the `JobsQuery` branch.
    pub fn page_token(&self) -> Option<&str> {
        match self {
            Self::JobsQuery(res) => Some(&res.page_token),
            Self::JobsInsert(_) => None,
        }
    }

    /// Returns the initial page of query result rows.
    ///
    /// This field only exists in the `JobsQuery` branch.
    pub fn rows(&self) -> Option<&[wkt::Struct]> {
        match self {
            Self::JobsQuery(res) => Some(&res.rows),
            Self::JobsInsert(_) => None,
        }
    }

    /// Returns the total number of bytes processed for this query.
    ///
    /// This field only exists in the `JobsQuery` branch.
    pub fn total_bytes_processed(&self) -> Option<i64> {
        match self {
            Self::JobsQuery(res) => res.total_bytes_processed,
            Self::JobsInsert(_) => None,
        }
    }

    /// Returns the total number of bytes billed for the job.
    ///
    /// This field only exists in the `JobsQuery` branch.
    pub fn total_bytes_billed(&self) -> Option<i64> {
        match self {
            Self::JobsQuery(res) => res.total_bytes_billed,
            Self::JobsInsert(_) => None,
        }
    }

    /// Returns the number of slot milliseconds the user is actually billed for.
    ///
    /// This field only exists in the `JobsQuery` branch.
    pub fn total_slot_ms(&self) -> Option<i64> {
        match self {
            Self::JobsQuery(res) => res.total_slot_ms,
            Self::JobsInsert(_) => None,
        }
    }

    /// Returns whether the query job has completed or not.
    ///
    /// This field only exists in the `JobsQuery` branch.
    pub fn job_complete(&self) -> Option<bool> {
        match self {
            Self::JobsQuery(res) => res.job_complete,
            Self::JobsInsert(_) => None,
        }
    }

    /// Returns the first errors or warnings encountered during the running of the job.
    ///
    /// This field only exists in the `JobsQuery` branch.
    pub fn errors(&self) -> Option<&[ErrorProto]> {
        match self {
            Self::JobsQuery(res) => Some(&res.errors),
            Self::JobsInsert(_) => None,
        }
    }

    /// Returns whether the query result was fetched from the query cache.
    ///
    /// This field only exists in the `JobsQuery` branch.
    pub fn cache_hit(&self) -> Option<bool> {
        match self {
            Self::JobsQuery(res) => res.cache_hit,
            Self::JobsInsert(_) => None,
        }
    }

    /// Returns the number of rows affected by a DML statement.
    ///
    /// This field only exists in the `JobsQuery` branch.
    pub fn num_dml_affected_rows(&self) -> Option<i64> {
        match self {
            Self::JobsQuery(res) => res.num_dml_affected_rows,
            Self::JobsInsert(_) => None,
        }
    }

    /// Returns information about the session if this job is part of one.
    ///
    /// This field only exists in the `JobsQuery` branch.
    pub fn session_info(&self) -> Option<&SessionInfo> {
        match self {
            Self::JobsQuery(res) => res.session_info.as_ref(),
            Self::JobsInsert(_) => None,
        }
    }

    /// Returns detailed statistics for DML statements.
    ///
    /// This field only exists in the `JobsQuery` branch.
    pub fn dml_stats(&self) -> Option<&DmlStats> {
        match self {
            Self::JobsQuery(res) => res.dml_stats.as_ref(),
            Self::JobsInsert(_) => None,
        }
    }

    /// Returns the creation time of this query, in milliseconds since the epoch.
    ///
    /// This field only exists in the `JobsQuery` branch.
    pub fn creation_time(&self) -> Option<i64> {
        match self {
            Self::JobsQuery(res) => res.creation_time,
            Self::JobsInsert(_) => None,
        }
    }

    /// Returns the start time of this query, in milliseconds since the epoch.
    ///
    /// This field only exists in the `JobsQuery` branch.
    pub fn start_time(&self) -> Option<i64> {
        match self {
            Self::JobsQuery(res) => res.start_time,
            Self::JobsInsert(_) => None,
        }
    }

    /// Returns the end time of this query, in milliseconds since the epoch.
    ///
    /// This field only exists in the `JobsQuery` branch.
    pub fn end_time(&self) -> Option<i64> {
        match self {
            Self::JobsQuery(res) => res.end_time,
            Self::JobsInsert(_) => None,
        }
    }

    /// Returns the hash of the job resource.
    ///
    /// This field only exists in the `JobsInsert` branch.
    pub fn etag(&self) -> Option<&str> {
        match self {
            Self::JobsInsert(res) => Some(&res.etag),
            Self::JobsQuery(_) => None,
        }
    }

    /// Returns the opaque ID field of the job.
    ///
    /// This field only exists in the `JobsInsert` branch.
    pub fn id(&self) -> Option<&str> {
        match self {
            Self::JobsInsert(res) => Some(&res.id),
            Self::JobsQuery(_) => None,
        }
    }

    /// Returns the URL that can be used to access the job resource again.
    ///
    /// This field only exists in the `JobsInsert` branch.
    pub fn self_link(&self) -> Option<&str> {
        match self {
            Self::JobsInsert(res) => Some(&res.self_link),
            Self::JobsQuery(_) => None,
        }
    }

    /// Returns the email address of the user who ran the job.
    ///
    /// This field only exists in the `JobsInsert` branch.
    pub fn user_email(&self) -> Option<&str> {
        match self {
            Self::JobsInsert(res) => Some(&res.user_email),
            Self::JobsQuery(_) => None,
        }
    }

    /// Returns the job configuration.
    ///
    /// This field only exists in the `JobsInsert` branch.
    pub fn configuration(&self) -> Option<&JobConfiguration> {
        match self {
            Self::JobsInsert(res) => res.configuration.as_ref(),
            Self::JobsQuery(_) => None,
        }
    }

    /// Returns information about the job, including start and end times.
    ///
    /// This field only exists in the `JobsInsert` branch.
    pub fn statistics(&self) -> Option<&JobStatistics> {
        match self {
            Self::JobsInsert(res) => res.statistics.as_ref(),
            Self::JobsQuery(_) => None,
        }
    }

    /// Returns the status of this job, including execution state and errors.
    ///
    /// This field only exists in the `JobsInsert` branch.
    pub fn status(&self) -> Option<&JobStatus> {
        match self {
            Self::JobsInsert(res) => res.status.as_ref(),
            Self::JobsQuery(_) => None,
        }
    }

    /// Returns the string representation of the identity of the requesting party.
    ///
    /// This field only exists in the `JobsInsert` branch.
    pub fn principal_subject(&self) -> Option<&str> {
        match self {
            Self::JobsInsert(res) => Some(&res.principal_subject),
            Self::JobsQuery(_) => None,
        }
    }
}

/// Metadata associated with a completed BigQuery query.
///
/// Depending on how the query was executed and polled, this metadata originates from either
/// a `jobs.query` response or a `jobs.getQueryResults` response.
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum QueryMetadata {
    /// Metadata originating from a `jobs.query` API call.
    JobsQuery(QueryResponse),
    /// Metadata originating from a `jobs.getQueryResults` API call.
    GetQueryResultsResponse(GetQueryResultsResponse),
}

impl QueryMetadata {
    /// Returns the resource type of the response.
    pub fn kind(&self) -> &str {
        match self {
            Self::JobsQuery(res) => &res.kind,
            Self::GetQueryResultsResponse(res) => &res.kind,
        }
    }

    /// Returns the schema of the query results.
    pub fn schema(&self) -> Option<&TableSchema> {
        match self {
            Self::JobsQuery(res) => res.schema.as_ref(),
            Self::GetQueryResultsResponse(res) => res.schema.as_ref(),
        }
    }

    /// Returns the reference to the BigQuery job created to run the query.
    pub fn job_reference(&self) -> Option<&JobReference> {
        match self {
            Self::JobsQuery(res) => res.job_reference.as_ref(),
            Self::GetQueryResultsResponse(res) => res.job_reference.as_ref(),
        }
    }

    /// Returns the total number of rows in the complete query result set.
    pub fn total_rows(&self) -> u64 {
        match self {
            Self::GetQueryResultsResponse(res) => res.total_rows.unwrap_or(0),
            Self::JobsQuery(res) => res.total_rows.unwrap_or(0),
        }
    }

    /// Returns the token used for paging results.
    pub fn page_token(&self) -> &str {
        match self {
            Self::JobsQuery(res) => &res.page_token,
            Self::GetQueryResultsResponse(res) => &res.page_token,
        }
    }

    /// Returns the initial page of query result rows.
    pub fn rows(&self) -> &[wkt::Struct] {
        match self {
            Self::JobsQuery(res) => &res.rows,
            Self::GetQueryResultsResponse(res) => &res.rows,
        }
    }

    /// Returns the total number of bytes processed for this query.
    pub fn total_bytes_processed(&self) -> Option<i64> {
        match self {
            Self::JobsQuery(res) => res.total_bytes_processed,
            Self::GetQueryResultsResponse(res) => res.total_bytes_processed,
        }
    }

    /// Returns whether the query job has completed or not.
    pub fn job_complete(&self) -> Option<bool> {
        match self {
            Self::JobsQuery(res) => res.job_complete,
            Self::GetQueryResultsResponse(res) => res.job_complete,
        }
    }

    /// Returns the first errors or warnings encountered during the running of the job.
    pub fn errors(&self) -> &[ErrorProto] {
        match self {
            Self::JobsQuery(res) => &res.errors,
            Self::GetQueryResultsResponse(res) => &res.errors,
        }
    }

    /// Returns whether the query result was fetched from the query cache.
    pub fn cache_hit(&self) -> Option<bool> {
        match self {
            Self::JobsQuery(res) => res.cache_hit,
            Self::GetQueryResultsResponse(res) => res.cache_hit,
        }
    }

    /// Returns the number of rows affected by a DML statement.
    pub fn num_dml_affected_rows(&self) -> i64 {
        match self {
            Self::GetQueryResultsResponse(res) => res.num_dml_affected_rows.unwrap_or(0),
            Self::JobsQuery(res) => res.num_dml_affected_rows.unwrap_or(0),
        }
    }

    /// Returns the reason why the BigQuery job was created.
    ///
    /// This field only exists in the `JobsQuery` branch.
    pub fn job_creation_reason(&self) -> Option<&JobCreationReason> {
        match self {
            Self::JobsQuery(res) => res.job_creation_reason.as_ref(),
            Self::GetQueryResultsResponse(_) => None,
        }
    }

    /// Returns the auto-generated ID for the query.
    ///
    /// This field only exists in the `JobsQuery` branch.
    pub fn query_id(&self) -> Option<&str> {
        match self {
            Self::JobsQuery(res) => Some(&res.query_id),
            Self::GetQueryResultsResponse(_) => None,
        }
    }

    /// Returns the geographic location of the query.
    ///
    /// This field only exists in the `JobsQuery` branch.
    pub fn location(&self) -> Option<&str> {
        match self {
            Self::JobsQuery(res) => Some(&res.location),
            Self::GetQueryResultsResponse(_) => None,
        }
    }

    /// Returns the total number of bytes billed for the job.
    ///
    /// This field only exists in the `JobsQuery` branch.
    pub fn total_bytes_billed(&self) -> Option<i64> {
        match self {
            Self::JobsQuery(res) => res.total_bytes_billed,
            Self::GetQueryResultsResponse(_) => None,
        }
    }

    /// Returns the number of slot milliseconds the user is actually billed for.
    ///
    /// This field only exists in the `JobsQuery` branch.
    pub fn total_slot_ms(&self) -> Option<i64> {
        match self {
            Self::JobsQuery(res) => res.total_slot_ms,
            Self::GetQueryResultsResponse(_) => None,
        }
    }

    /// Returns information about the session if this job is part of one.
    ///
    /// This field only exists in the `JobsQuery` branch.
    pub fn session_info(&self) -> Option<&SessionInfo> {
        match self {
            Self::JobsQuery(res) => res.session_info.as_ref(),
            Self::GetQueryResultsResponse(_) => None,
        }
    }

    /// Returns detailed statistics for DML statements.
    ///
    /// This field only exists in the `JobsQuery` branch.
    pub fn dml_stats(&self) -> Option<&DmlStats> {
        match self {
            Self::JobsQuery(res) => res.dml_stats.as_ref(),
            Self::GetQueryResultsResponse(_) => None,
        }
    }

    /// Returns the creation time of this query, in milliseconds since the epoch.
    ///
    /// This field only exists in the `JobsQuery` branch.
    pub fn creation_time(&self) -> Option<i64> {
        match self {
            Self::JobsQuery(res) => res.creation_time,
            Self::GetQueryResultsResponse(_) => None,
        }
    }

    /// Returns the start time of this query, in milliseconds since the epoch.
    ///
    /// This field only exists in the `JobsQuery` branch.
    pub fn start_time(&self) -> Option<i64> {
        match self {
            Self::JobsQuery(res) => res.start_time,
            Self::GetQueryResultsResponse(_) => None,
        }
    }

    /// Returns the end time of this query, in milliseconds since the epoch.
    ///
    /// This field only exists in the `JobsQuery` branch.
    pub fn end_time(&self) -> Option<i64> {
        match self {
            Self::JobsQuery(res) => res.end_time,
            Self::GetQueryResultsResponse(_) => None,
        }
    }

    /// Returns the hash of the response.
    ///
    /// This field only exists in the `GetQueryResultsResponse` branch.
    pub fn etag(&self) -> Option<&str> {
        match self {
            Self::GetQueryResultsResponse(res) => Some(&res.etag),
            Self::JobsQuery(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::path::PathBuf;
    use syn::{Fields, Item};

    #[test]
    fn metadata_fields_sync_warning() {
        // Locate the generated model.rs file relative to the current crate manifest
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let model_path =
            PathBuf::from(manifest_dir).join("../generated/cloud/bigquery/v2/src/model.rs");

        let content = match std::fs::read_to_string(&model_path) {
            Ok(c) => c,
            Err(e) => {
                eprintln!(
                    "WARNING: Could not read model.rs for metadata sync check: {}",
                    e
                );
                return;
            }
        };

        let syntax_tree = match syn::parse_file(&content) {
            Ok(tree) => tree,
            Err(e) => {
                eprintln!("WARNING: Could not parse model.rs with syn: {}", e);
                return;
            }
        };

        // Helper to extract fields for a given struct name from the AST using a flat iterator chain
        let extract_fields = |struct_name: &str| -> HashSet<String> {
            syntax_tree
                .items
                .iter()
                .filter_map(|item| match item {
                    Item::Struct(s) if s.ident == struct_name => Some(s),
                    _ => None,
                })
                .flat_map(|s| match &s.fields {
                    Fields::Named(named) => named.named.iter().collect::<Vec<_>>(),
                    _ => Vec::new(),
                })
                .filter_map(|field| field.ident.as_ref())
                .map(|ident| ident.to_string())
                .filter(|name| !name.starts_with('_'))
                .collect()
        };

        let query_response_fields = extract_fields("QueryResponse");
        let job_fields = extract_fields("Job");
        let get_query_results_fields = extract_fields("GetQueryResultsResponse");

        // Dynamically derive known fields implemented in QueryCreationMetadata and QueryMetadata
        // directly from the AST of metadata.rs using a flat functional pipeline
        let metadata_path = PathBuf::from(manifest_dir).join("src/query/metadata.rs");
        let metadata_content = match std::fs::read_to_string(&metadata_path) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("WARNING: Could not read metadata.rs for sync check: {}", e);
                return;
            }
        };

        let metadata_tree = match syn::parse_file(&metadata_content) {
            Ok(tree) => tree,
            Err(e) => {
                eprintln!("WARNING: Could not parse metadata.rs with syn: {}", e);
                return;
            }
        };

        let mut creation_known_query_response = HashSet::new();
        let mut creation_known_job = HashSet::new();
        let mut meta_known_query_response = HashSet::new();
        let mut meta_known_get_query_results = HashSet::new();

        let reflection_pipeline = metadata_tree
            .items
            .iter()
            .filter_map(|item| match item {
                Item::Impl(item_impl) => Some(item_impl),
                _ => None,
            })
            .filter_map(|item_impl| match &*item_impl.self_ty {
                syn::Type::Path(p) if p.path.is_ident("QueryCreationMetadata") => {
                    Some(("QueryCreationMetadata", &item_impl.items))
                }
                syn::Type::Path(p) if p.path.is_ident("QueryMetadata") => {
                    Some(("QueryMetadata", &item_impl.items))
                }
                _ => None,
            })
            .flat_map(|(impl_name, items)| {
                items.iter().filter_map(move |impl_item| match impl_item {
                    syn::ImplItem::Fn(impl_fn) => Some((impl_name, impl_fn)),
                    _ => None,
                })
            })
            .flat_map(|(impl_name, impl_fn)| {
                let method_name = impl_fn.sig.ident.to_string();
                impl_fn
                    .block
                    .stmts
                    .iter()
                    .filter_map(move |stmt| match stmt {
                        syn::Stmt::Expr(syn::Expr::Match(expr_match), _) => {
                            Some((impl_name, method_name.clone(), expr_match))
                        }
                        _ => None,
                    })
            })
            .flat_map(|(impl_name, method_name, expr_match)| {
                expr_match
                    .arms
                    .iter()
                    .map(move |arm| (impl_name, method_name.clone(), arm))
            });

        for (impl_name, method_name, arm) in reflection_pipeline {
            // If the arm body returns None, this field does not exist in this variant
            if let syn::Expr::Path(expr_path) = &*arm.body {
                if expr_path.path.is_ident("None") {
                    continue;
                }
            }

            let syn::Pat::TupleStruct(pat_ts) = &arm.pat else {
                continue;
            };
            let Some(last_seg) = pat_ts.path.segments.last() else {
                continue;
            };
            let variant_name = last_seg.ident.to_string();

            if impl_name == "QueryCreationMetadata" {
                if variant_name == "JobsQuery" {
                    creation_known_query_response.insert(method_name);
                } else if variant_name == "JobsInsert" {
                    creation_known_job.insert(method_name);
                }
            } else if impl_name == "QueryMetadata" {
                if variant_name == "JobsQuery" {
                    meta_known_query_response.insert(method_name);
                } else if variant_name == "GetQueryResultsResponse" {
                    meta_known_get_query_results.insert(method_name);
                }
            }
        }

        for field in query_response_fields.difference(&creation_known_query_response) {
            eprintln!(
                "WARNING: Upstream model struct 'QueryResponse' has field '{}' not exposed in QueryCreationMetadata",
                field
            );
        }

        for field in job_fields.difference(&creation_known_job) {
            eprintln!(
                "WARNING: Upstream model struct 'Job' has field '{}' not exposed in QueryCreationMetadata",
                field
            );
        }

        for field in query_response_fields.difference(&meta_known_query_response) {
            eprintln!(
                "WARNING: Upstream model struct 'QueryResponse' has field '{}' not exposed in QueryMetadata",
                field
            );
        }

        for field in get_query_results_fields.difference(&meta_known_get_query_results) {
            eprintln!(
                "WARNING: Upstream model struct 'GetQueryResultsResponse' has field '{}' not exposed in QueryMetadata",
                field
            );
        }
    }
}
