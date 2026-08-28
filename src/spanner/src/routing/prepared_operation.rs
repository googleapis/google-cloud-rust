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

//! Prepared request descriptors and shape fingerprinting for Spanner location-aware routing.
//!
//! Provides deterministic 64-bit FNV-1a fingerprinting ([`fingerprint_execute_sql_request`],
//! [`fingerprint_read_request`], [`fingerprint_proto_read_request`]) and prepared request descriptors
//! ([`PreparedQuery`], [`PreparedRead`]) to identify structurally identical query and read RPCs.
//!
//! Requests sharing the same shape (SQL text, parameter names/types, table/index/columns, and query options)
//! produce identical fingerprints, allowing client routing layers to reuse operation UIDs, avoid repetitive
//! key recipe compilation, and correctly validate hash collision boundaries via [`PreparedQuery::matches`]
//! and [`PreparedRead::matches`].

// TODO(#6236): Remove dead_code allowance once request routing interceptors utilize prepared operations in subsequent PRs.
#![allow(dead_code)]

use crate::model::execute_sql_request::QueryOptions;
use crate::model::{ExecuteSqlRequest, ReadRequest as ProtoReadRequest, Type};
use crate::read::ReadRequest;
use serde_json::Value as JsonValue;

/// Cached query parameter descriptor pairing name with declared type or value kind.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct PreparedQueryParam {
    pub(crate) name: String,
    pub(crate) type_ref: Option<Type>,
    pub(crate) kind: u32,
}

/// Cached prepared query descriptor representing a specific SQL query structure.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct PreparedQuery {
    pub(crate) sql: String,
    pub(crate) params: Vec<PreparedQueryParam>,
    pub(crate) query_options: Option<QueryOptions>,
    pub(crate) operation_uid: u64,
}

impl PreparedQuery {
    /// Creates a new `PreparedQuery` from an [`ExecuteSqlRequest`] and assigned `operation_uid`.
    pub(crate) fn new(request: &ExecuteSqlRequest, operation_uid: u64) -> Self {
        let params = match &request.params {
            Some(request_params) => {
                let mut params = Vec::with_capacity(request_params.len());
                for (name, value) in request_params {
                    let (type_ref, kind) = match request.param_types.get(name) {
                        Some(param_type) => (Some(param_type.clone()), 0),
                        None => (None, json_value_kind(value)),
                    };
                    params.push(PreparedQueryParam {
                        name: name.clone(),
                        type_ref,
                        kind,
                    });
                }
                if params.len() > 1 {
                    params.sort_unstable_by(|left, right| left.name.cmp(&right.name));
                }
                params
            }
            None => Vec::new(),
        };

        Self {
            sql: request.sql.clone(),
            params,
            query_options: normalize_query_options(request.query_options.as_ref()).cloned(),
            operation_uid,
        }
    }

    /// Returns `true` if the [`ExecuteSqlRequest`] matches the prepared query shape.
    pub(crate) fn matches(&self, request: &ExecuteSqlRequest) -> bool {
        // 1. Verify SQL text matches exactly.
        if self.sql != request.sql {
            return false;
        }

        // 2. Verify query optimizer options match (treating unset and default empty options equivalently).
        if self.query_options.as_ref() != normalize_query_options(request.query_options.as_ref()) {
            return false;
        }

        // 3. Verify parameter shape (count, names, declared protobuf types, and untyped value kinds).
        self.matches_params(request)
    }

    /// Returns `true` if the request's parameters match the prepared parameter shape.
    ///
    /// Note: Parameter values themselves are intentionally not compared. Two requests match if and only if
    /// they share identical parameter names, identical declared protobuf [`Type`]s for typed parameters, and
    /// identical JSON value kinds (e.g. Number vs String vs Bool vs Null) for untyped parameters.
    fn matches_params(&self, request: &ExecuteSqlRequest) -> bool {
        let Some(request_params) = &request.params else {
            return self.params.is_empty();
        };
        if self.params.len() != request_params.len() {
            return false;
        }

        for param in &self.params {
            let Some(value) = request_params.get(&param.name) else {
                return false;
            };
            if let Some(expected_type) = &param.type_ref {
                if request.param_types.get(&param.name) != Some(expected_type) {
                    return false;
                }
            } else if request.param_types.contains_key(&param.name)
                || param.kind != json_value_kind(value)
            {
                return false;
            }
        }

        true
    }
}

/// Returns `Some(&QueryOptions)` if non-empty options are present, or `None` if options are unset or empty.
fn normalize_query_options(options: Option<&QueryOptions>) -> Option<&QueryOptions> {
    options.filter(|options| {
        !options.optimizer_version.is_empty() || !options.optimizer_statistics_package.is_empty()
    })
}

/// Cached prepared read descriptor representing a specific table/index read shape.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct PreparedRead {
    pub(crate) table: String,
    pub(crate) index: Option<String>,
    pub(crate) columns: Vec<String>,
    pub(crate) operation_uid: u64,
}

impl PreparedRead {
    /// Creates a new `PreparedRead` from table, index, columns, and assigned `operation_uid`.
    pub(crate) fn new(
        table: impl Into<String>,
        index: Option<&str>,
        columns: &[String],
        operation_uid: u64,
    ) -> Self {
        Self {
            table: table.into(),
            index: index
                .filter(|index_name| !index_name.is_empty())
                .map(str::to_string),
            columns: columns.to_vec(),
            operation_uid,
        }
    }

    /// Creates a new `PreparedRead` from a [`ReadRequest`] and assigned `operation_uid`.
    pub(crate) fn from_read_request(request: &ReadRequest, operation_uid: u64) -> Self {
        Self::new(
            &request.table,
            request.index.as_deref(),
            &request.columns,
            operation_uid,
        )
    }

    /// Creates a new `PreparedRead` from a protobuf [`ProtoReadRequest`] and assigned `operation_uid`.
    pub(crate) fn from_proto_read_request(request: &ProtoReadRequest, operation_uid: u64) -> Self {
        Self::new(
            &request.table,
            Some(&request.index),
            &request.columns,
            operation_uid,
        )
    }

    /// Returns `true` if the read parameters match the prepared read descriptor.
    pub(crate) fn matches(&self, table: &str, index: Option<&str>, columns: &[String]) -> bool {
        let normalized_index = index.filter(|index_name| !index_name.is_empty());
        self.table == table && self.index.as_deref() == normalized_index && self.columns == columns
    }

    /// Returns `true` if the [`ReadRequest`] matches the prepared read descriptor.
    pub(crate) fn matches_read_request(&self, request: &ReadRequest) -> bool {
        self.matches(&request.table, request.index.as_deref(), &request.columns)
    }

    /// Returns `true` if the protobuf [`ProtoReadRequest`] matches the prepared read descriptor.
    pub(crate) fn matches_proto_read_request(&self, request: &ProtoReadRequest) -> bool {
        self.matches(&request.table, Some(&request.index), &request.columns)
    }
}

/// Computes a deterministic 64-bit FNV-1a fingerprint for an [`ExecuteSqlRequest`].
///
/// The fingerprint hashes the structural request shape:
/// - SQL text.
/// - Alphabetically sorted parameter names and their declared protobuf [`Type`] or untyped JSON value kind.
/// - Query options (`optimizer_version` and `optimizer_statistics_package`).
///
/// Requests with identical shape (same SQL, parameter names, and parameter types) yield the same fingerprint,
/// enabling efficient routing key extraction across varying parameter values.
pub(crate) fn fingerprint_execute_sql_request(request: &ExecuteSqlRequest) -> u64 {
    let mut hasher = FnvHasher::new();
    hasher.write_str(&request.sql);

    // Collect and hash parameters in deterministic alphabetical order.
    if let Some(params) = &request.params {
        let mut hash_param = |name: &str, value: &JsonValue| {
            hasher.write_str(name);
            if let Some(param_type) = request.param_types.get(name) {
                hash_type(&mut hasher, param_type);
            } else {
                hasher.write_u64(json_value_kind(value) as u64);
            }
        };

        const MAX_STACK_PARAMS: usize = 4;
        let param_count = params.len();
        if param_count == 1 {
            if let Some((name, value)) = params.iter().next() {
                hash_param(name.as_str(), value);
            }
        } else if param_count > 1 && param_count <= MAX_STACK_PARAMS {
            let mut stack_entries: [(&str, &JsonValue); MAX_STACK_PARAMS] =
                [("", &JsonValue::Null); MAX_STACK_PARAMS];
            for (index, (name, value)) in params.iter().enumerate() {
                stack_entries[index] = (name.as_str(), value);
            }
            let entries_slice = &mut stack_entries[..param_count];
            entries_slice.sort_unstable_by_key(|(name, _)| *name);
            for &(name, value) in entries_slice.iter() {
                hash_param(name, value);
            }
        } else if param_count > MAX_STACK_PARAMS {
            let mut parameter_entries: Vec<(&str, &JsonValue)> = params
                .iter()
                .map(|(name, value)| (name.as_str(), value))
                .collect();
            parameter_entries.sort_unstable_by_key(|(name, _)| *name);
            for (name, value) in parameter_entries {
                hash_param(name, value);
            }
        }
    }

    let (optimizer_version, optimizer_statistics_package) = match &request.query_options {
        Some(options) => (
            options.optimizer_version.as_str(),
            options.optimizer_statistics_package.as_str(),
        ),
        None => ("", ""),
    };
    hasher.write_str(optimizer_version);
    hasher.write_str(optimizer_statistics_package);

    hasher.finish()
}

/// Computes a deterministic 64-bit FNV-1a fingerprint for read request parameters.
pub(crate) fn fingerprint_read_shape(table: &str, index: &str, columns: &[String]) -> u64 {
    let mut hasher = FnvHasher::new();
    hasher.write_str(table);
    hasher.write_str(index);
    hasher.write_u64(columns.len() as u64);
    for column in columns {
        hasher.write_str(column);
    }
    hasher.finish()
}

/// Computes a deterministic 64-bit FNV-1a fingerprint for a [`ReadRequest`].
pub(crate) fn fingerprint_read_request(request: &ReadRequest) -> u64 {
    fingerprint_read_shape(
        &request.table,
        request.index.as_deref().unwrap_or(""),
        &request.columns,
    )
}

/// Computes a deterministic 64-bit FNV-1a fingerprint for a protobuf [`ProtoReadRequest`].
pub(crate) fn fingerprint_proto_read_request(request: &ProtoReadRequest) -> u64 {
    fingerprint_read_shape(&request.table, &request.index, &request.columns)
}

/// Returns the numerical discriminator for a [`JsonValue`] kind to distinguish JSON value variants.
pub(crate) fn json_value_kind(value: &JsonValue) -> u32 {
    match value {
        JsonValue::Null => 1,
        JsonValue::Number(_) => 2,
        JsonValue::String(_) => 3,
        JsonValue::Bool(_) => 4,
        JsonValue::Object(_) => 5,
        JsonValue::Array(_) => 6,
    }
}

/// Recursively hashes a Spanner protobuf [`Type`] into the provided [`FnvHasher`].
fn hash_type(hasher: &mut FnvHasher, param_type: &Type) {
    hasher.write_u64(param_type.code.value().unwrap_or(0) as u64);
    hasher.write_u64(param_type.type_annotation.value().unwrap_or(0) as u64);
    hasher.write_str(&param_type.proto_type_fqn);

    hash_optional_type(hasher, param_type.array_element_type.as_deref());

    if let Some(struct_type) = &param_type.struct_type {
        hasher.write_u64(1);
        hasher.write_u64(struct_type.fields.len() as u64);
        for field in &struct_type.fields {
            hasher.write_str(&field.name);
            hash_optional_type(hasher, field.r#type.as_deref());
        }
    } else {
        hasher.write_u64(0);
    }
}

/// Hashes an optional protobuf [`Type`], writing `1` and the nested type if present, or `0` if absent.
fn hash_optional_type(hasher: &mut FnvHasher, optional_type: Option<&Type>) {
    match optional_type {
        Some(inner_type) => {
            hasher.write_u64(1);
            hash_type(hasher, inner_type);
        }
        None => hasher.write_u64(0),
    }
}

/// 64-bit FNV-1a non-cryptographic deterministic hasher.
#[derive(Clone, Debug)]
pub(crate) struct FnvHasher {
    state: u64,
}

impl Default for FnvHasher {
    fn default() -> Self {
        Self::new()
    }
}

impl FnvHasher {
    const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    pub(crate) fn new() -> Self {
        Self {
            state: Self::FNV_OFFSET_BASIS,
        }
    }

    pub(crate) fn write(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.state ^= byte as u64;
            self.state = self.state.wrapping_mul(Self::FNV_PRIME);
        }
    }

    pub(crate) fn write_u64(&mut self, value: u64) {
        let bytes = value.to_le_bytes();
        self.write(&bytes);
    }

    pub(crate) fn write_bytes(&mut self, bytes: &[u8]) {
        self.write_u64(bytes.len() as u64);
        if !bytes.is_empty() {
            self.write(bytes);
        }
    }

    pub(crate) fn write_str(&mut self, string: &str) {
        self.write_bytes(string.as_bytes());
    }

    pub(crate) fn finish(&self) -> u64 {
        self.state
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key::KeySet;
    use crate::model::execute_sql_request::QueryOptions;
    use crate::model::struct_type::Field;
    use crate::model::{StructType, TypeAnnotationCode, TypeCode};
    use std::collections::HashMap;
    use std::fmt::Debug;
    use wkt::Struct as WktStruct;

    #[test]
    fn traits() {
        static_assertions::assert_impl_all!(PreparedQuery: Send, Sync, Debug, Clone, PartialEq);
        static_assertions::assert_impl_all!(
            PreparedQueryParam: Send,
            Sync,
            Debug,
            Clone,
            PartialEq
        );
        static_assertions::assert_impl_all!(PreparedRead: Send, Sync, Debug, Clone, PartialEq);
        static_assertions::assert_impl_all!(FnvHasher: Send, Sync, Debug, Clone, Default);
    }

    fn make_test_sql_request(
        sql: &str,
        params: Option<HashMap<&str, JsonValue>>,
        param_types: Option<HashMap<&str, Type>>,
        query_options: Option<QueryOptions>,
    ) -> ExecuteSqlRequest {
        let mut request = ExecuteSqlRequest::default().set_sql(sql);
        if let Some(param_map) = params {
            let mut struct_map = WktStruct::new();
            for (key, value) in param_map {
                struct_map.insert(key.to_string(), value);
            }
            request = request.set_params(struct_map);
        }
        if let Some(types) = param_types {
            request = request.set_param_types(
                types
                    .into_iter()
                    .map(|(name, param_type)| (name.to_string(), param_type))
                    .collect::<HashMap<_, _>>(),
            );
        }
        if let Some(options) = query_options {
            request.query_options = Some(options);
        }
        request
    }

    #[test]
    fn fnv_hasher_operations() {
        let mut hasher_default = FnvHasher::default();
        let hasher_new = FnvHasher::new();
        assert_eq!(
            hasher_default.finish(),
            hasher_new.finish(),
            "default() must initialize to offset basis"
        );

        hasher_default.write_str("test_string");
        hasher_default.write_u64(42);
        hasher_default.write_bytes(&[1, 2, 3, 4]);
        assert_ne!(
            hasher_default.finish(),
            hasher_new.finish(),
            "written values must mutate hasher state"
        );
    }

    #[test]
    fn json_value_kind_all_variants() {
        assert_eq!(json_value_kind(&JsonValue::Null), 1);
        assert_eq!(json_value_kind(&serde_json::json!(42)), 2);
        assert_eq!(json_value_kind(&JsonValue::String("str".to_string())), 3);
        assert_eq!(json_value_kind(&JsonValue::Bool(true)), 4);
        assert_eq!(
            json_value_kind(&JsonValue::Object(serde_json::Map::new())),
            5
        );
        assert_eq!(json_value_kind(&JsonValue::Array(Vec::new())), 6);
    }

    #[test]
    fn hash_type_nested_array_and_struct() {
        let mut hasher1 = FnvHasher::new();
        let array_type = Type::default()
            .set_code(TypeCode::Array)
            .set_array_element_type(Type::default().set_code(TypeCode::String));
        hash_type(&mut hasher1, &array_type);

        let mut hasher2 = FnvHasher::new();
        let different_array_type = Type::default()
            .set_code(TypeCode::Array)
            .set_array_element_type(Type::default().set_code(TypeCode::Int64));
        hash_type(&mut hasher2, &different_array_type);
        assert_ne!(
            hasher1.finish(),
            hasher2.finish(),
            "different array element types must produce different hashes"
        );

        let mut struct_hasher = FnvHasher::new();
        let struct_type = Type::default().set_code(TypeCode::Struct).set_struct_type(
            StructType::default().set_fields(vec![
                Field::default()
                    .set_name("id")
                    .set_type(Type::default().set_code(TypeCode::Int64)),
                Field::default()
                    .set_name("name")
                    .set_type(Type::default().set_code(TypeCode::String)),
            ]),
        );
        hash_type(&mut struct_hasher, &struct_type);
        assert_ne!(
            struct_hasher.finish(),
            hasher1.finish(),
            "struct type must produce distinct hash"
        );

        let mut annotated_hasher = FnvHasher::new();
        let annotated_type = Type::default()
            .set_code(TypeCode::Numeric)
            .set_type_annotation(TypeAnnotationCode::PgNumeric)
            .set_proto_type_fqn("google.protobuf.Value");
        hash_type(&mut annotated_hasher, &annotated_type);
        assert_ne!(
            annotated_hasher.finish(),
            hasher1.finish(),
            "type annotation and proto type FQN must be factored into hash"
        );
    }

    #[test]
    fn fingerprint_execute_sql_request_uses_request_shape() {
        let mut params1 = HashMap::new();
        params1.insert("p1", JsonValue::String("foo".to_string()));
        params1.insert("p2", serde_json::json!(1));
        let request1 = make_test_sql_request(
            "SELECT * FROM T WHERE p1=@p1 AND p2=@p2",
            Some(params1),
            None,
            None,
        );

        let mut params_same_shape = HashMap::new();
        params_same_shape.insert("p2", serde_json::json!(2));
        params_same_shape.insert("p1", JsonValue::String("bar".to_string()));
        let request_same_shape = make_test_sql_request(
            "SELECT * FROM T WHERE p1=@p1 AND p2=@p2",
            Some(params_same_shape),
            None,
            Some(QueryOptions::default()),
        );

        assert_eq!(
            fingerprint_execute_sql_request(&request1),
            fingerprint_execute_sql_request(&request_same_shape),
            "requests with same SQL and parameter shape must produce identical fingerprints"
        );

        let mut params_kind_change = HashMap::new();
        params_kind_change.insert("p1", JsonValue::String("foo".to_string()));
        params_kind_change.insert("p2", JsonValue::String("2".to_string()));
        let request_kind_change = make_test_sql_request(
            "SELECT * FROM T WHERE p1=@p1 AND p2=@p2",
            Some(params_kind_change),
            None,
            None,
        );

        assert_ne!(
            fingerprint_execute_sql_request(&request1),
            fingerprint_execute_sql_request(&request_kind_change),
            "changing untyped parameter kind from Number to String must change fingerprint"
        );

        let mut params_typed1 = HashMap::new();
        params_typed1.insert("p1", JsonValue::String("1".to_string()));
        let mut types = HashMap::new();
        types.insert("p1", Type::default().set_code(TypeCode::Int64));
        let request_typed1 = make_test_sql_request(
            "SELECT * FROM T WHERE p1=@p1",
            Some(params_typed1),
            Some(types.clone()),
            None,
        );

        let mut params_typed2 = HashMap::new();
        params_typed2.insert("p1", JsonValue::Bool(true));
        let request_typed2 = make_test_sql_request(
            "SELECT * FROM T WHERE p1=@p1",
            Some(params_typed2),
            Some(types),
            None,
        );

        assert_eq!(
            fingerprint_execute_sql_request(&request_typed1),
            fingerprint_execute_sql_request(&request_typed2),
            "declared protobuf Type must take precedence over parameter JSON value kind"
        );
    }

    #[test]
    fn fingerprint_execute_sql_request_differentiates_query_options() {
        let request_no_opts = make_test_sql_request("SELECT * FROM T", None, None, None);
        let request_with_opts = make_test_sql_request(
            "SELECT * FROM T",
            None,
            None,
            Some(QueryOptions::default().set_optimizer_version("1")),
        );

        assert_ne!(
            fingerprint_execute_sql_request(&request_no_opts),
            fingerprint_execute_sql_request(&request_with_opts),
            "differing query optimizer versions must produce different fingerprints"
        );
    }

    #[test]
    fn fingerprint_execute_sql_request_parameter_count_tiers() {
        let request_0_params = make_test_sql_request("SELECT 1", None, None, None);
        assert_eq!(
            fingerprint_execute_sql_request(&request_0_params),
            fingerprint_execute_sql_request(&request_0_params),
            "0 parameters should produce deterministic fingerprints"
        );

        let mut params_1 = HashMap::new();
        params_1.insert("param_a", serde_json::json!(10));
        let request_1_param = make_test_sql_request("SELECT 1", Some(params_1), None, None);
        assert_eq!(
            fingerprint_execute_sql_request(&request_1_param),
            fingerprint_execute_sql_request(&request_1_param),
            "1 parameter fast path should produce deterministic fingerprints"
        );

        let mut params_3_a = HashMap::new();
        params_3_a.insert("param_c", serde_json::json!("val_c"));
        params_3_a.insert("param_a", serde_json::json!("val_a"));
        params_3_a.insert("param_b", serde_json::json!("val_b"));
        let request_3_params_a = make_test_sql_request("SELECT 1", Some(params_3_a), None, None);

        let mut params_3_b = HashMap::new();
        params_3_b.insert("param_a", serde_json::json!("val_a2"));
        params_3_b.insert("param_b", serde_json::json!("val_b2"));
        params_3_b.insert("param_c", serde_json::json!("val_c2"));
        let request_3_params_b = make_test_sql_request("SELECT 1", Some(params_3_b), None, None);
        assert_eq!(
            fingerprint_execute_sql_request(&request_3_params_a),
            fingerprint_execute_sql_request(&request_3_params_b),
            "small stack params (2-4) must produce identical fingerprints regardless of map order"
        );

        let mut params_6_a = HashMap::new();
        for name in [
            "param_f", "param_e", "param_d", "param_c", "param_b", "param_a",
        ] {
            params_6_a.insert(name, serde_json::json!(1));
        }
        let request_6_params_a = make_test_sql_request("SELECT 1", Some(params_6_a), None, None);

        let mut params_6_b = HashMap::new();
        for name in [
            "param_a", "param_b", "param_c", "param_d", "param_e", "param_f",
        ] {
            params_6_b.insert(name, serde_json::json!(2));
        }
        let request_6_params_b = make_test_sql_request("SELECT 1", Some(params_6_b), None, None);
        assert_eq!(
            fingerprint_execute_sql_request(&request_6_params_a),
            fingerprint_execute_sql_request(&request_6_params_b),
            "large params (>4) must produce identical fingerprints regardless of map order"
        );
    }

    #[test]
    fn fingerprint_execute_sql_request_null_param_kind() {
        let mut null_params1 = HashMap::new();
        null_params1.insert("p1", JsonValue::Null);
        let request_null1 = make_test_sql_request(
            "SELECT * FROM T WHERE p1=@p1",
            Some(null_params1),
            None,
            None,
        );

        let mut null_params2 = HashMap::new();
        null_params2.insert("p1", JsonValue::Null);
        let request_null2 = make_test_sql_request(
            "SELECT * FROM T WHERE p1=@p1",
            Some(null_params2),
            None,
            None,
        );

        assert_eq!(
            fingerprint_execute_sql_request(&request_null1),
            fingerprint_execute_sql_request(&request_null2),
            "requests with identical null parameters must produce same fingerprint"
        );

        let mut string_params = HashMap::new();
        string_params.insert("p1", JsonValue::String("null".to_string()));
        let request_string = make_test_sql_request(
            "SELECT * FROM T WHERE p1=@p1",
            Some(string_params),
            None,
            None,
        );

        assert_ne!(
            fingerprint_execute_sql_request(&request_null1),
            fingerprint_execute_sql_request(&request_string),
            "null parameter kind must differ from string parameter kind"
        );
    }

    #[test]
    fn fingerprint_read_request_uses_request_shape() {
        let request = ReadRequest::builder("Users", vec!["id", "name"])
            .with_index("UsersByEmail", KeySet::all())
            .build();

        let fingerprint = fingerprint_read_request(&request);
        assert_ne!(fingerprint, 0, "fingerprint must be non-zero");
        assert_eq!(
            fingerprint,
            fingerprint_read_request(&request),
            "fingerprint must be deterministic"
        );

        let request_different_table = ReadRequest::builder("Accounts", vec!["id", "name"])
            .with_index("UsersByEmail", KeySet::all())
            .build();
        assert_ne!(
            fingerprint,
            fingerprint_read_request(&request_different_table),
            "different table must produce different fingerprint"
        );

        let request_different_index = ReadRequest::builder("Users", vec!["id", "name"])
            .with_index("OtherIndex", KeySet::all())
            .build();
        assert_ne!(
            fingerprint,
            fingerprint_read_request(&request_different_index),
            "different index must produce different fingerprint"
        );

        let request_different_columns = ReadRequest::builder("Users", vec!["id"])
            .with_index("UsersByEmail", KeySet::all())
            .build();
        assert_ne!(
            fingerprint,
            fingerprint_read_request(&request_different_columns),
            "different column list must produce different fingerprint"
        );
    }

    #[test]
    fn fingerprint_proto_read_request_uses_request_shape() {
        let request = ProtoReadRequest::default()
            .set_table("Users")
            .set_index("UsersByEmail")
            .set_columns(vec!["id".to_string(), "name".to_string()]);

        let fingerprint = fingerprint_proto_read_request(&request);
        assert_ne!(fingerprint, 0, "proto fingerprint must be non-zero");
        assert_eq!(
            fingerprint,
            fingerprint_proto_read_request(&request),
            "proto fingerprint must be deterministic"
        );

        let request_diff_table = ProtoReadRequest::default()
            .set_table("Accounts")
            .set_index("UsersByEmail")
            .set_columns(vec!["id".to_string(), "name".to_string()]);
        assert_ne!(
            fingerprint,
            fingerprint_proto_read_request(&request_diff_table),
            "different table in proto request must produce different fingerprint"
        );

        let request_diff_index = ProtoReadRequest::default()
            .set_table("Users")
            .set_index("OtherIndex")
            .set_columns(vec!["id".to_string(), "name".to_string()]);
        assert_ne!(
            fingerprint,
            fingerprint_proto_read_request(&request_diff_index),
            "different index in proto request must produce different fingerprint"
        );

        let request_diff_columns = ProtoReadRequest::default()
            .set_table("Users")
            .set_index("UsersByEmail")
            .set_columns(vec!["id".to_string()]);
        assert_ne!(
            fingerprint,
            fingerprint_proto_read_request(&request_diff_columns),
            "different columns in proto request must produce different fingerprint"
        );
    }

    #[test]
    fn prepared_query_matches_on_kinds_and_types() {
        let mut untyped_params = HashMap::new();
        untyped_params.insert("p1", JsonValue::String("foo".to_string()));
        let untyped_request = make_test_sql_request(
            "SELECT * FROM T WHERE p1=@p1",
            Some(untyped_params),
            None,
            None,
        );

        let prepared_untyped = PreparedQuery::new(&untyped_request, 100);

        let mut same_kind_params = HashMap::new();
        same_kind_params.insert("p1", JsonValue::String("bar".to_string()));
        let same_kind_request = make_test_sql_request(
            "SELECT * FROM T WHERE p1=@p1",
            Some(same_kind_params),
            None,
            None,
        );
        assert!(
            prepared_untyped.matches(&same_kind_request),
            "untyped query with same parameter kind must match"
        );

        let mut diff_kind_params = HashMap::new();
        diff_kind_params.insert("p1", JsonValue::Bool(true));
        let diff_kind_request = make_test_sql_request(
            "SELECT * FROM T WHERE p1=@p1",
            Some(diff_kind_params),
            None,
            None,
        );
        assert!(
            !prepared_untyped.matches(&diff_kind_request),
            "untyped query with different parameter kind must mismatch"
        );

        let mut typed_params = HashMap::new();
        typed_params.insert("p1", JsonValue::String("1".to_string()));
        let mut param_types = HashMap::new();
        param_types.insert("p1", Type::default().set_code(TypeCode::Int64));
        let typed_request = make_test_sql_request(
            "SELECT * FROM T WHERE p1=@p1",
            Some(typed_params),
            Some(param_types.clone()),
            None,
        );

        let prepared_typed = PreparedQuery::new(&typed_request, 200);

        let mut match_typed_params = HashMap::new();
        match_typed_params.insert("p1", JsonValue::Bool(true));
        let match_typed_request = make_test_sql_request(
            "SELECT * FROM T WHERE p1=@p1",
            Some(match_typed_params),
            Some(param_types),
            None,
        );
        assert!(
            prepared_typed.matches(&match_typed_request),
            "typed query with same declared Type must match regardless of value kind"
        );

        let mut mismatch_type_params = HashMap::new();
        mismatch_type_params.insert("p1", JsonValue::String("1".to_string()));
        let mut mismatch_types = HashMap::new();
        mismatch_types.insert("p1", Type::default().set_code(TypeCode::String));
        let mismatch_type_request = make_test_sql_request(
            "SELECT * FROM T WHERE p1=@p1",
            Some(mismatch_type_params),
            Some(mismatch_types),
            None,
        );
        assert!(
            !prepared_typed.matches(&mismatch_type_request),
            "typed query must mismatch when declared Type differs"
        );

        let mut no_type_decl_params = HashMap::new();
        no_type_decl_params.insert("p1", JsonValue::String("1".to_string()));
        let no_type_decl_request = make_test_sql_request(
            "SELECT * FROM T WHERE p1=@p1",
            Some(no_type_decl_params),
            None,
            None,
        );
        assert!(
            !prepared_typed.matches(&no_type_decl_request),
            "typed query must mismatch when declared Type is removed"
        );
    }

    #[test]
    fn prepared_query_matches_negative_cases() {
        let request = make_test_sql_request("SELECT * FROM T WHERE id=@id", None, None, None);
        let prepared = PreparedQuery::new(&request, 100);

        let diff_sql_request =
            make_test_sql_request("SELECT * FROM OtherTable WHERE id=@id", None, None, None);
        assert!(
            !prepared.matches(&diff_sql_request),
            "different SQL statement must not match"
        );

        let mut extra_params = HashMap::new();
        extra_params.insert("id", JsonValue::Number(serde_json::Number::from(1)));
        let extra_param_request = make_test_sql_request(
            "SELECT * FROM T WHERE id=@id",
            Some(extra_params),
            None,
            None,
        );
        assert!(
            !prepared.matches(&extra_param_request),
            "differing parameter count must not match"
        );

        let mut base_params = HashMap::new();
        base_params.insert("p1", JsonValue::String("val".to_string()));
        let base_request = make_test_sql_request("SELECT 1", Some(base_params), None, None);
        let prepared_with_param = PreparedQuery::new(&base_request, 101);

        let mut diff_param_name = HashMap::new();
        diff_param_name.insert("p2", JsonValue::String("val".to_string()));
        let diff_param_request =
            make_test_sql_request("SELECT 1", Some(diff_param_name), None, None);
        assert!(
            !prepared_with_param.matches(&diff_param_request),
            "different parameter name must not match"
        );

        let empty_params_request = make_test_sql_request("SELECT 1", None, None, None);
        let prepared_empty = PreparedQuery::new(&empty_params_request, 102);
        assert!(
            prepared_empty.matches(&empty_params_request),
            "empty params request must match prepared query with empty params"
        );
    }

    #[test]
    fn prepared_query_matches_with_null_param_kind() {
        let mut null_params = HashMap::new();
        null_params.insert("p1", JsonValue::Null);
        let request_null = make_test_sql_request(
            "SELECT * FROM T WHERE p1=@p1",
            Some(null_params),
            None,
            None,
        );

        let prepared = PreparedQuery::new(&request_null, 100);

        let mut match_null = HashMap::new();
        match_null.insert("p1", JsonValue::Null);
        let request_match =
            make_test_sql_request("SELECT * FROM T WHERE p1=@p1", Some(match_null), None, None);
        assert!(
            prepared.matches(&request_match),
            "query with matching null parameter must match"
        );

        let mut non_null = HashMap::new();
        non_null.insert("p1", JsonValue::Bool(false));
        let request_non_null =
            make_test_sql_request("SELECT * FROM T WHERE p1=@p1", Some(non_null), None, None);
        assert!(
            !prepared.matches(&request_non_null),
            "query with non-null parameter must mismatch when prepared with null"
        );
    }

    #[test]
    fn prepared_query_matches_with_query_options() {
        let request_with_opts = make_test_sql_request(
            "SELECT 1",
            None,
            None,
            Some(
                QueryOptions::default()
                    .set_optimizer_version("1")
                    .set_optimizer_statistics_package("custom_pkg"),
            ),
        );

        let prepared = PreparedQuery::new(&request_with_opts, 100);

        let request_matching_opts = make_test_sql_request(
            "SELECT 1",
            None,
            None,
            Some(
                QueryOptions::default()
                    .set_optimizer_version("1")
                    .set_optimizer_statistics_package("custom_pkg"),
            ),
        );
        assert!(
            prepared.matches(&request_matching_opts),
            "prepared query with matching query options must match"
        );

        let request_different_pkg = make_test_sql_request(
            "SELECT 1",
            None,
            None,
            Some(
                QueryOptions::default()
                    .set_optimizer_version("1")
                    .set_optimizer_statistics_package("other_pkg"),
            ),
        );
        assert!(
            !prepared.matches(&request_different_pkg),
            "prepared query with different statistics package must mismatch"
        );

        let request_different_version = make_test_sql_request(
            "SELECT 1",
            None,
            None,
            Some(
                QueryOptions::default()
                    .set_optimizer_version("2")
                    .set_optimizer_statistics_package("custom_pkg"),
            ),
        );
        assert!(
            !prepared.matches(&request_different_version),
            "prepared query with different optimizer version must mismatch"
        );

        let request_no_opts = make_test_sql_request("SELECT 1", None, None, None);
        assert!(
            !prepared.matches(&request_no_opts),
            "prepared query with options must mismatch request without options"
        );

        let prepared_default_opts = PreparedQuery::new(
            &make_test_sql_request("SELECT 1", None, None, Some(QueryOptions::default())),
            101,
        );
        assert!(
            prepared_default_opts.matches(&request_no_opts),
            "default empty options must match None options"
        );
    }

    #[test]
    fn prepared_read_matches_table_index_and_columns() {
        let prepared = PreparedRead::new(
            "Users",
            Some("UsersByEmail"),
            &["id".to_string(), "name".to_string()],
            100,
        );

        assert!(
            prepared.matches(
                "Users",
                Some("UsersByEmail"),
                &["id".to_string(), "name".to_string()]
            ),
            "identical read parameters must match"
        );
        assert!(
            !prepared.matches(
                "OtherTable",
                Some("UsersByEmail"),
                &["id".to_string(), "name".to_string()]
            ),
            "different table must mismatch"
        );
        assert!(
            !prepared.matches(
                "Users",
                Some("OtherIndex"),
                &["id".to_string(), "name".to_string()]
            ),
            "different index must mismatch"
        );
        assert!(
            !prepared.matches("Users", None, &["id".to_string(), "name".to_string()]),
            "missing index when index expected must mismatch"
        );
        assert!(
            !prepared.matches("Users", Some("UsersByEmail"), &["id".to_string()]),
            "different columns count must mismatch"
        );
        assert!(
            !prepared.matches(
                "Users",
                Some("UsersByEmail"),
                &["name".to_string(), "id".to_string()]
            ),
            "different column order must mismatch"
        );

        let prepared_empty_index = PreparedRead::new("Users", Some(""), &["id".to_string()], 101);
        assert!(
            prepared_empty_index.matches("Users", None, &["id".to_string()]),
            "prepared read with empty string index must match request with None index"
        );
    }

    #[test]
    fn prepared_read_from_and_matches_read_request() {
        let request = ReadRequest::builder("Users", vec!["id", "name"])
            .with_index("UsersByEmail", KeySet::all())
            .build();
        let prepared = PreparedRead::from_read_request(&request, 100);

        assert!(
            prepared.matches_read_request(&request),
            "matching ReadRequest must match"
        );

        let request_mismatch = ReadRequest::builder("Users", vec!["id"])
            .with_index("UsersByEmail", KeySet::all())
            .build();
        assert!(
            !prepared.matches_read_request(&request_mismatch),
            "mismatching ReadRequest must not match"
        );
    }

    #[test]
    fn prepared_read_from_and_matches_proto_read_request() {
        let proto_request = ProtoReadRequest::default()
            .set_table("Users")
            .set_index("UsersByEmail")
            .set_columns(vec!["id".to_string(), "name".to_string()]);
        let prepared = PreparedRead::from_proto_read_request(&proto_request, 100);

        assert!(
            prepared.matches_proto_read_request(&proto_request),
            "matching ProtoReadRequest must match"
        );

        let proto_mismatch = ProtoReadRequest::default()
            .set_table("Users")
            .set_index("OtherIndex")
            .set_columns(vec!["id".to_string(), "name".to_string()]);
        assert!(
            !prepared.matches_proto_read_request(&proto_mismatch),
            "mismatching ProtoReadRequest must not match"
        );
    }

    #[test]
    fn fingerprint_read_shape_consistency() {
        let columns = vec!["id".to_string(), "name".to_string()];
        let read_request = ReadRequest::builder("Users", vec!["id", "name"])
            .with_index("UsersByEmail", KeySet::all())
            .build();
        let proto_request = ProtoReadRequest::default()
            .set_table("Users")
            .set_index("UsersByEmail")
            .set_columns(columns.clone());

        let shape_fingerprint = fingerprint_read_shape("Users", "UsersByEmail", &columns);
        assert_eq!(
            shape_fingerprint,
            fingerprint_read_request(&read_request),
            "fingerprint_read_shape must match fingerprint_read_request"
        );
        assert_eq!(
            shape_fingerprint,
            fingerprint_proto_read_request(&proto_request),
            "fingerprint_read_shape must match fingerprint_proto_read_request"
        );
    }
}
