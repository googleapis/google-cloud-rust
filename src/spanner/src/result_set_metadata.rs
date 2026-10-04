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

use crate::types::Type;
use std::collections::BTreeMap;
use std::sync::Arc;

/// Metadata about a [`ResultSet`](crate::result::ResultSet).
///
/// # Example
///
/// ```
/// # use google_cloud_spanner::client::Spanner;
/// # use google_cloud_spanner::statement::Statement;
/// # use google_cloud_spanner::value::TypeCode;
/// # async fn test_doc() -> Result<(), Box<dyn std::error::Error>> {
/// let client = Spanner::builder().build().await?;
/// let db = client.database_client("projects/p/instances/i/databases/d").build().await?;
/// let tx = db.single_use().build();
/// let mut rs = tx.execute_query(Statement::builder("SELECT 1 AS Number").build()).await?;
///
/// let metadata = rs.metadata().expect("metadata available");
///
/// for (name, spanner_type) in metadata.column_names().iter().zip(metadata.column_types().iter()) {
///     println!("Column: {} has type: {:?}", name, spanner_type.code());
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, PartialEq)]
pub struct ResultSetMetadata {
    pub(crate) column_names: Arc<Vec<String>>,
    pub(crate) column_types: Arc<Vec<Type>>,
    pub(crate) undeclared_parameters: Arc<BTreeMap<String, Type>>,
}

impl ResultSetMetadata {
    pub(crate) fn new(mut metadata: Option<crate::google::spanner::v1::ResultSetMetadata>) -> Self {
        let mut column_names = Vec::new();
        let mut column_types = Vec::new();
        let mut undeclared_parameters = BTreeMap::new();

        if let Some(m) = &mut metadata
            && let Some(undeclared) = m.undeclared_parameters.take()
        {
            for field in undeclared.fields {
                let param_type = field.r#type.map(Type::from_proto).unwrap_or_default();
                undeclared_parameters.insert(field.name, param_type);
            }
        }

        let fields = metadata
            .and_then(|m| m.row_type)
            .into_iter()
            .flat_map(|r| r.fields.into_iter());
        for field in fields {
            column_names.push(field.name);
            let column_type = field.r#type.map(Type::from_proto).unwrap_or_default();
            column_types.push(column_type);
        }

        Self {
            column_names: Arc::new(column_names),
            column_types: Arc::new(column_types),
            undeclared_parameters: Arc::new(undeclared_parameters),
        }
    }

    /// Returns the names of the columns in the result set.
    pub fn column_names(&self) -> &[String] {
        &self.column_names
    }

    /// Returns the types of the columns in the result set.
    pub fn column_types(&self) -> &[crate::types::Type] {
        &self.column_types
    }
    /// Returns the types of the undeclared parameters in the result set.
    pub fn undeclared_parameters(&self) -> &std::collections::BTreeMap<String, crate::types::Type> {
        &self.undeclared_parameters
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_traits() {
        static_assertions::assert_impl_all!(
            ResultSetMetadata: Clone,
            std::fmt::Debug,
            PartialEq,
            Send,
            Sync
        );
    }

    #[test]
    fn new_and_accessors() {
        use crate::google::spanner::v1 as spanner_v1;

        let proto = spanner_v1::ResultSetMetadata {
            row_type: Some(spanner_v1::StructType {
                fields: vec![
                    spanner_v1::struct_type::Field {
                        name: "col1".to_string(),
                        r#type: Some(spanner_v1::Type {
                            code: spanner_v1::TypeCode::String.into(),
                            ..Default::default()
                        }),
                    },
                    spanner_v1::struct_type::Field {
                        name: "col2".to_string(),
                        r#type: Some(spanner_v1::Type {
                            code: spanner_v1::TypeCode::Int64.into(),
                            ..Default::default()
                        }),
                    },
                ],
            }),
            undeclared_parameters: Some(spanner_v1::StructType {
                fields: vec![spanner_v1::struct_type::Field {
                    name: "p1".to_string(),
                    r#type: Some(spanner_v1::Type {
                        code: spanner_v1::TypeCode::Bool.into(),
                        ..Default::default()
                    }),
                }],
            }),
            ..Default::default()
        };

        let metadata = ResultSetMetadata::new(Some(proto));

        assert_eq!(
            metadata.column_names(),
            &["col1".to_string(), "col2".to_string()]
        );
        assert_eq!(metadata.column_types().len(), 2);
        assert_eq!(
            metadata.column_types()[0].code(),
            crate::types::TypeCode::String
        );
        assert_eq!(
            metadata.column_types()[1].code(),
            crate::types::TypeCode::Int64
        );
        assert_eq!(metadata.undeclared_parameters().len(), 1);
        assert_eq!(
            metadata
                .undeclared_parameters()
                .get("p1")
                .map(|parameter_type| parameter_type.code()),
            Some(crate::types::TypeCode::Bool)
        );
    }
}
