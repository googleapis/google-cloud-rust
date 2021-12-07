// Copyright 2022 Google LLC
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

//! This module is used to build up the Discovery schema structs.

use anyhow::{anyhow, Result};
use std::collections::BTreeMap;

use super::model::*;
use super::util::*;

/// Represents a mapping of all required request/response objects needed to interact
/// with the API.
#[derive(Debug, Default)]
pub struct StructSchemas {
    pub schemas: BTreeMap<String, StructSchema>,
}

impl StructSchemas {
    /// Getter for schemas.
    pub fn schemas(&self) -> &BTreeMap<String, StructSchema> {
        &self.schemas
    }

    /// Adds a field to a Schema struct.
    fn push_field(&mut self, schema: &str, mut field: StructField) -> Result<()> {
        self.init_struct(schema, None);
        let struct_schema = self
            .schemas
            .get_mut(schema)
            .ok_or_else(|| anyhow!("unable to find key `{}`", schema))?;
        field.append_prefix("#[serde(skip_serializing_if = \"Option::is_none\")]");
        struct_schema.fields.push(field);
        Ok(())
    }

    /// Initializes a new schema if needed.
    fn init_struct(&mut self, schema: &str, doc: Option<String>) {
        if !self.schemas.contains_key(schema) {
            self.schemas.insert(
                schema.into(),
                StructSchema {
                    doc,
                    ..Default::default()
                },
            );
        }
    }
}

/// Represents a request/response struct and all of its fields.
#[derive(Debug, Default)]
pub struct StructSchema {
    pub doc: Option<String>,
    pub fields: Vec<StructField>,
}

/// Represents the fields of a request/response struct.
#[derive(Clone, Eq, Debug, Default)]
pub struct StructField {
    pub prefix: String,
    pub name: String,
    pub field_type: String,
    pub doc: Option<String>,
}

impl StructField {
    fn append_prefix(&mut self, prefix: &str) {
        if self.prefix.is_empty() {
            self.prefix.push_str(prefix)
        } else {
            self.prefix.push_str(&format!("\n    {}", prefix))
        }
    }
}

impl Ord for StructField {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.name.cmp(&other.name)
    }
}

impl PartialOrd for StructField {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for StructField {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

/// Recursively processes all schemas to build up a mapping of the structs that
/// will need to be generated to represent them.
pub fn schema_structs(schemas: &BTreeMap<String, Schema>) -> Result<StructSchemas> {
    let mut structs: StructSchemas = Default::default();
    for (name, schema) in schemas {
        structs.init_struct(name, schema.description.clone());
        flat_schema_structs("", name, schema, &mut structs)?;
    }
    Ok(structs)
}

/// Recursively build up schema structs.
fn flat_schema_structs(
    struct_name: &str,
    schema_name: &str,
    schema: &Schema,
    structs: &mut StructSchemas,
) -> Result<()> {
    if let Some(ref_schema) = &schema.schema_ref {
        structs.push_field(
            struct_name,
            StructField {
                name: camel_to_snake(schema_name),
                field_type: ref_schema.clone(),
                doc: schema.description.clone(),
                ..Default::default()
            },
        )?;
        return Ok(());
    }
    let schema_type = schema
        .schema_type
        .as_ref()
        .ok_or_else(|| anyhow!("no schema_type provided for {:?}", schema))?
        .as_str();
    match schema_type {
        "object" => {
            let field_name = camel_to_snake(schema_name);
            let (field_name, field_prefix) = field_attrs(&field_name);
            let mut struct_field = StructField {
                name: field_name,
                prefix: field_prefix,
                field_type: format!(
                    "{}{}",
                    struct_name,
                    to_title_case(&mut schema_name.to_owned())
                ),
                doc: schema.description.clone(),
            };
            if let Some(add_prop) = &schema.additional_properties {
                if let Some(ref_schema) = &add_prop.schema_ref {
                    struct_field.field_type =
                        format!("std::collections::HashMap<String, {}>", ref_schema);
                } else if let Some(add_prop_schema_type) = add_prop.schema_type.as_ref() {
                    match add_prop_schema_type.as_ref() {
                        "any" => {
                            struct_field.field_type = "Vec<u8>".into();
                        }
                        _ => {
                            struct_field.field_type = format!(
                                "std::collections::HashMap<String, {}>",
                                basic_struct_type(add_prop_schema_type)
                            );
                        }
                    }
                } else {
                    panic!("unable to handle additional_properties: {:?}", add_prop)
                }
            } else if let Some(schema_ref) = &schema.schema_ref {
                struct_field.field_type = schema_ref.clone()
            } else if schema.properties.is_empty() {
                structs.init_struct(
                    &format!(
                        "{}{}",
                        struct_name,
                        to_title_case(&mut schema_name.to_owned())
                    ),
                    schema.description.clone(),
                )
            } else {
                let type_name = format!(
                    "{}{}",
                    struct_name,
                    to_title_case(&mut schema_name.to_owned())
                );
                structs.init_struct(&type_name, schema.description.clone());
                for (prop_name, prop_schema) in &schema.properties {
                    flat_schema_structs(&type_name, prop_name, prop_schema, structs)?;
                }
            }
            if !struct_name.is_empty() {
                structs.push_field(struct_name, struct_field)?;
            }
        }
        "array" => {
            let item_schema = schema
                .items
                .as_ref()
                .ok_or_else(|| anyhow!("no items found for schema: {:?}", schema))?;
            let field_name = camel_to_snake(schema_name);
            let (field_name, field_prefix) = field_attrs(&field_name);
            let mut struct_field = StructField {
                prefix: field_prefix,
                name: field_name,
                doc: schema.description.clone(),
                ..Default::default()
            };
            if let Some(ref_type) = &item_schema.schema_ref {
                struct_field.field_type = format!("Vec<{}>", ref_type);
            } else if let Some(item_schema_type) = item_schema.schema_type.as_ref() {
                match item_schema_type.as_ref() {
                    "object" => {
                        let type_name = format!(
                            "{}{}",
                            struct_name,
                            to_title_case(&mut schema_name.to_owned())
                        );
                        struct_field.field_type = format!("Vec<{}>", type_name);
                        structs.init_struct(&type_name, item_schema.description.clone());
                        for (prop_name, prop_schema) in &item_schema.properties {
                            flat_schema_structs(&type_name, prop_name, prop_schema, structs)?;
                        }
                    }
                    _ => {
                        struct_field.field_type =
                            format!("Vec<{}>", basic_struct_type(item_schema_type))
                    }
                }
            } else {
                panic!("unsupported array format: {:?}", item_schema)
            }
            structs.push_field(struct_name, struct_field)?;
        }
        _ => {
            let field_name = camel_to_snake(schema_name);
            let (field_name, field_prefix) = field_attrs(&field_name);
            structs.push_field(
                struct_name,
                StructField {
                    name: field_name,
                    prefix: field_prefix,
                    field_type: basic_struct_type(schema_type),
                    doc: schema.description.clone(),
                },
            )?;
        }
    }
    Ok(())
}

/// field_attrs sanitizes the name and returns the name and prefix.
fn field_attrs(field_name: &str) -> (String, String) {
    if is_keyword(field_name) {
        return (
            format!("{}_", field_name),
            format!("#[serde(rename = \"{}\")]", field_name),
        );
    }
    (field_name.to_owned(), String::new())
}
