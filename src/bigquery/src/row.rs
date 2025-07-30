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

use crate::Result;
use crate::schema::Schema;
use crate::value::FromSql;
use serde_json::Number;
use wkt::{Struct, Value};

#[derive(Clone, Debug)]
pub struct Row {
    values: Value,
    schema: Schema,
}

impl Row {
    pub(crate) fn try_new(values: wkt::Struct, schema: Schema) -> Result<Self> {
        convert_row(values, schema)
    }

    pub fn get<T: FromSql>(&self, name: &str) -> Result<Option<T>> {
        let Some(_) = self.schema.get_field(name) else {
            return Err(crate::Error::ser(ParsingError::MissingField {
                field: name.to_string(),
            }));
        };
        let value = self.values.get(name).cloned();
        if value.is_none() {
            return Ok(None);
        }
        T::from_sql(value.unwrap())
            .map(Some)
            .map_err(|_e| crate::Error::ser(ParsingError::UnknownFieldValueType))
    }

    pub fn to_value(&self) -> Value {
        self.values.clone()
    }
}

/// Represents errors that can occur when parsing query results.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum ParsingError {
    /// Only complete Query Jobs can be read.
    #[error("Query is not complete: Only complete Query Jobs can be read.")]
    MissingField { field: String },
    /// Only complete Query Jobs can be read.
    #[error("attribute 'f' field is missing")]
    MissingValueFields,
    /// Only complete Query Jobs can be read.
    #[error("row 'f' field is not a list")]
    InvalidValueFields,
    /// Only complete Query Jobs can be read.
    #[error("field value is not an object")]
    InvalidFieldValue,
    /// Only complete Query Jobs can be read.
    #[error("unkown field value type")]
    UnknownFieldValueType,
    /// Only complete Query Jobs can be read.
    #[error("schema/row length mismatch")]
    SchemaMismatch,
}

fn get_field_list(row: Struct) -> Result<Vec<Value>> {
    let fields = row
        .into_iter()
        .find(|(name, _)| name == "f")
        .map(|(_, value)| value);

    if fields.is_none() {
        return Err(crate::Error::deser(ParsingError::MissingValueFields));
    }

    return match fields.unwrap().as_array() {
        Some(v) => Ok(v.to_vec()),
        _ => return Err(crate::Error::deser(ParsingError::InvalidValueFields)),
    };
}

fn get_field_value(value: Value) -> Result<Value> {
    let value = value.as_object();
    if value.is_none() {
        return Err(crate::Error::deser(ParsingError::InvalidFieldValue));
    }

    let value = value
        .unwrap()
        .to_owned()
        .into_iter()
        .find(|(name, _)| name == "v")
        .map(|(_, value)| value);

    value.ok_or(crate::Error::deser(ParsingError::InvalidFieldValue))
}

pub(crate) fn convert_row(row: Struct, schema: Schema) -> Result<Row> {
    let field_list = get_field_list(row)?;

    let mut values = wkt::Struct::new();
    for (i, cell) in field_list.iter().enumerate() {
        let value = get_field_value(cell.clone())?;
        let f = schema.get_field_by_index(i);
        if f.is_some() {
            let f = f.unwrap().clone();
            let field_name = f.name.clone();
            let field_type = f.r#type.clone();
            let value = convert_value(value, field_type, Schema::new_from_field(f))?;
            values.insert(field_name, value);
        }
    }

    if values.len() != schema.len() {
        return Err(crate::Error::ser(ParsingError::SchemaMismatch));
    }

    Ok(Row {
        values: Value::Object(values),
        schema,
    })
}

fn convert_value(value: Value, field_type: String, schema: Schema) -> Result<Value> {
    match value {
        Value::Null => Ok(Value::Null),
        Value::String(v) => convert_basic_type(v, field_type),
        Value::Object(v) => convert_nested_record(v, schema),
        Value::Array(v) => convert_repeated_record(v, field_type, schema),
        _ => Err(crate::Error::ser(ParsingError::UnknownFieldValueType)),
    }
}

fn convert_repeated_record(
    value: wkt::ListValue,
    field_type: String,
    schema: Schema,
) -> Result<Value> {
    let mut values = wkt::ListValue::new();
    for cell in value {
        // each cell contains a single entry, keyed by "v"
        let val = get_field_value(cell)?;
        let v = convert_value(val, field_type.clone(), schema.clone())?;
        values.push(v);
    }
    Ok(Value::Array(values))
}

fn convert_nested_record(value: wkt::Struct, schema: Schema) -> Result<Value> {
    let row = convert_row(value, schema)?;
    Ok(row.values)
}

fn convert_basic_type(value: String, field_type: String) -> Result<Value> {
    match field_type.as_str() {
        "STRING" => Ok(Value::String(value)),
        "BYTES" => Ok(Value::String(value)),
        "TIMESTAMP" => Ok(Value::String(value)),
        "DATE" => Ok(Value::String(value)),
        "TIME" => Ok(Value::String(value)),
        "DATETIME" => Ok(Value::String(value)),
        "NUMERIC" => Ok(Value::String(value)),
        "BIGINT" => Ok(Value::String(value)),
        "GEOGRAPHY" => Ok(Value::String(value)),
        "JSON" => Ok(Value::String(value)),
        "INTERVAL" => Ok(Value::String(value)),
        "INTEGER" => {
            let num = value
                .parse::<i64>()
                .map_err(|_e| crate::Error::ser(ParsingError::UnknownFieldValueType))?;
            Ok(Value::Number(num.into()))
        }
        "FLOAT" => {
            let num = value
                .parse::<f64>()
                .map_err(|_e| crate::Error::ser(ParsingError::UnknownFieldValueType))?;
            Ok(Value::Number(Number::from_f64(num).unwrap()))
        }
        "BOOLEAN" => {
            let b = value
                .parse::<bool>()
                .map_err(|_e| crate::Error::ser(ParsingError::UnknownFieldValueType))?;
            Ok(Value::Bool(b))
        }
        _ => Err(crate::Error::ser(ParsingError::UnknownFieldValueType)),
    }
}
