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

//! Shared helper functions for parsing protobuf `textproto` test fixtures.

use crate::model::key_recipe::part::{NullOrder, Order};
use crate::model::key_recipe::{Part, Target};
use crate::model::mutation::{
    Ack as ProtoAck, Delete as ProtoDelete, Send as ProtoSend, Write as ProtoWrite,
};
use crate::model::tablet::Role;
use crate::model::{
    CacheUpdate, Group, KeyRange as ProtoKeyRange, KeyRecipe, KeySet as ProtoKeySet,
    Mutation as ProtoMutation, Range, RecipeList, Tablet, Type, TypeCode,
};
use crate::routing::key_range_cache::RangeMode;
use crate::value::{ToValue, Value};
use bytes::Bytes;
use serde_json::{Number as JsonNumber, Value as JsonValue};
use std::collections::BTreeMap;
use std::iter::Peekable;
use std::mem;

/// Unescapes C-style octal escape sequences (e.g. `\206`, `\310`, `\002`), hex escapes (e.g. `\x1b`, `\x0A`),
/// and standard ASCII escapes from Protobuf `textproto` byte strings.
pub(crate) fn unescape_bytes(escaped_string: &str) -> Vec<u8> {
    let mut output = Vec::with_capacity(escaped_string.len());
    let mut bytes = escaped_string.bytes().peekable();

    while let Some(byte) = bytes.next() {
        if byte != b'\\' {
            output.push(byte);
            continue;
        }

        // Try to parse up to 3 octal digits (`\ooo`) which represent raw byte values.
        if let Some(octal_byte) = try_parse_octal_escape(&mut bytes) {
            output.push(octal_byte);
            continue;
        }

        // Try to parse hex escapes (`\xHH`).
        if bytes.peek() == Some(&b'x') {
            bytes.next();
            if let Some(hex_byte) = try_parse_hex_escape(&mut bytes) {
                output.push(hex_byte);
                continue;
            }
            output.push(b'x');
            continue;
        }

        // Otherwise, handle standard ASCII escape sequences (`\n`, `\r`, `\t`, etc.).
        if let Some(next_byte) = bytes.next() {
            let escaped = match next_byte {
                b'n' => b'\n',
                b'r' => b'\r',
                b't' => b'\t',
                b'\\' => b'\\',
                b'"' => b'"',
                _ => next_byte,
            };
            output.push(escaped);
        }
    }

    output
}

/// Helper that attempts to read up to 3 consecutive octal digits from a byte stream.
/// Returns `Some(byte)` if at least one octal digit was consumed, or `None` otherwise.
pub(crate) fn try_parse_octal_escape<I: Iterator<Item = u8>>(
    bytes: &mut Peekable<I>,
) -> Option<u8> {
    let mut value = 0u8;
    let mut parsed_digits = 0;

    for _ in 0..3 {
        if let Some(&byte) = bytes.peek() {
            if (b'0'..=b'7').contains(&byte) {
                let digit = byte - b'0';
                value = value.wrapping_mul(8).wrapping_add(digit);
                bytes.next(); // Consume the octal digit byte.
                parsed_digits += 1;
            } else {
                break;
            }
        }
    }

    if parsed_digits > 0 { Some(value) } else { None }
}

/// Helper that attempts to read up to 2 consecutive hex digits from a byte stream after `\x`.
/// Returns `Some(byte)` if at least one hex digit was consumed, or `None` otherwise.
pub(crate) fn try_parse_hex_escape<I: Iterator<Item = u8>>(bytes: &mut Peekable<I>) -> Option<u8> {
    let mut value = 0u8;
    let mut parsed_digits = 0;

    for _ in 0..2 {
        if let Some(&byte) = bytes.peek() {
            let digit = match byte {
                b'0'..=b'9' => byte - b'0',
                b'a'..=b'f' => byte - b'a' + 10,
                b'A'..=b'F' => byte - b'A' + 10,
                _ => break,
            };
            value = (value << 4) | digit;
            bytes.next();
            parsed_digits += 1;
        }
    }

    if parsed_digits > 0 { Some(value) } else { None }
}

/// Skips an entire nested protobuf block `{ ... }` including any sub-blocks.
pub(crate) fn skip_block<'a, I: Iterator<Item = &'a str>>(lines: &mut Peekable<I>) {
    let mut depth = 1;
    for line in lines.by_ref() {
        let trimmed = line.trim();
        if trimmed.ends_with('{') {
            depth += 1;
        } else if trimmed == "}" {
            depth -= 1;
            if depth == 0 {
                break;
            }
        }
    }
}

/// Converts a JSON value ([`JsonValue`]) into a Spanner [`Value`].
pub(crate) fn json_to_spanner_value(json_value: &JsonValue) -> Value {
    match json_value {
        JsonValue::Bool(boolean_value) => boolean_value.to_value(),
        JsonValue::Number(number) => {
            if let Some(integer) = number.as_i64() {
                integer.to_value()
            } else if let Some(float_number) = number.as_f64() {
                float_number.to_value()
            } else {
                Value::null()
            }
        }
        JsonValue::String(string_value) => {
            if let Ok(integer) = string_value.parse::<i64>() {
                integer.to_value()
            } else {
                string_value.as_str().to_value()
            }
        }
        JsonValue::Null => Value::null(),
        _ => Value::null(),
    }
}

/// Simple line-trimming helper that extracts the value after a specified field prefix
/// (e.g., stripping `"name: "` and removing surrounding quotes).
pub(crate) fn extract_value<'a>(line: &'a str, prefix: &str) -> Option<&'a str> {
    line.trim()
        .strip_prefix(prefix)
        .map(|rest| rest.trim_matches(|character| character == ' ' || character == '"'))
}

/// Maps Spanner data type code names in textproto to [`TypeCode`].
pub(crate) fn parse_type_code(code_string: &str) -> Option<TypeCode> {
    match code_string {
        "BOOL" => Some(TypeCode::Bool),
        "INT64" => Some(TypeCode::Int64),
        "FLOAT64" => Some(TypeCode::Float64),
        "STRING" => Some(TypeCode::String),
        "BYTES" => Some(TypeCode::Bytes),
        "DATE" => Some(TypeCode::Date),
        "TIMESTAMP" => Some(TypeCode::Timestamp),
        "UUID" => Some(TypeCode::Uuid),
        "ENUM" => Some(TypeCode::Enum),
        _ => None,
    }
}

/// Parses sort order strings (`"ASCENDING"`, `"DESCENDING"`) into [`Order`].
pub(crate) fn parse_order(order_string: &str) -> Option<Order> {
    match order_string {
        "DESCENDING" => Some(Order::Descending),
        "ASCENDING" => Some(Order::Ascending),
        _ => None,
    }
}

/// Parses null order strings (`"NULLS_FIRST"`, `"NULLS_LAST"`, `"NOT_NULL"`) into [`NullOrder`].
pub(crate) fn parse_null_order(null_order_string: &str) -> Option<NullOrder> {
    match null_order_string {
        "NULLS_FIRST" => Some(NullOrder::NullsFirst),
        "NULLS_LAST" => Some(NullOrder::NullsLast),
        "NOT_NULL" => Some(NullOrder::NotNull),
        _ => None,
    }
}

/// Parses a constant value (`string_value`, `number_value`, `bool_value`, `null_value`) from a textproto line.
pub(crate) fn parse_constant_value(trimmed: &str) -> Option<JsonValue> {
    let (field_name, field_value) = trimmed.split_once(':')?;
    let value = field_value.trim().trim_matches('"');
    match field_name.trim() {
        "string_value" => Some(JsonValue::String(value.to_string())),
        "number_value" => {
            if let Ok(integer) = value.parse::<i64>() {
                Some(JsonValue::Number(integer.into()))
            } else {
                value
                    .parse::<f64>()
                    .ok()
                    .and_then(JsonNumber::from_f64)
                    .map(JsonValue::Number)
            }
        }
        "bool_value" => Some(JsonValue::Bool(value == "true")),
        "null_value" => Some(JsonValue::Null),
        _ => None,
    }
}

/// Parses a `part { ... }` block inside a Spanner key recipe.
pub(crate) fn parse_part_block<'a, I: Iterator<Item = &'a str>>(lines: &mut Peekable<I>) -> Part {
    let mut tag = 0u32;
    let mut order = Order::Ascending;
    let mut null_order = NullOrder::Unspecified;
    let mut type_code = None;
    let mut identifier = None;
    let mut struct_identifiers = Vec::new();
    let mut random = false;
    let mut constant_val = None;
    let mut depth = 1;

    for line in lines.by_ref() {
        let trimmed = line.trim();
        if trimmed.ends_with('{') {
            depth += 1;
        } else if trimmed == "}" {
            depth -= 1;
            if depth == 0 {
                break;
            }
        }

        if let Some((field, value)) = trimmed.split_once(':') {
            let field = field.trim();
            let value = value.trim().trim_matches('"');
            match field {
                "tag" => {
                    if let Ok(tag_number) = value.parse::<u32>() {
                        tag = tag_number;
                    }
                }
                "order" => {
                    if let Some(parsed_order) = parse_order(value) {
                        order = parsed_order;
                    }
                }
                "null_order" => {
                    if let Some(parsed_null_order) = parse_null_order(value) {
                        null_order = parsed_null_order;
                    }
                }
                "code" => {
                    type_code = parse_type_code(value);
                }
                "identifier" => {
                    identifier = Some(value.to_string());
                }
                "struct_identifiers" => {
                    if let Ok(id_val) = value.parse::<i32>() {
                        struct_identifiers.push(id_val);
                    }
                }
                "random" => {
                    random = value == "true";
                }
                _ => {
                    if let Some(constant) = parse_constant_value(trimmed) {
                        constant_val = Some(constant);
                    }
                }
            }
        }
    }

    let mut part = Part::new().set_tag(tag).set_order(order);

    if null_order != NullOrder::Unspecified {
        part = part.set_null_order(null_order);
    }
    if let Some(code) = type_code {
        part = part.set_type(Type::new().set_code(code));
    }
    if let Some(id) = identifier {
        part = part.set_identifier(id);
    }
    if !struct_identifiers.is_empty() {
        part = part.set_struct_identifiers(struct_identifiers);
    }
    if random {
        part = part.set_random(true);
    }
    if let Some(val) = constant_val {
        part = part.set_value(val);
    }

    part
}

/// Parses a `recipe { ... }` block inside textproto test fixtures.
pub(crate) fn parse_recipe_block<'a, I: Iterator<Item = &'a str>>(
    lines: &mut Peekable<I>,
) -> KeyRecipe {
    let mut recipe = KeyRecipe::new();
    let mut parts = Vec::new();
    let mut depth = 1;

    while let Some(line) = lines.next() {
        let trimmed = line.trim();
        if trimmed.starts_with("part {") {
            parts.push(parse_part_block(lines));
            continue;
        }

        if trimmed.ends_with('{') {
            depth += 1;
        } else if trimmed == "}" {
            depth -= 1;
            if depth == 0 {
                break;
            }
        }

        if let Some(table_name) = extract_value(trimmed, "table_name:") {
            recipe = recipe.set_table_name(table_name);
        } else if let Some(index_name) = extract_value(trimmed, "index_name:") {
            recipe = recipe.set_index_name(index_name);
        } else if let Some(operation_uid_string) = extract_value(trimmed, "operation_uid:")
            && let Ok(operation_uid) = operation_uid_string.parse::<u64>()
        {
            recipe = recipe.set_target(Target::OperationUid(operation_uid));
        }
    }

    recipe.set_part(parts)
}

/// Parses a `key_recipes { ... }` block inside textproto test fixtures.
pub(crate) fn parse_recipes_block<'a, I: Iterator<Item = &'a str>>(
    lines: &mut Peekable<I>,
) -> RecipeList {
    let mut schema_generation = Bytes::new();
    let mut recipes = Vec::new();
    let mut depth = 1;

    while let Some(line) = lines.next() {
        let trimmed = line.trim();
        if trimmed.starts_with("recipe {") {
            recipes.push(parse_recipe_block(lines));
        } else if let Some(generation_string) = extract_value(trimmed, "schema_generation:") {
            schema_generation = Bytes::from(unescape_bytes(generation_string));
        } else if trimmed.ends_with('{') {
            depth += 1;
        } else if trimmed == "}" {
            depth -= 1;
            if depth == 0 {
                break;
            }
        }
    }

    RecipeList {
        schema_generation,
        recipe: recipes,
        ..Default::default()
    }
}

/// Parses a `tablets { ... }` block inside textproto test fixtures.
pub(crate) fn parse_tablet_block<'a, I: Iterator<Item = &'a str>>(
    lines: &mut Peekable<I>,
) -> Tablet {
    let mut tablet_uid = 0u64;
    let mut server_address = String::new();
    let mut location = String::new();
    let mut role = Role::Unspecified;
    let mut incarnation = Bytes::new();
    let mut distance = 0u32;
    let mut skip = false;
    let mut depth = 1;

    for line in lines.by_ref() {
        let trimmed = line.trim();
        if trimmed.ends_with('{') {
            depth += 1;
        } else if trimmed == "}" {
            depth -= 1;
            if depth == 0 {
                break;
            }
        }

        if let Some((field, value)) = trimmed.split_once(':') {
            let field = field.trim();
            let value = value.trim().trim_matches('"');
            match field {
                "tablet_uid" => tablet_uid = value.parse::<u64>().unwrap_or(0),
                "server_address" => server_address = value.to_string(),
                "location" => location = value.to_string(),
                "role" => {
                    role = match value {
                        "READ_WRITE" => Role::ReadWrite,
                        "READ_ONLY" => Role::ReadOnly,
                        _ => Role::Unspecified,
                    };
                }
                "incarnation" => incarnation = Bytes::from(unescape_bytes(value)),
                "distance" => distance = value.parse::<u32>().unwrap_or(0),
                "skip" => skip = value == "true",
                _ => {}
            }
        }
    }

    Tablet {
        tablet_uid,
        server_address,
        location,
        role,
        incarnation,
        distance,
        skip,
        ..Default::default()
    }
}

/// Parses a `group { ... }` block inside textproto test fixtures.
pub(crate) fn parse_group_block<'a, I: Iterator<Item = &'a str>>(lines: &mut Peekable<I>) -> Group {
    let mut group_uid = 0u64;
    let mut generation = Bytes::new();
    let mut leader_index = -1i32;
    let mut tablets = Vec::new();
    let mut depth = 1;

    while let Some(line) = lines.next() {
        let trimmed = line.trim();
        if trimmed.starts_with("tablets {") {
            tablets.push(parse_tablet_block(lines));
        } else if let Some((field, value)) = trimmed.split_once(':') {
            let field = field.trim();
            let value = value.trim().trim_matches('"');
            match field {
                "group_uid" => group_uid = value.parse::<u64>().unwrap_or(0),
                "generation" => generation = Bytes::from(unescape_bytes(value)),
                "leader_index" => leader_index = value.parse::<i32>().unwrap_or(-1),
                _ => {}
            }
        } else if trimmed.ends_with('{') {
            depth += 1;
        } else if trimmed == "}" {
            depth -= 1;
            if depth == 0 {
                break;
            }
        }
    }

    Group {
        group_uid,
        generation,
        tablets,
        leader_index,
        ..Default::default()
    }
}

/// Parses a `range { ... }` block inside textproto test fixtures.
pub(crate) fn parse_range_block<'a, I: Iterator<Item = &'a str>>(lines: &mut Peekable<I>) -> Range {
    let mut start_key = Bytes::new();
    let mut limit_key = Bytes::new();
    let mut group_uid = 0u64;
    let mut split_id = 0u64;
    let mut generation = Bytes::new();
    let mut depth = 1;

    for line in lines.by_ref() {
        let trimmed = line.trim();
        if trimmed.ends_with('{') {
            depth += 1;
        } else if trimmed == "}" {
            depth -= 1;
            if depth == 0 {
                break;
            }
        }

        if let Some((field, value)) = trimmed.split_once(':') {
            let field = field.trim();
            let value = value.trim().trim_matches('"');
            match field {
                "start_key" => start_key = Bytes::from(unescape_bytes(value)),
                "limit_key" => limit_key = Bytes::from(unescape_bytes(value)),
                "group_uid" => group_uid = value.parse::<u64>().unwrap_or(0),
                "split_id" => split_id = value.parse::<u64>().unwrap_or(0),
                "generation" => generation = Bytes::from(unescape_bytes(value)),
                _ => {}
            }
        }
    }

    Range {
        start_key,
        limit_key,
        group_uid,
        split_id,
        generation,
        ..Default::default()
    }
}

/// Parses a `cache_update { ... }` block inside textproto test fixtures.
pub(crate) fn parse_cache_update_block<'a, I: Iterator<Item = &'a str>>(
    lines: &mut Peekable<I>,
) -> CacheUpdate {
    let mut update = CacheUpdate::new();
    let mut ranges = Vec::new();
    let mut groups = Vec::new();
    let mut key_recipes = None;
    let mut depth = 1;

    while let Some(line) = lines.next() {
        let trimmed = line.trim();
        if trimmed.starts_with("range {") {
            ranges.push(parse_range_block(lines));
        } else if trimmed.starts_with("group {") {
            groups.push(parse_group_block(lines));
        } else if trimmed.starts_with("key_recipes {") {
            key_recipes = Some(parse_recipes_block(lines));
        } else if let Some(val) = extract_value(trimmed, "database_id:") {
            if let Ok(id) = val.parse::<u64>() {
                update = update.set_database_id(id);
            }
        } else if trimmed.ends_with('{') {
            depth += 1;
        } else if trimmed == "}" {
            depth -= 1;
            if depth == 0 {
                break;
            }
        }
    }

    if !ranges.is_empty() {
        update = update.set_range(ranges);
    }
    if !groups.is_empty() {
        update = update.set_group(groups);
    }
    if let Some(recipes) = key_recipes {
        update = update.set_key_recipes(recipes);
    }

    update
}

// -----------------------------------------------------------------------------
// Recipe Test Fixture Parsers (`recipe_test.textproto`)
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub(crate) struct ParsedRecipeTestCase {
    pub name: String,
    pub recipe: KeyRecipe,
    pub tests: Vec<ParsedRecipeTest>,
}

#[derive(Debug, Clone)]
pub(crate) struct ParsedRecipeTest {
    pub values: Vec<Value>,
    pub query_params: Option<BTreeMap<String, Value>>,
    pub mutation: Option<ProtoMutation>,
    pub start: Vec<u8>,
    pub approximate: bool,
}

/// Consumes lines belonging to a `mutation { ... }` block inside a test block.
pub(crate) fn parse_mutation_block<'a, I: Iterator<Item = &'a str>>(
    lines: &mut Peekable<I>,
) -> Option<ProtoMutation> {
    let mutation = ProtoMutation::default();
    let mut current_table = String::new();
    let mut current_columns = Vec::new();
    let mut current_rows = Vec::new();
    let mut current_row_values = Vec::new();
    let mut current_queue = String::new();
    let mut current_key_values = Vec::new();
    let mut delete_keys = Vec::new();
    let mut delete_ranges = Vec::new();
    let mut current_range_start_closed = Vec::new();
    let mut current_range_start_open = Vec::new();
    let mut delete_all = false;
    let mut op_type = "";
    let mut in_values_item = false;
    let mut in_range_start_closed = false;
    let mut in_range_start_open = false;
    let mut in_delete_keys = false;
    let mut in_key_block = false;
    let mut depth = 1;

    for line in lines.by_ref() {
        let trimmed = line.trim();
        if trimmed.ends_with('{') {
            depth += 1;
            if trimmed.starts_with("insert {") {
                op_type = "insert";
            } else if trimmed.starts_with("update {") {
                op_type = "update";
            } else if trimmed.starts_with("insert_or_update {") {
                op_type = "insert_or_update";
            } else if trimmed.starts_with("replace {") {
                op_type = "replace";
            } else if trimmed.starts_with("delete {") {
                op_type = "delete";
            } else if trimmed.starts_with("send {") {
                op_type = "send";
            } else if trimmed.starts_with("ack {") {
                op_type = "ack";
            } else if trimmed.starts_with("key {") {
                in_key_block = true;
            } else if trimmed.starts_with("keys {") {
                in_delete_keys = true;
            } else if trimmed.starts_with("start_closed {") {
                in_range_start_closed = true;
            } else if trimmed.starts_with("start_open {") {
                in_range_start_open = true;
            } else if trimmed.starts_with("values {") && depth >= 4 {
                in_values_item = true;
            }
        } else if trimmed == "}" {
            if in_values_item {
                in_values_item = false;
                if !current_row_values.is_empty() {
                    current_rows.push(mem::take(&mut current_row_values));
                }
            } else if in_delete_keys {
                in_delete_keys = false;
                if !current_key_values.is_empty() {
                    delete_keys.push(mem::take(&mut current_key_values));
                }
            } else if in_key_block && depth == 3 {
                in_key_block = false;
            } else if in_range_start_closed || in_range_start_open {
                in_range_start_closed = false;
                in_range_start_open = false;
            } else if depth == 3
                && op_type == "delete"
                && (!current_range_start_closed.is_empty() || !current_range_start_open.is_empty())
            {
                let mut range = ProtoKeyRange::new();
                if !current_range_start_closed.is_empty() {
                    range = range.set_start_closed(mem::take(&mut current_range_start_closed));
                }
                if !current_range_start_open.is_empty() {
                    range = range.set_start_open(mem::take(&mut current_range_start_open));
                }
                delete_ranges.push(range);
            }
            depth -= 1;
            if depth == 0 {
                break;
            }
        } else if let Some(table_value) = extract_value(trimmed, "table:") {
            current_table = table_value.to_string();
        } else if let Some(queue_value) = extract_value(trimmed, "queue:") {
            current_queue = queue_value.to_string();
        } else if let Some(column_value) = extract_value(trimmed, "columns:") {
            current_columns.push(column_value.to_string());
        } else if trimmed == "all: true" {
            delete_all = true;
        } else if let Some(string_value) = extract_value(trimmed, "string_value:") {
            let json_val = JsonValue::String(string_value.to_string());
            if in_range_start_closed {
                current_range_start_closed.push(json_val);
            } else if in_range_start_open {
                current_range_start_open.push(json_val);
            } else if in_delete_keys || in_key_block {
                current_key_values.push(json_val);
            } else if in_values_item {
                current_row_values.push(json_val);
            }
        } else if let Some(number_string) = extract_value(trimmed, "number_value:")
            && let Ok(number) = number_string.parse::<f64>()
        {
            let json_val = serde_json::json!(number);
            if in_range_start_closed {
                current_range_start_closed.push(json_val);
            } else if in_range_start_open {
                current_range_start_open.push(json_val);
            } else if in_delete_keys || in_key_block {
                current_key_values.push(json_val);
            } else if in_values_item {
                current_row_values.push(json_val);
            }
        }
    }

    match op_type {
        "insert" => {
            let mut write = ProtoWrite::new();
            write.table = current_table;
            write.columns = current_columns;
            write.values = current_rows;
            Some(ProtoMutation::new().set_insert(write))
        }
        "update" => {
            let mut write = ProtoWrite::new();
            write.table = current_table;
            write.columns = current_columns;
            write.values = current_rows;
            Some(ProtoMutation::new().set_update(write))
        }
        "insert_or_update" => {
            let mut write = ProtoWrite::new();
            write.table = current_table;
            write.columns = current_columns;
            write.values = current_rows;
            Some(ProtoMutation::new().set_insert_or_update(write))
        }
        "replace" => {
            let mut write = ProtoWrite::new();
            write.table = current_table;
            write.columns = current_columns;
            write.values = current_rows;
            Some(ProtoMutation::new().set_replace(write))
        }
        "delete" => {
            let mut delete = ProtoDelete::new();
            delete.table = current_table;
            let mut key_set = ProtoKeySet::new();
            key_set.all = delete_all;
            key_set.keys = delete_keys;
            key_set.ranges = delete_ranges;
            delete.key_set = Some(key_set);
            Some(ProtoMutation::new().set_delete(delete))
        }
        "send" => {
            let mut send = ProtoSend::new();
            send.queue = current_queue;
            send.key = Some(current_key_values);
            Some(ProtoMutation::new().set_send(send))
        }
        "ack" => {
            let mut ack = ProtoAck::new();
            ack.queue = current_queue;
            ack.key = Some(current_key_values);
            Some(ProtoMutation::new().set_ack(ack))
        }
        _ => Some(mutation),
    }
}

/// Consumes lines belonging to a `test { ... }` block inside a test case.
pub(crate) fn parse_recipe_test_block<'a, I: Iterator<Item = &'a str>>(
    lines: &mut Peekable<I>,
) -> Option<ParsedRecipeTest> {
    let mut values = Vec::new();
    let mut query_params: Option<BTreeMap<String, Value>> = None;
    let mut mutation: Option<ProtoMutation> = None;
    let mut current_field_key: Option<String> = None;
    let mut in_query_params = false;
    let mut start = None;
    let mut approximate = false;
    let mut is_point_key_or_query_or_mutation = false;
    let mut depth = 1;

    while let Some(line) = lines.next() {
        let trimmed = line.trim();
        if trimmed.ends_with('{') {
            depth += 1;
        } else if trimmed == "}" {
            depth -= 1;
            if depth == 0 {
                break;
            }
        }

        if trimmed.starts_with("key {") {
            is_point_key_or_query_or_mutation = true;
            in_query_params = false;
        } else if trimmed.starts_with("query_params {") {
            is_point_key_or_query_or_mutation = true;
            in_query_params = true;
            query_params = Some(BTreeMap::new());
        } else if trimmed.starts_with("mutation {") {
            is_point_key_or_query_or_mutation = true;
            in_query_params = false;
            depth -= 1;
            mutation = parse_mutation_block(lines);
        } else if trimmed.starts_with("key_range {") || trimmed.starts_with("key_set {") {
            is_point_key_or_query_or_mutation = false;
        } else if in_query_params && let Some(key_string) = extract_value(trimmed, "key:") {
            current_field_key = Some(key_string.to_string());
        } else if in_query_params
            && let Some(value_string) = extract_value(trimmed, "string_value:")
        {
            if let (Some(params), Some(key)) = (query_params.as_mut(), current_field_key.take()) {
                params.insert(key, value_string.to_value());
            }
        } else if in_query_params && let Some(value_string) = extract_value(trimmed, "bool_value:")
        {
            if let (Some(params), Some(key)) = (query_params.as_mut(), current_field_key.take()) {
                params.insert(key, (value_string == "true").to_value());
            }
        } else if in_query_params
            && let Some(value_string) = extract_value(trimmed, "number_value:")
        {
            if let (Some(params), Some(key)) = (query_params.as_mut(), current_field_key.take())
                && let Ok(number) = value_string.parse::<f64>()
            {
                params.insert(key, number.to_value());
            }
        } else if in_query_params && trimmed == "null_value: NULL_VALUE" {
            if let (Some(params), Some(key)) = (query_params.as_mut(), current_field_key.take()) {
                params.insert(key, Value::null());
            }
        } else if let Some(start_string) = extract_value(trimmed, "start:") {
            start = Some(unescape_bytes(start_string));
        } else if !in_query_params
            && let Some(boolean_string) = extract_value(trimmed, "bool_value:")
        {
            values.push((boolean_string == "true").to_value());
        } else if !in_query_params
            && let Some(string_value) = extract_value(trimmed, "string_value:")
        {
            values.push(string_value.to_value());
        } else if !in_query_params
            && let Some(number_string) = extract_value(trimmed, "number_value:")
        {
            if let Ok(number) = number_string.parse::<f64>() {
                values.push(number.to_value());
            }
        } else if !in_query_params && trimmed == "null_value: NULL_VALUE" {
            values.push(Value::null());
        } else if trimmed == "approximate: true" {
            approximate = true;
        }
    }

    if let (true, Some(start_bytes)) = (is_point_key_or_query_or_mutation, start) {
        Some(ParsedRecipeTest {
            values,
            query_params,
            mutation,
            start: start_bytes,
            approximate,
        })
    } else {
        None
    }
}

/// Consumes lines belonging to a top-level `test_case { ... }` block in `recipe_test.textproto`.
pub(crate) fn parse_recipe_test_case_block<'a, I: Iterator<Item = &'a str>>(
    lines: &mut Peekable<I>,
) -> Option<ParsedRecipeTestCase> {
    let mut name = String::new();
    let mut parts = Vec::new();
    let mut tests = Vec::new();
    let mut depth = 1;

    while let Some(line) = lines.next() {
        let trimmed = line.trim();
        if let Some(case_name) = extract_value(trimmed, "name:") {
            name = case_name.to_string();
        } else if trimmed.starts_with("part {") {
            parts.push(parse_part_block(lines));
        } else if trimmed.starts_with("test {") {
            if let Some(test) = parse_recipe_test_block(lines) {
                tests.push(test);
            }
        } else if trimmed.ends_with('{') {
            depth += 1;
        } else if trimmed == "}" {
            depth -= 1;
            if depth == 0 {
                break;
            }
        }
    }

    if !name.is_empty() && !parts.is_empty() {
        Some(ParsedRecipeTestCase {
            name,
            recipe: KeyRecipe::new().set_part(parts),
            tests,
        })
    } else {
        None
    }
}

/// Parses `recipe_test.textproto` fixture content.
pub(crate) fn parse_recipe_golden_textproto(content: &str) -> Vec<ParsedRecipeTestCase> {
    let mut cases = Vec::new();
    let mut lines = content.lines().peekable();

    while let Some(line) = lines.next() {
        if line.trim().starts_with("test_case {")
            && let Some(case) = parse_recipe_test_case_block(&mut lines)
        {
            cases.push(case);
        }
    }

    cases
}

// -----------------------------------------------------------------------------
// Key Range Cache Test Fixture Parsers (`cache_test.textproto`)
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub(crate) struct RangeCacheTestCase {
    pub name: String,
    pub steps: Vec<RangeCacheStep>,
}

#[derive(Debug, Clone)]
pub(crate) struct RangeCacheStep {
    pub update: Option<CacheUpdate>,
    pub tests: Vec<RangeCacheQueryTest>,
}

#[derive(Debug, Clone)]
pub(crate) struct RangeCacheQueryTest {
    pub key: Option<Vec<u8>>,
    pub limit_key: Option<Vec<u8>>,
    pub min_cache_entries_for_random_pick: Option<usize>,
    pub range_mode: RangeMode,
    pub leader: bool,
    pub expected_result: RangeCacheExpectedResult,
    pub expected_server: Option<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct RangeCacheExpectedResult {
    pub key: Option<Vec<u8>>,
    pub limit_key: Option<Vec<u8>>,
    pub group_uid: Option<u64>,
    pub split_id: Option<u64>,
    pub tablet_uid: Option<u64>,
}

pub(crate) fn parse_range_cache_result_block<'a, I: Iterator<Item = &'a str>>(
    lines: &mut Peekable<I>,
) -> RangeCacheExpectedResult {
    let mut result = RangeCacheExpectedResult::default();
    let mut depth = 1;

    for line in lines.by_ref() {
        let trimmed = line.trim();
        if trimmed.ends_with('{') {
            depth += 1;
        } else if trimmed == "}" {
            depth -= 1;
            if depth == 0 {
                break;
            }
        }

        if let Some((field, value)) = trimmed.split_once(':') {
            let field = field.trim();
            let value = value.trim().trim_matches('"');
            match field {
                "key" => result.key = Some(unescape_bytes(value)),
                "limit_key" => result.limit_key = Some(unescape_bytes(value)),
                "group_uid" => result.group_uid = value.parse::<u64>().ok(),
                "split_id" => result.split_id = value.parse::<u64>().ok(),
                "tablet_uid" => result.tablet_uid = value.parse::<u64>().ok(),
                _ => {}
            }
        }
    }

    result
}

pub(crate) fn parse_range_cache_query_test_block<'a, I: Iterator<Item = &'a str>>(
    lines: &mut Peekable<I>,
) -> RangeCacheQueryTest {
    let mut key = None;
    let mut limit_key = None;
    let mut min_cache_entries_for_random_pick = None;
    let mut range_mode = RangeMode::CoveringSplit;
    let mut leader = false;
    let mut expected_result = RangeCacheExpectedResult::default();
    let mut expected_server = None;
    let mut depth = 1;

    while let Some(line) = lines.next() {
        let trimmed = line.trim();
        if trimmed.starts_with("result {") {
            expected_result = parse_range_cache_result_block(lines);
        } else if trimmed.starts_with("directed_read_options {") {
            skip_block(lines);
        } else if let Some((field, value)) = trimmed.split_once(':') {
            let field = field.trim();
            let value = value.trim().trim_matches('"');
            match field {
                "key" => key = Some(unescape_bytes(value)),
                "limit_key" => limit_key = Some(unescape_bytes(value)),
                "min_cache_entries_for_random_pick" => {
                    min_cache_entries_for_random_pick = value.parse::<usize>().ok()
                }
                "range_mode" => {
                    if value == "PICK_RANDOM" {
                        range_mode = RangeMode::PickRandom;
                    }
                }
                "leader" => leader = value == "true",
                "server" => expected_server = Some(value.to_string()),
                _ => {}
            }
        } else if trimmed.ends_with('{') {
            depth += 1;
        } else if trimmed == "}" {
            depth -= 1;
            if depth == 0 {
                break;
            }
        }
    }

    RangeCacheQueryTest {
        key,
        limit_key,
        min_cache_entries_for_random_pick,
        range_mode,
        leader,
        expected_result,
        expected_server,
    }
}

pub(crate) fn parse_range_cache_step_block<'a, I: Iterator<Item = &'a str>>(
    lines: &mut Peekable<I>,
) -> RangeCacheStep {
    let mut update = None;
    let mut tests = Vec::new();
    let mut depth = 1;

    while let Some(line) = lines.next() {
        let trimmed = line.trim();
        if trimmed.starts_with("update {") {
            update = Some(parse_cache_update_block(lines));
        } else if trimmed.starts_with("test {") {
            tests.push(parse_range_cache_query_test_block(lines));
        } else if trimmed.ends_with('{') {
            depth += 1;
        } else if trimmed == "}" {
            depth -= 1;
            if depth == 0 {
                break;
            }
        }
    }

    RangeCacheStep { update, tests }
}

pub(crate) fn parse_range_cache_test_case_block<'a, I: Iterator<Item = &'a str>>(
    lines: &mut Peekable<I>,
) -> Option<RangeCacheTestCase> {
    let mut name = String::new();
    let mut steps = Vec::new();
    let mut depth = 1;

    while let Some(line) = lines.next() {
        let trimmed = line.trim();
        if let Some(case_name) = extract_value(trimmed, "name:") {
            name = case_name.to_string();
        } else if trimmed.starts_with("step {") {
            steps.push(parse_range_cache_step_block(lines));
        } else if trimmed.ends_with('{') {
            depth += 1;
        } else if trimmed == "}" {
            depth -= 1;
            if depth == 0 {
                break;
            }
        }
    }

    if !name.is_empty() {
        Some(RangeCacheTestCase { name, steps })
    } else {
        None
    }
}

/// Parses `cache_test.textproto` fixture content.
pub(crate) fn parse_range_cache_golden_textproto(content: &str) -> Vec<RangeCacheTestCase> {
    let mut cases = Vec::new();
    let mut lines = content.lines().peekable();

    while let Some(line) = lines.next() {
        if line.trim().starts_with("test_case {")
            && let Some(case) = parse_range_cache_test_case_block(&mut lines)
        {
            cases.push(case);
        }
    }

    cases
}

// -----------------------------------------------------------------------------
// Finder Test Fixture Parsers (`finder_test.textproto`)
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub(crate) struct FinderTestCase {
    pub name: String,
    pub events: Vec<FinderEvent>,
}

#[derive(Debug, Clone)]
pub(crate) struct FinderEvent {
    pub name: String,
    pub cache_update: Option<CacheUpdate>,
    pub unhealthy_servers: Vec<String>,
    pub request: Option<FinderRequest>,
    pub expected_server: Option<String>,
    pub expected_hint: Option<ParsedRoutingHint>,
}

#[derive(Debug, Clone)]
pub(crate) enum FinderRequest {
    Read(ParsedReadRequest),
    Sql(ParsedSqlRequest),
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct ParsedReadRequest {
    pub table: String,
    pub index: String,
    pub columns: Vec<String>,
    pub key_set: ParsedKeySet,
    pub prefer_leader: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct ParsedKeySet {
    pub all: bool,
    pub keys: Vec<Vec<String>>,
    pub ranges: Vec<ParsedKeyRange>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) enum StartKeyType {
    #[default]
    Unspecified,
    Closed(Vec<String>),
    Open(Vec<String>),
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) enum EndKeyType {
    #[default]
    Unspecified,
    Closed(Vec<String>),
    Open(Vec<String>),
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct ParsedKeyRange {
    pub start: StartKeyType,
    pub end: EndKeyType,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct ParsedSqlRequest {
    pub sql: String,
    pub params: BTreeMap<String, JsonValue>,
    pub prefer_leader: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct ParsedTabletUid {
    pub tablet_uid: u64,
    pub incarnation: Option<Vec<u8>>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct ParsedRoutingHint {
    pub operation_uid: Option<u64>,
    pub database_id: Option<u64>,
    pub schema_generation: Option<Vec<u8>>,
    pub key: Option<Vec<u8>>,
    pub limit_key: Option<Vec<u8>>,
    pub group_uid: Option<u64>,
    pub split_id: Option<u64>,
    pub tablet_uid: Option<u64>,
    pub skipped_tablet_uids: Vec<ParsedTabletUid>,
}

pub(crate) fn update_range_boundary_value(
    string_value: &str,
    current_range: &mut ParsedKeyRange,
    in_start_closed: bool,
    in_start_open: bool,
    in_end_closed: bool,
    in_end_open: bool,
) {
    if in_start_closed && let StartKeyType::Closed(vector) = &mut current_range.start {
        vector.push(string_value.to_string());
    } else if in_start_open && let StartKeyType::Open(vector) = &mut current_range.start {
        vector.push(string_value.to_string());
    } else if in_end_closed && let EndKeyType::Closed(vector) = &mut current_range.end {
        vector.push(string_value.to_string());
    } else if in_end_open && let EndKeyType::Open(vector) = &mut current_range.end {
        vector.push(string_value.to_string());
    }
}

pub(crate) fn parse_routing_hint_block<'a, I: Iterator<Item = &'a str>>(
    lines: &mut Peekable<I>,
) -> ParsedRoutingHint {
    let mut hint = ParsedRoutingHint::default();
    let mut depth = 1;

    while let Some(line) = lines.next() {
        let trimmed = line.trim();
        if trimmed.starts_with("skipped_tablet_uid {") {
            let mut skipped = ParsedTabletUid::default();
            let mut sub_depth = 1;
            for sub_line in lines.by_ref() {
                let sub_trimmed = sub_line.trim();
                if sub_trimmed.ends_with('{') {
                    sub_depth += 1;
                } else if sub_trimmed == "}" {
                    sub_depth -= 1;
                    if sub_depth == 0 {
                        break;
                    }
                } else if let Some((sub_field, sub_value)) = sub_trimmed.split_once(':') {
                    let sub_field = sub_field.trim();
                    let sub_value = sub_value.trim().trim_matches('"');
                    match sub_field {
                        "tablet_uid" => {
                            if let Ok(uid) = sub_value.parse::<u64>() {
                                skipped.tablet_uid = uid;
                            }
                        }
                        "incarnation" => skipped.incarnation = Some(unescape_bytes(sub_value)),
                        _ => {}
                    }
                }
            }
            hint.skipped_tablet_uids.push(skipped);
        } else if trimmed.ends_with('{') {
            depth += 1;
        } else if trimmed == "}" {
            depth -= 1;
            if depth == 0 {
                break;
            }
        } else if let Some((field, value)) = trimmed.split_once(':') {
            let field = field.trim();
            let value = value.trim().trim_matches('"');
            match field {
                "operation_uid" => hint.operation_uid = value.parse::<u64>().ok(),
                "database_id" => hint.database_id = value.parse::<u64>().ok(),
                "schema_generation" => hint.schema_generation = Some(unescape_bytes(value)),
                "key" => hint.key = Some(unescape_bytes(value)),
                "limit_key" => hint.limit_key = Some(unescape_bytes(value)),
                "group_uid" => hint.group_uid = value.parse::<u64>().ok(),
                "split_id" => hint.split_id = value.parse::<u64>().ok(),
                "tablet_uid" => hint.tablet_uid = value.parse::<u64>().ok(),
                _ => {}
            }
        }
    }

    hint
}

pub(crate) fn parse_read_request_block<'a, I: Iterator<Item = &'a str>>(
    lines: &mut Peekable<I>,
) -> ParsedReadRequest {
    let mut read = ParsedReadRequest::default();
    let mut depth = 1;
    let mut in_key_set = false;
    let mut in_ranges = false;
    let mut in_keys = false;
    let mut current_range = ParsedKeyRange::default();
    let mut current_key_tuple = Vec::new();
    let mut in_start_closed = false;
    let mut in_start_open = false;
    let mut in_end_closed = false;
    let mut in_end_open = false;

    for line in lines.by_ref() {
        let trimmed = line.trim();
        if trimmed.starts_with("key_set {") {
            in_key_set = true;
            depth += 1;
        } else if trimmed.starts_with("ranges {") {
            in_ranges = true;
            current_range = ParsedKeyRange::default();
            depth += 1;
        } else if trimmed.starts_with("keys {") {
            in_keys = true;
            current_key_tuple = Vec::new();
            depth += 1;
        } else if trimmed.starts_with("start_closed {") {
            in_start_closed = true;
            current_range.start = StartKeyType::Closed(Vec::new());
            depth += 1;
        } else if trimmed.starts_with("start_open {") {
            in_start_open = true;
            current_range.start = StartKeyType::Open(Vec::new());
            depth += 1;
        } else if trimmed.starts_with("end_closed {") {
            in_end_closed = true;
            current_range.end = EndKeyType::Closed(Vec::new());
            depth += 1;
        } else if trimmed.starts_with("end_open {") {
            in_end_open = true;
            current_range.end = EndKeyType::Open(Vec::new());
            depth += 1;
        } else if trimmed.ends_with('{') {
            depth += 1;
        } else if trimmed == "}" {
            depth -= 1;
            if in_start_closed && depth <= 3 {
                in_start_closed = false;
            } else if in_start_open && depth <= 3 {
                in_start_open = false;
            } else if in_end_closed && depth <= 3 {
                in_end_closed = false;
            } else if in_end_open && depth <= 3 {
                in_end_open = false;
            } else if in_ranges && depth <= 2 {
                read.key_set.ranges.push(mem::take(&mut current_range));
                in_ranges = false;
            } else if in_keys && depth <= 2 {
                read.key_set.keys.push(mem::take(&mut current_key_tuple));
                in_keys = false;
            } else if in_key_set && depth <= 1 {
                in_key_set = false;
            }
            if depth == 0 {
                break;
            }
        }

        if let Some((field, value)) = trimmed.split_once(':') {
            let field = field.trim();
            let value = value.trim().trim_matches('"');
            match field {
                "table" => read.table = value.to_string(),
                "index" => read.index = value.to_string(),
                "columns" => read.columns.push(value.to_string()),
                "all" => read.key_set.all = value == "true",
                "string_value" => {
                    update_range_boundary_value(
                        value,
                        &mut current_range,
                        in_start_closed,
                        in_start_open,
                        in_end_closed,
                        in_end_open,
                    );
                    if in_keys {
                        current_key_tuple.push(value.to_string());
                    }
                }
                "strong" if value == "true" => {
                    read.prefer_leader = true;
                }
                _ => {}
            }
        }
    }

    read
}

pub(crate) fn parse_sql_request_block<'a, I: Iterator<Item = &'a str>>(
    lines: &mut Peekable<I>,
) -> ParsedSqlRequest {
    let mut sql = ParsedSqlRequest::default();
    let mut depth = 1;
    let mut current_field_key = String::new();

    while let Some(line) = lines.next() {
        let trimmed = line.trim();
        if trimmed.starts_with("list_value {") {
            let mut list_elements = Vec::new();
            let mut list_depth = 1;
            for list_line in lines.by_ref() {
                let list_trimmed = list_line.trim();
                if list_trimmed.ends_with('{') {
                    list_depth += 1;
                } else if list_trimmed == "}" {
                    list_depth -= 1;
                    if list_depth == 0 {
                        break;
                    }
                } else if let Some(parameter_value) = parse_constant_value(list_trimmed) {
                    list_elements.push(parameter_value);
                }
            }
            if !current_field_key.is_empty() {
                sql.params.insert(
                    mem::take(&mut current_field_key),
                    JsonValue::Array(list_elements),
                );
            }
        } else if trimmed.ends_with('{') {
            depth += 1;
        } else if trimmed == "}" {
            depth -= 1;
            if depth == 0 {
                break;
            }
        } else if let Some((field, value)) = trimmed.split_once(':') {
            let field = field.trim();
            let value_unquoted = value.trim().trim_matches('"');
            match field {
                "sql" => sql.sql = value_unquoted.to_string(),
                "key" => current_field_key = value_unquoted.to_string(),
                "strong" if value_unquoted == "true" => {
                    sql.prefer_leader = true;
                }
                _ => {
                    if let Some(parameter_value) = parse_constant_value(trimmed)
                        && !current_field_key.is_empty()
                    {
                        sql.params
                            .insert(mem::take(&mut current_field_key), parameter_value);
                    }
                }
            }
        }
    }

    sql
}

pub(crate) fn parse_finder_event_block<'a, I: Iterator<Item = &'a str>>(
    lines: &mut Peekable<I>,
) -> Option<FinderEvent> {
    let mut name = String::new();
    let mut cache_update = None;
    let mut unhealthy_servers = Vec::new();
    let mut request = None;
    let mut expected_server = None;
    let mut expected_hint = None;
    let mut depth = 1;

    while let Some(line) = lines.next() {
        let trimmed = line.trim();
        if trimmed.starts_with("cache_update {") {
            cache_update = Some(parse_cache_update_block(lines));
        } else if trimmed.starts_with("read {") {
            request = Some(FinderRequest::Read(parse_read_request_block(lines)));
        } else if trimmed.starts_with("sql {") {
            request = Some(FinderRequest::Sql(parse_sql_request_block(lines)));
        } else if trimmed.starts_with("hint {") {
            expected_hint = Some(parse_routing_hint_block(lines));
        } else if trimmed.ends_with('{') {
            depth += 1;
        } else if trimmed == "}" {
            depth -= 1;
            if depth == 0 {
                break;
            }
        }

        if let Some((field, value)) = trimmed.split_once(':') {
            let field = field.trim();
            let value = value.trim().trim_matches('"');
            match field {
                "name" => name = value.to_string(),
                "server" => expected_server = Some(value.to_string()),
                "unhealthy_servers" | "unhealthy_server" => {
                    unhealthy_servers.push(value.to_string())
                }
                _ => {}
            }
        }
    }

    Some(FinderEvent {
        name,
        cache_update,
        unhealthy_servers,
        request,
        expected_server,
        expected_hint,
    })
}

pub(crate) fn parse_finder_test_case_block<'a, I: Iterator<Item = &'a str>>(
    lines: &mut Peekable<I>,
) -> Option<FinderTestCase> {
    let mut name = String::new();
    let mut events = Vec::new();
    let mut depth = 1;

    while let Some(line) = lines.next() {
        let trimmed = line.trim();
        if trimmed.starts_with("event {") {
            if let Some(event) = parse_finder_event_block(lines) {
                events.push(event);
            }
        } else if trimmed.ends_with('{') {
            depth += 1;
        } else if trimmed == "}" {
            depth -= 1;
            if depth == 0 {
                break;
            }
        }

        if let Some(case_name) = extract_value(trimmed, "name:") {
            name = case_name.to_string();
        }
    }

    if !name.is_empty() {
        Some(FinderTestCase { name, events })
    } else {
        None
    }
}

/// Parses `finder_test.textproto` fixture content.
pub(crate) fn parse_finder_golden_textproto(content: &str) -> Vec<FinderTestCase> {
    let mut cases = Vec::new();
    let mut lines = content.lines().peekable();

    while let Some(line) = lines.next() {
        let trimmed = line.trim();
        if trimmed.starts_with("test_case {")
            && let Some(case) = parse_finder_test_case_block(&mut lines)
        {
            cases.push(case);
        }
    }

    cases
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unescape_bytes_all_branches() {
        assert_eq!(unescape_bytes(""), Vec::<u8>::new());
        assert_eq!(unescape_bytes("hello world"), b"hello world");
        assert_eq!(
            unescape_bytes(r#"\n\r\t\\\""#),
            vec![b'\n', b'\r', b'\t', b'\\', b'\"']
        );
        assert_eq!(unescape_bytes(r"\z\a"), vec![b'z', b'a']);
        assert_eq!(unescape_bytes(r#"\001\377\07"#), vec![1, 255, 7]);
        assert_eq!(
            unescape_bytes(r"\x1b\x0a\x41\x4a\x9"),
            vec![0x1b, 0x0a, b'A', 0x4a, 0x09]
        );
        assert_eq!(unescape_bytes(r"\x"), b"x");
        assert_eq!(unescape_bytes(r"\xz"), b"xz");
        assert_eq!(unescape_bytes(r"\8"), b"8");
        assert_eq!(unescape_bytes(r"abc\"), b"abc");
    }

    #[test]
    fn try_parse_octal_escape_digits() {
        let mut bytes1 = b"5abc".iter().copied().peekable();
        assert_eq!(try_parse_octal_escape(&mut bytes1), Some(5));

        let mut bytes2 = b"77xyz".iter().copied().peekable();
        assert_eq!(try_parse_octal_escape(&mut bytes2), Some(63));

        let mut bytes3 = b"377xyz".iter().copied().peekable();
        assert_eq!(try_parse_octal_escape(&mut bytes3), Some(255));

        let mut non_octal = b"89".iter().copied().peekable();
        assert_eq!(try_parse_octal_escape(&mut non_octal), None);
    }

    #[test]
    fn try_parse_hex_escape_digits() {
        let mut bytes1 = b"1bxyz".iter().copied().peekable();
        assert_eq!(try_parse_hex_escape(&mut bytes1), Some(0x1b));

        let mut bytes2 = b"FFxyz".iter().copied().peekable();
        assert_eq!(try_parse_hex_escape(&mut bytes2), Some(0xff));

        let mut bytes3 = b"a".iter().copied().peekable();
        assert_eq!(try_parse_hex_escape(&mut bytes3), Some(0x0a));

        let mut non_hex = b"z1".iter().copied().peekable();
        assert_eq!(try_parse_hex_escape(&mut non_hex), None);
    }

    #[test]
    fn extract_value_prefixes_and_quotes() {
        assert_eq!(
            extract_value(r#"  name: "my_test"  "#, "name: "),
            Some("my_test")
        );
        assert_eq!(
            extract_value("  group_uid: 42  ", "group_uid: "),
            Some("42")
        );
        assert_eq!(extract_value("other_field: 1", "name: "), None);
    }

    #[test]
    fn parse_type_code_mappings() {
        assert_eq!(parse_type_code("STRING"), Some(TypeCode::String));
        assert_eq!(parse_type_code("INT64"), Some(TypeCode::Int64));
        assert_eq!(parse_type_code("FLOAT64"), Some(TypeCode::Float64));
        assert_eq!(parse_type_code("BOOL"), Some(TypeCode::Bool));
        assert_eq!(parse_type_code("BYTES"), Some(TypeCode::Bytes));
        assert_eq!(parse_type_code("TIMESTAMP"), Some(TypeCode::Timestamp));
        assert_eq!(parse_type_code("DATE"), Some(TypeCode::Date));
        assert_eq!(parse_type_code("UUID"), Some(TypeCode::Uuid));
        assert_eq!(parse_type_code("ENUM"), Some(TypeCode::Enum));
        assert_eq!(parse_type_code("UNKNOWN"), None);
    }

    #[test]
    fn parse_order_and_null_order() {
        assert_eq!(parse_order("ASCENDING"), Some(Order::Ascending));
        assert_eq!(parse_order("DESCENDING"), Some(Order::Descending));
        assert_eq!(parse_order("OTHER"), None);

        assert_eq!(parse_null_order("NULLS_FIRST"), Some(NullOrder::NullsFirst));
        assert_eq!(parse_null_order("NULLS_LAST"), Some(NullOrder::NullsLast));
        assert_eq!(parse_null_order("NOT_NULL"), Some(NullOrder::NotNull));
        assert_eq!(parse_null_order("OTHER"), None);
    }

    #[test]
    fn parse_constant_value_unit() {
        assert_eq!(
            parse_constant_value(r#"string_value: "val""#),
            Some(JsonValue::String("val".to_string()))
        );
        assert_eq!(
            parse_constant_value("number_value: 123"),
            Some(JsonValue::Number(123.into()))
        );
        assert_eq!(parse_constant_value("number_value: invalid"), None);
        assert_eq!(
            parse_constant_value("bool_value: true"),
            Some(JsonValue::Bool(true))
        );
        assert_eq!(
            parse_constant_value("bool_value: false"),
            Some(JsonValue::Bool(false))
        );
        assert_eq!(parse_constant_value("other: 1"), None);
    }

    #[test]
    fn parse_part_block_unit() {
        let text = r#"
            tag: 50020
            order: DESCENDING
            null_order: NOT_NULL
            code: BYTES
            identifier: "Col1"
            struct_identifiers: 1
            struct_identifiers: 2
        "#;
        let mut lines = text.lines().peekable();
        let part = parse_part_block(&mut lines);
        assert_eq!(part.tag, 50020);
        assert_eq!(part.order, Order::Descending);
        assert_eq!(part.null_order, NullOrder::NotNull);
        assert_eq!(part.identifier().map(String::as_str), Some("Col1"));
        assert_eq!(&part.struct_identifiers, &[1, 2]);

        let random_part_text = r#"
            random: true
        "#;
        let mut random_lines = random_part_text.lines().peekable();
        let random_part = parse_part_block(&mut random_lines);
        assert_eq!(random_part.random(), Some(&true));

        let value_part_text = r#"
            string_value: "default"
        "#;
        let mut value_lines = value_part_text.lines().peekable();
        let value_part = parse_part_block(&mut value_lines);
        assert_eq!(
            value_part.value().map(|v| &**v),
            Some(&JsonValue::String("default".to_string()))
        );
    }

    #[test]
    fn parse_recipe_block_unit() {
        let table_recipe_text = r#"
            table_name: "MyTable"
            part {
                tag: 1
            }
            part {
                code: INT64
                identifier: "Id"
            }
        "#;
        let mut lines = table_recipe_text.lines().peekable();
        let table_recipe = parse_recipe_block(&mut lines);
        assert_eq!(
            table_recipe.table_name().map(String::as_str),
            Some("MyTable")
        );
        assert_eq!(table_recipe.part.len(), 2);

        let index_recipe_text = r#"
            index_name: "MyIndex"
        "#;
        let mut index_lines = index_recipe_text.lines().peekable();
        let index_recipe = parse_recipe_block(&mut index_lines);
        assert_eq!(
            index_recipe.index_name().map(String::as_str),
            Some("MyIndex")
        );

        let op_recipe_text = r#"
            operation_uid: 999
        "#;
        let mut op_lines = op_recipe_text.lines().peekable();
        let op_recipe = parse_recipe_block(&mut op_lines);
        assert_eq!(op_recipe.target, Some(Target::OperationUid(999)));
    }

    #[test]
    fn parse_recipes_block_unit() {
        let text = r#"
            schema_generation: "\001\002"
            recipe {
                table_name: "T1"
            }
        "#;
        let mut lines = text.lines().peekable();
        let list = parse_recipes_block(&mut lines);
        assert_eq!(list.schema_generation.as_ref(), &[1, 2]);
        assert_eq!(list.recipe.len(), 1);
    }

    #[test]
    fn parse_tablet_block_unit() {
        let text = r#"
            tablet_uid: 42
            server_address: "localhost:15000"
            location: "us-central1"
            role: READ_ONLY
            incarnation: "\001"
            distance: 10
            skip: true
        "#;
        let mut lines = text.lines().peekable();
        let tablet = parse_tablet_block(&mut lines);
        assert_eq!(tablet.tablet_uid, 42);
        assert_eq!(tablet.server_address, "localhost:15000");
        assert_eq!(tablet.location, "us-central1");
        assert_eq!(tablet.role, Role::ReadOnly);
        assert_eq!(tablet.incarnation.as_ref(), &[1]);
        assert_eq!(tablet.distance, 10);
        assert!(tablet.skip);
    }

    #[test]
    fn parse_cache_update_block_unit() {
        let text = r#"
            database_id: 123456
            range {
                start_key: "start"
                limit_key: "limit"
                group_uid: 100
                split_id: 200
                generation: "\001"
            }
            group {
                group_uid: 100
                generation: "\002"
                leader_index: 0
                tablets {
                    tablet_uid: 10
                    server_address: "localhost:15000"
                    role: READ_WRITE
                }
            }
            key_recipes {
                schema_generation: "\003"
            }
        "#;
        let mut lines = text.lines().peekable();
        let update = parse_cache_update_block(&mut lines);
        assert_eq!(update.database_id, 123456);
        assert_eq!(update.range.len(), 1);
        assert_eq!(update.range[0].group_uid, 100);
        assert_eq!(update.range[0].split_id, 200);
        assert_eq!(update.range[0].generation.as_ref(), &[1]);
        assert_eq!(update.group.len(), 1);
        assert_eq!(update.group[0].generation.as_ref(), &[2]);
        assert_eq!(update.group[0].leader_index, 0);
        assert_eq!(update.group[0].tablets.len(), 1);
        assert_eq!(update.group[0].tablets[0].role, Role::ReadWrite);
        assert!(update.key_recipes.is_some());
    }

    #[test]
    fn parse_mutation_block_operations() {
        let insert_text = r#"
            insert {
                table: "T1"
                columns: "C1"
                values {
                    string_value: "val1"
                    number_value: 12.0
                }
            }
        "#;
        let mut lines = insert_text.lines().peekable();
        let mutation = parse_mutation_block(&mut lines).expect("insert mutation");
        assert!(mutation.insert().is_some());

        let update_text = r#"
            update {
                table: "T1"
                columns: "C1"
                values { string_value: "val" }
            }
        "#;
        let mut lines = update_text.lines().peekable();
        let mutation = parse_mutation_block(&mut lines).expect("update mutation");
        assert!(mutation.update().is_some());

        let insert_or_update_text = r#"
            insert_or_update {
                table: "T1"
                columns: "C1"
                values { string_value: "val" }
            }
        "#;
        let mut lines = insert_or_update_text.lines().peekable();
        let mutation = parse_mutation_block(&mut lines).expect("insert_or_update mutation");
        assert!(mutation.insert_or_update().is_some());

        let replace_text = r#"
            replace {
                table: "T1"
                columns: "C1"
                values { string_value: "val" }
            }
        "#;
        let mut lines = replace_text.lines().peekable();
        let mutation = parse_mutation_block(&mut lines).expect("replace mutation");
        assert!(mutation.replace().is_some());

        let delete_text = r#"
            delete {
                table: "T1"
                all: true
                keys { string_value: "k1" number_value: 2 }
                key { string_value: "k2" }
                start_closed { string_value: "sc" }
                start_open { string_value: "so" }
            }
        "#;
        let mut lines = delete_text.lines().peekable();
        let mutation = parse_mutation_block(&mut lines).expect("delete mutation");
        assert!(mutation.delete().is_some());

        let send_text = r#"
            send {
                queue: "Q1"
                key { string_value: "k1" }
            }
        "#;
        let mut lines = send_text.lines().peekable();
        let mutation = parse_mutation_block(&mut lines).expect("send mutation");
        assert!(mutation.send().is_some());

        let ack_text = r#"
            ack {
                queue: "Q2"
                key { string_value: "k2" }
            }
        "#;
        let mut lines = ack_text.lines().peekable();
        let mutation = parse_mutation_block(&mut lines).expect("ack mutation");
        assert!(mutation.ack().is_some());
    }

    #[test]
    fn parse_recipe_test_block_and_golden() {
        let text = r#"
            test_case {
                name: "TestCase1"
                part {
                    tag: 1
                }
                test {
                    key {
                        string_value: "k1"
                        bool_value: true
                        number_value: 42
                        null_value: NULL_VALUE
                    }
                    start: "\001\002"
                    approximate: true
                }
                test {
                    query_params {
                        key: "p1"
                        string_value: "v1"
                        key: "p2"
                        bool_value: false
                        key: "p3"
                        number_value: 123.456
                        key: "p4"
                        null_value: NULL_VALUE
                    }
                    start: "\003"
                }
                test {
                    mutation {
                        insert {
                            table: "T"
                        }
                    }
                    start: "\004"
                }
            }
        "#;
        let cases = parse_recipe_golden_textproto(text);
        assert_eq!(cases.len(), 1);
        assert_eq!(cases[0].name, "TestCase1");
        assert_eq!(cases[0].tests.len(), 3);
        assert_eq!(cases[0].tests[0].values.len(), 4);
        assert!(cases[0].tests[0].approximate);
        assert!(cases[0].tests[1].query_params.is_some());
        assert!(cases[0].tests[2].mutation.is_some());
    }

    #[test]
    fn parse_range_cache_golden_and_query_tests() {
        let text = r#"
            test_case {
                name: "RangeCacheTest1"
                step {
                    update {
                        database_id: 100
                    }
                    test {
                        key: "start_k"
                        limit_key: "limit_k"
                        min_cache_entries_for_random_pick: 5
                        range_mode: PICK_RANDOM
                        leader: true
                        server: "localhost:1000"
                        directed_read_options {
                            include_replicas {
                                replica_selection {
                                    location: "us-central1"
                                }
                            }
                        }
                        result {
                            key: "r_key"
                            limit_key: "r_limit"
                            group_uid: 10
                            split_id: 20
                            tablet_uid: 30
                        }
                    }
                }
            }
        "#;
        let cases = parse_range_cache_golden_textproto(text);
        assert_eq!(cases.len(), 1);
        assert_eq!(cases[0].name, "RangeCacheTest1");
        assert_eq!(cases[0].steps.len(), 1);
        let query_test = &cases[0].steps[0].tests[0];
        assert_eq!(query_test.key.as_deref(), Some(b"start_k".as_ref()));
        assert_eq!(query_test.min_cache_entries_for_random_pick, Some(5));
        assert_eq!(query_test.range_mode, RangeMode::PickRandom);
        assert!(query_test.leader);
        assert_eq!(
            query_test.expected_server.as_deref(),
            Some("localhost:1000")
        );
        assert_eq!(query_test.expected_result.group_uid, Some(10));
        assert_eq!(query_test.expected_result.split_id, Some(20));
        assert_eq!(query_test.expected_result.tablet_uid, Some(30));
    }

    #[test]
    fn update_range_boundary_values_all_variants() {
        let mut range_closed = ParsedKeyRange {
            start: StartKeyType::Closed(Vec::new()),
            end: EndKeyType::Unspecified,
        };
        update_range_boundary_value("c1", &mut range_closed, true, false, false, false);
        assert_eq!(
            range_closed.start,
            StartKeyType::Closed(vec!["c1".to_string()])
        );

        let mut range_open = ParsedKeyRange {
            start: StartKeyType::Open(Vec::new()),
            end: EndKeyType::Unspecified,
        };
        update_range_boundary_value("o1", &mut range_open, false, true, false, false);
        assert_eq!(range_open.start, StartKeyType::Open(vec!["o1".to_string()]));

        let mut range_end_closed = ParsedKeyRange {
            start: StartKeyType::Unspecified,
            end: EndKeyType::Closed(Vec::new()),
        };
        update_range_boundary_value("ec1", &mut range_end_closed, false, false, true, false);
        assert_eq!(
            range_end_closed.end,
            EndKeyType::Closed(vec!["ec1".to_string()])
        );

        let mut range_end_open = ParsedKeyRange {
            start: StartKeyType::Unspecified,
            end: EndKeyType::Open(Vec::new()),
        };
        update_range_boundary_value("eo1", &mut range_end_open, false, false, false, true);
        assert_eq!(
            range_end_open.end,
            EndKeyType::Open(vec!["eo1".to_string()])
        );
    }

    #[test]
    fn parse_routing_hint_block_all_fields() {
        let text = r#"
            operation_uid: 5
            database_id: 999
            schema_generation: "\001\002"
            key: "test_key"
            limit_key: "limit_key"
            group_uid: 42
            split_id: 84
            tablet_uid: 100
            skipped_tablet_uid {
                tablet_uid: 200
                incarnation: "\005"
            }
        "#;
        let mut lines = text.lines().peekable();
        let hint = parse_routing_hint_block(&mut lines);
        assert_eq!(hint.operation_uid, Some(5));
        assert_eq!(hint.database_id, Some(999));
        assert_eq!(hint.schema_generation, Some(vec![1, 2]));
        assert_eq!(hint.key, Some(b"test_key".to_vec()));
        assert_eq!(hint.limit_key, Some(b"limit_key".to_vec()));
        assert_eq!(hint.group_uid, Some(42));
        assert_eq!(hint.split_id, Some(84));
        assert_eq!(hint.tablet_uid, Some(100));
        assert_eq!(hint.skipped_tablet_uids.len(), 1);
        assert_eq!(hint.skipped_tablet_uids[0].tablet_uid, 200);
        assert_eq!(hint.skipped_tablet_uids[0].incarnation, Some(vec![5]));
    }

    #[test]
    fn parse_read_request_block_unit() {
        let text = r#"
            table: "MyTable"
            index: "MyIndex"
            columns: "col1"
            columns: "col2"
            strong: true
            key_set {
                all: true
                keys {
                    string_value: "k1"
                }
                ranges {
                    start_closed {
                        string_value: "sc"
                    }
                    end_open {
                        string_value: "eo"
                    }
                }
            }
        "#;
        let mut lines = text.lines().peekable();
        let req = parse_read_request_block(&mut lines);
        assert_eq!(req.table, "MyTable");
        assert_eq!(req.index, "MyIndex");
        assert_eq!(req.columns, &["col1", "col2"]);
        assert!(req.prefer_leader);
        assert!(req.key_set.all);
        assert_eq!(req.key_set.keys.len(), 1);
        assert_eq!(req.key_set.ranges.len(), 1);
    }

    #[test]
    fn parse_sql_request_block_unit() {
        let text = r#"
            sql: "SELECT * FROM Table WHERE id = @id AND tags = @tags"
            strong: true
            key: "id"
            number_value: 42
            key: "tags"
            list_value {
                string_value: "tag1"
                string_value: "tag2"
            }
        "#;
        let mut lines = text.lines().peekable();
        let sql = parse_sql_request_block(&mut lines);
        assert_eq!(
            sql.sql,
            "SELECT * FROM Table WHERE id = @id AND tags = @tags"
        );
        assert!(sql.prefer_leader);
        assert_eq!(sql.params.get("id"), Some(&JsonValue::Number(42.into())));
        assert_eq!(
            sql.params.get("tags"),
            Some(&JsonValue::Array(vec![
                JsonValue::String("tag1".to_string()),
                JsonValue::String("tag2".to_string()),
            ]))
        );
    }

    #[test]
    fn parse_finder_golden_textproto_unit() {
        let text = r#"
            test_case {
                name: "FinderCase1"
                event {
                    name: "Event1"
                    unhealthy_servers: "localhost:15000"
                    unhealthy_server: "localhost:15001"
                    cache_update {
                        database_id: 10
                    }
                    read {
                        table: "T"
                    }
                    server: "localhost:15002"
                    hint {
                        operation_uid: 1
                    }
                }
                event {
                    name: "Event2"
                    sql {
                        sql: "SELECT 1"
                    }
                }
            }
        "#;
        let cases = parse_finder_golden_textproto(text);
        assert_eq!(cases.len(), 1);
        assert_eq!(cases[0].name, "FinderCase1");
        assert_eq!(cases[0].events.len(), 2);
        assert_eq!(cases[0].events[0].unhealthy_servers.len(), 2);
        assert!(matches!(
            cases[0].events[0].request,
            Some(FinderRequest::Read(_))
        ));
        assert!(matches!(
            cases[0].events[1].request,
            Some(FinderRequest::Sql(_))
        ));
    }

    #[test]
    fn skip_block_nested() {
        let text = r#"
            nested {
                sub_nested {
                    field: 123
                }
            }
            after: true
        "#;
        let mut lines = text.lines().peekable();
        let first = lines.next().expect("first line");
        assert!(first.trim().is_empty());
        let _nested_start = lines.next().expect("nested start");
        skip_block(&mut lines);
        let after_line = lines.next().expect("after line");
        assert_eq!(extract_value(after_line, "after:"), Some("true"));
    }

    #[test]
    fn json_to_spanner_value_conversions() {
        let bool_json = JsonValue::Bool(true);
        assert_eq!(json_to_spanner_value(&bool_json), Value::from(true));

        let int_json = serde_json::json!(42);
        assert_eq!(json_to_spanner_value(&int_json), Value::from(42i64));

        let float_json = serde_json::json!(42.5);
        assert_eq!(json_to_spanner_value(&float_json), Value::from(42.5f64));

        let string_int_json = JsonValue::String("100".to_string());
        assert_eq!(json_to_spanner_value(&string_int_json), Value::from(100i64));

        let string_text_json = JsonValue::String("hello".to_string());
        assert_eq!(
            json_to_spanner_value(&string_text_json),
            Value::from("hello")
        );

        let null_json = JsonValue::Null;
        assert_eq!(json_to_spanner_value(&null_json), Value::null());

        let array_json = JsonValue::Array(vec![]);
        assert_eq!(json_to_spanner_value(&array_json), Value::null());
    }

    #[test]
    fn parse_constant_value_variants() {
        assert_eq!(
            parse_constant_value("string_value: \"hello\""),
            Some(JsonValue::String("hello".to_string()))
        );
        assert_eq!(
            parse_constant_value("number_value: 42"),
            Some(serde_json::json!(42))
        );
        assert_eq!(
            parse_constant_value("number_value: 42.5"),
            Some(serde_json::json!(42.5))
        );
        assert_eq!(
            parse_constant_value("bool_value: true"),
            Some(JsonValue::Bool(true))
        );
        assert_eq!(
            parse_constant_value("bool_value: false"),
            Some(JsonValue::Bool(false))
        );
        assert_eq!(
            parse_constant_value("null_value: NULL_VALUE"),
            Some(JsonValue::Null)
        );
        assert_eq!(parse_constant_value("null_value: 0"), Some(JsonValue::Null));
        assert_eq!(parse_constant_value("unknown_field: \"foo\""), None);
    }

    #[test]
    fn debug_and_clone_implementations() {
        let req = ParsedReadRequest::default();
        let _ = format!("{req:?}");
        let _ = req.key_set.clone();

        let sql = ParsedSqlRequest::default();
        let _ = format!("{sql:?}");

        let hint = ParsedRoutingHint::default();
        let _ = format!("{hint:?}");
        let _ = hint.clone();

        let tablet_uid = ParsedTabletUid::default();
        let _ = format!("{tablet_uid:?}");
        let _ = tablet_uid.clone();

        let range = ParsedKeyRange::default();
        let _ = format!("{range:?}");
        let _ = range.clone();

        let res = RangeCacheExpectedResult::default();
        let _ = format!("{res:?}");
    }

    #[test]
    fn parse_mutation_block_detailed_delete_and_queues() {
        let delete_text = r#"
            delete {
                table: "Users"
                all: true
                keys {
                    string_value: "k1"
                    number_value: 100
                }
                keys {
                    string_value: "single_key"
                }
                ranges {
                    start_closed {
                        string_value: "c1"
                        number_value: 200
                    }
                    start_open {
                        string_value: "o1"
                        number_value: 300
                    }
                }
            }
        "#;
        let mut lines = delete_text.lines().peekable();
        let mutation = parse_mutation_block(&mut lines).expect("delete mutation");
        let delete = mutation.delete().expect("delete operation");
        assert_eq!(delete.table, "Users");
        let key_set = delete.key_set.as_ref().expect("key_set present");
        assert!(key_set.all);
        assert_eq!(key_set.keys.len(), 2);
        assert_eq!(key_set.ranges.len(), 1);

        let send_text = r#"
            send {
                queue: "Tasks"
                key {
                    string_value: "task_1"
                }
            }
        "#;
        let mut lines = send_text.lines().peekable();
        let mutation = parse_mutation_block(&mut lines).expect("send mutation");
        let send = mutation.send().expect("send operation");
        assert_eq!(send.queue, "Tasks");
        assert_eq!(send.key.as_ref().expect("send key").len(), 1);

        let ack_text = r#"
            ack {
                queue: "Completed"
                key {
                    string_value: "ack_1"
                }
            }
        "#;
        let mut lines = ack_text.lines().peekable();
        let mutation = parse_mutation_block(&mut lines).expect("ack mutation");
        let ack = mutation.ack().expect("ack operation");
        assert_eq!(ack.queue, "Completed");
        assert_eq!(ack.key.as_ref().expect("ack key").len(), 1);
    }

    #[test]
    fn parse_read_request_block_range_boundary_variants() {
        let text = r#"
            table: "Accounts"
            index: "AccountsByBranch"
            columns: "balance"
            strong: false
            key_set {
                ranges {
                    start_open {
                        string_value: "a"
                    }
                    end_closed {
                        string_value: "z"
                    }
                }
            }
        "#;
        let mut lines = text.lines().peekable();
        let req = parse_read_request_block(&mut lines);
        assert_eq!(req.table, "Accounts");
        assert_eq!(req.index, "AccountsByBranch");
        assert_eq!(req.columns, &["balance"]);
        assert!(!req.prefer_leader);
        assert_eq!(req.key_set.ranges.len(), 1);
        assert!(matches!(req.key_set.ranges[0].start, StartKeyType::Open(_)));
        assert!(matches!(req.key_set.ranges[0].end, EndKeyType::Closed(_)));
    }

    #[test]
    fn parse_sql_request_block_all_json_types() {
        let text = r#"
            sql: "SELECT 1 WHERE a = @a AND b = @b AND c = @c AND d = @d"
            strong: false
            key: "a"
            string_value: "str"
            key: "b"
            bool_value: true
            key: "c"
            number_value: 123.456
            key: "d"
            null_value: NULL_VALUE
        "#;
        let mut lines = text.lines().peekable();
        let sql = parse_sql_request_block(&mut lines);
        assert_eq!(
            sql.sql,
            "SELECT 1 WHERE a = @a AND b = @b AND c = @c AND d = @d"
        );
        assert!(!sql.prefer_leader);
        assert_eq!(
            sql.params.get("a"),
            Some(&JsonValue::String("str".to_string()))
        );
        assert_eq!(sql.params.get("b"), Some(&JsonValue::Bool(true)));
        assert_eq!(sql.params.get("c"), Some(&serde_json::json!(123.456)));
        assert_eq!(sql.params.get("d"), Some(&JsonValue::Null));
    }
}
