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

use super::wire_format::{
    bool_field, bytes_field, double_field, encode_bool, encode_bytes, encode_double, encode_float,
    encode_int64, encode_length_delimited, encode_string, float_field, int64_field, message_field,
    repeated_field, string_field,
};
use crate::datatypes::{Interval, Range, RangeElement};
use crate::error::ConvertError;
use crate::model::ProtoSchema;
use bytes::Bytes;
use std::any::TypeId;
use std::borrow::Cow;
use std::collections::HashMap;
use std::io::Write;
use wkt::{DescriptorProto, FieldDescriptorProto};

/// A trait for converting Rust types into rows for the [Proto] data format.
///
/// A writer that uses the [Proto] data format needs two things: a schema,
/// which describes the rows, and the rows themselves, encoded as protocol
/// buffers. Types that implement this trait provide both.
///
/// Use [`#[derive(ToRow)]`](derive@crate::write::ToRow) to implement this
/// trait. Each field is written to the column with the same name, without
/// any `r#` prefix, so `r#type` writes to the column `type`. Use
/// `#[bigquery(rename = "column_name")]` when the names differ.
///
/// Column names must start with an ASCII letter or `_`, and contain only
/// ASCII letters, digits, and `_`, because they are also the names of
/// protobuf fields. Two fields cannot write to the same column, and BigQuery
/// column names are case-insensitive, so `a` and `A` are the same column.
/// Both are compile errors.
///
/// # Supported types
///
/// | Rust type | BigQuery type |
/// | --- | --- |
/// | `bool` | `BOOL` |
/// | `i32`, `i64` | `INT64` |
/// | `f32`, `f64` | `FLOAT64` |
/// | [`rust_decimal::Decimal`], [`google_cloud_type::model::Decimal`] | `NUMERIC`, `BIGNUMERIC` |
/// | `String` | `STRING`, `GEOGRAPHY` |
/// | `Vec<u8>`, [`Bytes`] | `BYTES` |
/// | [`Date`](google_cloud_type::model::Date) | `DATE` |
/// | [`DateTime`](google_cloud_type::model::DateTime) | `DATETIME` |
/// | [`TimeOfDay`](google_cloud_type::model::TimeOfDay) | `TIME` |
/// | [`Timestamp`](wkt::Timestamp) | `TIMESTAMP` |
/// | [`Interval`] | `INTERVAL` |
/// | [`Value`](wkt::Value), [`Struct`](wkt::Struct) | `JSON` |
/// | [`Range<T>`] | `RANGE<T>` |
/// | A struct with `#[derive(ToRow)]` | `STRUCT` |
/// | `Option<T>` | The type for `T`. `None` writes `NULL`. |
/// | `Vec<T>` | An `ARRAY` of the type for `T`. |
///
/// Some types have limits:
///
/// - BigQuery keeps microseconds, so any nanoseconds are rounded down, or
///   toward zero in an [`Interval`].
/// - Dates must have a year, a month, and a day, between the years 1 and
///   9999. [`to_row`](ToRow::to_row) returns an error for other dates, and
///   for times such as `24:00:00` or leap seconds.
/// - A [`DateTime`](google_cloud_type::model::DateTime) with a time zone or a
///   UTC offset is an error. Use a [`Timestamp`](wkt::Timestamp) instead.
/// - BigQuery keeps an `INTERVAL` as three parts: months, days, and time. It
///   combines the fields of an [`Interval`] in each part, so `minutes: 90`
///   reads back as `hours: 1, minutes: 30`. The parts can be at most 10,000
///   years, 3,660,000 days, and 87,840,000 hours, positive or negative.
///   [`to_row`](ToRow::to_row) returns an error for longer intervals.
/// - A `NUMERIC` value has up to 38 digits, 9 of them after the decimal
///   point. A `BIGNUMERIC` value has up to 76 digits, 38 of them after the
///   decimal point. A [`rust_decimal::Decimal`] holds about 28 digits, so use
///   a [`google_cloud_type::model::Decimal`] for longer values. BigQuery
///   rejects values that do not fit the column.
/// - A [`google_cloud_type::model::Decimal`] must hold a decimal number, such
///   as `-1.5` or `2.5E+8`. An empty string writes `0`, as the type
///   documents. [`to_row`](ToRow::to_row) returns an error for other strings.
/// - A `GEOGRAPHY` value is a `String` in the WKT or GeoJSON format, such as
///   `POINT(1 2)`.
/// - `Value::Null` writes the JSON value `null`, not a SQL `NULL`.
/// - BigQuery arrays cannot be `NULL`, and cannot contain `NULL` values or
///   other arrays. So an `Option<T>` or a `Vec<T>` cannot hold an `Option`,
///   or a `Vec` other than `Vec<u8>`, which is a `BYTES` value. An empty
///   `Vec<T>` writes an empty array.
/// - A [`Range<T>`] without a `start` or an `end` is unbounded on that side.
///   When it has both, the start must be before the end, after rounding down
///   any nanoseconds. [`to_row`](ToRow::to_row) returns an error for other
///   ranges, which BigQuery SQL cannot create either.
/// - Structs can be nested at most 14 levels deep. BigQuery limits the depth
///   of a schema to 15, and the innermost field counts too. Structs cannot
///   contain themselves, not even in a `Vec<T>`.
///
/// # Example
///
/// ```
/// # use google_cloud_bigquery::client::Write;
/// # use google_cloud_bigquery::model::ProtoRows;
/// use google_cloud_bigquery::write::ToRow;
///
/// #[derive(ToRow)]
/// struct Row {
///     name: String,
///     age: Option<i64>,
/// }
///
/// # async fn sample(client: Write) -> anyhow::Result<()> {
/// let writer = client
///     .open_default_stream("projects/my-project/datasets/my_dataset/tables/my_table")
///     .build_proto(Row::schema())
///     .await?;
/// let rows = [
///     Row { name: "alice".to_string(), age: Some(30) },
///     // `None` writes `NULL` to the `age` column.
///     Row { name: "bob".to_string(), age: None },
/// ];
/// let serialized_rows = rows.iter().map(Row::to_row).collect::<Result<Vec<_>, _>>()?;
/// writer
///     .append(ProtoRows::new().set_serialized_rows(serialized_rows))
///     .send()
///     .await?;
/// # Ok(())
/// # }
/// ```
///
/// # Nested structs and ranges
///
/// A struct with `#[derive(ToRow)]` can be a field in another one, for a
/// `STRUCT` column.
///
/// ```
/// use google_cloud_bigquery::datatypes::Range;
/// use google_cloud_bigquery::write::ToRow;
/// use google_cloud_type::model::Date;
///
/// // For a `STRUCT<city STRING, zip STRING>` column.
/// #[derive(ToRow)]
/// struct Address {
///     city: String,
///     zip: Option<String>,
/// }
///
/// #[derive(ToRow)]
/// struct Customer {
///     name: String,
///     // `None` writes a `NULL` struct.
///     address: Option<Address>,
///     // For an `ARRAY<STRUCT<city STRING, zip STRING>>` column.
///     previous_addresses: Vec<Address>,
///     // For a `RANGE<DATE>` column.
///     membership: Range<Date>,
/// }
///
/// # fn main() -> anyhow::Result<()> {
/// let start = Date::new().set_year(2025).set_month(1).set_day(1);
/// let customer = Customer {
///     name: "alice".to_string(),
///     address: Some(Address { city: "Paris".to_string(), zip: None }),
///     previous_addresses: Vec::new(),
///     // From 2025-01-01, with no end.
///     membership: Range::new().set_start(start),
/// };
/// // Use them as in the example above.
/// let schema = Customer::schema();
/// let row = customer.to_row()?;
/// # Ok(())
/// # }
/// ```
///
/// [Proto]: crate::write::format::Proto
pub trait ToRow {
    /// Returns the schema for rows of this type.
    ///
    /// Use it to create a writer with [`build_proto`].
    ///
    /// # Panics
    ///
    /// If structs are nested more than 14 levels deep, or if a struct contains
    /// itself. Both depend only on the types, not on any values, so any test
    /// that calls this function finds them.
    ///
    /// [`build_proto`]: crate::builder::write::WriterBuilder::build_proto
    fn schema() -> ProtoSchema {
        message_schema::<Self>()
    }

    /// Encodes `self` as one row, in the protobuf wire format.
    ///
    /// Add the rows to a request with [`set_serialized_rows`].
    ///
    /// # Errors
    ///
    /// If a value is one that BigQuery cannot store, such as a date without a
    /// year, or a range that does not start before it ends. The limits for
    /// each type are in the [ToRow] documentation.
    ///
    /// [`set_serialized_rows`]: crate::model::ProtoRows::set_serialized_rows
    fn to_row(&self) -> Result<Bytes, ConvertError> {
        encode_row(self)
    }

    /// Visits the fields of this type, in order.
    ///
    /// `#[derive(ToRow)]` implements this function, with one call for each
    /// field. [`schema`] calls it to describe the fields, and [`to_row`]
    /// calls it to encode them. Applications do not need to call it.
    ///
    /// [`schema`]: ToRow::schema
    /// [`to_row`]: ToRow::to_row
    fn visit_fields(fields: &mut StructFields<'_, Self>) -> Result<(), ConvertError>;
}

/// A Rust type that can be a field in a row.
///
/// `#[derive(ToRow)]` uses this trait to describe and encode each field of a
/// struct. The library implements it for the types in the [ToRow]
/// documentation, and `#[derive(ToRow)]` implements it for each struct, so a
/// struct can be a field in another one.
///
/// In a generic struct, `#[derive(ToRow)]` adds this trait as a bound to each
/// type parameter.
///
/// <div class="warning">
///
/// **Do not implement this trait directly.**
///
/// This trait is not intended for manual implementation. Its functions may
/// change in future versions, which would break manual implementations. Use
/// `#[derive(ToRow)]` for your structs instead.
///
/// </div>
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a supported field type for `#[derive(ToRow)]`",
    label = "unsupported field type",
    note = "structs must have `#[derive(ToRow)]` to be used as fields",
    note = "see the `ToRow` documentation for the supported types"
)]
pub trait ProtoValue {
    /// Describes a field of this type.
    ///
    /// `#[derive(ToRow)]` uses this function. Applications do not need to call
    /// it.
    fn describe(field: FieldSchema<'_>);

    /// Encodes `self` as a field.
    ///
    /// BigQuery reads a missing field as `NULL`, so implementations encode the
    /// field even for default values such as `""` or `0`. Only `None` and
    /// empty vectors leave the field out.
    ///
    /// `#[derive(ToRow)]` uses this function. Applications do not need to call
    /// it.
    fn encode(&self, field: FieldEncoder<'_>) -> Result<(), ConvertError>;
}

/// A Rust type that can be inside an `Option<T>` or a `Vec<T>` field.
///
/// BigQuery arrays cannot be `NULL`, and cannot contain `NULL` values or other
/// arrays. And an `Option<Option<T>>` would write `Some(None)` and `None` the
/// same way, as `NULL`. So `Option<T>` and `Vec<T>` do not implement this
/// trait. `Vec<u8>` does, because it is a `BYTES` value, not an array. Structs
/// with `#[derive(ToRow)]` implement it too.
///
/// In a generic struct, use this trait as the bound for the type inside an
/// `Option<T>` or a `Vec<T>` field:
///
/// ```
/// use google_cloud_bigquery::write::{ProtoElement, ToRow};
///
/// #[derive(ToRow)]
/// struct Batch<T: ProtoElement> {
///     first: Option<T>,
///     items: Vec<T>,
/// }
///
/// let schema = Batch::<i64>::schema();
/// assert!(schema.proto_descriptor.is_some());
/// ```
///
/// Like [ProtoValue], this trait is not intended for manual implementation.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a supported type inside `Option<T>` or `Vec<T>` for `#[derive(ToRow)]`",
    label = "unsupported type inside `Option<T>` or `Vec<T>`",
    note = "`Option<T>` and `Vec<T>` cannot hold an `Option<T>`, or a `Vec<T>` other than `Vec<u8>`",
    note = "BigQuery arrays cannot be `NULL`, and cannot contain `NULL` values or other arrays",
    note = "structs must have `#[derive(ToRow)]` to be inside `Option<T>` or `Vec<T>`",
    note = "see the `ToRow` documentation for the supported types"
)]
pub trait ProtoElement: ProtoValue {}

/// The number of the first field in a message. Protobuf numbers fields from 1.
const FIRST_FIELD_NUMBER: u32 = 1;

/// The fields of a struct, as `#[derive(ToRow)]` visits them.
///
/// [`ToRow::visit_fields`] receives this type. Each call to [`field`] visits
/// the next field: it describes the field for the schema, or encodes it for a
/// row. So the fields are numbered in the order of the calls, and the schema
/// and the rows always agree.
///
/// [`field`]: StructFields::field
#[derive(Debug)]
pub struct StructFields<'a, R: ?Sized> {
    mode: FieldsMode<'a, R>,
    /// The number of the next field.
    next: u32,
}

/// What a [StructFields] does with each field.
#[derive(Debug)]
enum FieldsMode<'a, R: ?Sized> {
    /// Collects the column names, without describing the fields.
    Names(&'a mut Vec<&'static str>),
    /// Describes the fields, and adds the message types they need to `types`.
    Describe {
        types: &'a mut NestedTypes,
        fields: Vec<FieldDescriptorProto>,
    },
    /// Encodes the fields of `row`, and appends them to `buf`.
    Encode { row: &'a R, buf: &'a mut Vec<u8> },
}

impl<'a, R: ?Sized> StructFields<'a, R> {
    /// Returns a visitor that only collects the column names in `names`.
    fn names(names: &'a mut Vec<&'static str>) -> Self {
        Self::new(FieldsMode::Names(names))
    }

    /// Returns a visitor that describes the fields, and adds the message
    /// types they need to `types`.
    fn describe(types: &'a mut NestedTypes) -> Self {
        Self::new(FieldsMode::Describe {
            types,
            fields: Vec::new(),
        })
    }

    /// Returns a visitor that encodes the fields of `row`, and appends them to
    /// `buf`.
    fn encode(row: &'a R, buf: &'a mut Vec<u8>) -> Self {
        Self::new(FieldsMode::Encode { row, buf })
    }

    fn new(mode: FieldsMode<'a, R>) -> Self {
        Self {
            mode,
            next: FIRST_FIELD_NUMBER,
        }
    }

    /// Visits the next field, with the given column name and type. When
    /// encoding a row, `get` returns the value of the field.
    ///
    /// `#[derive(ToRow)]` uses this function. Applications do not need to call
    /// it.
    pub fn field<T: ProtoValue>(
        &mut self,
        name: &'static str,
        get: impl FnOnce(&R) -> &T,
    ) -> Result<(), ConvertError> {
        let number = self.next;
        self.next += 1;
        match &mut self.mode {
            FieldsMode::Names(names) => names.push(name),
            FieldsMode::Describe { types, fields } => {
                fields.push(describe_field::<T>(name, number, types));
            }
            FieldsMode::Encode { row, buf } => encode_field(get(*row), number, buf)?,
        }
        Ok(())
    }

    /// Returns the value whose fields are being encoded, if any.
    fn row(&self) -> Option<&'a R> {
        match self.mode {
            FieldsMode::Encode { row, .. } => Some(row),
            FieldsMode::Names(_) | FieldsMode::Describe { .. } => None,
        }
    }

    /// Returns the descriptions of the fields.
    fn into_descriptors(self) -> Vec<FieldDescriptorProto> {
        match self.mode {
            FieldsMode::Describe { fields, .. } => fields,
            FieldsMode::Names(_) | FieldsMode::Encode { .. } => Vec::new(),
        }
    }
}

/// One field, as [`ProtoValue::describe`] describes it.
#[derive(Debug)]
pub struct FieldSchema<'a> {
    /// The column name.
    name: &'static str,
    /// The field number.
    number: u32,
    /// Whether the field is repeated, for the elements of a `Vec<T>`.
    repeated: bool,
    /// The message types that the schema needs.
    types: &'a mut NestedTypes,
    /// Receives the description of the field.
    slot: &'a mut Option<FieldDescriptorProto>,
}

impl FieldSchema<'_> {
    /// Describes a field that holds the struct `T`, for a `STRUCT` column.
    ///
    /// `#[derive(ToRow)]` uses this function. Applications do not need to call
    /// it.
    ///
    /// `T` must be `'static`, because the schema describes each nested struct
    /// type once, and tells them apart by their [TypeId].
    ///
    /// # Panics
    ///
    /// If `T` is nested more than 14 levels deep, which includes any struct
    /// that contains itself.
    pub fn message<T: ToRow + 'static>(self) {
        let type_name = self.types.add_struct::<T>();
        self.message_type(&type_name);
    }

    /// Makes the field repeated, for the elements of a `Vec<T>`.
    fn repeated(self) -> Self {
        Self {
            repeated: true,
            ..self
        }
    }

    /// Describes a field of a scalar type, with a function such as
    /// `string_field`.
    fn scalar(self, describe: fn(&str, u32) -> FieldDescriptorProto) {
        let field = describe(self.name, self.number);
        self.finish(field);
    }

    /// Describes a field that holds a message of the given type.
    fn message_type(self, type_name: &str) {
        let field = message_field(self.name, self.number, type_name);
        self.finish(field);
    }

    /// Stores the description of the field.
    fn finish(self, field: FieldDescriptorProto) {
        let field = if self.repeated {
            repeated_field(field)
        } else {
            field
        };
        *self.slot = Some(field);
    }
}

/// One field, as [`ProtoValue::encode`] encodes it.
#[derive(Debug)]
pub struct FieldEncoder<'a> {
    /// The field number.
    number: u32,
    /// Receives the encoded field.
    buf: &'a mut Vec<u8>,
}

impl FieldEncoder<'_> {
    /// Encodes `value`, a struct, for a `STRUCT` column.
    ///
    /// `#[derive(ToRow)]` uses this function. Applications do not need to call
    /// it.
    pub fn message<T: ToRow>(self, value: &T) -> Result<(), ConvertError> {
        self.nested(value, T::visit_fields)
    }

    /// Encodes `row` as a message, with the fields that `visit` visits.
    fn nested<R: ?Sized>(
        self,
        row: &R,
        visit: impl FnOnce(&mut StructFields<'_, R>) -> Result<(), ConvertError>,
    ) -> Result<(), ConvertError> {
        // The fields go straight into the buffer, after the tag. An empty
        // message still appends the tag and a zero length, which is not the
        // same as leaving the field out.
        encode_length_delimited(self.number, self.buf, |buf| {
            visit(&mut StructFields::encode(row, buf))
        })
    }
}

/// Describes a field of type `T`, with the given column name and number.
fn describe_field<T: ProtoValue>(
    name: &'static str,
    number: u32,
    types: &mut NestedTypes,
) -> FieldDescriptorProto {
    let mut slot = None;
    T::describe(FieldSchema {
        name,
        number,
        repeated: false,
        types,
        slot: &mut slot,
    });
    slot.expect(
        "`ProtoValue::describe` must describe its field, for example with `FieldSchema::message`",
    )
}

/// Appends `value` to `buf`, as the field with the given number.
fn encode_field<T: ProtoValue>(
    value: &T,
    number: u32,
    buf: &mut Vec<u8>,
) -> Result<(), ConvertError> {
    value.encode(FieldEncoder { number, buf })
}

/// Appends a `string` field, with the text that `write` appends to the buffer.
///
/// Writing the text in place saves building a [String] first. If `write`
/// fails, this appends nothing.
fn encode_text<T, E>(
    field: FieldEncoder<'_>,
    write: impl FnOnce(&mut Vec<u8>) -> Result<T, E>,
) -> Result<(), ConvertError>
where
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    encode_length_delimited(field.number, field.buf, |buf| write(buf).map(|_| ()))
        .map_err(|e| ConvertError::Convert(e.into()))
}

impl ProtoValue for String {
    fn describe(field: FieldSchema<'_>) {
        field.scalar(string_field);
    }

    fn encode(&self, field: FieldEncoder<'_>) -> Result<(), ConvertError> {
        encode_string(field.number, self, field.buf);
        Ok(())
    }
}

impl ProtoValue for i64 {
    fn describe(field: FieldSchema<'_>) {
        field.scalar(int64_field);
    }

    fn encode(&self, field: FieldEncoder<'_>) -> Result<(), ConvertError> {
        encode_int64(field.number, *self, field.buf);
        Ok(())
    }
}

impl ProtoValue for i32 {
    fn describe(field: FieldSchema<'_>) {
        // `INT64` columns also take `int32` fields, but the bytes would be the
        // same: protobuf sign-extends negative `int32` values to 64 bits.
        field.scalar(int64_field);
    }

    fn encode(&self, field: FieldEncoder<'_>) -> Result<(), ConvertError> {
        encode_int64(field.number, i64::from(*self), field.buf);
        Ok(())
    }
}

impl ProtoValue for bool {
    fn describe(field: FieldSchema<'_>) {
        field.scalar(bool_field);
    }

    fn encode(&self, field: FieldEncoder<'_>) -> Result<(), ConvertError> {
        encode_bool(field.number, *self, field.buf);
        Ok(())
    }
}

impl ProtoValue for f64 {
    fn describe(field: FieldSchema<'_>) {
        field.scalar(double_field);
    }

    fn encode(&self, field: FieldEncoder<'_>) -> Result<(), ConvertError> {
        encode_double(field.number, *self, field.buf);
        Ok(())
    }
}

impl ProtoValue for f32 {
    fn describe(field: FieldSchema<'_>) {
        // BigQuery converts `float` fields to `FLOAT64` values. They take 4
        // bytes instead of 8, which keeps rows smaller.
        field.scalar(float_field);
    }

    fn encode(&self, field: FieldEncoder<'_>) -> Result<(), ConvertError> {
        encode_float(field.number, *self, field.buf);
        Ok(())
    }
}

impl ProtoValue for Vec<u8> {
    fn describe(field: FieldSchema<'_>) {
        field.scalar(bytes_field);
    }

    fn encode(&self, field: FieldEncoder<'_>) -> Result<(), ConvertError> {
        encode_bytes(field.number, self, field.buf);
        Ok(())
    }
}

impl ProtoValue for Bytes {
    fn describe(field: FieldSchema<'_>) {
        field.scalar(bytes_field);
    }

    fn encode(&self, field: FieldEncoder<'_>) -> Result<(), ConvertError> {
        encode_bytes(field.number, self, field.buf);
        Ok(())
    }
}

impl ProtoValue for wkt::Timestamp {
    fn describe(field: FieldSchema<'_>) {
        // BigQuery takes `TIMESTAMP` values as microseconds since the Unix
        // epoch, in an `int64` field.
        field.scalar(int64_field);
    }

    fn encode(&self, field: FieldEncoder<'_>) -> Result<(), ConvertError> {
        encode_int64(field.number, timestamp_micros(self), field.buf);
        Ok(())
    }
}

/// The number of microseconds in a second.
const MICROS_PER_SECOND: i64 = 1_000_000;

/// The number of nanoseconds in a microsecond.
const NANOS_PER_MICRO: i32 = 1_000;

/// Returns the microseconds since the Unix epoch, rounded down.
///
/// The nanoseconds in a [wkt::Timestamp] are never negative, even before the
/// epoch, so dividing them rounds down. The result cannot overflow: a
/// [wkt::Timestamp] is between the years 1 and 9999, like a BigQuery
/// `TIMESTAMP`.
fn timestamp_micros(timestamp: &wkt::Timestamp) -> i64 {
    timestamp.seconds() * MICROS_PER_SECOND + i64::from(timestamp.nanos() / NANOS_PER_MICRO)
}

impl ProtoValue for google_cloud_type::model::Date {
    fn describe(field: FieldSchema<'_>) {
        // BigQuery takes `DATE` values as days since the Unix epoch, in an
        // `int64` field.
        field.scalar(int64_field);
    }

    fn encode(&self, field: FieldEncoder<'_>) -> Result<(), ConvertError> {
        encode_int64(field.number, date_days(self)?, field.buf);
        Ok(())
    }
}

/// Returns the days since the Unix epoch, for a BigQuery `DATE` value.
fn date_days(value: &google_cloud_type::model::Date) -> Result<i64, ConvertError> {
    let date = civil_date(value.year, value.month, value.day)?;
    Ok((date - UNIX_EPOCH).whole_days())
}

impl ProtoValue for google_cloud_type::model::DateTime {
    fn describe(field: FieldSchema<'_>) {
        // BigQuery takes `DATETIME` values as strings, such as
        // "2025-05-16 09:46:12.123456".
        field.scalar(string_field);
    }

    fn encode(&self, field: FieldEncoder<'_>) -> Result<(), ConvertError> {
        let value = civil_datetime(self)?;
        encode_text(field, |buf| value.format_into(buf, DATETIME_FORMAT))
    }
}

/// Returns the date and time for a BigQuery `DATETIME` value.
fn civil_datetime(
    value: &google_cloud_type::model::DateTime,
) -> Result<time::PrimitiveDateTime, ConvertError> {
    if value.time_offset.is_some() {
        return Err(ConvertError::Convert(
            "a `DATETIME` has no time zone or UTC offset; use a `Timestamp` instead".into(),
        ));
    }
    let date = civil_date(value.year, value.month, value.day)?;
    let time = civil_time(value.hours, value.minutes, value.seconds, value.nanos)?;
    Ok(time::PrimitiveDateTime::new(date, time))
}

impl ProtoValue for google_cloud_type::model::TimeOfDay {
    fn describe(field: FieldSchema<'_>) {
        // BigQuery takes `TIME` values as strings, such as "09:46:12.123456".
        field.scalar(string_field);
    }

    fn encode(&self, field: FieldEncoder<'_>) -> Result<(), ConvertError> {
        let value = civil_time(self.hours, self.minutes, self.seconds, self.nanos)?;
        encode_text(field, |buf| value.format_into(buf, TIME_FORMAT))
    }
}

/// The first day of the Unix epoch, 1970-01-01.
const UNIX_EPOCH: time::Date = time::OffsetDateTime::UNIX_EPOCH.date();

/// The earliest year in a BigQuery `DATE` or `DATETIME`.
const MIN_YEAR: i32 = 1;

/// The latest year in a BigQuery `DATE` or `DATETIME`.
const MAX_YEAR: i32 = 9999;

/// Formats a `DATETIME` value, with six digits for the microseconds.
const DATETIME_FORMAT: &[time::format_description::FormatItem<'static>] = time::macros::format_description!(
    "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond digits:6]"
);

/// Formats a `TIME` value, with six digits for the microseconds.
const TIME_FORMAT: &[time::format_description::FormatItem<'static>] =
    time::macros::format_description!("[hour]:[minute]:[second].[subsecond digits:6]");

/// Returns the date for a BigQuery `DATE` or `DATETIME` value.
///
/// A [Date](google_cloud_type::model::Date) may leave out the year, the
/// month, or the day, by setting them to 0. BigQuery dates need all three.
fn civil_date(year: i32, month: i32, day: i32) -> Result<time::Date, ConvertError> {
    let invalid = || {
        let date = format!("{year:04}-{month:02}-{day:02}");
        ConvertError::Convert(format!("{date} is not a valid BigQuery date").into())
    };
    if !(MIN_YEAR..=MAX_YEAR).contains(&year) {
        return Err(invalid());
    }
    let month = u8::try_from(month)
        .ok()
        .and_then(|month| time::Month::try_from(month).ok())
        .ok_or_else(invalid)?;
    let day = u8::try_from(day).map_err(|_| invalid())?;
    time::Date::from_calendar_date(year, month, day).map_err(|_| invalid())
}

/// Returns the time of day for a BigQuery `TIME` or `DATETIME` value.
///
/// A [TimeOfDay](google_cloud_type::model::TimeOfDay) may allow `24:00:00`,
/// or leap seconds. BigQuery times do not. BigQuery keeps microseconds, so
/// this drops the last three digits of the nanoseconds.
fn civil_time(
    hours: i32,
    minutes: i32,
    seconds: i32,
    nanos: i32,
) -> Result<time::Time, ConvertError> {
    let invalid = || {
        let time = format!("{hours:02}:{minutes:02}:{seconds:02}.{nanos:09}");
        ConvertError::Convert(format!("{time} is not a valid BigQuery time").into())
    };
    let hour = u8::try_from(hours).map_err(|_| invalid())?;
    let minute = u8::try_from(minutes).map_err(|_| invalid())?;
    let second = u8::try_from(seconds).map_err(|_| invalid())?;
    let nanosecond = u32::try_from(nanos).map_err(|_| invalid())?;
    let microsecond = nanosecond / NANOS_PER_MICRO.unsigned_abs();
    time::Time::from_hms_micro(hour, minute, second, microsecond).map_err(|_| invalid())
}

impl ProtoValue for Interval {
    fn describe(field: FieldSchema<'_>) {
        // BigQuery takes `INTERVAL` values as strings, such as
        // "1-2 3 4:5:6.789000".
        field.scalar(string_field);
    }

    fn encode(&self, field: FieldEncoder<'_>) -> Result<(), ConvertError> {
        let parts = interval_parts(self)?;
        encode_text(field, |buf| write!(buf, "{parts}"))
    }
}

/// The number of months in a year.
const MONTHS_PER_YEAR: i128 = 12;

/// The number of nanoseconds in a second.
const NANOS_PER_SECOND: i128 = 1_000_000_000;

/// The number of nanoseconds in a minute.
const NANOS_PER_MINUTE: i128 = 60 * NANOS_PER_SECOND;

/// The number of nanoseconds in an hour.
const NANOS_PER_HOUR: i128 = 60 * NANOS_PER_MINUTE;

/// The most months in a BigQuery `INTERVAL`, positive or negative: 10,000
/// years.
const MAX_INTERVAL_MONTHS: i128 = 10_000 * MONTHS_PER_YEAR;

/// The most days in a BigQuery `INTERVAL`, positive or negative.
const MAX_INTERVAL_DAYS: i128 = 3_660_000;

/// The most time in a BigQuery `INTERVAL`, positive or negative: 87,840,000
/// hours.
const MAX_INTERVAL_NANOS: i128 = 87_840_000 * NANOS_PER_HOUR;

/// The three parts of a BigQuery `INTERVAL` value, each with its own sign.
///
/// The [Display](std::fmt::Display) implementation writes the string that
/// BigQuery receives, such as "1-2 3 4:5:6.789000".
#[derive(Debug)]
struct IntervalParts {
    /// The years and months, in months.
    months: i128,
    /// The days.
    days: i128,
    /// The hours, minutes, and seconds, in nanoseconds. The value is whole
    /// microseconds.
    time: i128,
}

impl std::fmt::Display for IntervalParts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The format is `[sign]Y-M [sign]D [sign]H:M:S[.F]`.
        let Self { months, days, time } = *self;
        let sign = |part: i128| if part < 0 { "-" } else { "" };
        write!(
            f,
            "{}{}-{} {days} {}{}:{}:{}",
            sign(months),
            (months / MONTHS_PER_YEAR).abs(),
            (months % MONTHS_PER_YEAR).abs(),
            sign(time),
            (time / NANOS_PER_HOUR).abs(),
            (time % NANOS_PER_HOUR / NANOS_PER_MINUTE).abs(),
            (time % NANOS_PER_MINUTE / NANOS_PER_SECOND).abs(),
        )?;
        match (time % NANOS_PER_SECOND / i128::from(NANOS_PER_MICRO)).abs() {
            0 => Ok(()),
            micros => write!(f, ".{micros:06}"),
        }
    }
}

/// Returns the parts of a BigQuery `INTERVAL` value.
///
/// BigQuery keeps three parts, each with its own sign: the months, the days,
/// and the time. Like BigQuery, this combines the fields in each part, so
/// `months: 14` is `1-2`, and `minutes: 90` is `1:30:0`. Hours never become
/// days, and days never become months.
fn interval_parts(value: &Interval) -> Result<IntervalParts, ConvertError> {
    // An `i128` holds these sums, and their absolute values, without
    // overflowing.
    let months = i128::from(value.years) * MONTHS_PER_YEAR + i128::from(value.months);
    let days = i128::from(value.days);
    let nanos = i128::from(value.hours) * NANOS_PER_HOUR
        + i128::from(value.minutes) * NANOS_PER_MINUTE
        + i128::from(value.seconds) * NANOS_PER_SECOND
        + i128::from(value.nanos);
    // BigQuery keeps microseconds, so this drops the last three digits.
    // Integer division rounds toward zero, for positive and negative values.
    let nanos_per_micro = i128::from(NANOS_PER_MICRO);
    let time = nanos / nanos_per_micro * nanos_per_micro;
    let parts = IntervalParts { months, days, time };
    // BigQuery SQL cannot represent longer intervals, even if the Write API
    // accepts them.
    if months.abs() > MAX_INTERVAL_MONTHS
        || days.abs() > MAX_INTERVAL_DAYS
        || time.abs() > MAX_INTERVAL_NANOS
    {
        return Err(ConvertError::Convert(
            format!("{parts} is outside the range of a BigQuery `INTERVAL`").into(),
        ));
    }
    Ok(parts)
}

impl ProtoValue for rust_decimal::Decimal {
    fn describe(field: FieldSchema<'_>) {
        // BigQuery takes `NUMERIC` and `BIGNUMERIC` values as strings, such as
        // "123.45". Unlike a `double`, a string keeps every digit.
        field.scalar(string_field);
    }

    fn encode(&self, field: FieldEncoder<'_>) -> Result<(), ConvertError> {
        encode_text(field, |buf| write!(buf, "{self}"))
    }
}

impl ProtoValue for google_cloud_type::model::Decimal {
    fn describe(field: FieldSchema<'_>) {
        // BigQuery takes `NUMERIC` and `BIGNUMERIC` values as strings.
        field.scalar(string_field);
    }

    fn encode(&self, field: FieldEncoder<'_>) -> Result<(), ConvertError> {
        encode_string(field.number, decimal_string(&self.value)?, field.buf);
        Ok(())
    }
}

/// What a [Decimal](google_cloud_type::model::Decimal) with an empty string
/// writes. The empty string means 0, but BigQuery does not accept it.
const EMPTY_DECIMAL: &str = "0";

/// Returns the string for a BigQuery `NUMERIC` or `BIGNUMERIC` value, from the
/// string in a [Decimal](google_cloud_type::model::Decimal).
///
/// BigQuery checks the range and the precision of the value. This checks the
/// format, so invalid strings fail here, like other invalid values.
fn decimal_string(value: &str) -> Result<&str, ConvertError> {
    if value.is_empty() {
        return Ok(EMPTY_DECIMAL);
    }
    if !is_decimal(value) {
        return Err(ConvertError::Convert(
            format!("{value:?} is not a valid decimal number").into(),
        ));
    }
    Ok(value)
}

/// Returns true if `value` matches the grammar in the
/// [Decimal](google_cloud_type::model::Decimal) documentation, other than the
/// empty string:
///
/// ```text
/// [Sign] Significand [Exponent]
/// Significand = Digits ['.'] [Digits] | [Digits] '.' Digits
/// Exponent = ('e' | 'E') [Sign] Digits
/// ```
///
/// `Sign` is `+` or `-`, and `Digits` is one or more ASCII digits.
fn is_decimal(value: &str) -> bool {
    /// Removes a leading `+` or `-`.
    fn unsigned(s: &str) -> &str {
        s.strip_prefix(['+', '-']).unwrap_or(s)
    }
    /// Returns true if `s` is one or more ASCII digits.
    fn digits(s: &str) -> bool {
        !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())
    }
    let value = unsigned(value);
    let (significand, exponent) = match value.split_once(['e', 'E']) {
        Some((significand, exponent)) => (significand, Some(exponent)),
        None => (value, None),
    };
    // The significand needs a digit, before or after the decimal point.
    let significand_ok = match significand.split_once('.') {
        None => digits(significand),
        Some(("", fraction)) => digits(fraction),
        Some((integer, "")) => digits(integer),
        Some((integer, fraction)) => digits(integer) && digits(fraction),
    };
    significand_ok && exponent.is_none_or(|exponent| digits(unsigned(exponent)))
}

impl ProtoValue for wkt::Value {
    fn describe(field: FieldSchema<'_>) {
        // BigQuery takes `JSON` values as strings.
        field.scalar(string_field);
    }

    fn encode(&self, field: FieldEncoder<'_>) -> Result<(), ConvertError> {
        encode_text(field, |buf| serde_json::to_writer(buf, self))
    }
}

impl ProtoValue for wkt::Struct {
    fn describe(field: FieldSchema<'_>) {
        // BigQuery takes `JSON` values as strings.
        field.scalar(string_field);
    }

    fn encode(&self, field: FieldEncoder<'_>) -> Result<(), ConvertError> {
        encode_text(field, |buf| serde_json::to_writer(buf, self))
    }
}

// The value cannot be another `Option`, which would write `Some(None)` and
// `None` the same way, or a `Vec<T>`, because arrays cannot be `NULL`.
impl<T: ProtoElement> ProtoValue for Option<T> {
    fn describe(field: FieldSchema<'_>) {
        // All fields are optional in the schema, so `NULL` needs no changes.
        T::describe(field);
    }

    fn encode(&self, field: FieldEncoder<'_>) -> Result<(), ConvertError> {
        match self {
            Some(value) => value.encode(field),
            // BigQuery reads a missing field as `NULL`.
            None => Ok(()),
        }
    }
}

// Any of the types above can be inside an `Option<T>` or a `Vec<T>`, except
// `Option<T>`. There is no implementation for `u8`, so `Vec<u8>` is always a
// `BYTES` value.
impl ProtoElement for String {}
impl ProtoElement for i64 {}
impl ProtoElement for i32 {}
impl ProtoElement for bool {}
impl ProtoElement for f64 {}
impl ProtoElement for f32 {}
impl ProtoElement for Vec<u8> {}
impl ProtoElement for Bytes {}
impl ProtoElement for wkt::Timestamp {}
impl ProtoElement for google_cloud_type::model::Date {}
impl ProtoElement for google_cloud_type::model::DateTime {}
impl ProtoElement for google_cloud_type::model::TimeOfDay {}
impl ProtoElement for Interval {}
impl ProtoElement for rust_decimal::Decimal {}
impl ProtoElement for google_cloud_type::model::Decimal {}
impl ProtoElement for wkt::Value {}
impl ProtoElement for wkt::Struct {}

impl<T: ProtoElement> ProtoValue for Vec<T> {
    fn describe(field: FieldSchema<'_>) {
        T::describe(field.repeated());
    }

    fn encode(&self, field: FieldEncoder<'_>) -> Result<(), ConvertError> {
        // Each element is a separate field, with the same field number. An
        // empty vector appends nothing, which BigQuery reads as an empty array.
        for value in self {
            encode_field(value, field.number, field.buf)?;
        }
        Ok(())
    }
}

/// The name of the message type for `RANGE` values.
const RANGE_MESSAGE: &str = "Range";

/// The name of the field for the start of a `RANGE` value.
const RANGE_START: &str = "start";

/// The name of the field for the end of a `RANGE` value.
const RANGE_END: &str = "end";

/// A type for the bounds of a [Range].
///
/// BigQuery SQL cannot create a range unless its start is before its end, so
/// [`ToRow::to_row`] returns an error for other ranges. It compares the values
/// that BigQuery receives, which keep microseconds, not nanoseconds.
///
/// [RangeElement] requires this trait, so code that is generic over the element
/// type, such as a struct with `#[derive(ToRow)]`, can write ranges.
///
/// This is an implementation detail of [ToRow]. It is not part of the public
/// API.
pub trait RangeBound {
    /// The value that BigQuery receives for a bound. The values sort in the
    /// same order as the bounds.
    type Key: Ord;

    /// Returns the value that BigQuery receives for this bound.
    fn range_key(&self) -> Result<Self::Key, ConvertError>;
}

impl RangeBound for wkt::Timestamp {
    type Key = i64;

    fn range_key(&self) -> Result<i64, ConvertError> {
        Ok(timestamp_micros(self))
    }
}

impl RangeBound for google_cloud_type::model::Date {
    type Key = i64;

    fn range_key(&self) -> Result<i64, ConvertError> {
        date_days(self)
    }
}

impl RangeBound for google_cloud_type::model::DateTime {
    // The values keep microseconds, like the strings that BigQuery receives.
    type Key = time::PrimitiveDateTime;

    fn range_key(&self) -> Result<time::PrimitiveDateTime, ConvertError> {
        civil_datetime(self)
    }
}

// BigQuery takes `RANGE<T>` values as a message with two fields, `start` and
// `end`, of the type for `T`.
impl<T: RangeElement> ProtoValue for Range<T> {
    fn describe(field: FieldSchema<'_>) {
        // A `RANGE` column is not a `STRUCT` column, so it does not count
        // toward the limit on nested `STRUCT` columns.
        let type_name = field
            .types
            .add_message(RANGE_MESSAGE.into(), visit_range::<T>);
        field.message_type(&type_name);
    }

    fn encode(&self, field: FieldEncoder<'_>) -> Result<(), ConvertError> {
        field.nested(self, visit_range::<T>)
    }
}

impl<T: RangeElement> ProtoElement for Range<T> {}

/// Visits the fields of a `RANGE` value.
fn visit_range<T: RangeElement>(
    fields: &mut StructFields<'_, Range<T>>,
) -> Result<(), ConvertError> {
    if let Some(range) = fields.row()
        && let (Some(start), Some(end)) = (&range.start, &range.end)
        && start.range_key()? >= end.range_key()?
    {
        return Err(ConvertError::Convert(
            "the start of a `RANGE` must be before its end".into(),
        ));
    }
    // `None` leaves the field out, which BigQuery reads as unbounded.
    fields.field::<Option<T>>(RANGE_START, |range| &range.start)?;
    fields.field::<Option<T>>(RANGE_END, |range| &range.end)
}

/// The most levels of nested `STRUCT` columns that BigQuery supports.
///
/// BigQuery limits the depth of a schema to 15, and counts every part of a
/// field path, such as `a.b.c`. The innermost field is not a `STRUCT`, so at
/// most 14 parts can be.
const MAX_DEPTH: usize = 14;

/// The suffix for the second message type with the same name, as in
/// `Address_2`.
const FIRST_SUFFIX: usize = 2;

/// The message types that a schema needs, besides the message for the row.
///
/// BigQuery needs a self-contained schema, so these types are nested in the
/// message for the row. They are all nested at the same level, even types
/// that only appear inside other nested types, so each type is declared once.
#[derive(Debug, Default)]
struct NestedTypes {
    /// The name before any suffix, and the descriptor, of each type. In the
    /// order they were added.
    types: Vec<(Cow<'static, str>, DescriptorProto)>,
    /// The names of the fields in the message for the row. The nested types
    /// share a scope with these fields, so they cannot use these names.
    reserved: Vec<&'static str>,
    /// The structs described so far. A struct that many fields hold, at any
    /// level, is described once.
    structs: HashMap<TypeId, StructType>,
    /// How many levels of nested structs are being described.
    depth: usize,
    /// The largest height of the structs described so far, at the current
    /// level.
    height: usize,
}

/// A nested struct that a schema has described.
#[derive(Debug)]
struct StructType {
    /// The name of its message type.
    name: String,
    /// How many levels of structs it has, counting itself. A struct with no
    /// struct fields has a height of 1.
    height: usize,
}

impl NestedTypes {
    /// Returns an empty set of types, for a row with the given column names.
    fn new(columns: Vec<&'static str>) -> Self {
        Self {
            reserved: columns,
            ..Self::default()
        }
    }

    /// Adds the message type for the nested struct `T`, and returns its name.
    ///
    /// A struct that was described before is not described again. Without
    /// that, a struct reached through many paths, such as `T` in a struct
    /// with four fields of type `T`, would be described once for each path,
    /// which grows exponentially with the depth.
    ///
    /// # Panics
    ///
    /// If `T` is nested more than [MAX_DEPTH] levels deep, which includes any
    /// struct that contains itself.
    fn add_struct<T: ToRow + ?Sized + 'static>(&mut self) -> String {
        let depth = self.depth + 1;
        // A struct at `depth` with a height of `height` reaches the level
        // `depth + height - 1`.
        if let Some(described) = self.structs.get(&TypeId::of::<T>())
            && depth + described.height - 1 <= MAX_DEPTH
        {
            self.height = self.height.max(described.height);
            return described.name.clone();
        }
        // A struct described before also gets here when it is nested too
        // deep at this level. Describing it again panics at the struct that
        // is too deep, with the same message as any other struct.
        self.depth = depth;
        assert!(
            self.depth <= MAX_DEPTH,
            "`{}` is nested more than {MAX_DEPTH} levels deep, or contains itself. \
             BigQuery supports at most {MAX_DEPTH} levels of nested `STRUCT` columns.",
            std::any::type_name::<T>()
        );
        // The structs inside `T` start a new level.
        let outer = std::mem::take(&mut self.height);
        let name = self.add_message(message_name::<T>(), T::visit_fields);
        let height = self.height + 1;
        self.height = outer.max(height);
        self.depth -= 1;
        self.structs.insert(
            TypeId::of::<T>(),
            StructType {
                name: name.clone(),
                height,
            },
        );
        name
    }

    /// Adds a message type with the fields that `visit` visits, and returns
    /// its name.
    ///
    /// A type with the same name and the same fields as an earlier type is not
    /// added again, so a struct in many fields is declared once. Different
    /// types with the same name, such as `Address` structs from two modules,
    /// get a suffix.
    fn add_message<R: ?Sized>(
        &mut self,
        base: Cow<'static, str>,
        visit: impl FnOnce(&mut StructFields<'_, R>) -> Result<(), ConvertError>,
    ) -> String {
        // The fields come first, because they may add types too.
        let fields = describe_fields(self, visit);
        let existing = self
            .types
            .iter()
            .find(|(name, descriptor)| *name == base && descriptor.field == fields);
        if let Some((_, descriptor)) = existing {
            return descriptor.name.clone();
        }
        let name = self.unused_name(&base);
        let descriptor = DescriptorProto::new().set_name(&name).set_field(fields);
        self.types.push((base, descriptor));
        name
    }

    /// Returns `name`, or `name` with the first suffix that is not taken.
    fn unused_name(&self, name: &str) -> String {
        let taken = |candidate: &str| {
            self.reserved.contains(&candidate)
                || self.types.iter().any(|(_, d)| d.name == candidate)
        };
        if !taken(name) {
            return name.to_string();
        }
        (FIRST_SUFFIX..)
            .map(|suffix| format!("{name}_{suffix}"))
            .find(|candidate| !taken(candidate))
            .expect("there are more suffixes than types")
    }

    /// Returns the descriptors of the types, in the order they were added.
    fn into_descriptors(self) -> Vec<DescriptorProto> {
        self.types.into_iter().map(|(_, d)| d).collect()
    }
}

/// The name of a message type when the name of its Rust type is empty.
///
/// [`std::any::type_name`] does not promise a format, so this keeps the schema
/// valid even if a name has no last segment.
const FALLBACK_MESSAGE_NAME: &str = "Message";

/// Returns the name of the message type for `T`, such as `Address`.
///
/// BigQuery ignores the names of message types, so the name only needs to be
/// readable: the last segment of the Rust type's path, without its generic
/// arguments. Different types with the same name get suffixes later.
fn message_name<T: ?Sized>() -> Cow<'static, str> {
    proto_name(last_segment(std::any::type_name::<T>()))
}

/// Returns the last segment of a type's path, without its generic arguments.
///
/// Generic arguments and impl blocks can come before the last segment, as in
/// `<i32 as my_app::MakeRow>::make::Row` or `my_app::Maker<_>::make::Row`, so
/// only a `::` outside of `<...>` separates two segments.
fn last_segment(path: &str) -> &str {
    let mut depth = 0_usize;
    let mut start = 0;
    let mut end = None;
    let mut prev = None;
    let mut chars = path.char_indices().peekable();
    while let Some((i, c)) = chars.next() {
        match c {
            '<' => {
                if depth == 0 && end.is_none() {
                    end = Some(i);
                }
                depth += 1;
            }
            // The `>` in `->`, as in `fn() -> i64`, closes nothing.
            '>' if prev != Some('-') => depth = depth.saturating_sub(1),
            ':' if depth == 0 && chars.peek().is_some_and(|&(_, next)| next == ':') => {
                chars.next();
                start = i + 2;
                end = None;
            }
            _ => {}
        }
        prev = Some(c);
    }
    &path[start..end.unwrap_or(path.len())]
}

/// Returns `name` as a protobuf name, which can only have ASCII letters,
/// digits, and `_`. Other characters, such as `é`, become `_`, and an empty
/// name becomes [`FALLBACK_MESSAGE_NAME`].
///
/// `name` is the name of a Rust type, which cannot start with a digit.
fn proto_name(name: &str) -> Cow<'_, str> {
    if name.is_empty() {
        return Cow::Borrowed(FALLBACK_MESSAGE_NAME);
    }
    let valid = |c: char| c.is_ascii_alphanumeric() || c == '_';
    if name.chars().all(valid) {
        return Cow::Borrowed(name);
    }
    name.chars()
        .map(|c| if valid(c) { c } else { '_' })
        .collect::<String>()
        .into()
}

/// The panic message when [`ToRow::visit_fields`] fails while describing a
/// schema.
///
/// [`StructFields::field`] only fails while encoding a row, so this can only
/// happen in a hand-written implementation.
const VISIT_FIELDS_FAILED: &str = "`ToRow::visit_fields` must only return the errors from `StructFields::field`, which never fails while describing a schema";

/// Returns the schema for rows of type `T`.
fn message_schema<T: ToRow + ?Sized>() -> ProtoSchema {
    // The nested types share a scope with the columns, so they cannot use the
    // column names. Collect the names first.
    let mut columns = Vec::new();
    T::visit_fields(&mut StructFields::names(&mut columns)).expect(VISIT_FIELDS_FAILED);
    let mut types = NestedTypes::new(columns);
    let fields = describe_fields(&mut types, T::visit_fields);
    let descriptor = DescriptorProto::new()
        .set_name(message_name::<T>())
        .set_field(fields)
        .set_nested_type(types.into_descriptors());
    ProtoSchema::new().set_proto_descriptor(descriptor)
}

/// Returns the descriptions of the fields that `visit` visits, and adds the
/// message types they need to `types`.
fn describe_fields<R: ?Sized>(
    types: &mut NestedTypes,
    visit: impl FnOnce(&mut StructFields<'_, R>) -> Result<(), ConvertError>,
) -> Vec<FieldDescriptorProto> {
    let mut fields = StructFields::describe(types);
    visit(&mut fields).expect(VISIT_FIELDS_FAILED);
    fields.into_descriptors()
}

/// Encodes `row` as one row, in the protobuf wire format.
fn encode_row<T: ToRow + ?Sized>(row: &T) -> Result<Bytes, ConvertError> {
    let mut buf = Vec::new();
    T::visit_fields(&mut StructFields::encode(row, &mut buf))?;
    Ok(buf.into())
}

#[cfg(test)]
mod tests {
    use super::{
        FALLBACK_MESSAGE_NAME, FieldEncoder, FieldSchema, MAX_DEPTH, NestedTypes, ProtoElement,
        ProtoValue, StructFields, describe_field, encode_field, interval_parts, last_segment,
        message_name, proto_name, timestamp_micros, visit_range,
    };
    use crate::datatypes::{Interval, Range, RangeElement};
    use crate::error::ConvertError;
    use crate::google::cloud::bigquery::storage::v1;
    use crate::query::{FromSql, SqlValue};
    use crate::write::ToRow;
    use bytes::Bytes;
    use gaxi::prost::ToProto;
    use google_cloud_type::model::{Date, DateTime, TimeOfDay, TimeZone};
    use prost::Message;
    use test_case::test_case;
    use wkt::Timestamp;

    // Generated code runs in the application's crate, so it names everything
    // through `google_cloud_bigquery::...` paths. This makes those paths work
    // inside this crate too.
    use crate as google_cloud_bigquery;

    /// The table we write to. The mock server accepts any name.
    const TABLE: &str = "projects/p/datasets/d/tables/t";

    /// The name of the message that describes our row.
    const ROW: &str = "Row";

    /// The name of the first column in our row.
    const NAME_COLUMN: &str = "name";

    /// The field number of the `name` column.
    const NAME_FIELD: u32 = 1;

    /// A sample value for the `name` column.
    const NAME: &str = "alice";

    /// The name of the second column in our row.
    const COUNT_COLUMN: &str = "count";

    /// The field number of the `count` column.
    const COUNT_FIELD: u32 = 2;

    /// A sample value for the `count` column. It is not zero, because `prost`
    /// skips zeros, and then there would be nothing to compare.
    const COUNT: i64 = 42;

    /// The name of the third column in our row.
    const CREATED_AT_COLUMN: &str = "created_at";

    /// The field number of the `created_at` column.
    const CREATED_AT_FIELD: u32 = 3;

    /// A sample value for the `created_at` column, with nanoseconds.
    const CREATED_AT: &str = "2025-05-16T09:46:12.123456789Z";

    /// `CREATED_AT` in microseconds since the Unix epoch. The last three
    /// digits of the nanoseconds are dropped.
    const CREATED_AT_MICROS: i64 = 1_747_388_772_123_456;

    /// The name of the fourth column in our row, which can be `NULL`.
    const NICKNAME_COLUMN: &str = "nickname";

    /// The field number of the `nickname` column.
    const NICKNAME_FIELD: u32 = 4;

    /// A sample value for the `nickname` column.
    const NICKNAME: &str = "ally";

    /// The name of the fifth column in our row, an `ARRAY<STRING>`.
    const TAGS_COLUMN: &str = "tags";

    /// The field number of the `tags` column.
    const TAGS_FIELD: u32 = 5;

    /// Sample values for the `tags` column.
    const TAGS: [&str; 2] = ["admin", "beta"];

    /// The earliest BigQuery `TIMESTAMP`, 0001-01-01 00:00:00 UTC, in
    /// microseconds since the Unix epoch.
    const MIN_MICROS: i64 = -62_135_596_800_000_000;

    /// The latest BigQuery `TIMESTAMP`, 9999-12-31 23:59:59.999999 UTC, in
    /// microseconds since the Unix epoch.
    const MAX_MICROS: i64 = 253_402_300_799_999_999;

    /// A row with `STRING`, `INT64`, `TIMESTAMP`, nullable `STRING`, and
    /// `ARRAY<STRING>` columns.
    #[derive(ToRow)]
    struct Row {
        name: String,
        count: i64,
        created_at: Timestamp,
        nickname: Option<String>,
        tags: Vec<String>,
    }

    /// What `prost` generates for:
    ///
    /// `message Row { string name = 1; int64 count = 2; int64 created_at = 3;
    /// optional string nickname = 4; repeated string tags = 5; }`
    #[derive(Clone, PartialEq, Message)]
    struct ProstRow {
        #[prost(string, tag = "1")]
        name: String,
        #[prost(int64, tag = "2")]
        count: i64,
        /// BigQuery takes `TIMESTAMP` values as microseconds since the epoch.
        #[prost(int64, tag = "3")]
        created_at: i64,
        /// Like `ToRow`, `prost` writes any `Some` value, even an empty
        /// string, and leaves out `None`.
        #[prost(string, optional, tag = "4")]
        nickname: Option<String>,
        /// Like `ToRow`, `prost` writes each element as a separate field.
        #[prost(string, repeated, tag = "5")]
        tags: Vec<String>,
    }

    /// A row with the sample values.
    fn sample_row() -> anyhow::Result<Row> {
        Ok(Row {
            name: NAME.to_string(),
            count: COUNT,
            created_at: Timestamp::try_from(CREATED_AT)?,
            nickname: Some(NICKNAME.to_string()),
            tags: TAGS.map(String::from).into(),
        })
    }

    /// The `prost` message with the same values as `sample_row()`.
    fn sample_prost_row() -> ProstRow {
        ProstRow {
            name: NAME.to_string(),
            count: COUNT,
            created_at: CREATED_AT_MICROS,
            nickname: Some(NICKNAME.to_string()),
            tags: TAGS.map(String::from).into(),
        }
    }

    /// What BigQuery receives for `Row::schema()`, written by hand.
    fn sent_descriptor() -> anyhow::Result<prost_types::DescriptorProto> {
        use prost_types::field_descriptor_proto::Type;
        Ok(prost_types::DescriptorProto {
            name: Some(ROW.to_string()),
            field: vec![
                sent_field(NAME_COLUMN, NAME_FIELD, Type::String)?,
                sent_field(COUNT_COLUMN, COUNT_FIELD, Type::Int64)?,
                sent_field(CREATED_AT_COLUMN, CREATED_AT_FIELD, Type::Int64)?,
                // `Option<String>` has the same schema as `String`.
                sent_field(NICKNAME_COLUMN, NICKNAME_FIELD, Type::String)?,
                sent_repeated_field(TAGS_COLUMN, TAGS_FIELD, Type::String)?,
            ],
            ..Default::default()
        })
    }

    /// What BigQuery receives for one optional field, written by hand.
    fn sent_field(
        name: &str,
        number: u32,
        field_type: prost_types::field_descriptor_proto::Type,
    ) -> anyhow::Result<prost_types::FieldDescriptorProto> {
        use prost_types::field_descriptor_proto::Label;
        Ok(prost_types::FieldDescriptorProto {
            name: Some(name.to_string()),
            number: Some(i32::try_from(number)?),
            label: Some(Label::Optional as i32),
            r#type: Some(field_type as i32),
            ..Default::default()
        })
    }

    /// What BigQuery receives for one repeated field, written by hand.
    fn sent_repeated_field(
        name: &str,
        number: u32,
        field_type: prost_types::field_descriptor_proto::Type,
    ) -> anyhow::Result<prost_types::FieldDescriptorProto> {
        use prost_types::field_descriptor_proto::Label;
        Ok(prost_types::FieldDescriptorProto {
            label: Some(Label::Repeated as i32),
            ..sent_field(name, number, field_type)?
        })
    }

    #[test]
    fn schema_matches_descriptor() -> anyhow::Result<()> {
        // Convert the schema the same way the client does before sending it.
        let got: v1::ProtoSchema = Row::schema().to_proto()?;
        assert_eq!(got.proto_descriptor, Some(sent_descriptor()?));
        Ok(())
    }

    #[test]
    fn to_row_matches_prost() -> anyhow::Result<()> {
        assert_eq!(sample_row()?.to_row()?, sample_prost_row().encode_to_vec());
        Ok(())
    }

    #[test]
    fn default_values_are_encoded() -> anyhow::Result<()> {
        let row = Row {
            name: String::new(),
            count: 0,
            created_at: Timestamp::default(),
            nickname: Some(String::new()),
            // An empty array appends nothing.
            tags: Vec::new(),
        };
        let want = [
            0x0A, 0x00, // name: tag (field 1, length-delimited), length 0
            0x10, 0x00, // count: tag (field 2, varint), value 0
            0x18, 0x00, // created_at: tag (field 3, varint), the epoch is 0
            0x22, 0x00, // nickname: tag (field 4, length-delimited), length 0
        ];
        assert_eq!(row.to_row()?, want.as_slice());
        // `prost` skips default values, which BigQuery would read as NULLs.
        let encoded = ProstRow::default().encode_to_vec();
        assert!(encoded.is_empty(), "{encoded:?}");
        Ok(())
    }

    #[test]
    fn none_is_left_out() -> anyhow::Result<()> {
        let row = Row {
            nickname: None,
            ..sample_row()?
        };
        // There is no `nickname` field, so BigQuery writes `NULL`.
        let want = ProstRow {
            nickname: None,
            ..sample_prost_row()
        };
        assert_eq!(row.to_row()?, want.encode_to_vec());
        Ok(())
    }

    #[test]
    fn some_zero_is_not_none() -> anyhow::Result<()> {
        let mut buf = Vec::new();
        encode_field(&None::<i64>, COUNT_FIELD, &mut buf)?;
        assert!(buf.is_empty(), "{buf:?}");
        encode_field(&Some(0_i64), COUNT_FIELD, &mut buf)?;
        assert_eq!(buf, [0x10, 0x00]); // tag (field 2, varint), value 0
        Ok(())
    }

    /// Returns the protobuf type of a field of type `T`. The name and number
    /// do not matter.
    fn field_type<T: ProtoValue>() -> wkt::field_descriptor_proto::Type {
        describe_field::<T>(NAME_COLUMN, NAME_FIELD, &mut NestedTypes::default()).r#type
    }

    #[test]
    fn field_types() {
        use wkt::field_descriptor_proto::Type;
        assert_eq!(field_type::<bool>(), Type::Bool);
        assert_eq!(field_type::<i32>(), Type::Int64);
        assert_eq!(field_type::<i64>(), Type::Int64);
        assert_eq!(field_type::<f32>(), Type::Float);
        assert_eq!(field_type::<f64>(), Type::Double);
        assert_eq!(field_type::<rust_decimal::Decimal>(), Type::String);
        assert_eq!(
            field_type::<google_cloud_type::model::Decimal>(),
            Type::String
        );
        assert_eq!(field_type::<String>(), Type::String);
        assert_eq!(field_type::<Vec<u8>>(), Type::Bytes);
        assert_eq!(field_type::<Bytes>(), Type::Bytes);
        assert_eq!(field_type::<Date>(), Type::Int64);
        assert_eq!(field_type::<DateTime>(), Type::String);
        assert_eq!(field_type::<TimeOfDay>(), Type::String);
        assert_eq!(field_type::<Timestamp>(), Type::Int64);
        assert_eq!(field_type::<Interval>(), Type::String);
        assert_eq!(field_type::<wkt::Value>(), Type::String);
        assert_eq!(field_type::<wkt::Struct>(), Type::String);
        assert_eq!(field_type::<Option<String>>(), Type::String);
        // Nested structs and ranges are messages.
        assert_eq!(field_type::<Address>(), Type::Message);
        assert_eq!(field_type::<Range<Date>>(), Type::Message);
        // An array has the type of its elements.
        assert_eq!(field_type::<Vec<i64>>(), Type::Int64);
        assert_eq!(field_type::<Vec<Vec<u8>>>(), Type::Bytes);
        assert_eq!(field_type::<Vec<Address>>(), Type::Message);
    }

    /// Returns the label of a field of type `T`. The name and number do not
    /// matter.
    fn field_label<T: ProtoValue>() -> wkt::field_descriptor_proto::Label {
        describe_field::<T>(NAME_COLUMN, NAME_FIELD, &mut NestedTypes::default()).label
    }

    #[test]
    fn field_labels() {
        use wkt::field_descriptor_proto::Label;
        assert_eq!(field_label::<i64>(), Label::Optional);
        assert_eq!(field_label::<Option<i64>>(), Label::Optional);
        assert_eq!(field_label::<Vec<i64>>(), Label::Repeated);
        // `Vec<u8>` is a `BYTES` value, not an array.
        assert_eq!(field_label::<Vec<u8>>(), Label::Optional);
        assert_eq!(field_label::<Vec<Vec<u8>>>(), Label::Repeated);
        assert_eq!(field_label::<Option<Vec<u8>>>(), Label::Optional);
        assert_eq!(field_label::<Option<Address>>(), Label::Optional);
        assert_eq!(field_label::<Vec<Address>>(), Label::Repeated);
        assert_eq!(field_label::<Vec<Range<Date>>>(), Label::Repeated);
    }

    /// What `prost` generates for:
    ///
    /// `message Counts { repeated int64 counts = 2 [packed = false]; }`
    #[derive(Clone, PartialEq, Message)]
    struct ProstCounts {
        /// By default, `prost` packs repeated numbers: one tag, the length,
        /// and then all the values. Protobuf parsers accept both forms.
        #[prost(int64, repeated, packed = "false", tag = "2")]
        counts: Vec<i64>,
    }

    #[test]
    fn repeated_field_by_hand() -> anyhow::Result<()> {
        let mut buf = Vec::new();
        encode_field(&vec![COUNT, 0], COUNT_FIELD, &mut buf)?;
        // Each element is a separate field, with the same tag.
        let want = [
            0x10, 0x2A, // tag (field 2, varint), 42
            0x10, 0x00, // tag (field 2, varint), 0
        ];
        assert_eq!(buf, want);
        Ok(())
    }

    #[test]
    fn repeated_field_matches_prost() -> anyhow::Result<()> {
        let counts = vec![COUNT, 0, -COUNT];
        let mut buf = Vec::new();
        encode_field(&counts, COUNT_FIELD, &mut buf)?;
        assert_eq!(buf, ProstCounts { counts }.encode_to_vec());
        Ok(())
    }

    /// A sample value for the `level` column. It is negative, because
    /// protobuf sign-extends negative values to 64 bits.
    const LEVEL: i32 = -7;

    /// A sample value for the `ratio` column.
    const RATIO: f64 = 0.25;

    /// A sample value for the `score` column.
    const SCORE: f32 = 1.5;

    /// A sample value for the `payload` and `digest` columns. It is not valid
    /// UTF-8, which `BYTES` columns allow.
    const PAYLOAD: &[u8] = &[0x00, 0xFF];

    /// A row with the scalar types that `Row` does not have.
    #[derive(Default, ToRow)]
    struct Scalars {
        active: bool,
        level: i32,
        ratio: f64,
        score: f32,
        payload: Vec<u8>,
        digest: Bytes,
    }

    /// What `prost` generates for:
    ///
    /// `message Scalars { bool active = 1; int64 level = 2; double ratio = 3;
    /// float score = 4; bytes payload = 5; bytes digest = 6; }`
    #[derive(Clone, PartialEq, Message)]
    struct ProstScalars {
        #[prost(bool, tag = "1")]
        active: bool,
        /// `i32` values are sent as `int64`.
        #[prost(int64, tag = "2")]
        level: i64,
        #[prost(double, tag = "3")]
        ratio: f64,
        #[prost(float, tag = "4")]
        score: f32,
        #[prost(bytes = "vec", tag = "5")]
        payload: Vec<u8>,
        #[prost(bytes = "bytes", tag = "6")]
        digest: Bytes,
    }

    #[test]
    fn scalars_match_prost() -> anyhow::Result<()> {
        let row = Scalars {
            active: true,
            level: LEVEL,
            ratio: RATIO,
            score: SCORE,
            payload: PAYLOAD.to_vec(),
            digest: Bytes::from_static(PAYLOAD),
        };
        let want = ProstScalars {
            active: true,
            level: i64::from(LEVEL),
            ratio: RATIO,
            score: SCORE,
            payload: PAYLOAD.to_vec(),
            digest: Bytes::from_static(PAYLOAD),
        };
        assert_eq!(row.to_row()?, want.encode_to_vec());
        Ok(())
    }

    #[test]
    fn default_scalars_are_encoded() -> anyhow::Result<()> {
        let mut want: Vec<u8> = Vec::new();
        want.extend([0x08, 0x00]); // active: tag (field 1, varint), false
        want.extend([0x10, 0x00]); // level: tag (field 2, varint), 0
        want.push(0x19); // ratio: tag (field 3, 64-bit), then 0.0 in 8 bytes
        want.extend([0x00; 8]);
        want.push(0x25); // score: tag (field 4, 32-bit), then 0.0 in 4 bytes
        want.extend([0x00; 4]);
        want.extend([0x2A, 0x00]); // payload: tag (field 5, length-delimited), length 0
        want.extend([0x32, 0x00]); // digest: tag (field 6, length-delimited), length 0
        assert_eq!(Scalars::default().to_row()?, want);
        // `prost` skips default values, which BigQuery would read as NULLs.
        let encoded = ProstScalars::default().encode_to_vec();
        assert!(encoded.is_empty(), "{encoded:?}");
        Ok(())
    }

    /// Returns `value` encoded as field 1. The field number does not matter.
    fn field_bytes<T: ProtoValue>(value: &T) -> Result<Vec<u8>, ConvertError> {
        let mut buf = Vec::new();
        encode_field(value, NAME_FIELD, &mut buf)?;
        Ok(buf)
    }

    /// Returns a date with the given fields.
    fn date(year: i32, month: i32, day: i32) -> Date {
        Date::new().set_year(year).set_month(month).set_day(day)
    }

    /// Returns a time of day with the given fields.
    fn time_of_day(hours: i32, minutes: i32, seconds: i32, nanos: i32) -> TimeOfDay {
        TimeOfDay::new()
            .set_hours(hours)
            .set_minutes(minutes)
            .set_seconds(seconds)
            .set_nanos(nanos)
    }

    /// Returns a date and time, without a time zone or a UTC offset.
    fn datetime(date: Date, time: TimeOfDay) -> DateTime {
        DateTime::new()
            .set_year(date.year)
            .set_month(date.month)
            .set_day(date.day)
            .set_hours(time.hours)
            .set_minutes(time.minutes)
            .set_seconds(time.seconds)
            .set_nanos(time.nanos)
    }

    // The earliest and latest dates are the limits of a BigQuery `DATE`.
    #[test_case(1970, 1, 1, 0; "epoch")]
    #[test_case(1969, 12, 31, -1; "before the epoch")]
    #[test_case(2025, 5, 16, 20_224; "after the epoch")]
    #[test_case(1, 1, 1, -719_162; "earliest")]
    #[test_case(9999, 12, 31, 2_932_896; "latest")]
    fn dates_are_days_since_epoch(
        year: i32,
        month: i32,
        day: i32,
        days: i64,
    ) -> anyhow::Result<()> {
        // A `Date` is written like an `i64` with the days since the epoch.
        assert_eq!(field_bytes(&date(year, month, day))?, field_bytes(&days)?);
        Ok(())
    }

    #[test_case(0, 1, 1; "no year")]
    #[test_case(10_000, 1, 1; "after 9999")]
    #[test_case(2025, 0, 1; "no month")]
    #[test_case(2025, 13, 1; "month 13")]
    #[test_case(2025, 1, 0; "no day")]
    #[test_case(2025, 2, 29; "not a leap year")]
    fn invalid_dates_are_errors(year: i32, month: i32, day: i32) {
        let got = field_bytes(&date(year, month, day));
        assert!(got.is_err(), "{got:?}");
    }

    #[test_case(0, 0, 0, 0, "00:00:00.000000"; "midnight")]
    #[test_case(9, 46, 12, 123_456_789, "09:46:12.123456"; "rounds down nanoseconds")]
    #[test_case(23, 59, 59, 999_999_999, "23:59:59.999999"; "latest")]
    fn times_are_strings(
        hours: i32,
        minutes: i32,
        seconds: i32,
        nanos: i32,
        want: &str,
    ) -> anyhow::Result<()> {
        let time = time_of_day(hours, minutes, seconds, nanos);
        assert_eq!(field_bytes(&time)?, field_bytes(&want.to_string())?);
        Ok(())
    }

    #[test_case(24, 0, 0, 0; "end of day")]
    #[test_case(23, 59, 60, 0; "leap second")]
    #[test_case(0, 60, 0, 0; "minute 60")]
    #[test_case(-1, 0, 0, 0; "negative hours")]
    #[test_case(0, 0, 0, 1_000_000_000; "nanoseconds over one second")]
    fn invalid_times_are_errors(hours: i32, minutes: i32, seconds: i32, nanos: i32) {
        let got = field_bytes(&time_of_day(hours, minutes, seconds, nanos));
        assert!(got.is_err(), "{got:?}");
    }

    #[test_case(
        date(2025, 5, 16),
        time_of_day(9, 46, 12, 123_456_789),
        "2025-05-16 09:46:12.123456";
        "rounds down nanoseconds"
    )]
    #[test_case(
        date(1, 1, 1),
        time_of_day(0, 0, 0, 0),
        "0001-01-01 00:00:00.000000";
        "earliest"
    )]
    fn datetimes_are_strings(date: Date, time: TimeOfDay, want: &str) -> anyhow::Result<()> {
        let value = datetime(date, time);
        assert_eq!(field_bytes(&value)?, field_bytes(&want.to_string())?);
        Ok(())
    }

    #[test_case(date(0, 1, 1), time_of_day(0, 0, 0, 0); "no year")]
    #[test_case(date(1970, 1, 1), time_of_day(24, 0, 0, 0); "end of day")]
    fn invalid_datetimes_are_errors(date: Date, time: TimeOfDay) {
        let got = field_bytes(&datetime(date, time));
        assert!(got.is_err(), "{got:?}");
    }

    /// The Unix epoch, 1970-01-01 00:00:00, without a time zone.
    fn local_epoch() -> DateTime {
        datetime(date(1970, 1, 1), time_of_day(0, 0, 0, 0))
    }

    #[test_case(local_epoch().set_utc_offset(wkt::Duration::default()); "UTC offset")]
    #[test_case(local_epoch().set_time_zone(TimeZone::new().set_id("UTC")); "time zone")]
    fn datetime_with_time_zone_is_an_error(value: DateTime) -> anyhow::Result<()> {
        let got = field_bytes(&value);
        assert!(got.is_err(), "{got:?}");
        // Without the time zone or UTC offset, the same value is valid.
        let mut local = value;
        local.time_offset = None;
        field_bytes(&local)?;
        Ok(())
    }

    // BigQuery combines the fields in each part of an interval: the months,
    // the days, and the time. Most of these strings come from the examples in
    // the BigQuery documentation.
    #[test_case(Interval::new(), "0-0 0 0:0:0"; "zero")]
    #[test_case(Interval::new().set_years(1), "1-0 0 0:0:0"; "one year")]
    #[test_case(Interval::new().set_months(14), "1-2 0 0:0:0"; "months become years")]
    #[test_case(Interval::new().set_months(-25), "-2-1 0 0:0:0"; "negative months")]
    #[test_case(
        Interval::new().set_years(1).set_months(-2),
        "0-10 0 0:0:0";
        "combines years and months"
    )]
    #[test_case(Interval::new().set_days(-5), "0-0 -5 0:0:0"; "negative days")]
    #[test_case(Interval::new().set_hours(25), "0-0 0 25:0:0"; "hours do not become days")]
    #[test_case(Interval::new().set_minutes(90), "0-0 0 1:30:0"; "minutes become hours")]
    #[test_case(Interval::new().set_seconds(90), "0-0 0 0:1:30"; "seconds become minutes")]
    #[test_case(Interval::new().set_minutes(-90), "0-0 0 -1:30:0"; "negative minutes")]
    #[test_case(
        Interval::new().set_months(8).set_days(-20).set_hours(17),
        "0-8 -20 17:0:0";
        "each part has its own sign"
    )]
    #[test_case(
        Interval::new().set_months(-2).set_days(10).set_minutes(30),
        "-0-2 10 0:30:0";
        "negative months under a year"
    )]
    #[test_case(
        Interval::new().set_minutes(-30).set_seconds(-10),
        "0-0 0 -0:30:10";
        "negative time under an hour"
    )]
    #[test_case(
        Interval::new().set_years(-1).set_months(-2).set_days(-3).set_hours(-4).set_minutes(-5).set_seconds(-6).set_nanos(-789_000_000),
        "-1-2 -3 -4:5:6.789000";
        "all negative"
    )]
    // BigQuery shows this one as `10:20:30.520`, which is the same value.
    #[test_case(
        Interval::new().set_hours(10).set_minutes(20).set_seconds(30).set_nanos(520_000_000),
        "0-0 0 10:20:30.520000";
        "six fraction digits"
    )]
    #[test_case(
        Interval::new().set_seconds(1).set_nanos(-1_000),
        "0-0 0 0:0:0.999999";
        "combines seconds and nanoseconds"
    )]
    #[test_case(Interval::new().set_nanos(2_000_000_000), "0-0 0 0:0:2"; "nanoseconds become seconds")]
    #[test_case(Interval::new().set_nanos(123_456_789), "0-0 0 0:0:0.123456"; "drops the last nanosecond digits")]
    #[test_case(Interval::new().set_nanos(-1_999), "0-0 0 -0:0:0.000001"; "rounds toward zero")]
    #[test_case(Interval::new().set_nanos(-999), "0-0 0 0:0:0"; "rounds to zero without a sign")]
    #[test_case(
        Interval::new().set_years(10_000).set_days(3_660_000).set_hours(87_840_000),
        "10000-0 3660000 87840000:0:0";
        "longest"
    )]
    #[test_case(
        Interval::new().set_years(-10_000).set_days(-3_660_000).set_hours(-87_840_000),
        "-10000-0 -3660000 -87840000:0:0";
        "longest negative"
    )]
    #[test_case(
        Interval::new().set_hours(87_840_000).set_nanos(999),
        "0-0 0 87840000:0:0";
        "checks the range after rounding"
    )]
    fn intervals_are_strings(value: Interval, want: &str) -> anyhow::Result<()> {
        assert_eq!(field_bytes(&value)?, field_bytes(&want.to_string())?);
        Ok(())
    }

    // The error shows the value that BigQuery would receive, with the fields
    // combined.
    #[test_case(
        Interval::new().set_years(10_000).set_months(1),
        "10000-1 0 0:0:0";
        "too many months"
    )]
    #[test_case(Interval::new().set_months(-120_001), "-10000-1 0 0:0:0"; "too many negative months")]
    #[test_case(Interval::new().set_days(3_660_001), "0-0 3660001 0:0:0"; "too many days")]
    #[test_case(Interval::new().set_days(-3_660_001), "0-0 -3660001 0:0:0"; "too many negative days")]
    #[test_case(
        Interval::new().set_hours(87_840_000).set_nanos(1_000),
        "0-0 0 87840000:0:0.000001";
        "too much time"
    )]
    #[test_case(
        Interval::new().set_hours(-87_840_000).set_seconds(-1),
        "0-0 0 -87840000:0:1";
        "too much negative time"
    )]
    fn intervals_out_of_range_are_errors(value: Interval, want: &str) {
        let err = field_bytes(&value).expect_err("the interval is out of range");
        assert_eq!(
            err.to_string(),
            format!("cannot convert value: {want} is outside the range of a BigQuery `INTERVAL`")
        );
    }

    /// Returns an interval with every field set to `value`.
    fn every_field(value: i32) -> Interval {
        Interval::new()
            .set_years(value)
            .set_months(value)
            .set_days(value)
            .set_hours(value)
            .set_minutes(value)
            .set_seconds(value)
            .set_nanos(value)
    }

    #[test_case(i32::MAX; "largest")]
    #[test_case(i32::MIN; "smallest")]
    fn interval_fields_do_not_overflow(value: i32) {
        // Combining the fields does not overflow, and the result is far out
        // of range.
        let got = field_bytes(&every_field(value));
        assert!(got.is_err(), "{got:?}");
    }

    // These intervals have the fields that `FromSql` returns: the fields in
    // each part are combined, and the nanoseconds are whole microseconds.
    #[test_case(Interval::new(); "zero")]
    #[test_case(
        Interval::new().set_years(1).set_months(2).set_days(3).set_hours(4).set_minutes(5).set_seconds(6).set_nanos(789_000_000);
        "positive"
    )]
    #[test_case(
        Interval::new().set_years(-1).set_months(-2).set_days(-3).set_hours(-4).set_minutes(-5).set_seconds(-6).set_nanos(-1_000);
        "negative"
    )]
    #[test_case(Interval::new().set_months(8).set_days(-20).set_hours(17); "mixed signs")]
    #[test_case(Interval::new().set_months(-2).set_minutes(-30).set_seconds(-10); "negative under a unit")]
    #[test_case(Interval::new().set_hours(744); "hours beyond a day")]
    #[test_case(
        Interval::new().set_years(10_000).set_days(3_660_000).set_hours(87_840_000);
        "longest"
    )]
    fn intervals_round_trip(value: Interval) -> anyhow::Result<()> {
        let sent = interval_parts(&value)?.to_string();
        let got = Interval::from_value(SqlValue::new(wkt::Value::String(sent)))?;
        assert_eq!(got, value);
        Ok(())
    }

    /// A sample `NUMERIC` value, with more digits than an `f64` keeps.
    const DECIMAL: &str = "12345678901234567890.123456789";

    #[test]
    fn decimals_are_strings() -> anyhow::Result<()> {
        let want = field_bytes(&DECIMAL.to_string())?;
        let value: rust_decimal::Decimal = DECIMAL.parse()?;
        assert_eq!(field_bytes(&value)?, want);
        let value = google_cloud_type::model::Decimal::new().set_value(DECIMAL);
        assert_eq!(field_bytes(&value)?, want);
        Ok(())
    }

    #[test_case("0"; "zero")]
    #[test_case("-1.5"; "negative")]
    #[test_case("+2.5E-3"; "signs and an exponent")]
    #[test_case(".5"; "no integer digits")]
    #[test_case("5."; "no fraction digits")]
    #[test_case("1e10"; "lowercase exponent")]
    fn valid_decimals_are_sent_as_is(value: &str) -> anyhow::Result<()> {
        let decimal = google_cloud_type::model::Decimal::new().set_value(value);
        assert_eq!(field_bytes(&decimal)?, field_bytes(&value.to_string())?);
        Ok(())
    }

    #[test]
    fn empty_decimal_is_zero() -> anyhow::Result<()> {
        // The empty string means 0, but BigQuery does not accept it.
        let decimal = google_cloud_type::model::Decimal::new();
        let zero = rust_decimal::Decimal::ZERO;
        assert_eq!(field_bytes(&decimal)?, field_bytes(&zero)?);
        Ok(())
    }

    #[test_case("abc"; "letters")]
    #[test_case("1.2.3"; "two decimal points")]
    #[test_case("."; "only a decimal point")]
    #[test_case("-"; "only a sign")]
    #[test_case("--1"; "two signs")]
    #[test_case("e5"; "no significand")]
    #[test_case("1e"; "no exponent digits")]
    #[test_case("1e+"; "only an exponent sign")]
    #[test_case(" 1"; "whitespace")]
    #[test_case("1,000"; "thousands separator")]
    #[test_case("NaN"; "not a number")]
    #[test_case("١"; "non-ascii digit")]
    fn invalid_decimals_are_errors(value: &str) {
        let decimal = google_cloud_type::model::Decimal::new().set_value(value);
        let err = field_bytes(&decimal).expect_err("the value is not a decimal number");
        assert_eq!(
            err.to_string(),
            format!("cannot convert value: {value:?} is not a valid decimal number")
        );
    }

    /// What `prost` generates for `message Json { string json = 1; }`.
    #[derive(Clone, PartialEq, Message)]
    struct ProstJson {
        #[prost(string, tag = "1")]
        json: String,
    }

    /// Encodes `value`, and parses the JSON string that BigQuery receives.
    fn sent_json<T: ProtoValue>(value: &T) -> anyhow::Result<wkt::Value> {
        let json = ProstJson::decode(field_bytes(value)?.as_slice())?.json;
        Ok(serde_json::from_str(&json)?)
    }

    #[test]
    fn json_values_are_strings() -> anyhow::Result<()> {
        let value = serde_json::json!({
            NAME_COLUMN: NAME,
            COUNT_COLUMN: COUNT,
            TAGS_COLUMN: TAGS,
        });
        assert_eq!(sent_json(&value)?, value);
        let object = value.as_object().cloned().expect("value is an object");
        assert_eq!(sent_json(&object)?, value);
        // A JSON `null` is a value, not a SQL `NULL`.
        assert_eq!(sent_json(&wkt::Value::Null)?, wkt::Value::Null);
        Ok(())
    }

    /// The longest field value whose length takes one byte, as a varint.
    const MAX_ONE_BYTE_LEN: usize = 127;

    /// Returns enough copies of `text` to be longer than [MAX_ONE_BYTE_LEN]
    /// bytes.
    fn longer_than_one_byte_len(text: &str) -> String {
        text.repeat(MAX_ONE_BYTE_LEN / text.len() + 1)
    }

    #[test]
    fn long_json_values_are_strings() -> anyhow::Result<()> {
        // The JSON is longer than 127 bytes, so its length takes two bytes,
        // and the encoder moves the text over to make room.
        let value = serde_json::json!({ NAME_COLUMN: longer_than_one_byte_len(NAME) });
        let want = ProstJson {
            json: value.to_string(),
        };
        assert_eq!(field_bytes(&value)?, want.encode_to_vec());
        let object = value.as_object().cloned().expect("value is an object");
        assert_eq!(field_bytes(&object)?, want.encode_to_vec());
        Ok(())
    }

    #[test_case(0, 0, 0; "epoch")]
    #[test_case(1, 1_999, 1_000_001; "drops the last nanosecond digits")]
    #[test_case(-1, 999_999_500, -1; "rounds down before the epoch")]
    fn timestamp_micros_rounds_down(seconds: i64, nanos: i32, want: i64) -> anyhow::Result<()> {
        let timestamp = Timestamp::new(seconds, nanos)?;
        assert_eq!(timestamp_micros(&timestamp), want);
        Ok(())
    }

    #[test]
    fn timestamp_micros_does_not_overflow() -> anyhow::Result<()> {
        // The earliest and latest `wkt::Timestamp` values are also the earliest
        // and latest BigQuery `TIMESTAMP` values.
        let min = Timestamp::new(Timestamp::MIN_SECONDS, Timestamp::MIN_NANOS)?;
        assert_eq!(timestamp_micros(&min), MIN_MICROS);
        let max = Timestamp::new(Timestamp::MAX_SECONDS, Timestamp::MAX_NANOS)?;
        assert_eq!(timestamp_micros(&max), MAX_MICROS);
        Ok(())
    }

    #[tokio::test]
    async fn append_to_mock_server() -> anyhow::Result<()> {
        use crate::client::Write;
        use crate::model::ProtoRows;
        use bigquery_grpc_mock::google::cloud::bigquery::storage::v1 as mock_v1;
        use bigquery_grpc_mock::{MockBigQueryWrite, start};
        use gaxi::grpc::tonic::Response as TonicResponse;
        use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
        use mock_v1::append_rows_request::Rows;
        use mock_v1::append_rows_response::{AppendResult, Response};
        use tokio::sync::{mpsc, oneshot};

        // The mock server hands us the requests it receives, and replies with
        // the responses we queue.
        let (requests_tx, requests_rx) = oneshot::channel();
        let (response_tx, response_rx) = mpsc::channel(1);
        let mut mock = MockBigQueryWrite::new();
        mock.expect_append_rows().return_once(move |request| {
            let _ = requests_tx.send(request.into_inner());
            Ok(TonicResponse::from(response_rx))
        });
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;

        // This is what an application writes with `ToRow`.
        let client = Write::builder()
            .with_endpoint(endpoint)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;
        let writer = client
            .open_default_stream(TABLE)
            .build_proto(Row::schema())
            .await?;
        let rows = ProtoRows::new().set_serialized_rows([sample_row()?.to_row()?]);

        // Appends to the default stream succeed without an offset.
        let success = mock_v1::AppendRowsResponse {
            response: Some(Response::AppendResult(AppendResult { offset: None })),
            ..Default::default()
        };
        response_tx.send(Ok(success)).await?;
        let response = writer.append(rows).send().await?;
        assert_eq!(response.offset, None);

        // The mock received the schema and the bytes we expect.
        let mut requests = requests_rx.await?;
        let request = requests
            .recv()
            .await
            .expect("the mock received a request")?;
        assert_eq!(request.write_stream, format!("{TABLE}/streams/_default"));
        let data = match request.rows {
            Some(Rows::ProtoRows(data)) => data,
            other => panic!("expected proto rows, got {other:?}"),
        };
        let schema = data.writer_schema.and_then(|s| s.proto_descriptor);
        assert_eq!(schema, Some(sent_descriptor()?));
        let rows = data.rows.map(|r| r.serialized_rows);
        assert_eq!(rows, Some(vec![sample_prost_row().encode_to_vec()]));
        Ok(())
    }

    /// The name of the message type for `Address`.
    const ADDRESS: &str = "Address";

    /// The name of the only field in `Address`.
    const CITY_COLUMN: &str = "city";

    /// The field number of `city`.
    const CITY_FIELD: u32 = 1;

    /// A sample value for `city`.
    const CITY: &str = "NYC";

    /// A struct for a `STRUCT<city STRING>` column.
    #[derive(ToRow)]
    struct Address {
        city: Option<String>,
    }

    /// The name of the message type for `Customer`.
    const CUSTOMER: &str = "Customer";

    /// The name of the `STRUCT` column in `Customer`.
    const ADDRESS_COLUMN: &str = "address";

    /// The field number of `address`.
    const ADDRESS_FIELD: u32 = 2;

    /// The name of the `ARRAY<STRUCT>` column in `Customer`.
    const PREVIOUS_COLUMN: &str = "previous";

    /// The field number of `previous`.
    const PREVIOUS_FIELD: u32 = 3;

    /// A row with a `STRING` column, a nullable `STRUCT` column, and an
    /// `ARRAY<STRUCT>` column.
    #[derive(ToRow)]
    struct Customer {
        name: String,
        address: Option<Address>,
        previous: Vec<Address>,
    }

    /// What `prost` generates for `message Address { optional string city = 1; }`.
    #[derive(Clone, PartialEq, Message)]
    struct ProstAddress {
        #[prost(string, optional, tag = "1")]
        city: Option<String>,
    }

    /// What `prost` generates for:
    ///
    /// `message Customer { string name = 1; optional Address address = 2;
    /// repeated Address previous = 3; }`
    #[derive(Clone, PartialEq, Message)]
    struct ProstCustomer {
        #[prost(string, tag = "1")]
        name: String,
        /// Like `ToRow`, `prost` writes any `Some` message, even an empty one.
        #[prost(message, optional, tag = "2")]
        address: Option<ProstAddress>,
        #[prost(message, repeated, tag = "3")]
        previous: Vec<ProstAddress>,
    }

    /// An address with a city.
    fn nyc() -> Address {
        Address {
            city: Some(CITY.to_string()),
        }
    }

    #[test]
    fn nested_structs_match_prost() -> anyhow::Result<()> {
        let customer = Customer {
            name: NAME.to_string(),
            address: Some(nyc()),
            previous: vec![nyc(), Address { city: None }],
        };
        let prost_nyc = ProstAddress {
            city: Some(CITY.to_string()),
        };
        let want = ProstCustomer {
            name: NAME.to_string(),
            address: Some(prost_nyc.clone()),
            previous: vec![prost_nyc, ProstAddress::default()],
        };
        assert_eq!(customer.to_row()?, want.encode_to_vec());
        Ok(())
    }

    /// What `prost` generates for `message Nest { optional Customer inner = 1; }`.
    #[derive(Clone, PartialEq, Message)]
    struct ProstNest {
        #[prost(message, optional, tag = "1")]
        inner: Option<ProstCustomer>,
    }

    #[test]
    fn long_nested_structs_match_prost() -> anyhow::Result<()> {
        // Each message is longer than 127 bytes, so its length takes two
        // bytes, and the encoder moves the fields over to make room. The
        // `Address` messages move inside a `Customer` message that moves too.
        let city = longer_than_one_byte_len(CITY);
        let address = || Address {
            city: Some(city.clone()),
        };
        let row = Nest {
            inner: Customer {
                name: NAME.to_string(),
                address: Some(address()),
                previous: vec![address(), address()],
            },
        };
        let prost_address = ProstAddress { city: Some(city) };
        let want = ProstNest {
            inner: Some(ProstCustomer {
                name: NAME.to_string(),
                address: Some(prost_address.clone()),
                previous: vec![prost_address.clone(), prost_address],
            }),
        };
        assert_eq!(row.to_row()?, want.encode_to_vec());
        Ok(())
    }

    #[test]
    fn nested_struct_by_hand() -> anyhow::Result<()> {
        let customer = Customer {
            name: NAME.to_string(),
            address: Some(nyc()),
            previous: vec![Address { city: None }],
        };
        let mut want = vec![0x0A, 0x05]; // name: tag (field 1, length-delimited), length 5
        want.extend_from_slice(NAME.as_bytes());
        // address: tag (field 2, length-delimited), then the length of the
        // `Address` message. It has 5 bytes: the `city` field.
        want.extend([0x12, 0x05]);
        want.extend([0x0A, 0x03]); // city: tag (field 1, length-delimited), length 3
        want.extend_from_slice(CITY.as_bytes());
        // previous: tag (field 3, length-delimited), then an empty `Address`
        // message. `city` is `None`, so the message has no fields.
        want.extend([0x1A, 0x00]);
        assert_eq!(customer.to_row()?, want);
        Ok(())
    }

    #[test]
    fn none_struct_is_left_out() -> anyhow::Result<()> {
        let customer = Customer {
            name: NAME.to_string(),
            address: None,
            previous: Vec::new(),
        };
        // Only `name` is written. BigQuery reads the missing `address` as a
        // `NULL` struct, and the missing `previous` as an empty array.
        let want = ProstCustomer {
            name: NAME.to_string(),
            ..Default::default()
        };
        assert_eq!(customer.to_row()?, want.encode_to_vec());
        Ok(())
    }

    /// What BigQuery receives for one field that holds a message, written by
    /// hand.
    fn sent_message_field(
        name: &str,
        number: u32,
        type_name: &str,
    ) -> anyhow::Result<prost_types::FieldDescriptorProto> {
        use prost_types::field_descriptor_proto::Type;
        Ok(prost_types::FieldDescriptorProto {
            type_name: Some(type_name.to_string()),
            ..sent_field(name, number, Type::Message)?
        })
    }

    #[test]
    fn nested_schema_matches_descriptor() -> anyhow::Result<()> {
        use prost_types::field_descriptor_proto::{Label, Type};
        let address = prost_types::DescriptorProto {
            name: Some(ADDRESS.to_string()),
            field: vec![sent_field(CITY_COLUMN, CITY_FIELD, Type::String)?],
            ..Default::default()
        };
        let want = prost_types::DescriptorProto {
            name: Some(CUSTOMER.to_string()),
            field: vec![
                sent_field(NAME_COLUMN, NAME_FIELD, Type::String)?,
                sent_message_field(ADDRESS_COLUMN, ADDRESS_FIELD, ADDRESS)?,
                prost_types::FieldDescriptorProto {
                    label: Some(Label::Repeated as i32),
                    ..sent_message_field(PREVIOUS_COLUMN, PREVIOUS_FIELD, ADDRESS)?
                },
            ],
            // Both fields use the same `Address` type, declared once inside
            // `Customer`. This keeps the schema self-contained.
            nested_type: vec![address],
            ..Default::default()
        };
        let got: v1::ProtoSchema = Customer::schema().to_proto()?;
        assert_eq!(got.proto_descriptor, Some(want));
        Ok(())
    }

    /// Returns the names of the types nested in `descriptor`.
    fn nested_names(descriptor: &wkt::DescriptorProto) -> Vec<&str> {
        descriptor
            .nested_type
            .iter()
            .map(|t| t.name.as_str())
            .collect()
    }

    /// Returns the type names of the fields in `descriptor`. They are empty
    /// for fields that do not hold messages.
    fn field_type_names(descriptor: &wkt::DescriptorProto) -> Vec<&str> {
        descriptor
            .field
            .iter()
            .map(|f| f.type_name.as_str())
            .collect()
    }

    /// The name of the message type for `Geo`.
    const GEO: &str = "Geo";

    /// The name of the message type for `Place`.
    const PLACE: &str = "Place";

    /// A struct for a `STRUCT<lat FLOAT64, lng FLOAT64>` column.
    #[derive(ToRow)]
    struct Geo {
        lat: f64,
        lng: f64,
    }

    /// A struct with a nested struct.
    #[derive(ToRow)]
    struct Place {
        geo: Geo,
    }

    /// A row with two columns of the same `STRUCT` type.
    #[derive(ToRow)]
    struct Trip {
        from: Place,
        to: Place,
    }

    #[test]
    fn nested_types_are_flattened() {
        let descriptor = Trip::schema().proto_descriptor.expect("has a descriptor");
        // `Geo` only appears inside `Place`, but it is declared in the row,
        // next to `Place`. Each type is declared once.
        assert_eq!(nested_names(&descriptor), [GEO, PLACE]);
        assert_eq!(field_type_names(&descriptor), [PLACE, PLACE]);
        // `Place` refers to `Geo` by its short name. Protobuf looks for it in
        // `Place` first, and then in the enclosing message, the row.
        assert_eq!(field_type_names(&descriptor.nested_type[1]), [GEO]);
    }

    /// The name of the message type for the first `Wrapper` type.
    const WRAPPER: &str = "Wrapper";

    /// The name of the message type for the second `Wrapper` type.
    const WRAPPER_2: &str = "Wrapper_2";

    /// A struct with a field of any type.
    #[derive(ToRow)]
    struct Wrapper<T> {
        value: T,
    }

    /// A row with columns that hold different `Wrapper` types.
    #[derive(ToRow)]
    struct Wrapped {
        count: Wrapper<i64>,
        name: Wrapper<String>,
        total: Wrapper<i64>,
    }

    #[test]
    fn same_names_get_suffixes() {
        let descriptor = Wrapped::schema()
            .proto_descriptor
            .expect("has a descriptor");
        // `Wrapper<i64>` and `Wrapper<String>` have different fields, so they
        // need two message types, with different names. `total` reuses the
        // type for `count`.
        assert_eq!(nested_names(&descriptor), [WRAPPER, WRAPPER_2]);
        assert_eq!(field_type_names(&descriptor), [WRAPPER, WRAPPER_2, WRAPPER]);
    }

    /// The name of the message type for `Address`, when a column already has
    /// the name `Address`.
    const ADDRESS_2: &str = "Address_2";

    /// A row with a column that has the same name as the type of the column.
    #[derive(ToRow)]
    struct Legacy {
        #[bigquery(rename = "Address")]
        home: Address,
    }

    #[test]
    fn column_names_are_reserved() {
        let descriptor = Legacy::schema().proto_descriptor.expect("has a descriptor");
        // A message cannot have a field and a nested type with the same name,
        // so the type for `Address` gets a suffix.
        assert_eq!(nested_names(&descriptor), [ADDRESS_2]);
        assert_eq!(field_type_names(&descriptor), [ADDRESS_2]);
    }

    /// A struct with one field, to nest structs many levels deep.
    #[derive(ToRow)]
    struct Nest<T> {
        inner: T,
    }

    /// A field of this type nests 3 levels of `STRUCT` columns.
    type Nest3<T> = Nest<Nest<Nest<T>>>;

    /// A field of this type nests 13 levels of `STRUCT` columns.
    type Nest13 = Nest<Nest3<Nest3<Nest3<Nest3<i64>>>>>;

    /// A field of this type nests 14 levels of `STRUCT` columns, the most that
    /// BigQuery supports.
    type Nest14 = Nest<Nest13>;

    #[test]
    fn fourteen_levels_are_supported() {
        // A row with a field of type `Nest14`. The row is not a level.
        let descriptor = Nest::<Nest14>::schema()
            .proto_descriptor
            .expect("has a descriptor");
        // Each level has a different type, so each level needs a message type.
        assert_eq!(descriptor.nested_type.len(), MAX_DEPTH);
    }

    #[test]
    #[should_panic(expected = "is nested more than 14 levels deep")]
    fn fifteen_levels_panic() {
        let _ = Nest::<Nest<Nest14>>::schema();
    }

    /// A struct with four fields of the same type. Nesting it gives each type
    /// four times as many paths as the type above it.
    #[derive(ToRow)]
    struct Fan<T> {
        a: T,
        b: T,
        c: T,
        d: T,
    }

    /// A field of this type nests 3 levels of `Fan` structs.
    type Fan3<T> = Fan<Fan<Fan<T>>>;

    /// A field of this type nests 14 levels of `Fan` structs.
    type Fan14 = Fan<Fan<Fan3<Fan3<Fan3<Fan3<i64>>>>>>;

    #[test]
    fn each_nested_type_is_described_once() {
        // In this row, the innermost struct is reached through 4^14 paths.
        // Describing each path on its own would take about 358 million steps.
        let descriptor = Fan::<Fan14>::schema()
            .proto_descriptor
            .expect("has a descriptor");
        assert_eq!(descriptor.nested_type.len(), MAX_DEPTH);
    }

    /// A row that holds `Nest13` at level 1, and then at level 3, where its
    /// innermost struct is at level 15.
    #[derive(ToRow)]
    struct Deeper {
        shallow: Nest13,
        deep: Nest<Nest<Nest13>>,
    }

    #[test]
    #[should_panic(expected = "is nested more than 14 levels deep")]
    fn described_types_count_toward_the_depth() {
        // `Nest13` is described for `shallow`. Reusing it for `deep` must still
        // check the depth of the structs inside it.
        let _ = Deeper::schema();
    }

    /// A struct that contains itself. BigQuery schemas cannot do that.
    #[derive(ToRow)]
    struct Node {
        name: String,
        children: Vec<Node>,
    }

    #[test]
    #[should_panic(expected = "contains itself")]
    fn recursive_structs_panic() {
        let _ = Node::schema();
    }

    /// A sample start for a range: 2025-05-16, which is 20,224 days after the
    /// epoch.
    fn check_in() -> Date {
        date(2025, 5, 16)
    }

    /// `check_in()` in days since the epoch.
    const CHECK_IN_DAYS: i64 = 20_224;

    /// A sample end for a range: 2025-05-18, which is 20,226 days after the
    /// epoch.
    fn check_out() -> Date {
        date(2025, 5, 18)
    }

    /// `check_out()` in days since the epoch.
    const CHECK_OUT_DAYS: i64 = 20_226;

    #[test]
    fn range_by_hand() -> anyhow::Result<()> {
        let stay: Range<Date> = Range::new().set_start(check_in()).set_end(check_out());
        let want = [
            0x0A, 0x08, // tag (field 1, length-delimited), then 8 bytes of fields
            0x08, 0x80, 0x9E, 0x01, // start: tag (field 1, varint), 20224
            0x10, 0x82, 0x9E, 0x01, // end: tag (field 2, varint), 20226
        ];
        assert_eq!(field_bytes(&stay)?, want);
        Ok(())
    }

    /// What `prost` generates for:
    ///
    /// `message Range { optional int64 start = 1; optional int64 end = 2; }`
    #[derive(Clone, PartialEq, Message)]
    struct ProstRange {
        #[prost(int64, optional, tag = "1")]
        start: Option<i64>,
        #[prost(int64, optional, tag = "2")]
        end: Option<i64>,
    }

    /// Returns the fields of `range`, without a tag or a length.
    fn range_bytes<T: RangeElement>(range: &Range<T>) -> Result<Vec<u8>, ConvertError> {
        let mut buf = Vec::new();
        visit_range(&mut StructFields::encode(range, &mut buf))?;
        Ok(buf)
    }

    #[test_case(true, true; "bounded")]
    #[test_case(false, true; "unbounded start")]
    #[test_case(true, false; "unbounded end")]
    #[test_case(false, false; "unbounded")]
    fn range_matches_prost(has_start: bool, has_end: bool) -> anyhow::Result<()> {
        let range: Range<Date> = Range::new()
            .set_or_clear_start(has_start.then(check_in))
            .set_or_clear_end(has_end.then(check_out));
        // A missing `start` or `end` field is unbounded.
        let want = ProstRange {
            start: has_start.then_some(CHECK_IN_DAYS),
            end: has_end.then_some(CHECK_OUT_DAYS),
        };
        assert_eq!(range_bytes(&range)?, want.encode_to_vec());
        Ok(())
    }

    /// Returns the fields of a range from `start` to `end`.
    fn bounded_range_bytes<T: RangeElement>(start: T, end: T) -> Result<Vec<u8>, ConvertError> {
        let range: Range<T> = Range::new().set_start(start).set_end(end);
        range_bytes(&range)
    }

    #[test_case(check_in(), check_in(); "empty")]
    #[test_case(check_out(), check_in(); "start after end")]
    fn invalid_date_ranges_are_errors(start: Date, end: Date) {
        let got = bounded_range_bytes(start, end);
        assert!(got.is_err(), "{got:?}");
    }

    // BigQuery keeps microseconds, so bounds that differ only in the last three
    // digits of the nanoseconds are the same.
    #[test_case(1_000, 1_000; "empty")]
    #[test_case(1_000, 1_999; "same microsecond")]
    #[test_case(2_000, 1_000; "start after end")]
    fn invalid_timestamp_ranges_are_errors(start_nanos: i32, end_nanos: i32) -> anyhow::Result<()> {
        let start = Timestamp::new(0, start_nanos)?;
        let end = Timestamp::new(0, end_nanos)?;
        let got = bounded_range_bytes(start, end);
        assert!(got.is_err(), "{got:?}");
        Ok(())
    }

    #[test_case(1_000, 1_000; "empty")]
    #[test_case(1_000, 1_999; "same microsecond")]
    #[test_case(2_000, 1_000; "start after end")]
    fn invalid_datetime_ranges_are_errors(start_nanos: i32, end_nanos: i32) {
        let start = datetime(check_in(), time_of_day(0, 0, 0, start_nanos));
        let end = datetime(check_in(), time_of_day(0, 0, 0, end_nanos));
        let got = bounded_range_bytes(start, end);
        assert!(got.is_err(), "{got:?}");
    }

    #[test]
    fn range_bounds_compare_microseconds() -> anyhow::Result<()> {
        // 999 nanoseconds round down to 0 microseconds, and 1,000 nanoseconds
        // are 1 microsecond, so these ranges start before they end.
        bounded_range_bytes(Timestamp::new(0, 999)?, Timestamp::new(0, 1_000)?)?;
        let start = datetime(check_in(), time_of_day(0, 0, 0, 999));
        let end = datetime(check_in(), time_of_day(0, 0, 0, 1_000));
        bounded_range_bytes(start, end)?;
        Ok(())
    }

    /// The name of the first message type for `RANGE` values.
    const RANGE: &str = "Range";

    /// The name of the second message type for `RANGE` values.
    const RANGE_2: &str = "Range_2";

    /// The name of the field for the start of a `RANGE` value.
    const START_COLUMN: &str = "start";

    /// The field number of `start`.
    const START_FIELD: u32 = 1;

    /// The name of the field for the end of a `RANGE` value.
    const END_COLUMN: &str = "end";

    /// The field number of `end`.
    const END_FIELD: u32 = 2;

    /// A row with a `RANGE` column for each element type.
    #[derive(ToRow)]
    struct Ranges {
        dates: Range<Date>,
        timestamps: Range<Timestamp>,
        datetimes: Range<DateTime>,
    }

    /// What BigQuery receives for a `RANGE` message type, written by hand.
    fn sent_range(
        name: &str,
        field_type: prost_types::field_descriptor_proto::Type,
    ) -> anyhow::Result<prost_types::DescriptorProto> {
        Ok(prost_types::DescriptorProto {
            name: Some(name.to_string()),
            field: vec![
                sent_field(START_COLUMN, START_FIELD, field_type)?,
                sent_field(END_COLUMN, END_FIELD, field_type)?,
            ],
            ..Default::default()
        })
    }

    #[test]
    fn range_schema_matches_descriptor() -> anyhow::Result<()> {
        use prost_types::field_descriptor_proto::Type;
        let descriptor = Ranges::schema().proto_descriptor.expect("has a descriptor");
        assert_eq!(field_type_names(&descriptor), [RANGE, RANGE, RANGE_2]);
        // `DATE` and `TIMESTAMP` values are both `int64` fields, so their
        // ranges share one message type. `DATETIME` values are `string`
        // fields, so their ranges need another one.
        let got: v1::ProtoSchema = Ranges::schema().to_proto()?;
        let want = vec![
            sent_range(RANGE, Type::Int64)?,
            sent_range(RANGE_2, Type::String)?,
        ];
        assert_eq!(got.proto_descriptor.map(|d| d.nested_type), Some(want));
        Ok(())
    }

    /// A row that is generic over the element type of its range.
    #[derive(ToRow)]
    struct Stay<T: RangeElement> {
        period: Range<T>,
    }

    #[test]
    fn to_row_rejects_invalid_ranges() -> anyhow::Result<()> {
        let valid: Stay<Date> = Stay {
            period: Range::new().set_start(check_in()).set_end(check_out()),
        };
        valid.to_row()?;
        let invalid: Stay<Date> = Stay {
            period: Range::new().set_start(check_out()).set_end(check_in()),
        };
        let got = invalid.to_row();
        assert!(matches!(got, Err(ConvertError::Convert(_))), "{got:?}");
        Ok(())
    }

    /// Encodes a `Stay` in code that is generic over the element type. It
    /// needs no bound besides `T: RangeElement`.
    fn stay_row<T: RangeElement>(stay: &Stay<T>) -> Result<Bytes, ConvertError> {
        stay.to_row()
    }

    #[test]
    fn generic_code_writes_ranges() -> anyhow::Result<()> {
        let stay: Stay<Date> = Stay {
            period: Range::new().set_start(check_in()).set_end(check_out()),
        };
        let want = [
            0x0A, 0x08, // period: tag (field 1, length-delimited), then 8 bytes
            0x08, 0x80, 0x9E, 0x01, // start: tag (field 1, varint), 20224
            0x10, 0x82, 0x9E, 0x01, // end: tag (field 2, varint), 20226
        ];
        assert_eq!(stay_row(&stay)?, want.as_slice());
        Ok(())
    }

    /// A row with an array of any element type.
    #[derive(ToRow)]
    struct Batch<T: ProtoElement> {
        items: Vec<T>,
    }

    #[test]
    fn generic_arrays() -> anyhow::Result<()> {
        use wkt::field_descriptor_proto::{Label, Type};
        let descriptor = Batch::<i64>::schema()
            .proto_descriptor
            .expect("has a descriptor");
        let field = &descriptor.field[0];
        assert_eq!(field.r#type, Type::Int64);
        assert_eq!(field.label, Label::Repeated);
        let batch = Batch {
            items: vec![COUNT, 0],
        };
        let want = [
            0x08, 0x2A, // tag (field 1, varint), 42
            0x08, 0x00, // tag (field 1, varint), 0
        ];
        assert_eq!(batch.to_row()?, want.as_slice());
        Ok(())
    }

    #[test]
    fn message_names_drop_paths_and_generics() {
        assert_eq!(message_name::<Address>(), ADDRESS);
        // The generic arguments have paths too, which do not count.
        assert_eq!(message_name::<Wrapper<Range<Date>>>(), WRAPPER);
    }

    #[test_case("Row", "Row"; "no path")]
    #[test_case("my_app::Row", "Row"; "path")]
    #[test_case("my_app::Row<my_app::Inner<i64>>", "Row"; "generic arguments")]
    #[test_case("my_app::Row<fn() -> i64>", "Row"; "function type argument")]
    #[test_case("<i32 as my_app::MakeRow>::make::Row", "Row"; "trait impl")]
    #[test_case("my_app::Maker<_>::make::Row", "Row"; "generic impl")]
    #[test_case("my_app::main::{{closure}}::Row", "Row"; "closure")]
    fn last_segments(path: &str, want: &str) {
        assert_eq!(last_segment(path), want);
    }

    /// Returns the schema of a struct that is declared in a trait impl.
    trait LocalSchema {
        fn local_schema() -> crate::model::ProtoSchema;
    }

    impl LocalSchema for i32 {
        fn local_schema() -> crate::model::ProtoSchema {
            #[derive(ToRow)]
            struct Local {
                value: i64,
            }
            Local::schema()
        }
    }

    #[test]
    fn message_names_in_impl_blocks() {
        // The type name of `Local` starts with `<i32 as ...::LocalSchema>`.
        let descriptor = <i32 as LocalSchema>::local_schema()
            .proto_descriptor
            .expect("has a descriptor");
        assert_eq!(descriptor.name, "Local");
    }

    #[test_case("Address", "Address"; "ascii")]
    #[test_case("Größe", "Gr__e"; "non-ascii letters")]
    #[test_case("r#match", "r_match"; "raw identifier")]
    #[test_case("", FALLBACK_MESSAGE_NAME; "empty")]
    fn message_names_are_proto_names(name: &str, want: &str) {
        assert_eq!(proto_name(name), want);
    }

    /// A hand-written `ToRow` that fails while its fields are described.
    struct FailingRow;

    impl ToRow for FailingRow {
        fn visit_fields(_: &mut StructFields<'_, Self>) -> Result<(), ConvertError> {
            Err(ConvertError::Convert(
                "not from `StructFields::field`".into(),
            ))
        }
    }

    #[test]
    #[should_panic(expected = "must only return the errors from `StructFields::field`")]
    fn failing_visit_fields_panics() {
        let _ = FailingRow::schema();
    }

    /// A hand-written `ProtoValue` that does not describe its field.
    struct Undescribed;

    impl ProtoValue for Undescribed {
        fn describe(_: FieldSchema<'_>) {}

        fn encode(&self, _: FieldEncoder<'_>) -> Result<(), ConvertError> {
            Ok(())
        }
    }

    #[test]
    #[should_panic(expected = "must describe its field")]
    fn undescribed_fields_panic() {
        let _ = describe_field::<Undescribed>(NAME_COLUMN, NAME_FIELD, &mut NestedTypes::default());
    }
}
