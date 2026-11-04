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

use crate::error::ConvertError;
use crate::error::RowError;
use crate::query::ColumnIndex;
use crate::query::from_sql::SqlValueInner;
use crate::query::{FromSql, SqlValue};
use arrow::array::{
    ArrayRef, as_large_list_array, as_list_array, as_primitive_array, as_struct_array,
};
use arrow::datatypes::{DataType, IntervalMonthDayNano, IntervalMonthDayNanoType, IntervalUnit};
use arrow::record_batch::RecordBatch;
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use std::sync::Arc;

/// Incremental Arrow IPC stream decoder that parses an initial `ArrowSchema`
/// once and decodes incoming `ArrowRecordBatch` frames without re-parsing the
/// schema on every message.
#[derive(Debug)]
pub(crate) struct ArrowStreamDecoder {
    decoder: arrow::ipc::reader::StreamDecoder,
}

impl ArrowStreamDecoder {
    pub(crate) fn new() -> Self {
        Self {
            decoder: arrow::ipc::reader::StreamDecoder::new(),
        }
    }

    /// Feeds serialized Arrow IPC schema bytes into the underlying decoder.
    pub(crate) fn set_schema_bytes(&mut self, schema_bytes: bytes::Bytes) -> Result<(), RowError> {
        let mut buf = arrow::buffer::Buffer::from(schema_bytes);
        self.decoder.decode(&mut buf).map_err(|e| {
            RowError::InvalidRowFormat(format!("failed to decode arrow schema: {e}"))
        })?;
        Ok(())
    }

    /// Decodes a single Arrow IPC record batch frame from [`bytes::Bytes`].
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn decode_batch(
        &mut self,
        batch_bytes: bytes::Bytes,
    ) -> Result<Option<RecordBatch>, RowError> {
        let mut buf = arrow::buffer::Buffer::from(batch_bytes);
        self.decode_buffer(&mut buf)
    }

    /// Decodes the next Arrow IPC record batch from an advancing [`arrow::buffer::Buffer`].
    pub(crate) fn decode_buffer(
        &mut self,
        buf: &mut arrow::buffer::Buffer,
    ) -> Result<Option<RecordBatch>, RowError> {
        self.decoder
            .decode(buf)
            .map_err(|e| RowError::InvalidRowFormat(format!("failed to decode arrow batch: {e}")))
    }

    /// Signals end-of-stream and validates that no partial frame remains in the decoder.
    pub(crate) fn finish(&mut self) -> Result<(), RowError> {
        self.decoder
            .finish()
            .map_err(|e| RowError::InvalidRowFormat(format!("failed to decode arrow stream: {e}")))
    }
}

/// An asynchronous source of decoded Arrow [`RecordBatch`]es.
pub(crate) trait RecordBatchSource {
    /// Returns the next non-empty [`RecordBatch`], or `Ok(None)` when the stream is exhausted.
    async fn next_record_batch(&mut self) -> Result<Option<RecordBatch>, RowError>;
}

/// Reads Arrow [`RecordBatch`]es from in-memory `jobs.query` response buffers.
#[derive(Debug)]
pub(crate) struct ArrowResponseReader {
    decoder: ArrowStreamDecoder,
    schema_bytes: Option<bytes::Bytes>,
    batch_buf: arrow::buffer::Buffer,
}

impl ArrowResponseReader {
    pub(crate) fn new(
        serialized_schema: bytes::Bytes,
        serialized_record_batch: bytes::Bytes,
    ) -> Self {
        Self {
            decoder: ArrowStreamDecoder::new(),
            schema_bytes: Some(serialized_schema),
            batch_buf: arrow::buffer::Buffer::from(serialized_record_batch),
        }
    }
}

impl RecordBatchSource for ArrowResponseReader {
    async fn next_record_batch(&mut self) -> Result<Option<RecordBatch>, RowError> {
        if self.batch_buf.is_empty() {
            self.decoder.finish()?;
            return Ok(None);
        }
        if let Some(schema_bytes) = self.schema_bytes.take() {
            self.decoder.set_schema_bytes(schema_bytes)?;
        }
        while !self.batch_buf.is_empty() {
            if let Some(batch) = self.decoder.decode_buffer(&mut self.batch_buf)?
                && batch.num_rows() > 0
            {
                return Ok(Some(batch));
            }
        }
        self.decoder.finish()?;
        Ok(None)
    }
}

/// Unified Arrow [`RecordBatch`] reader over inline query responses or BigQuery Storage Read streams.
#[derive(Debug)]
pub(crate) enum ArrowReader {
    Response(ArrowResponseReader),
    #[cfg(google_cloud_unstable_bigquery_storage_read)]
    Storage(crate::query::storage_reader::StorageReader),
}

impl RecordBatchSource for ArrowReader {
    async fn next_record_batch(&mut self) -> Result<Option<RecordBatch>, RowError> {
        match self {
            Self::Response(r) => r.next_record_batch().await,
            #[cfg(google_cloud_unstable_bigquery_storage_read)]
            Self::Storage(r) => r.next_record_batch().await,
        }
    }
}

/// A reference to a single cell within an Arrow array.
#[derive(Clone, Debug)]
pub(crate) struct ArrowCell {
    array: ArrayRef,
    pub(crate) row_idx: usize,
}

impl PartialEq for ArrowCell {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.array, &other.array) && self.row_idx == other.row_idx
    }
}

impl ArrowCell {
    /// Creates a new `ArrowCell`.
    pub(crate) fn new(array: ArrayRef, row_idx: usize) -> Self {
        Self { array, row_idx }
    }

    /// Returns true if the cell is null.
    pub(crate) fn is_null(&self) -> bool {
        self.array.is_null(self.row_idx)
    }

    /// Returns the data type of the underlying array.
    pub(crate) fn data_type(&self) -> &arrow::datatypes::DataType {
        self.array.data_type()
    }

    /// Returns a string representation of the data type.
    pub(crate) fn data_type_str(&self) -> String {
        format!("{:?}", self.array.data_type())
    }

    /// Returns the cell's value as a boolean.
    pub(crate) fn as_bool(&self) -> Result<bool, ConvertError> {
        if self.is_null() {
            return Err(ConvertError::NotNull);
        }
        match self.array.data_type() {
            arrow::datatypes::DataType::Boolean => {
                let arr = arrow::array::as_boolean_array(&self.array);
                Ok(arr.value(self.row_idx))
            }
            _ => Err(ConvertError::TypeMismatch {
                expected: "BooleanArray".to_string(),
                got: self.data_type_str(),
            }),
        }
    }

    /// Returns the cell's value as an `i64`.
    pub(crate) fn as_i64(&self) -> Result<i64, ConvertError> {
        if self.is_null() {
            return Err(ConvertError::NotNull);
        }
        match self.array.data_type() {
            arrow::datatypes::DataType::Int64 => {
                let arr =
                    arrow::array::as_primitive_array::<arrow::datatypes::Int64Type>(&self.array);
                Ok(arr.value(self.row_idx))
            }
            _ => Err(ConvertError::TypeMismatch {
                expected: "Int64Array".to_string(),
                got: self.data_type_str(),
            }),
        }
    }

    /// Returns the cell's value as an `i32`.
    pub(crate) fn as_i32(&self) -> Result<i32, ConvertError> {
        if self.is_null() {
            return Err(ConvertError::NotNull);
        }
        match self.array.data_type() {
            arrow::datatypes::DataType::Int32 => {
                let arr =
                    arrow::array::as_primitive_array::<arrow::datatypes::Int32Type>(&self.array);
                Ok(arr.value(self.row_idx))
            }
            arrow::datatypes::DataType::Int64 => {
                let arr =
                    arrow::array::as_primitive_array::<arrow::datatypes::Int64Type>(&self.array);
                i32::try_from(arr.value(self.row_idx))
                    .map_err(|e| ConvertError::Convert(Box::new(e)))
            }
            _ => Err(ConvertError::TypeMismatch {
                expected: "Int64Array or Int32Array".to_string(),
                got: self.data_type_str(),
            }),
        }
    }

    /// Returns the cell's value as an `f64`.
    pub(crate) fn as_f64(&self) -> Result<f64, ConvertError> {
        if self.is_null() {
            return Err(ConvertError::NotNull);
        }
        match self.array.data_type() {
            arrow::datatypes::DataType::Float64 => {
                let arr =
                    arrow::array::as_primitive_array::<arrow::datatypes::Float64Type>(&self.array);
                Ok(arr.value(self.row_idx))
            }
            _ => Err(ConvertError::TypeMismatch {
                expected: "Float64Array".to_string(),
                got: self.data_type_str(),
            }),
        }
    }

    /// Returns the cell's value as an `f32`.
    pub(crate) fn as_f32(&self) -> Result<f32, ConvertError> {
        if self.is_null() {
            return Err(ConvertError::NotNull);
        }
        match self.array.data_type() {
            arrow::datatypes::DataType::Float32 => {
                let arr =
                    arrow::array::as_primitive_array::<arrow::datatypes::Float32Type>(&self.array);
                Ok(arr.value(self.row_idx))
            }
            arrow::datatypes::DataType::Float64 => {
                let arr =
                    arrow::array::as_primitive_array::<arrow::datatypes::Float64Type>(&self.array);
                Ok(arr.value(self.row_idx) as f32)
            }
            _ => Err(ConvertError::TypeMismatch {
                expected: "Float64Array or Float32Array".to_string(),
                got: self.data_type_str(),
            }),
        }
    }

    /// Returns the cell's value as a string slice (`&str`).
    pub(crate) fn as_str(&self) -> Result<&str, ConvertError> {
        if self.is_null() {
            return Err(ConvertError::NotNull);
        }
        match self.array.data_type() {
            arrow::datatypes::DataType::Utf8 => {
                let arr = arrow::array::as_string_array(&self.array);
                Ok(arr.value(self.row_idx))
            }
            arrow::datatypes::DataType::LargeUtf8 => {
                let arr = arrow::array::as_largestring_array(&self.array);
                Ok(arr.value(self.row_idx))
            }
            _ => Err(ConvertError::TypeMismatch {
                expected: "StringArray or LargeStringArray".to_string(),
                got: self.data_type_str(),
            }),
        }
    }

    /// Returns the cell's value as an owned `String`, formatting SQL types that
    /// BigQuery's JSON wire format represents as strings (`STRING`, `BYTES`,
    /// `DATE`, `TIME`, `TIMESTAMP`, `DATETIME`, `NUMERIC`, `BIGNUMERIC`, and
    /// `INTERVAL`).
    pub(crate) fn to_sql_string(&self) -> Result<String, ConvertError> {
        if self.is_null() {
            return Err(ConvertError::NotNull);
        }
        match self.array.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 => self.as_str().map(str::to_owned),
            DataType::Binary | DataType::LargeBinary => {
                Ok(BASE64_STANDARD.encode(self.as_bytes()?))
            }
            DataType::Date32 => {
                let d = google_cloud_type::model::Date::from_value(SqlValue::from_inner(
                    SqlValueInner::Arrow(self.clone()),
                ))?;
                Ok(format!("{:04}-{:02}-{:02}", d.year, d.month, d.day))
            }
            DataType::Time64(
                arrow::datatypes::TimeUnit::Microsecond | arrow::datatypes::TimeUnit::Nanosecond,
            ) => {
                let t = google_cloud_type::model::TimeOfDay::from_value(SqlValue::from_inner(
                    SqlValueInner::Arrow(self.clone()),
                ))?;
                if t.nanos == 0 {
                    Ok(format!("{:02}:{:02}:{:02}", t.hours, t.minutes, t.seconds))
                } else {
                    let subsec_micros = t.nanos / 1_000;
                    Ok(format!(
                        "{:02}:{:02}:{:02}.{subsec_micros:06}",
                        t.hours, t.minutes, t.seconds
                    ))
                }
            }
            DataType::Timestamp(_, tz) => {
                if tz.is_none() {
                    let dt = google_cloud_type::model::DateTime::from_value(SqlValue::from_inner(
                        SqlValueInner::Arrow(self.clone()),
                    ))?;
                    if dt.nanos == 0 {
                        Ok(format!(
                            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
                            dt.year, dt.month, dt.day, dt.hours, dt.minutes, dt.seconds
                        ))
                    } else {
                        let subsec_micros = dt.nanos / 1_000;
                        Ok(format!(
                            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{subsec_micros:06}",
                            dt.year, dt.month, dt.day, dt.hours, dt.minutes, dt.seconds
                        ))
                    }
                } else {
                    let micros = self.as_timestamp_micros()?;
                    Ok(micros.to_string())
                }
            }
            DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => self.as_decimal_str(),
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                let interval = crate::datatypes::Interval::from_value(SqlValue::from_inner(
                    SqlValueInner::Arrow(self.clone()),
                ))?;
                let ym_sign = if interval.years < 0 || interval.months < 0 {
                    "-"
                } else {
                    ""
                };
                let time_sign = if interval.hours < 0
                    || interval.minutes < 0
                    || interval.seconds < 0
                    || interval.nanos < 0
                {
                    "-"
                } else {
                    ""
                };
                let abs_nanos = interval.nanos.abs();
                let frac = if abs_nanos == 0 {
                    String::new()
                } else if abs_nanos % 1_000_000 == 0 {
                    format!(".{:03}", abs_nanos / 1_000_000)
                } else if abs_nanos % 1_000 == 0 {
                    format!(".{:06}", abs_nanos / 1_000)
                } else {
                    format!(".{abs_nanos:09}")
                };
                Ok(format!(
                    "{}{}-{} {} {}{}:{}:{}{}",
                    ym_sign,
                    interval.years.abs(),
                    interval.months.abs(),
                    interval.days,
                    time_sign,
                    interval.hours.abs(),
                    interval.minutes.abs(),
                    interval.seconds.abs(),
                    frac
                ))
            }
            _ => Err(ConvertError::TypeMismatch {
                expected: "StringArray or LargeStringArray".to_string(),
                got: self.data_type_str(),
            }),
        }
    }

    /// Returns the cell's value as a byte slice (`&[u8]`).
    pub(crate) fn as_bytes(&self) -> Result<&[u8], ConvertError> {
        if self.is_null() {
            return Err(ConvertError::NotNull);
        }
        match self.array.data_type() {
            arrow::datatypes::DataType::Binary => {
                let arr = arrow::array::as_generic_binary_array::<i32>(&self.array);
                Ok(arr.value(self.row_idx))
            }
            arrow::datatypes::DataType::LargeBinary => {
                let arr = arrow::array::as_generic_binary_array::<i64>(&self.array);
                Ok(arr.value(self.row_idx))
            }
            _ => Err(ConvertError::TypeMismatch {
                expected: "BinaryArray or LargeBinaryArray".to_string(),
                got: self.data_type_str(),
            }),
        }
    }

    /// Returns timestamp value in microseconds since Unix epoch.
    pub(crate) fn as_timestamp_micros(&self) -> Result<i64, ConvertError> {
        if self.is_null() {
            return Err(ConvertError::NotNull);
        }
        match self.array.data_type() {
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _) => {
                let arr = arrow::array::as_primitive_array::<
                    arrow::datatypes::TimestampMicrosecondType,
                >(&self.array);
                Ok(arr.value(self.row_idx))
            }
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, _) => {
                let arr = arrow::array::as_primitive_array::<
                    arrow::datatypes::TimestampMillisecondType,
                >(&self.array);
                arr.value(self.row_idx)
                    .checked_mul(1_000)
                    .ok_or_else(|| ConvertError::Convert("timestamp overflow".into()))
            }
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _) => {
                let arr = arrow::array::as_primitive_array::<
                    arrow::datatypes::TimestampNanosecondType,
                >(&self.array);
                Ok(arr.value(self.row_idx).div_euclid(1_000))
            }
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Second, _) => {
                let arr = arrow::array::as_primitive_array::<arrow::datatypes::TimestampSecondType>(
                    &self.array,
                );
                arr.value(self.row_idx)
                    .checked_mul(1_000_000)
                    .ok_or_else(|| ConvertError::Convert("timestamp overflow".into()))
            }
            _ => Err(ConvertError::TypeMismatch {
                expected: "TimestampArray".to_string(),
                got: self.data_type_str(),
            }),
        }
    }

    /// Returns date value in days since Unix epoch.
    pub(crate) fn as_date32(&self) -> Result<i32, ConvertError> {
        if self.is_null() {
            return Err(ConvertError::NotNull);
        }
        match self.array.data_type() {
            arrow::datatypes::DataType::Date32 => {
                let arr =
                    arrow::array::as_primitive_array::<arrow::datatypes::Date32Type>(&self.array);
                Ok(arr.value(self.row_idx))
            }
            _ => Err(ConvertError::TypeMismatch {
                expected: "Date32Array".to_string(),
                got: self.data_type_str(),
            }),
        }
    }

    /// Returns time value in microseconds since midnight.
    pub(crate) fn as_time64_micros(&self) -> Result<i64, ConvertError> {
        if self.is_null() {
            return Err(ConvertError::NotNull);
        }
        match self.array.data_type() {
            arrow::datatypes::DataType::Time64(arrow::datatypes::TimeUnit::Microsecond) => {
                let arr = arrow::array::as_primitive_array::<arrow::datatypes::Time64MicrosecondType>(
                    &self.array,
                );
                Ok(arr.value(self.row_idx))
            }
            arrow::datatypes::DataType::Time64(arrow::datatypes::TimeUnit::Nanosecond) => {
                let arr = arrow::array::as_primitive_array::<arrow::datatypes::Time64NanosecondType>(
                    &self.array,
                );
                Ok(arr.value(self.row_idx).div_euclid(1_000))
            }
            _ => Err(ConvertError::TypeMismatch {
                expected: "Time64MicrosecondArray".to_string(),
                got: self.data_type_str(),
            }),
        }
    }

    /// Returns the cell's value as an `IntervalMonthDayNano`.
    pub(crate) fn as_interval(&self) -> Result<IntervalMonthDayNano, ConvertError> {
        if self.is_null() {
            return Err(ConvertError::NotNull);
        }
        match self.array.data_type() {
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                let arr = as_primitive_array::<IntervalMonthDayNanoType>(&self.array);
                Ok(arr.value(self.row_idx))
            }
            _ => Err(ConvertError::TypeMismatch {
                expected: "IntervalMonthDayNanoArray".to_string(),
                got: self.data_type_str(),
            }),
        }
    }

    /// Returns the cell's decimal value as a formatted string.
    pub(crate) fn as_decimal_str(&self) -> Result<String, ConvertError> {
        if self.is_null() {
            return Err(ConvertError::NotNull);
        }
        let mut s = match self.array.data_type() {
            arrow::datatypes::DataType::Decimal128(_, _) => {
                let arr = arrow::array::as_primitive_array::<arrow::datatypes::Decimal128Type>(
                    &self.array,
                );
                arr.value_as_string(self.row_idx)
            }
            arrow::datatypes::DataType::Decimal256(_, _) => {
                let arr = arrow::array::as_primitive_array::<arrow::datatypes::Decimal256Type>(
                    &self.array,
                );
                arr.value_as_string(self.row_idx)
            }
            _ => {
                return Err(ConvertError::TypeMismatch {
                    expected: "Decimal128Array or Decimal256Array".to_string(),
                    got: self.data_type_str(),
                });
            }
        };
        if s.contains('.') {
            let trimmed_len = s.trim_end_matches('0').trim_end_matches('.').len();
            s.truncate(trimmed_len);
        }
        Ok(s)
    }

    /// Returns the cell's `Decimal128` value along with its non-negative scale.
    pub(crate) fn as_decimal128_with_scale(&self) -> Option<(i128, u32)> {
        if self.is_null() {
            return None;
        }
        match self.array.data_type() {
            arrow::datatypes::DataType::Decimal128(_, scale) => {
                let arr = arrow::array::as_primitive_array::<arrow::datatypes::Decimal128Type>(
                    &self.array,
                );
                let scale = u32::try_from(*scale).ok()?;
                Some((arr.value(self.row_idx), scale))
            }
            _ => None,
        }
    }

    /// Extracts a child `ArrowCell` by column index or field name from a `StructArray`.
    pub(crate) fn struct_field_cell<I: ColumnIndex>(
        &self,
        index: &I,
    ) -> Result<ArrowCell, ConvertError> {
        if self.is_null() {
            return Err(ConvertError::NotNull);
        }
        match self.array.data_type() {
            DataType::Struct(_) => {
                let struct_arr = as_struct_array(&self.array);
                let idx = index
                    .arrow_index(struct_arr)
                    .ok_or_else(|| ConvertError::MissingField(format!("{index}")))?;
                Ok(ArrowCell::new(struct_arr.column(idx).clone(), self.row_idx))
            }
            _ => Err(ConvertError::TypeMismatch {
                expected: "StructArray".to_string(),
                got: self.data_type_str(),
            }),
        }
    }

    /// Returns a reference to the nested array slice for this cell from a `ListArray` or `LargeListArray`.
    pub(crate) fn list_array_ref(&self) -> Result<ArrayRef, ConvertError> {
        if self.is_null() {
            return Err(ConvertError::NotNull);
        }
        match self.array.data_type() {
            DataType::List(_) => {
                let arr = as_list_array(&self.array);
                Ok(arr.value(self.row_idx))
            }
            DataType::LargeList(_) => {
                let arr = as_large_list_array(&self.array);
                Ok(arr.value(self.row_idx))
            }
            _ => Err(ConvertError::TypeMismatch {
                expected: "ListArray or LargeListArray".to_string(),
                got: self.data_type_str(),
            }),
        }
    }

    /// Extracts a child `ArrowCell` by element index from a `ListArray` or `LargeListArray`.
    pub(crate) fn list_element_cell(&self, index: usize) -> Result<ArrowCell, ConvertError> {
        let value_arr = self.list_array_ref()?;
        if index >= value_arr.len() {
            return Err(ConvertError::MissingField(index.to_string()));
        }
        Ok(ArrowCell::new(value_arr, index))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        BinaryArray, BooleanArray, Date32Array, Decimal128Array, Decimal256Array, Float32Array,
        Float64Array, Int32Array, Int64Array, IntervalMonthDayNanoArray, IntervalYearMonthArray,
        LargeBinaryArray, LargeListArray, LargeStringArray, ListArray, StringArray, StructArray,
        Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
    };
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use arrow::datatypes::{DataType, Field, Fields, Schema as ArrowSchema, i256};
    use arrow::ipc::writer::StreamWriter;
    use std::sync::Arc;
    use test_case::test_case;

    fn decimal128_array(values: Vec<Option<i128>>, precision: u8, scale: i8) -> ArrayRef {
        Arc::new(
            Decimal128Array::from(values)
                .with_precision_and_scale(precision, scale)
                .unwrap(),
        )
    }

    fn decimal256_array(values: Vec<Option<i256>>, precision: u8, scale: i8) -> ArrayRef {
        Arc::new(
            Decimal256Array::from(values)
                .with_precision_and_scale(precision, scale)
                .unwrap(),
        )
    }

    #[derive(Debug, PartialEq)]
    enum TestConvertError {
        NotNull,
        TypeMismatch { expected: String, got: String },
        Convert,
        MissingField(String),
    }

    impl TestConvertError {
        fn type_mismatch(expected: &str, got: &str) -> Self {
            Self::TypeMismatch {
                expected: expected.to_string(),
                got: got.to_string(),
            }
        }
    }

    impl From<ConvertError> for TestConvertError {
        fn from(err: ConvertError) -> Self {
            match err {
                ConvertError::NotNull => Self::NotNull,
                ConvertError::TypeMismatch { expected, got } => {
                    Self::TypeMismatch { expected, got }
                }
                ConvertError::Convert(_) => Self::Convert,
                ConvertError::MissingField(f) => Self::MissingField(f),
            }
        }
    }

    #[test]
    fn cell_metadata() {
        let arr: ArrayRef = Arc::new(Int64Array::from(vec![Some(10), None]));

        let cell_row0 = ArrowCell::new(arr.clone(), 0);
        let cell_row1 = ArrowCell::new(arr, 1);

        assert!(!cell_row0.is_null());
        assert!(cell_row1.is_null());
        assert_eq!(cell_row0.data_type(), &DataType::Int64);
        assert_eq!(cell_row0.data_type_str(), "Int64");
        assert_eq!(cell_row0.clone(), cell_row0);
        assert_ne!(cell_row0, cell_row1);
    }

    #[test_case(Arc::new(BooleanArray::from(vec![Some(true)])), 0 => Ok(true) ; "bool true")]
    #[test_case(Arc::new(BooleanArray::from(vec![Some(false)])), 0 => Ok(false) ; "bool false")]
    #[test_case(Arc::new(BooleanArray::from(vec![None])), 0 => Err(TestConvertError::NotNull) ; "bool null")]
    #[test_case(Arc::new(Int64Array::from(vec![1])), 0 => Err(TestConvertError::type_mismatch("BooleanArray", "Int64")) ; "bool type mismatch")]
    fn as_bool(arr: ArrayRef, row_idx: usize) -> Result<bool, TestConvertError> {
        ArrowCell::new(arr, row_idx)
            .as_bool()
            .map_err(TestConvertError::from)
    }

    #[test_case(Arc::new(Int64Array::from(vec![Some(42)])), 0 => Ok(42) ; "i64 valid")]
    #[test_case(Arc::new(Int64Array::from(vec![None])), 0 => Err(TestConvertError::NotNull) ; "i64 null")]
    #[test_case(Arc::new(BooleanArray::from(vec![true])), 0 => Err(TestConvertError::type_mismatch("Int64Array", "Boolean")) ; "i64 type mismatch")]
    fn as_i64(arr: ArrayRef, row_idx: usize) -> Result<i64, TestConvertError> {
        ArrowCell::new(arr, row_idx)
            .as_i64()
            .map_err(TestConvertError::from)
    }

    #[test_case(Arc::new(Int32Array::from(vec![Some(123)])), 0 => Ok(123) ; "i32 from Int32Array")]
    #[test_case(Arc::new(Int32Array::from(vec![None])), 0 => Err(TestConvertError::NotNull) ; "i32 null")]
    #[test_case(Arc::new(Int64Array::from(vec![Some(456)])), 0 => Ok(456) ; "i32 from Int64Array")]
    #[test_case(Arc::new(Int64Array::from(vec![Some(i64::from(i32::MAX) + 1)])), 0 => Err(TestConvertError::Convert) ; "i32 overflow from Int64Array")]
    #[test_case(Arc::new(BooleanArray::from(vec![true])), 0 => Err(TestConvertError::type_mismatch("Int64Array or Int32Array", "Boolean")) ; "i32 type mismatch")]
    fn as_i32(arr: ArrayRef, row_idx: usize) -> Result<i32, TestConvertError> {
        ArrowCell::new(arr, row_idx)
            .as_i32()
            .map_err(TestConvertError::from)
    }

    #[test_case(Arc::new(Float64Array::from(vec![Some(3.25)])), 0 => Ok(3.25) ; "f64 valid")]
    #[test_case(Arc::new(Float64Array::from(vec![None])), 0 => Err(TestConvertError::NotNull) ; "f64 null")]
    #[test_case(Arc::new(Int64Array::from(vec![1])), 0 => Err(TestConvertError::type_mismatch("Float64Array", "Int64")) ; "f64 type mismatch")]
    fn as_f64(arr: ArrayRef, row_idx: usize) -> Result<f64, TestConvertError> {
        ArrowCell::new(arr, row_idx)
            .as_f64()
            .map_err(TestConvertError::from)
    }

    #[test_case(Arc::new(Float32Array::from(vec![Some(1.5_f32)])), 0 => Ok(1.5) ; "f32 from Float32Array")]
    #[test_case(Arc::new(Float32Array::from(vec![None])), 0 => Err(TestConvertError::NotNull) ; "f32 null")]
    #[test_case(Arc::new(Float64Array::from(vec![Some(2.5_f64)])), 0 => Ok(2.5) ; "f32 from Float64Array")]
    #[test_case(Arc::new(Int64Array::from(vec![1])), 0 => Err(TestConvertError::type_mismatch("Float64Array or Float32Array", "Int64")) ; "f32 type mismatch")]
    fn as_f32(arr: ArrayRef, row_idx: usize) -> Result<f32, TestConvertError> {
        ArrowCell::new(arr, row_idx)
            .as_f32()
            .map_err(TestConvertError::from)
    }

    #[test_case(Arc::new(StringArray::from(vec![Some("hello")])), 0 => Ok("hello".to_string()) ; "str from StringArray")]
    #[test_case(Arc::new(StringArray::from(vec![None::<&str>])), 0 => Err(TestConvertError::NotNull) ; "str null")]
    #[test_case(Arc::new(LargeStringArray::from(vec![Some("world")])), 0 => Ok("world".to_string()) ; "str from LargeStringArray")]
    #[test_case(Arc::new(Int64Array::from(vec![1])), 0 => Err(TestConvertError::type_mismatch("StringArray or LargeStringArray", "Int64")) ; "str type mismatch")]
    fn as_str(arr: ArrayRef, row_idx: usize) -> Result<String, TestConvertError> {
        ArrowCell::new(arr, row_idx)
            .as_str()
            .map(str::to_owned)
            .map_err(TestConvertError::from)
    }

    #[test_case(Arc::new(StringArray::from(vec![Some("hello")])), 0 => Ok("hello".to_string()) ; "sql str from StringArray")]
    #[test_case(Arc::new(StringArray::from(vec![None::<&str>])), 0 => Err(TestConvertError::NotNull) ; "sql str null")]
    #[test_case(Arc::new(LargeStringArray::from(vec![Some("world")])), 0 => Ok("world".to_string()) ; "sql str from LargeStringArray")]
    #[test_case(Arc::new(BinaryArray::from(vec![Some(&[1_u8, 2, 3][..])])), 0 => Ok("AQID".to_string()) ; "sql str from BinaryArray")]
    #[test_case(Arc::new(Date32Array::from(vec![Some(20_601)])), 0 => Ok("2026-05-28".to_string()) ; "sql str from Date32Array")]
    #[test_case(Arc::new(Time64MicrosecondArray::from(vec![Some(55_800_000_000)])), 0 => Ok("15:30:00".to_string()) ; "sql str from Time64MicrosecondArray")]
    #[test_case(Arc::new(TimestampMicrosecondArray::from(vec![Some(1_779_982_200_123_456)]).with_timezone("UTC")), 0 => Ok("1779982200123456".to_string()) ; "sql str from TimestampMicrosecondArray with tz")]
    #[test_case(Arc::new(TimestampMicrosecondArray::from(vec![Some(1_779_982_200_000_000)])), 0 => Ok("2026-05-28T15:30:00".to_string()) ; "sql str from TimestampMicrosecondArray without tz zero nanos")]
    #[test_case(Arc::new(TimestampMicrosecondArray::from(vec![Some(1_779_982_200_123_456)])), 0 => Ok("2026-05-28T15:30:00.123456".to_string()) ; "sql str from TimestampMicrosecondArray without tz fractional")]
    #[test_case(decimal128_array(vec![Some(123_456)], 10, 3), 0 => Ok("123.456".to_string()) ; "sql str from Decimal128Array")]
    #[test_case(decimal128_array(vec![Some(42_500_000_000)], 38, 9), 0 => Ok("42.5".to_string()) ; "sql str from Decimal128Array trailing fractional zeros")]
    #[test_case(decimal128_array(vec![Some(42_000_000_000)], 38, 9), 0 => Ok("42".to_string()) ; "sql str from Decimal128Array whole integer")]
    #[test_case(decimal256_array(vec![Some(i256::from_string("4250000000000000000000000000000000000000").unwrap())], 76, 38), 0 => Ok("42.5".to_string()) ; "sql str from Decimal256Array scale 38")]
    #[test_case(Arc::new(IntervalMonthDayNanoArray::from(vec![Some(IntervalMonthDayNano::new(0, 1, 9_045_000_000_000))])), 0 => Ok("0-0 1 2:30:45".to_string()) ; "sql str from IntervalMonthDayNanoArray zero nanos")]
    #[test_case(Arc::new(IntervalMonthDayNanoArray::from(vec![Some(IntervalMonthDayNano::new(14, 3, 14_706_789_000_000))])), 0 => Ok("1-2 3 4:5:6.789".to_string()) ; "sql str from IntervalMonthDayNanoArray millis")]
    #[test_case(Arc::new(IntervalMonthDayNanoArray::from(vec![Some(IntervalMonthDayNano::new(0, 0, 123_456_000))])), 0 => Ok("0-0 0 0:0:0.123456".to_string()) ; "sql str from IntervalMonthDayNanoArray micros")]
    #[test_case(Arc::new(IntervalMonthDayNanoArray::from(vec![Some(IntervalMonthDayNano::new(14, 3, 14_706_000_000_789))])), 0 => Ok("1-2 3 4:5:6.000000789".to_string()) ; "sql str from IntervalMonthDayNanoArray nanos")]
    #[test_case(Arc::new(Int64Array::from(vec![1])), 0 => Err(TestConvertError::type_mismatch("StringArray or LargeStringArray", "Int64")) ; "sql str type mismatch")]
    fn to_sql_string(arr: ArrayRef, row_idx: usize) -> Result<String, TestConvertError> {
        ArrowCell::new(arr, row_idx)
            .to_sql_string()
            .map_err(TestConvertError::from)
    }

    #[test_case(Arc::new(BinaryArray::from(vec![Some(b"abc".as_slice())])), 0 => Ok(b"abc".to_vec()) ; "bytes from BinaryArray")]
    #[test_case(Arc::new(BinaryArray::from(vec![None::<&[u8]>])), 0 => Err(TestConvertError::NotNull) ; "bytes null")]
    #[test_case(Arc::new(LargeBinaryArray::from(vec![Some(b"xyz".as_slice())])), 0 => Ok(b"xyz".to_vec()) ; "bytes from LargeBinaryArray")]
    #[test_case(Arc::new(Int64Array::from(vec![1])), 0 => Err(TestConvertError::type_mismatch("BinaryArray or LargeBinaryArray", "Int64")) ; "bytes type mismatch")]
    fn as_bytes(arr: ArrayRef, row_idx: usize) -> Result<Vec<u8>, TestConvertError> {
        ArrowCell::new(arr, row_idx)
            .as_bytes()
            .map(<[u8]>::to_vec)
            .map_err(TestConvertError::from)
    }

    #[test_case(Arc::new(TimestampMicrosecondArray::from(vec![Some(1_779_982_200_123_456)])), 0 => Ok(1_779_982_200_123_456) ; "timestamp microsecond")]
    #[test_case(Arc::new(TimestampMillisecondArray::from(vec![Some(1_779_982_200_123)])), 0 => Ok(1_779_982_200_123_000) ; "timestamp millisecond")]
    #[test_case(Arc::new(TimestampNanosecondArray::from(vec![Some(1_779_982_200_123_456_789)])), 0 => Ok(1_779_982_200_123_456) ; "timestamp nanosecond")]
    #[test_case(Arc::new(TimestampNanosecondArray::from(vec![Some(-1_500)])), 0 => Ok(-2) ; "timestamp nanosecond pre epoch floor")]
    #[test_case(Arc::new(TimestampSecondArray::from(vec![Some(1_779_982_200)])), 0 => Ok(1_779_982_200_000_000) ; "timestamp second")]
    #[test_case(Arc::new(TimestampMillisecondArray::from(vec![Some(i64::MAX)])), 0 => Err(TestConvertError::Convert) ; "timestamp millisecond overflow")]
    #[test_case(Arc::new(TimestampSecondArray::from(vec![Some(i64::MAX)])), 0 => Err(TestConvertError::Convert) ; "timestamp second overflow")]
    #[test_case(Arc::new(TimestampMicrosecondArray::from(vec![None])), 0 => Err(TestConvertError::NotNull) ; "timestamp null")]
    #[test_case(Arc::new(Int64Array::from(vec![1])), 0 => Err(TestConvertError::type_mismatch("TimestampArray", "Int64")) ; "timestamp type mismatch")]
    fn as_timestamp_micros(arr: ArrayRef, row_idx: usize) -> Result<i64, TestConvertError> {
        ArrowCell::new(arr, row_idx)
            .as_timestamp_micros()
            .map_err(TestConvertError::from)
    }

    #[test_case(Arc::new(Date32Array::from(vec![Some(20_601)])), 0 => Ok(20_601) ; "date32 valid")]
    #[test_case(Arc::new(Date32Array::from(vec![None])), 0 => Err(TestConvertError::NotNull) ; "date32 null")]
    #[test_case(Arc::new(Int32Array::from(vec![20_601])), 0 => Err(TestConvertError::type_mismatch("Date32Array", "Int32")) ; "date32 type mismatch")]
    fn as_date32(arr: ArrayRef, row_idx: usize) -> Result<i32, TestConvertError> {
        ArrowCell::new(arr, row_idx)
            .as_date32()
            .map_err(TestConvertError::from)
    }

    #[test_case(Arc::new(Time64MicrosecondArray::from(vec![Some(55_800_123_456)])), 0 => Ok(55_800_123_456) ; "time64 microsecond")]
    #[test_case(Arc::new(Time64NanosecondArray::from(vec![Some(55_800_123_456_789)])), 0 => Ok(55_800_123_456) ; "time64 nanosecond")]
    #[test_case(Arc::new(Time64MicrosecondArray::from(vec![None])), 0 => Err(TestConvertError::NotNull) ; "time64 null")]
    #[test_case(Arc::new(Int64Array::from(vec![1])), 0 => Err(TestConvertError::type_mismatch("Time64MicrosecondArray", "Int64")) ; "time64 type mismatch")]
    fn as_time64_micros(arr: ArrayRef, row_idx: usize) -> Result<i64, TestConvertError> {
        ArrowCell::new(arr, row_idx)
            .as_time64_micros()
            .map_err(TestConvertError::from)
    }

    #[test_case(decimal128_array(vec![Some(123_456)], 10, 3), 0 => Ok("123.456".to_string()) ; "decimal str from Decimal128Array")]
    #[test_case(decimal128_array(vec![Some(-123_456)], 10, 3), 0 => Ok("-123.456".to_string()) ; "decimal str negative from Decimal128Array")]
    #[test_case(decimal128_array(vec![Some(42_500_000_000)], 38, 9), 0 => Ok("42.5".to_string()) ; "decimal str from Decimal128Array trailing fractional zeros")]
    #[test_case(decimal128_array(vec![Some(42_000_000_000)], 38, 9), 0 => Ok("42".to_string()) ; "decimal str from Decimal128Array whole integer")]
    #[test_case(decimal128_array(vec![None], 10, 3), 0 => Err(TestConvertError::NotNull) ; "decimal str null Decimal128Array")]
    #[test_case(decimal256_array(vec![Some(i256::from_i128(789_012))], 20, 3), 0 => Ok("789.012".to_string()) ; "decimal str from Decimal256Array")]
    #[test_case(decimal256_array(vec![Some(i256::from_string("4250000000000000000000000000000000000000").unwrap())], 76, 38), 0 => Ok("42.5".to_string()) ; "decimal str from Decimal256Array scale 38")]
    #[test_case(decimal256_array(vec![None], 20, 3), 0 => Err(TestConvertError::NotNull) ; "decimal str null Decimal256Array")]
    #[test_case(Arc::new(Int64Array::from(vec![1])), 0 => Err(TestConvertError::type_mismatch("Decimal128Array or Decimal256Array", "Int64")) ; "decimal str type mismatch")]
    fn as_decimal_str(arr: ArrayRef, row_idx: usize) -> Result<String, TestConvertError> {
        ArrowCell::new(arr, row_idx)
            .as_decimal_str()
            .map_err(TestConvertError::from)
    }

    #[test_case(decimal128_array(vec![Some(123_456)], 10, 3), 0 => Some((123_456, 3)) ; "decimal128 valid")]
    #[test_case(decimal128_array(vec![Some(-123_456)], 10, 3), 0 => Some((-123_456, 3)) ; "decimal128 negative")]
    #[test_case(decimal128_array(vec![Some(12)], 10, -2), 0 => None ; "decimal128 negative scale")]
    #[test_case(decimal128_array(vec![None], 10, 3), 0 => None ; "decimal128 null")]
    #[test_case(decimal256_array(vec![Some(i256::from_i128(123))], 20, 3), 0 => None ; "decimal128 type mismatch Decimal256Array")]
    #[test_case(Arc::new(Int64Array::from(vec![1])), 0 => None ; "decimal128 type mismatch Int64Array")]
    fn as_decimal128_with_scale(arr: ArrayRef, row_idx: usize) -> Option<(i128, u32)> {
        ArrowCell::new(arr, row_idx).as_decimal128_with_scale()
    }

    #[test_case(Arc::new(IntervalMonthDayNanoArray::from(vec![Some(IntervalMonthDayNano::new(14, 3, 456))])), 0 => Ok(IntervalMonthDayNano::new(14, 3, 456)) ; "interval valid")]
    #[test_case(Arc::new(IntervalMonthDayNanoArray::from(vec![Some(IntervalMonthDayNano::new(-14, -3, -456))])), 0 => Ok(IntervalMonthDayNano::new(-14, -3, -456)) ; "interval negative")]
    #[test_case(Arc::new(IntervalMonthDayNanoArray::from(vec![None])), 0 => Err(TestConvertError::NotNull) ; "interval null")]
    #[test_case(Arc::new(IntervalYearMonthArray::from(vec![Some(14)])), 0 => Err(TestConvertError::type_mismatch("IntervalMonthDayNanoArray", "Interval(YearMonth)")) ; "interval wrong unit")]
    #[test_case(Arc::new(Int64Array::from(vec![1])), 0 => Err(TestConvertError::type_mismatch("IntervalMonthDayNanoArray", "Int64")) ; "interval type mismatch")]
    fn as_interval(
        arr: ArrayRef,
        row_idx: usize,
    ) -> Result<IntervalMonthDayNano, TestConvertError> {
        ArrowCell::new(arr, row_idx)
            .as_interval()
            .map_err(TestConvertError::from)
    }

    fn sample_struct_array(nulls: Option<NullBuffer>) -> ArrayRef {
        let fields = Fields::from(vec![
            Field::new("start", DataType::Int64, true),
            Field::new("end", DataType::Int64, true),
        ]);
        let start_col: ArrayRef = Arc::new(Int64Array::from(vec![Some(10), None]));
        let end_col: ArrayRef = Arc::new(Int64Array::from(vec![Some(20), Some(30)]));
        Arc::new(StructArray::new(fields, vec![start_col, end_col], nulls))
    }

    #[test_case(sample_struct_array(None), 0, "start" => Ok(Some(10)) ; "struct field start row 0")]
    #[test_case(sample_struct_array(None), 0, "end" => Ok(Some(20)) ; "struct field end row 0")]
    #[test_case(sample_struct_array(None), 1, "start" => Ok(None) ; "struct field null child row 1")]
    #[test_case(sample_struct_array(None), 1, "end" => Ok(Some(30)) ; "struct field end row 1")]
    #[test_case(sample_struct_array(Some(NullBuffer::from(vec![false, true]))), 0, "start" => Err(TestConvertError::NotNull) ; "struct null row")]
    #[test_case(sample_struct_array(None), 0, "missing" => Err(TestConvertError::MissingField("missing".to_string())) ; "struct missing field")]
    #[test_case(Arc::new(Int64Array::from(vec![1])), 0, "start" => Err(TestConvertError::type_mismatch("StructArray", "Int64")) ; "struct type mismatch")]
    fn struct_field_cell_by_name(
        arr: ArrayRef,
        row_idx: usize,
        name: &str,
    ) -> Result<Option<i64>, TestConvertError> {
        let field_cell = ArrowCell::new(arr, row_idx)
            .struct_field_cell(&name)
            .map_err(TestConvertError::from)?;
        if field_cell.is_null() {
            return Ok(None);
        }
        field_cell
            .as_i64()
            .map(Some)
            .map_err(TestConvertError::from)
    }

    #[test_case(sample_struct_array(None), 0, 0 => Ok(Some(10)) ; "struct field index 0 row 0")]
    #[test_case(sample_struct_array(None), 0, 1 => Ok(Some(20)) ; "struct field index 1 row 0")]
    #[test_case(sample_struct_array(None), 1, 0 => Ok(None) ; "struct field index 0 null child row 1")]
    #[test_case(sample_struct_array(None), 0, 2 => Err(TestConvertError::MissingField("2".to_string())) ; "struct field index out of bounds")]
    #[test_case(sample_struct_array(Some(NullBuffer::from(vec![false, true]))), 0, 0 => Err(TestConvertError::NotNull) ; "struct field index null row")]
    #[test_case(Arc::new(Int64Array::from(vec![1])), 0, 0 => Err(TestConvertError::type_mismatch("StructArray", "Int64")) ; "struct field index type mismatch")]
    fn struct_field_cell_by_index(
        arr: ArrayRef,
        row_idx: usize,
        col_idx: usize,
    ) -> Result<Option<i64>, TestConvertError> {
        let field_cell = ArrowCell::new(arr, row_idx)
            .struct_field_cell(&col_idx)
            .map_err(TestConvertError::from)?;
        if field_cell.is_null() {
            return Ok(None);
        }
        field_cell
            .as_i64()
            .map(Some)
            .map_err(TestConvertError::from)
    }

    fn sample_list_array(nulls: Option<NullBuffer>) -> ArrayRef {
        let values: ArrayRef = Arc::new(Int64Array::from(vec![Some(10), None, Some(30)]));
        let offsets = OffsetBuffer::from_lengths(vec![2, 1]);
        Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Int64, true)),
            offsets,
            values,
            nulls,
        ))
    }

    fn sample_large_list_array() -> ArrayRef {
        let values: ArrayRef = Arc::new(Int64Array::from(vec![Some(100), Some(200)]));
        let offsets = OffsetBuffer::from_lengths(vec![2]);
        Arc::new(LargeListArray::new(
            Arc::new(Field::new("item", DataType::Int64, true)),
            offsets,
            values,
            None,
        ))
    }

    #[test_case(sample_list_array(None), 0, 0 => Ok(Some(10)) ; "list element 0 row 0")]
    #[test_case(sample_list_array(None), 0, 1 => Ok(None) ; "list element 1 null child row 0")]
    #[test_case(sample_list_array(None), 1, 0 => Ok(Some(30)) ; "list element 0 row 1")]
    #[test_case(sample_large_list_array(), 0, 1 => Ok(Some(200)) ; "large list element 1 row 0")]
    #[test_case(sample_list_array(None), 0, 2 => Err(TestConvertError::MissingField("2".to_string())) ; "list element out of bounds")]
    #[test_case(sample_list_array(Some(NullBuffer::from(vec![false, true]))), 0, 0 => Err(TestConvertError::NotNull) ; "list null row")]
    #[test_case(Arc::new(Int64Array::from(vec![1])), 0, 0 => Err(TestConvertError::type_mismatch("ListArray or LargeListArray", "Int64")) ; "list type mismatch")]
    fn list_element_cell(
        arr: ArrayRef,
        row_idx: usize,
        elem_idx: usize,
    ) -> Result<Option<i64>, TestConvertError> {
        let elem_cell = ArrowCell::new(arr, row_idx)
            .list_element_cell(elem_idx)
            .map_err(TestConvertError::from)?;
        if elem_cell.is_null() {
            return Ok(None);
        }
        elem_cell.as_i64().map(Some).map_err(TestConvertError::from)
    }

    fn create_test_arrow_schema() -> Arc<arrow::datatypes::Schema> {
        Arc::new(ArrowSchema::new(vec![
            Field::new("col", DataType::Utf8, false),
            Field::new("num", DataType::Int64, false),
        ]))
    }

    fn create_test_arrow_schema_bytes() -> bytes::Bytes {
        let arrow_schema = create_test_arrow_schema();
        let mut schema_buf = Vec::new();
        let _ =
            StreamWriter::try_new(&mut schema_buf, &arrow_schema).expect("valid test arrow schema");
        schema_buf.into()
    }

    #[test]
    fn arrow_stream_decoder_batches() -> anyhow::Result<()> {
        let arrow_schema = create_test_arrow_schema();
        let schema_bytes = create_test_arrow_schema_bytes();

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(Int64Array::from(vec![1, 2])),
            ],
        )?;

        let mut stream_buf = Vec::new();
        let mut writer = StreamWriter::try_new(&mut stream_buf, &arrow_schema)?;
        writer.write(&batch)?;
        writer.finish()?;
        let batch_bytes = bytes::Bytes::copy_from_slice(&stream_buf[schema_bytes.len()..]);

        let mut decoder = ArrowStreamDecoder::new();
        decoder.set_schema_bytes(bytes::Bytes::new())?;
        decoder.set_schema_bytes(schema_bytes)?;

        let decoded = decoder
            .decode_batch(batch_bytes)?
            .expect("should decode batch");
        assert_eq!(decoded.num_rows(), 2);
        decoder.finish()?;
        Ok(())
    }

    #[tokio::test]
    async fn arrow_response_reader_multiple_batches() -> anyhow::Result<()> {
        let arrow_schema = create_test_arrow_schema();
        let schema_bytes = create_test_arrow_schema_bytes();

        let batch1 = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["hello", "world"])),
                Arc::new(Int64Array::from(vec![42, 100])),
            ],
        )?;
        let empty_batch = RecordBatch::new_empty(arrow_schema.clone());
        let batch2 = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["last"])),
                Arc::new(Int64Array::from(vec![999])),
            ],
        )?;

        let mut stream_buf = Vec::new();
        let mut writer = StreamWriter::try_new(&mut stream_buf, &arrow_schema)?;
        writer.write(&batch1)?;
        writer.write(&empty_batch)?;
        writer.write(&batch2)?;
        writer.finish()?;
        let batch_buf = stream_buf[schema_bytes.len()..].to_vec();

        let mut reader =
            ArrowReader::Response(ArrowResponseReader::new(schema_bytes, batch_buf.into()));

        let b1 = reader
            .next_record_batch()
            .await?
            .expect("should return first batch");
        assert_eq!(b1.num_rows(), 2);

        let b2 = reader
            .next_record_batch()
            .await?
            .expect("should return second non-empty batch");
        assert_eq!(b2.num_rows(), 1);

        assert!(reader.next_record_batch().await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn arrow_response_reader_empty_record_batch() -> anyhow::Result<()> {
        let schema_bytes = create_test_arrow_schema_bytes();

        let mut reader =
            ArrowReader::Response(ArrowResponseReader::new(schema_bytes, bytes::Bytes::new()));
        assert!(reader.next_record_batch().await?.is_none());
        Ok(())
    }

    #[test_case(
        bytes::Bytes::from_static(b"\xff\xff\xff\xff\x08\x00\x00\x00invalid_schema"),
        bytes::Bytes::from_static(b"batch");
        "malformed schema"
    )]
    #[test_case(
        create_test_arrow_schema_bytes(),
        bytes::Bytes::from_static(b"\xff\xff\xff\xff\x08\x00\x00\x00invalid_batch");
        "malformed record batch"
    )]
    #[test_case(
        create_test_arrow_schema_bytes(),
        bytes::Bytes::from_static(b"\xff\xff\xff\xff\x10\x00\x00\x00truncated");
        "truncated stream"
    )]
    #[tokio::test]
    async fn arrow_response_reader_malformed_ipc(
        serialized_schema: bytes::Bytes,
        serialized_record_batch: bytes::Bytes,
    ) {
        let mut reader = ArrowReader::Response(ArrowResponseReader::new(
            serialized_schema,
            serialized_record_batch,
        ));
        let err = reader
            .next_record_batch()
            .await
            .expect_err("should fail on malformed IPC");
        assert!(matches!(err, RowError::InvalidRowFormat(_)), "{err:?}");
    }
}
