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
use arrow::array::ArrayRef;
use std::sync::Arc;

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

    /// Returns the cell's value as an `IntervalMonthDayNano`.
    pub(crate) fn as_interval(
        &self,
    ) -> Result<arrow::datatypes::IntervalMonthDayNano, ConvertError> {
        if self.is_null() {
            return Err(ConvertError::NotNull);
        }
        use arrow::array::as_primitive_array;
        use arrow::datatypes::{DataType, IntervalMonthDayNanoType, IntervalUnit};
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

    /// Extracts a child `ArrowCell` by field name from a `StructArray`.
    pub(crate) fn struct_field_by_name(&self, name: &str) -> Result<ArrowCell, ConvertError> {
        if self.is_null() {
            return Err(ConvertError::NotNull);
        }
        use arrow::array::as_struct_array;
        use arrow::datatypes::DataType;
        match self.array.data_type() {
            DataType::Struct(_) => {
                let struct_arr = as_struct_array(&self.array);
                let col = struct_arr
                    .column_by_name(name)
                    .ok_or_else(|| ConvertError::MissingField(name.to_string()))?;
                Ok(ArrowCell::new(col.clone(), self.row_idx))
            }
            _ => Err(ConvertError::TypeMismatch {
                expected: "StructArray".to_string(),
                got: self.data_type_str(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        BinaryArray, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
        IntervalMonthDayNanoArray, IntervalYearMonthArray, LargeBinaryArray, LargeStringArray,
        StringArray, StructArray,
    };
    use arrow::buffer::NullBuffer;
    use arrow::datatypes::{DataType, Field, Fields, IntervalMonthDayNano};
    use std::sync::Arc;
    use test_case::test_case;

    #[derive(Debug, PartialEq)]
    enum TestConvertError {
        NotNull,
        TypeMismatch { expected: String, got: String },
        Convert(String),
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
                ConvertError::Convert(e) => Self::Convert(e.to_string()),
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
    #[test_case(Arc::new(Int64Array::from(vec![Some(i64::from(i32::MAX) + 1)])), 0 => Err(TestConvertError::Convert("out of range integral type conversion attempted".to_string())) ; "i32 overflow from Int64Array")]
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
    fn struct_field_by_name(
        arr: ArrayRef,
        row_idx: usize,
        name: &str,
    ) -> Result<Option<i64>, TestConvertError> {
        let field_cell = ArrowCell::new(arr, row_idx)
            .struct_field_by_name(name)
            .map_err(TestConvertError::from)?;
        if field_cell.is_null() {
            Ok(None)
        } else {
            field_cell
                .as_i64()
                .map(Some)
                .map_err(TestConvertError::from)
        }
    }
}
