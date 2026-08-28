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
use crate::query::ColumnIndex;

/// A reference to a single cell within an Arrow array.
#[doc(hidden)]
#[derive(Clone, Copy, Debug)]
pub struct ArrowCell<'a> {
    array: &'a dyn arrow::array::Array,
    pub(crate) row_idx: usize,
}

impl<'a> ArrowCell<'a> {
    /// Creates a new `ArrowCell`.
    pub(crate) fn new(array: &'a dyn arrow::array::Array, row_idx: usize) -> Self {
        Self { array, row_idx }
    }

    /// Returns true if the cell is null.
    #[doc(hidden)]
    pub fn is_null(&self) -> bool {
        self.array.is_null(self.row_idx)
    }

    /// Extracts a field from a struct array cell by column index or name and converts it to `T`.
    #[doc(hidden)]
    pub fn take<T: crate::query::FromSql, I: ColumnIndex>(
        &self,
        index: I,
    ) -> Result<T, ConvertError> {
        let field_cell = self.struct_field_cell(&index)?;
        T::from_arrow(field_cell)
    }

    fn resolve_index<I: ColumnIndex>(
        &self,
        col: &I,
        struct_arr: &arrow::array::StructArray,
    ) -> Result<usize, ConvertError> {
        col.arrow_index(struct_arr)
            .ok_or_else(|| ConvertError::MissingField(format!("{col}")))
    }

    /// Returns the data type of the underlying array.
    pub(crate) fn data_type(&self) -> &arrow::datatypes::DataType {
        self.array.data_type()
    }

    /// Returns a string representation of the data type.
    pub(crate) fn data_type_str(&self) -> String {
        format!("{:?}", self.array.data_type())
    }

    /// Extracts a child `ArrowCell` from a struct array cell by column index or name.
    fn struct_field_cell<I: ColumnIndex>(&self, index: &I) -> Result<ArrowCell<'_>, ConvertError> {
        if self.is_null() {
            return Err(ConvertError::NotNull);
        }

        let Some(struct_arr) = self.downcast_ref::<arrow::array::StructArray>() else {
            return Err(ConvertError::TypeMismatch {
                expected: "struct array",
                got: wkt::Value::String(self.data_type_str()),
            });
        };

        let idx = self.resolve_index(index, struct_arr)?;
        let col = struct_arr.column(idx);
        Ok(ArrowCell {
            array: col.as_ref(),
            row_idx: self.row_idx,
        })
    }

    /// Downcasts the underlying Arrow array to a specific type.
    pub(crate) fn downcast_ref<T: arrow::array::Array + 'static>(&self) -> Option<&T> {
        self.array.as_any().downcast_ref::<T>()
    }

    /// Downcasts to the specified array type and returns the value at the cell's row index.
    pub(crate) fn downcast_value<T, V, F>(&self, f: F) -> Result<V, ConvertError>
    where
        T: arrow::array::Array + 'static,
        F: FnOnce(&T, usize) -> V,
    {
        if self.is_null() {
            return Err(ConvertError::NotNull);
        }
        let arr = self
            .downcast_ref::<T>()
            .ok_or_else(|| ConvertError::TypeMismatch {
                expected: std::any::type_name::<T>(),
                got: wkt::Value::String(self.data_type_str()),
            })?;
        Ok(f(arr, self.row_idx))
    }

    /// Returns the cell's value as a boolean.
    pub(crate) fn as_bool(&self) -> Result<bool, ConvertError> {
        self.downcast_value::<arrow::array::BooleanArray, _, _>(|arr, idx| arr.value(idx))
    }

    /// Returns the cell's value as an `i64`.
    pub(crate) fn as_i64(&self) -> Result<i64, ConvertError> {
        self.downcast_value::<arrow::array::Int64Array, _, _>(|arr, idx| arr.value(idx))
    }

    /// Returns the cell's value as an `i32`.
    pub(crate) fn as_i32(&self) -> Result<i32, ConvertError> {
        if self.is_null() {
            return Err(ConvertError::NotNull);
        }
        if let Some(arr) = self.downcast_ref::<arrow::array::Int64Array>() {
            return i32::try_from(arr.value(self.row_idx))
                .map_err(|e| ConvertError::Convert(Box::new(e)));
        }
        if let Some(arr) = self.downcast_ref::<arrow::array::Int32Array>() {
            return Ok(arr.value(self.row_idx));
        }
        Err(ConvertError::TypeMismatch {
            expected: "Int64Array or Int32Array",
            got: wkt::Value::String(self.data_type_str()),
        })
    }

    /// Returns the cell's value as an `f64`.
    pub(crate) fn as_f64(&self) -> Result<f64, ConvertError> {
        self.downcast_value::<arrow::array::Float64Array, _, _>(|arr, idx| arr.value(idx))
    }

    /// Returns the cell's value as an `f32`.
    pub(crate) fn as_f32(&self) -> Result<f32, ConvertError> {
        if self.is_null() {
            return Err(ConvertError::NotNull);
        }
        if let Some(arr) = self.downcast_ref::<arrow::array::Float64Array>() {
            return Ok(arr.value(self.row_idx) as f32);
        }
        if let Some(arr) = self.downcast_ref::<arrow::array::Float32Array>() {
            return Ok(arr.value(self.row_idx));
        }
        Err(ConvertError::TypeMismatch {
            expected: "Float64Array or Float32Array",
            got: wkt::Value::String(self.data_type_str()),
        })
    }

    /// Returns the cell's value as a string slice (`&str`).
    pub(crate) fn as_str(&self) -> Result<&str, ConvertError> {
        if self.is_null() {
            return Err(ConvertError::NotNull);
        }
        if let Some(arr) = self.downcast_ref::<arrow::array::StringArray>() {
            Ok(arr.value(self.row_idx))
        } else if let Some(arr) = self.downcast_ref::<arrow::array::LargeStringArray>() {
            Ok(arr.value(self.row_idx))
        } else {
            Err(ConvertError::TypeMismatch {
                expected: "StringArray or LargeStringArray",
                got: wkt::Value::String(self.data_type_str()),
            })
        }
    }

    /// Returns the cell's value as a byte slice (`&[u8]`).
    pub(crate) fn as_bytes(&self) -> Result<&[u8], ConvertError> {
        if self.is_null() {
            return Err(ConvertError::NotNull);
        }
        if let Some(arr) = self.downcast_ref::<arrow::array::BinaryArray>() {
            Ok(arr.value(self.row_idx))
        } else if let Some(arr) = self.downcast_ref::<arrow::array::LargeBinaryArray>() {
            Ok(arr.value(self.row_idx))
        } else {
            Err(ConvertError::TypeMismatch {
                expected: "BinaryArray or LargeBinaryArray",
                got: wkt::Value::String(self.data_type_str()),
            })
        }
    }
}
