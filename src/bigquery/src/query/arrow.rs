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
use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug)]
pub(crate) struct ArrowStreamDecoder {
    decoder: arrow::ipc::reader::StreamDecoder,
}

#[cfg_attr(not(test), allow(dead_code))]
impl ArrowStreamDecoder {
    pub(crate) fn new() -> Self {
        Self {
            decoder: arrow::ipc::reader::StreamDecoder::new(),
        }
    }

    pub(crate) fn set_schema_bytes(&mut self, schema_bytes: &[u8]) -> Result<(), RowError> {
        if schema_bytes.is_empty() {
            return Ok(());
        }
        let mut buf = arrow::buffer::Buffer::from_slice_ref(schema_bytes);
        while let Some(_msg) = self.decoder.decode(&mut buf).map_err(|e| {
            RowError::InvalidRowFormat(format!("failed to decode arrow schema: {e}"))
        })? {}
        Ok(())
    }

    pub(crate) fn decode_batch(
        &mut self,
        batch_bytes: &[u8],
    ) -> Result<Option<RecordBatch>, RowError> {
        let mut buf = arrow::buffer::Buffer::from_slice_ref(batch_bytes);
        self.decode_buffer(&mut buf)
    }

    pub(crate) fn decode_buffer(
        &mut self,
        buf: &mut arrow::buffer::Buffer,
    ) -> Result<Option<RecordBatch>, RowError> {
        self.decoder
            .decode(buf)
            .map_err(|e| RowError::InvalidRowFormat(format!("failed to decode arrow batch: {e}")))
    }

    pub(crate) fn finish(&mut self) -> Result<(), RowError> {
        self.decoder
            .finish()
            .map_err(|e| RowError::InvalidRowFormat(format!("failed to decode arrow stream: {e}")))
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) trait RecordBatchSource {
    async fn next_record_batch(&mut self) -> Result<Option<RecordBatch>, RowError>;
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug)]
pub(crate) struct ArrowResponseReader {
    decoder: ArrowStreamDecoder,
    schema_bytes: Option<bytes::Bytes>,
    batch_buf: arrow::buffer::Buffer,
}

#[cfg_attr(not(test), allow(dead_code))]
impl ArrowResponseReader {
    pub(crate) fn new(
        serialized_schema: bytes::Bytes,
        serialized_record_batch: bytes::Bytes,
    ) -> Self {
        Self {
            decoder: ArrowStreamDecoder::new(),
            schema_bytes: (!serialized_schema.is_empty()).then_some(serialized_schema),
            batch_buf: arrow::buffer::Buffer::from(serialized_record_batch),
        }
    }
}

impl RecordBatchSource for ArrowResponseReader {
    async fn next_record_batch(&mut self) -> Result<Option<RecordBatch>, RowError> {
        if let Some(schema_bytes) = self.schema_bytes.take() {
            self.decoder.set_schema_bytes(&schema_bytes)?;
        }
        while !self.batch_buf.is_empty() {
            let prev_len = self.batch_buf.len();
            if let Some(batch) = self.decoder.decode_buffer(&mut self.batch_buf)?
                && batch.num_rows() > 0
            {
                return Ok(Some(batch));
            }
            if self.batch_buf.len() == prev_len {
                break;
            }
        }
        self.decoder.finish()?;
        Ok(None)
    }
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug)]
pub(crate) enum ArrowReader {
    Response(ArrowResponseReader),
}

impl RecordBatchSource for ArrowReader {
    async fn next_record_batch(&mut self) -> Result<Option<RecordBatch>, RowError> {
        match self {
            Self::Response(r) => r.next_record_batch().await,
        }
    }
}

#[cfg_attr(not(test), allow(dead_code))]
impl ArrowReader {
    pub(crate) fn can_fallback_to_rest(&self) -> bool {
        match self {
            Self::Response(_) => false,
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        BinaryArray, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
        LargeBinaryArray, LargeStringArray, StringArray,
    };
    use arrow::datatypes::DataType;
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

    fn create_test_arrow_schema() -> Arc<arrow::datatypes::Schema> {
        use arrow::datatypes::{Field, Schema as ArrowSchema};

        Arc::new(ArrowSchema::new(vec![
            Field::new("col", DataType::Utf8, false),
            Field::new("num", DataType::Int64, false),
        ]))
    }

    fn create_test_arrow_schema_bytes() -> bytes::Bytes {
        use arrow::ipc::writer::StreamWriter;

        let arrow_schema = create_test_arrow_schema();
        let mut schema_buf = Vec::new();
        let _ =
            StreamWriter::try_new(&mut schema_buf, &arrow_schema).expect("valid test arrow schema");
        schema_buf.into()
    }

    #[test]
    fn arrow_stream_decoder_batches() -> anyhow::Result<()> {
        use arrow::ipc::writer::StreamWriter;

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
        let batch_bytes = &stream_buf[schema_bytes.len()..];

        let mut decoder = ArrowStreamDecoder::new();
        decoder.set_schema_bytes(&[])?;
        decoder.set_schema_bytes(&schema_bytes)?;

        let decoded = decoder
            .decode_batch(batch_bytes)?
            .expect("should decode batch");
        assert_eq!(decoded.num_rows(), 2);
        decoder.finish()?;
        Ok(())
    }

    #[tokio::test]
    async fn arrow_response_reader_multiple_batches() -> anyhow::Result<()> {
        use arrow::ipc::writer::StreamWriter;

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
        assert!(!reader.can_fallback_to_rest());

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

    #[test_case(
        bytes::Bytes::from_static(b"invalid arrow schema bytes"),
        bytes::Bytes::new();
        "malformed schema"
    )]
    #[test_case(
        create_test_arrow_schema_bytes(),
        bytes::Bytes::from_static(b"\xff\xff\xff\xff\x10\x00\x00\x00corrupted_batch_header");
        "malformed record batch"
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
