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

use crate::google::cloud::bigquery::storage::v1;
use crate::model::ProtoSchema;
use gaxi::prost::{ConvertError, FromProto, ToProto};

impl ToProto<v1::ProtoSchema> for ProtoSchema {
    type Output = v1::ProtoSchema;
    fn to_proto(self) -> Result<v1::ProtoSchema, ConvertError> {
        // TODO(#5315) - implement conversions for DescriptorProto
        Err(ConvertError::Unimplemented)
    }
}

impl FromProto<ProtoSchema> for v1::ProtoSchema {
    fn cnv(self) -> Result<ProtoSchema, ConvertError> {
        // TODO(#5315) - implement conversions for DescriptorProto
        Err(ConvertError::Unimplemented)
    }
}
