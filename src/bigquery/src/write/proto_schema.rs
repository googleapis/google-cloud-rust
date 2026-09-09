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
        Ok(v1::ProtoSchema {
            proto_descriptor: self.proto_descriptor.map(|v| v.to_proto()).transpose()?,
        })
    }
}

impl FromProto<ProtoSchema> for v1::ProtoSchema {
    fn cnv(self) -> Result<ProtoSchema, ConvertError> {
        Ok(ProtoSchema::new()
            .set_or_clear_proto_descriptor(self.proto_descriptor.map(|v| v.cnv()).transpose()?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gaxi::prost::{FromProto, ToProto};
    use wkt;

    #[test]
    fn test_proto_schema_conversion() -> anyhow::Result<()> {
        let descriptor = wkt::DescriptorProto::default().set_name("TestMessage".to_string());
        let input = ProtoSchema::new().set_proto_descriptor(descriptor.clone());

        let proto: v1::ProtoSchema = input.clone().to_proto()?;
        assert!(proto.proto_descriptor.is_some());
        assert_eq!(
            proto.proto_descriptor.as_ref().unwrap().name,
            Some("TestMessage".to_string())
        );

        let back: ProtoSchema = proto.cnv()?;
        assert_eq!(back, input);
        Ok(())
    }

    #[test]
    fn test_proto_schema_none_conversion() -> anyhow::Result<()> {
        let input = ProtoSchema::new(); // proto_descriptor is None by default

        let proto: v1::ProtoSchema = input.clone().to_proto()?;
        assert!(proto.proto_descriptor.is_none());

        let back: ProtoSchema = proto.cnv()?;
        assert_eq!(back, input);
        assert!(back.proto_descriptor.is_none());
        Ok(())
    }
}
