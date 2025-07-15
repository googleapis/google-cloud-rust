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

use crate::google;
use gaxi::grpc::status::{any_from_prost, any_to_prost};
use gaxi::prost::{ConvertError, FromProto, ToProto};

impl ToProto<google::rpc::Status> for rpc::model::Status {
    type Output = google::rpc::Status;
    fn to_proto(self) -> Result<Self::Output, ConvertError> {
        Ok(Self::Output {
            code: self.code.to_proto()?,
            message: self.message.to_proto()?,
            details: self.details.into_iter().filter_map(any_to_prost).collect(),
        })
    }
}

impl FromProto<rpc::model::Status> for google::rpc::Status {
    fn cnv(self) -> Result<rpc::model::Status, ConvertError> {
        Ok(rpc::model::Status::new()
            .set_code(self.code)
            .set_message(self.message)
            .set_details(self.details.into_iter().filter_map(any_from_prost)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_proto() -> anyhow::Result<()> {
        let input = google::rpc::Status {
            code: 12,
            message: "test-message".into(),
            ..Default::default()
        };
        let got = input.cnv()?;
        let want = rpc::model::Status::new()
            .set_code(12)
            .set_message("test-message");
        assert_eq!(got, want);
        Ok(())
    }

    #[test]
    fn to_proto() -> anyhow::Result<()> {
        let input = rpc::model::Status::new()
            .set_code(12)
            .set_message("test-message");
        let got: google::rpc::Status = input.to_proto()?;
        let want = google::rpc::Status {
            code: 12,
            message: "test-message".into(),
            ..Default::default()
        };
        assert_eq!(got, want);
        Ok(())
    }
}
