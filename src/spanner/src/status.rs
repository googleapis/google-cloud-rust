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

use crate::google;

impl gaxi::prost::ToProto<google::rpc::Status> for google_cloud_rpc::model::Status {
    type Output = google::rpc::Status;
    fn to_proto(self) -> std::result::Result<google::rpc::Status, gaxi::prost::ConvertError> {
        Ok(google::rpc::Status {
            code: self.code,
            message: self.message.to_string(),
            details: self
                .details
                .into_iter()
                .filter_map(gaxi::grpc::status::any_to_prost)
                .collect(),
        })
    }
}

impl gaxi::prost::FromProto<google_cloud_rpc::model::Status> for google::rpc::Status {
    fn cnv(
        self,
    ) -> std::result::Result<google_cloud_rpc::model::Status, gaxi::prost::ConvertError> {
        let mut status = google_cloud_rpc::model::Status::new();
        status = status.set_code(self.code);
        status = status.set_message(self.message);
        status = status.set_details(
            self.details
                .into_iter()
                .filter_map(gaxi::grpc::status::any_from_prost)
                .collect::<Vec<wkt::Any>>(),
        );
        Ok(status)
    }
}
