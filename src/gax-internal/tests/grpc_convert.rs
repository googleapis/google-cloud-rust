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

use google_cloud_gax_internal::grpc;

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::metadata::MetadataMap;

    #[test]
    fn test_to_gax_response() -> anyhow::Result<()> {
        let tonic_body = prost_types::Duration {
            seconds: 123,
            nanos: 456,
        };
        let mut tonic_headers = MetadataMap::new();
        tonic_headers.insert("key", "value".parse().unwrap());
        let tonic_response = tonic::Response::from_parts(
            tonic_headers.clone(),
            tonic_body,
            tonic::Extensions::new(),
        );

        let gax_response = grpc::to_gax_response(tonic_response)?;
        assert_eq!(
            gax_response.body().to_owned(),
            wkt::Duration::clamp(123, 456)
        );
        assert_eq!(
            gax_response.headers().to_owned(),
            tonic_headers.into_headers()
        );

        Ok(())
    }
}
