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

pub mod generated;

pub mod conformance {
    include!("generated/protos/conformance.rs");
    include!("generated/convert/convert.rs");
}

#[cfg(test)]
mod test {
    use super::*;
    use generated::test_protos::TestAllTypesProto3;
    use serde_json::json;

    #[test]
    fn field13() -> anyhow::Result<()> {
        let input = json!({"FieldName13": 0});
        let message = serde_json::from_value::<TestAllTypesProto3>(input)?;
        let value = serde_json::to_value(message)?;
        assert_eq!(value, json!({}));
        Ok(())
    }
}
