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

#[cfg(test)]
mod enums {
    use google_cloud_secretmanager_v1::model::{SecretVersion, secret_version::State};

    #[test]
    fn test_default_value() {
        let default = State::default();
        assert_eq!(default, State::Unspecified);
    }

    #[test]
    fn test_deserialize_default() {
        let input = serde_json::json!({
            "name": "projects/test-only/secrets/my-secret/versions/my-version",
        });
        let secret_version = serde_json::from_value::<SecretVersion>(input).unwrap();
        assert_eq!(secret_version.state, State::Unspecified);
    }

    #[test]
    fn branch() -> anyhow::Result<()> {
        use serde_json::json;
        let state = serde_json::from_value::<State>(json!("DISABLED"))?;
        #[warn(clippy::wildcard_enum_match_arm)]
        let initial = match state {
            State::Unspecified => "u",
            State::Enabled => "e",
            State::Disabled => "i",
            State::Destroyed => "d",
            State::UnknownValue(_) => "unknown value",
            _ => "",
        };
        assert_eq!(initial, "i");
        Ok(())
    }
}
