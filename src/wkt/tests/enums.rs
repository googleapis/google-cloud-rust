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
mod tests {
    use google_cloud_wkt::Syntax;
    use google_cloud_wkt::file_options::OptimizeMode;
    use serde_json::json;
    use test_case::test_case;
    type TestResult = Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn string_to_constant() {
        let got = Syntax::from("SYNTAX_PROTO2");
        assert_eq!(Syntax::Proto2, got)
    }

    #[test_case(OptimizeMode::Speed, 1)]
    #[test_case(OptimizeMode::CodeSize, 2)]
    #[test_case(OptimizeMode::LiteRuntime, 3)]
    #[test_case(OptimizeMode::from(42), 42)]
    fn serialize(input: OptimizeMode, want: i32) -> TestResult {
        let got = serde_json::to_value(input)?;
        let want = json!(want);
        assert_eq!(got, want);
        Ok(())
    }

    #[test_case(1, OptimizeMode::Speed)]
    #[test_case(2, OptimizeMode::CodeSize)]
    #[test_case(3, OptimizeMode::LiteRuntime)]
    #[test_case(42, OptimizeMode::from(42))]
    fn deserialize(input: i32, want: OptimizeMode) -> TestResult {
        let value = json!(input);
        let got = serde_json::from_value::<OptimizeMode>(value)?;
        assert_eq!(got, want);
        Ok(())
    }

    #[test_case(OptimizeMode::Speed, 1)]
    #[test_case(OptimizeMode::CodeSize, 2)]
    #[test_case(OptimizeMode::LiteRuntime, 3)]
    #[test_case(OptimizeMode::from(42), 42)]
    fn value(input: OptimizeMode, want: i32) {
        let got = input.value();
        assert_eq!(got, Some(want));
    }

    #[test_case(OptimizeMode::Speed, "SPEED")]
    #[test_case(OptimizeMode::CodeSize, "CODE_SIZE")]
    #[test_case(OptimizeMode::LiteRuntime, "LITE_RUNTIME")]
    fn to_string(input: OptimizeMode, want: &str) {
        let got = input.name();
        assert_eq!(got, Some(want));
    }

    #[test_case("SPEED", OptimizeMode::Speed)]
    #[test_case("CODE_SIZE", OptimizeMode::CodeSize)]
    #[test_case("LITE_RUNTIME", OptimizeMode::LiteRuntime)]
    fn from_string(input: &str, want: OptimizeMode) {
        let got = OptimizeMode::from(input);
        assert_eq!(got, want);
    }

    #[test]
    fn unknown_i32() -> TestResult {
        let input = OptimizeMode::from(32);
        let got = input.value();
        assert_eq!(got, Some(32));
        let got = input.name();
        assert_eq!(got, None);
        let got = serde_json::to_value(&input)?;
        assert_eq!(got, json!(32));
        Ok(())
    }

    #[test]
    fn unknown_str() -> TestResult {
        let input = OptimizeMode::from("HEAVY_RUNTIME_HA_HA");
        let got = input.value();
        assert_eq!(got, None);
        let got = input.name();
        assert_eq!(got, Some("HEAVY_RUNTIME_HA_HA"));
        let got = serde_json::to_value(&input)?;
        assert_eq!(got, json!("HEAVY_RUNTIME_HA_HA"));
        Ok(())
    }

    #[test]
    fn default() {
        let got = OptimizeMode::default();
        assert_eq!(got.value(), Some(0));
    }
}
