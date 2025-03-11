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
mod generated {
    use google_cloud_wkt::Syntax;

    #[test]
    fn string_to_constant() {
        let got = Syntax::from_str_name("SYNTAX_PROTO2");
        assert_eq!(Some(google_cloud_wkt::Syntax::SYNTAX_PROTO2), got)
    }
}

#[cfg(test)]
mod desired_protobuf {
    use google_cloud_wkt::file_options::OptimizeMode;
    use serde_json::json;
    use test_case::test_case;
    type TestResult = Result<(), Box<dyn std::error::Error>>;

    #[test_case(OptimizeMode::SPEED, 1)]
    #[test_case(OptimizeMode::CODE_SIZE, 2)]
    #[test_case(OptimizeMode::LITE_RUNTIME, 3)]
    #[test_case(OptimizeMode::from(42), 42)]
    fn serialize(input: OptimizeMode, want: i32) -> TestResult {
        let got = serde_json::to_value(input)?;
        let want = json!(want);
        assert_eq!(got, want);
        Ok(())
    }

    #[test_case(1, OptimizeMode::SPEED)]
    #[test_case(2, OptimizeMode::CODE_SIZE)]
    #[test_case(3, OptimizeMode::LITE_RUNTIME)]
    #[test_case(42, OptimizeMode::from(42))]
    fn deserialize(input: i32, want: OptimizeMode) -> TestResult {
        let value = json!(input);
        let got = serde_json::from_value::<OptimizeMode>(value)?;
        assert_eq!(got, want);
        Ok(())
    }

    #[test_case(OptimizeMode::SPEED, 1)]
    #[test_case(OptimizeMode::CODE_SIZE, 2)]
    #[test_case(OptimizeMode::LITE_RUNTIME, 3)]
    #[test_case(OptimizeMode::from(42), 42)]
    fn value(input: OptimizeMode, want: i32) {
        let got = input.value();
        assert_eq!(got, want);
    }

    #[test_case(OptimizeMode::SPEED, "SPEED")]
    #[test_case(OptimizeMode::CODE_SIZE, "CODE_SIZE")]
    #[test_case(OptimizeMode::LITE_RUNTIME, "LITE_RUNTIME")]
    #[test_case(OptimizeMode::from(42), "UNKNOWN-VALUE:42")]
    fn to_string(input: OptimizeMode, want: &str) {
        let got = input.as_str_name();
        assert_eq!(got.as_ref(), want);
    }

    #[test_case("SPEED", OptimizeMode::SPEED)]
    #[test_case("CODE_SIZE", OptimizeMode::CODE_SIZE)]
    #[test_case("LITE_RUNTIME", OptimizeMode::LITE_RUNTIME)]
    fn from_string(input: &str, want: OptimizeMode) {
        let got = OptimizeMode::from_str_name(input);
        assert_eq!(got, Some(want));
    }

    #[test]
    fn default() {
        let got = OptimizeMode::default();
        assert_eq!(got.value(), 0);
    }
}
