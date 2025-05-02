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

//! Examples showing how to use enumerations.

#[cfg(test)]
mod test {
    use test_case::test_case;

    #[test]
    fn known() -> Result<(), Box<dyn std::error::Error>> {
        // ANCHOR: known
        use google_cloud_secretmanager_v1::model::secret_version::State;
        let enabled = State::Enabled;
        println!("State::Enabled = {enabled}");
        assert_eq!(enabled.value(), Some(1));
        assert_eq!(enabled.name(), Some("ENABLED"));

        let state = State::from(1);
        println!("state = {state}");
        assert_eq!(state.value(), Some(1));
        assert_eq!(state.name(), Some("ENABLED"));

        let state = State::from("ENABLED");
        println!("state = {state}");
        assert_eq!(state.value(), Some(1));
        assert_eq!(state.name(), Some("ENABLED"));
        println!("json = {}", serde_json::to_value(&state)?);
        // ANCHOR_END: known
        Ok(())
    }

    #[test]
    fn unknown_string() -> Result<(), Box<dyn std::error::Error>> {
        // ANCHOR: unknown_string
        use google_cloud_secretmanager_v1::model::secret_version::State;
        use serde_json::json;
        let state = State::from("STATE_NAME_FROM_THE_FUTURE");
        println!("state = {state}");
        assert_eq!(state.value(), None);
        assert_eq!(state.name(), Some("STATE_NAME_FROM_THE_FUTURE"));
        println!("json = {}", serde_json::to_value(&state)?);
        let s2 = serde_json::from_value::<State>(json!("STATE_NAME_FROM_THE_FUTURE"))?;
        assert_eq!(state, s2);
        // ANCHOR_END: unknown_string
        Ok(())
    }

    #[test]
    fn unknown_integer() -> Result<(), Box<dyn std::error::Error>> {
        // ANCHOR: unknown_integer
        use google_cloud_secretmanager_v1::model::secret_version::State;
        use serde_json::json;
        const MAGIC_INT_FROM_THE_FUTURE: i32 = 17;
        let state = State::from(MAGIC_INT_FROM_THE_FUTURE);
        println!("state = {state}");
        assert_eq!(state.value(), Some(17));
        assert_eq!(state.name(), None);
        println!("json = {}", serde_json::to_value(&state)?);
        let s2 = serde_json::from_value::<State>(json!(17))?;
        assert_eq!(state, s2);
        // ANCHOR_END: unknown_integer
        Ok(())
    }

    // ANCHOR: use
    use google_cloud_secretmanager_v1::model::secret_version::State;
    // ANCHOR_END: use

    // ANCHOR: match_with_wildcard
    fn match_with_wildcard(state: State) {
        match state {
            State::Unspecified => unreachable!("the service documents this value as unused"),
            State::Enabled => println!("the secret is enabled and can be accessed"),
            State::Disabled => println!(
                "the secret version may not be accessed, but can be enabled and then accessed"
            ),
            State::Destroyed => {
                println!("the secret is destroyed, the data is no longer accessible")
            }
            State::UnknownValue(u) => {
                println!("unknown State variant ({u:?}) time to update the library")
            }
            _ => unreachable!("never happens"),
        };
    }
    // ANCHOR_END: match_with_wildcard

    // ANCHOR: match_with_warnings
    fn match_with_warnings(state: State) {
        #[warn(clippy::wildcard_enum_match_arm)]
        match state {
            State::Unspecified => unreachable!("the service documents this value as unused"),
            State::Enabled => println!("the secret is enabled and can be accessed"),
            State::Disabled => println!(
                "the secret version may not be accessed, but can be enabled and then accessed"
            ),
            State::Destroyed => {
                println!("the secret is destroyed, the data is no longer accessible")
            }
            State::UnknownValue(u) => {
                println!("unknown State variant ({u:?}) time to update the library")
            }
            _ => unreachable!("never happens"),
        };
    }
    // ANCHOR_END: match_with_warnings

    #[test_case(State::Enabled)]
    #[test_case(State::Disabled)]
    #[test_case(State::Destroyed)]
    #[test_case(State::from("UNKNOWN"))]
    fn drive_match_expression(state: State) {
        match_with_warnings(state.clone());
        match_with_wildcard(state);
    }
}
