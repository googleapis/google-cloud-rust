// Copyright 2024 Google LLC
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

use gcp_sdk_gax::error::Error;

#[cfg(test)]
mod test {
    use super::*;

    #[derive(Debug, Default)]
    struct LeafError {}

    impl LeafError {
        fn hey(&self) -> &'static str {
            "hey"
        }
    }

    impl std::fmt::Display for LeafError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "other error")
        }
    }

    impl std::error::Error for LeafError {}

    #[derive(Debug)]
    struct MiddleError {
        pub source: Option<Box<dyn std::error::Error + Send + Sync>>,
    }

    impl std::fmt::Display for MiddleError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "middle error")
        }
    }

    impl std::error::Error for MiddleError {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            match &self.source {
                Some(e) => Some(e.as_ref()),
                None => None,
            }
        }
    }

    #[test]
    fn downcast() -> Result<(), Box<dyn std::error::Error>> {
        let leaf_err = LeafError::default();
        let middle_err = MiddleError {
            source: Some(Box::new(leaf_err)),
        };
        let root_err = Error::other(middle_err);
        let msg = root_err.as_inner::<LeafError>().unwrap().hey();
        assert_eq!(msg, "hey");

        let root_err = Error::other(MiddleError { source: None });
        let inner_err = root_err.as_inner::<LeafError>();
        assert!(inner_err.is_none());
        Ok(())
    }
}
