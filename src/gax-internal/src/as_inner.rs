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

use std::error::Error;

/// Extract the first source error of type `T`.
pub fn as_inner<T, E>(error: &E) -> Option<&T>
where
    T: Error + 'static,
    E: Error,
{
    let mut e = error.source()?;
    // Prevent infinite loops due to cycles in the `source()` errors. This seems
    // unlikely, and it would require effort to create, but it is easy to
    // prevent.
    for _ in 0..32 {
        if let Some(value) = e.downcast_ref::<T>() {
            return Some(value);
        }
        e = e.source()?;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_found() {
        let err = BaseError::BaseError;

        let err = WrappedError::Inner(err.into());
        let inner = as_inner::<BaseError, _>(&err);
        assert!(inner.is_some());

        let err = WrappedError::Inner(err.into());
        let inner = as_inner::<BaseError, _>(&err);
        assert!(inner.is_some());

        let err = WrappedError::Inner(err.into());
        let inner = as_inner::<BaseError, _>(&err);
        assert!(inner.is_some());
    }

    #[test]
    fn simple_not_found() {
        let err = WrappedError::Inner("not a BaseError".into());
        let inner = as_inner::<BaseError, _>(&err);
        assert!(inner.is_none());

        let err = WrappedError::Inner(err.into());
        let inner = as_inner::<BaseError, _>(&err);
        assert!(inner.is_none());

        let err = WrappedError::Inner(err.into());
        let inner = as_inner::<BaseError, _>(&err);
        assert!(inner.is_none());
    }

    #[test]
    fn at_least_one_level_deep() {
        let err = BaseError::BaseError;
        let inner = as_inner::<BaseError, _>(&err);
        assert!(inner.is_none());
    }

    #[test]
    fn first_matching_type() {
        let err = RecursiveError;
        let inner = as_inner::<RecursiveError, _>(&err);
        assert!(inner.is_some());
    }

    #[test]
    fn avoid_infinite_loops() {
        let err = RecursiveError;
        let inner = as_inner::<BaseError, _>(&err);
        assert!(inner.is_none());
    }

    #[derive(Debug, thiserror::Error)]
    enum BaseError {
        #[error("base error")]
        BaseError,
    }

    #[derive(Debug, thiserror::Error)]
    enum WrappedError {
        #[error("source={0}")]
        Inner(#[source] Box<dyn Error + Send + Sync>),
    }

    #[derive(Debug)]
    struct RecursiveError;

    impl std::fmt::Display for RecursiveError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "RecursiveError")
        }
    }
    impl Error for RecursiveError {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            Some(self)
        }
    }
}
