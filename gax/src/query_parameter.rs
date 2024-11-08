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

pub type Error = serde_json::Error;
type Result = serde_json::Result<String>;

pub fn format<T>(
    name: &'static str,
    parameter: &T,
) -> serde_json::Result<Option<(&'static str, String)>>
where
    T: QueryParameter,
{
    QueryParameter::format(parameter.into())
        .map(|result| result.map(|s| (name, s)))
        .transpose()
}

pub trait QueryParameter {
    fn format(&self) -> Option<Result>;
}

impl<T: QueryParameter> QueryParameter for Option<T> {
    fn format(&self) -> Option<Result> {
        self.as_ref().map(|v| QueryParameter::format(v)).flatten()
    }
}

impl<T: RequiredQueryParameter> QueryParameter for T {
    fn format(&self) -> Option<Result> {
        Some(RequiredQueryParameter::format(self))
    }
}

/// Format query parameters as strings.
trait RequiredQueryParameter {
    fn format(&self) -> Result;
}

impl RequiredQueryParameter for String {
    fn format(&self) -> Result {
        Ok(self.clone())
    }
}

impl RequiredQueryParameter for i32 {
    fn format(&self) -> Result {
        Ok(format!("{self}"))
    }
}

impl RequiredQueryParameter for u32 {
    fn format(&self) -> Result {
        Ok(format!("{self}"))
    }
}

impl RequiredQueryParameter for i64 {
    fn format(&self) -> Result {
        Ok(format!("{self}"))
    }
}

impl RequiredQueryParameter for u64 {
    fn format(&self) -> Result {
        Ok(format!("{self}"))
    }
}

impl RequiredQueryParameter for f32 {
    fn format(&self) -> Result {
        Ok(format!("{self}"))
    }
}

impl RequiredQueryParameter for f64 {
    fn format(&self) -> Result {
        Ok(format!("{self}"))
    }
}

impl RequiredQueryParameter for types::Duration {
    fn format(&self) -> Result {
        Ok(serde_json::to_value(self)?.as_str().unwrap().to_string())
    }
}

impl RequiredQueryParameter for types::FieldMask {
    fn format(&self) -> Result {
        Ok(self.paths.join(","))
    }
}

impl RequiredQueryParameter for types::Timestamp {
    fn format(&self) -> Result {
        Ok(serde_json::to_value(self)?.as_str().unwrap().to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    type Result = std::result::Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn none() -> Result {
        assert_eq!(None, QueryParameter::format(&None::<i32>).transpose()?);
        assert_eq!(None, QueryParameter::format(&None::<i64>).transpose()?);
        assert_eq!(None, QueryParameter::format(&None::<u32>).transpose()?);
        assert_eq!(None, QueryParameter::format(&None::<u64>).transpose()?);
        assert_eq!(None, QueryParameter::format(&None::<f32>).transpose()?);
        assert_eq!(None, QueryParameter::format(&None::<f64>).transpose()?);
        Ok(())
    }

    #[test]
    fn with_value() -> Result {
        let want = Some("42".to_string());
        assert_eq!(want, QueryParameter::format(&Some(42 as i32)).transpose()?);
        assert_eq!(want, QueryParameter::format(&Some(42 as i64)).transpose()?);
        assert_eq!(want, QueryParameter::format(&Some(42 as u32)).transpose()?);
        assert_eq!(want, QueryParameter::format(&Some(42 as u64)).transpose()?);
        assert_eq!(want, QueryParameter::format(&Some(42 as f32)).transpose()?);
        assert_eq!(want, QueryParameter::format(&Some(42 as f64)).transpose()?);
        Ok(())
    }

    #[test]
    fn duration() -> Result {
        let d = types::Duration::new(12, 345_678_900);
        let f = QueryParameter::format(&d).transpose()?;
        assert_eq!(Some("12.345678900s".to_string()), f);
        Ok(())
    }

    #[test]
    fn field_mask() -> Result {
        let fm = types::FieldMask::default().set_paths(["a", "b"].map(str::to_string).to_vec());
        let f = QueryParameter::format(&fm).transpose()?;
        assert_eq!(Some("a,b".to_string()), f);
        Ok(())
    }

    #[test]
    fn timestamp() -> Result {
        let ts = types::Timestamp::default();
        let f = QueryParameter::format(&ts).transpose()?;
        assert_eq!(Some("1970-01-01T00:00:00Z".to_string()), f);
        Ok(())
    }
}
