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

use base64::Engine;

type Result = std::result::Result<String, Error>;

pub(crate) trait RequestParameter {
    fn format(&self) -> Result;
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("cannot format as request parameter {0:?}")]
    Format(Box<dyn std::error::Error + Send + Sync>),
}

impl RequestParameter for i32 {
    fn format(&self) -> Result {
        Ok(format!("{self}"))
    }
}
impl RequestParameter for i64 {
    fn format(&self) -> Result {
        Ok(format!("{self}"))
    }
}
impl RequestParameter for u32 {
    fn format(&self) -> Result {
        Ok(format!("{self}"))
    }
}
impl RequestParameter for u64 {
    fn format(&self) -> Result {
        Ok(format!("{self}"))
    }
}
impl RequestParameter for f32 {
    fn format(&self) -> Result {
        Ok(format!("{self}"))
    }
}
impl RequestParameter for f64 {
    fn format(&self) -> Result {
        Ok(format!("{self}"))
    }
}
impl RequestParameter for String {
    fn format(&self) -> Result {
        Ok(self.clone())
    }
}
impl RequestParameter for bool {
    fn format(&self) -> Result {
        Ok(format!("{self}"))
    }
}

impl RequestParameter for bytes::Bytes {
    fn format(&self) -> Result {
        use base64::engine::general_purpose::URL_SAFE_NO_PAD;
        Ok(URL_SAFE_NO_PAD.encode(self.iter()))
    }
}

impl RequestParameter for wkt::Duration {
    fn format(&self) -> Result {
        Ok(String::from(self))
    }
}

impl RequestParameter for wkt::FieldMask {
    fn format(&self) -> Result {
        Ok(self.paths.join(","))
    }
}

impl RequestParameter for wkt::Timestamp {
    fn format(&self) -> Result {
        self.to_json().map_err(|e| Error::Format(e.into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    type Result = std::result::Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn with_value() -> Result {
        let want = "42".to_string();
        assert_eq!(want, RequestParameter::format(&42_i32)?);
        assert_eq!(want, RequestParameter::format(&42_i64)?);
        assert_eq!(want, RequestParameter::format(&42_u32)?);
        assert_eq!(want, RequestParameter::format(&42_u64)?);
        assert_eq!(want, RequestParameter::format(&42_f32)?);
        assert_eq!(want, RequestParameter::format(&42_f64)?);
        Ok(())
    }

    #[test]
    fn duration() -> Result {
        let d = wkt::Duration::new(12, 345_678_900)?;
        let f = RequestParameter::format(&d)?;
        assert_eq!("12.345678900s", f);
        Ok(())
    }

    #[test]
    fn field_mask() -> Result {
        let fm = wkt::FieldMask::default().set_paths(["a", "b"].map(str::to_string).to_vec());
        let f = RequestParameter::format(&fm)?;
        assert_eq!("a,b", f);
        Ok(())
    }

    #[test]
    fn timestamp() -> Result {
        let ts = wkt::Timestamp::default();
        let f = RequestParameter::format(&ts)?;
        assert_eq!("1970-01-01T00:00:00Z", f);
        Ok(())
    }
}
