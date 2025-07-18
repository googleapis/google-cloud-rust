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

macro_rules! visitor_32 {
    ($name: ident, $t: ty, $msg: literal) => {
        struct $name;

        impl serde::de::Visitor<'_> for $name {
            type Value = $t;

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                // ProtoJSON says that both strings and numbers are accepted.
                // Parse the string as a `f64` number (all JSON numbers are
                // `f64`) and then try to parse the result as the target type.
                let number = value.parse::<f64>().map_err(E::custom)?;
                self.visit_f64(number)
            }

            fn visit_i64<E>(self, value: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    _ if value < <$t>::MIN as i64 => Err(self::value_error(value)),
                    _ if value > <$t>::MAX as i64 => Err(self::value_error(value)),
                    _ => Ok(value as Self::Value),
                }
            }

            fn visit_u64<E>(self, value: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    _ if value > <$t>::MAX as u64 => Err(self::value_error(value)),
                    _ => Ok(value as Self::Value),
                }
            }

            fn visit_f64<E>(self, value: f64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    _ if value < <$t>::MIN as f64 => Err(self::value_error(value)),
                    _ if value > <$t>::MAX as f64 => Err(self::value_error(value)),
                    _ if value.fract().abs() > 0.0 => Err(self::value_error(value)),
                    // In Rust floating point to integer conversions are
                    // "rounded towards zero":
                    //     https://doc.rust-lang.org/reference/expressions/operator-expr.html#r-expr.as.numeric.float-as-int
                    // Because we are in range, and the fractional part is 0,
                    // this conversion is safe.
                    _ => Ok(value as Self::Value),
                }
            }

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str($msg)
            }
        }

        fn value_error<T, E>(value: T) -> E
        where
            T: std::fmt::Display,
            E: serde::de::Error,
        {
            E::invalid_value(Other(&format!("{value}")), &$msg)
        }
    };
}
