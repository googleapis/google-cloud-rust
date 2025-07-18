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

macro_rules! visitor_64 {
    ($name: ident, $t: ty, $msg: literal) => {
        struct $name;

        impl serde::de::Visitor<'_> for $name {
            type Value = $t;

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                // ProtoJSON says that both strings and numbers are accepted.
                // The difficulty is dealing with integer strings that are just
                // outside the range for i64 or u64. Parsing as `f64` rounds
                // those numbers and may result in incorrectly accepting the
                // value.
                //
                // First try to parse the string as a i128, if it works and it
                // is in range, return that value.
                if let Ok(v) = value.parse::<i128>() {
                    return self.visit_i128(v);
                }
                // Next, try to parse it as a `f64` number (all JSON numbers are
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
                    _ if value.fract() != 0.0 => Err(self::value_error(value)),
                    // In Rust floating point to integer conversions are
                    // "rounded towards zero":
                    //     https://doc.rust-lang.org/reference/expressions/operator-expr.html#r-expr.as.numeric.float-as-int
                    // Because we are in range, and the fractional part is zero,
                    // this conversion is safe.
                    _ => Ok(value as Self::Value),
                }
            }

            fn visit_i128<E>(self, value: i128) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    _ if value < <$t>::MIN as i128 => Err(self::value_error(value)),
                    _ if value > <$t>::MAX as i128 => Err(self::value_error(value)),
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
