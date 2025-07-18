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

macro_rules! impl_serialize_as {
    ($t: ty, $ser_fn: ident) => {
        fn serialize_as<S>(value: &$t, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::ser::Serializer,
        {
            match value {
                x if x.is_nan() => serializer.serialize_str("NaN"),
                x if x.is_infinite() && x.is_sign_negative() => {
                    serializer.serialize_str("-Infinity")
                }
                x if x.is_infinite() => serializer.serialize_str("Infinity"),
                x => serializer.$ser_fn(*x),
            }
        }
    };
}

macro_rules! impl_visitor {
    ($name: ident, $t: ty, $msg: literal) => {
        struct $name;

        impl serde::de::Visitor<'_> for $name {
            type Value = $t;

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                // Handle special strings, see https://protobuf.dev/programming-guides/json/.
                match value {
                    "NaN" => Ok(<$t>::NAN),
                    "Infinity" => Ok(<$t>::INFINITY),
                    "-Infinity" => Ok(<$t>::NEG_INFINITY),
                    _ => self.visit_f64(value.parse::<f64>().map_err(E::custom)?),
                }
            }

            // Floats and doubles in serde_json may be serialized as integers
            // if they do not have a fractional part.
            fn visit_i64<E>(self, value: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                // This is trivial for `f64`. For `f32`, casting f64 to f32 is guaranteed to produce the closest possible float value:
                // See https://doc.rust-lang.org/reference/expressions/operator-expr.html#r-expr.as.numeric.float-narrowing
                Ok(value as Self::Value)
            }

            // Floats and doubles in serde_json may be serialized as integers
            // if they do not have a fractional part.
            fn visit_u64<E>(self, value: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                // This is trivial for `f64`. For `f32`, casting f64 to f32 is guaranteed to produce the closest possible float value:
                // See https://doc.rust-lang.org/reference/expressions/operator-expr.html#r-expr.as.numeric.float-narrowing
                Ok(value as Self::Value)
            }

            // Floats and doubles in serde_json are f64.
            fn visit_f64<E>(self, value: f64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                // This is trivial for `f64`. For `f32`, casting f64 to f32
                // is guaranteed to produce the closest possible float
                // value:
                //     https://doc.rust-lang.org/reference/expressions/operator-expr.html#r-expr.as.numeric.float-narrowing
                match value {
                    _ if value < <$t>::MIN as f64 => Err(self::value_error(value, $msg)),
                    _ if value > <$t>::MAX as f64 => Err(self::value_error(value, $msg)),
                    _ => Ok(value as Self::Value),
                }
            }

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str($msg)
            }
        }
    };
}

#[cfg(test)]
macro_rules! impl_assert_float_eq {
    ($fn: ident, $t: ty) => {
        fn $fn(left: $t, right: $t) {
            // Consider all NaN as equal.
            if left.is_nan() && right.is_nan() {
                return;
            }
            // Consider all infinites floats of the same sign as equal.
            if left.is_infinite()
                && right.is_infinite()
                && left.is_sign_positive() == right.is_sign_positive()
            {
                return;
            }
            assert_eq!(left, right);
        }
    };
}
