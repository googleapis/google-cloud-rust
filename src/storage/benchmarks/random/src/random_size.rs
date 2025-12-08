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

use anyhow::bail;
use rand::{distr::Uniform, seq::IndexedRandom};

/// Select a size at random from a range or list.
#[derive(Clone, Debug, PartialEq)]
pub enum RandomSize {
    /// Select a value from the range `[self.0, self.1]` both ends are inclusive.
    Range(u64, u64),
    /// Select a value at random from a list.
    Values(Vec<u64>),
}

impl std::fmt::Display for RandomSize {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Range(a, b) => write!(f, "{a}-{b}"),
            Self::Values(v) => v
                .iter()
                .try_fold("", |sep, x| write!(f, "{sep}{x}").map(|_| ","))
                .map(|_| ()),
        }
    }
}

impl RandomSize {
    pub fn min(&self) -> u64 {
        match self {
            Self::Range(a, _) => *a,
            Self::Values(v) => *v.iter().min().unwrap(),
        }
    }

    pub fn max(&self) -> u64 {
        match self {
            Self::Range(_, b) => *b,
            Self::Values(v) => *v.iter().max().unwrap(),
        }
    }

    pub fn sample<T>(&self, rng: &mut T) -> u64
    where
        T: rand::Rng,
    {
        match self {
            Self::Range(a, b) => {
                rng.sample(Uniform::new_inclusive(a, b).expect("size range is valid"))
            }
            Self::Values(v) => *v.choose(rng).expect("size list is not empty"),
        }
    }
}

fn parse_size_arg(arg: &str) -> anyhow::Result<u64> {
    let value = parse_size::parse_size(arg)?;
    Ok(value)
}

pub fn parse_random_size_arg(arg: &str) -> anyhow::Result<RandomSize> {
    if let Some(sep) = arg.find('-') {
        let min = parse_size_arg(&arg[..sep])?;
        let max = parse_size_arg(&arg[sep + 1..])?;
        if min > max {
            bail!("invalid range argument, min ({min}) > max ({max})")
        }
        return Ok(RandomSize::Range(min, max));
    }
    let values = arg
        .split(',')
        .map(parse_size_arg)
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(RandomSize::Values(values))
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;

    const KIB: u64 = 1024;
    const MIB: u64 = 1024 * KIB;
    const KB: u64 = 1000;
    const MB: u64 = 1000 * KB;

    #[test_case(RandomSize::Range(12, 34), "12-34")]
    #[test_case(RandomSize::Values(vec![]), "")]
    #[test_case(RandomSize::Values(vec![1]), "1")]
    #[test_case(RandomSize::Values(vec![1, 2, 3]), "1,2,3")]
    fn display(input: RandomSize, want: &str) -> anyhow::Result<()> {
        let got = format!("{input}");
        assert_eq!(got, want);
        Ok(())
    }

    #[test_case("2MiB", 2 * MIB)]
    #[test_case("2MB", 2 * MB)]
    #[test_case("42", 42)]
    fn parse_size_success(input: &str, want: u64) -> anyhow::Result<()> {
        let got = parse_size_arg(input)?;
        assert_eq!(got, want);
        Ok(())
    }

    #[test_case("abc")]
    #[test_case("-123")]
    #[test_case("abc123")]
    fn parse_size_error(input: &str) -> anyhow::Result<()> {
        let got = parse_size_arg(input);
        assert!(got.is_err(), "{got:?}");
        Ok(())
    }

    #[test_case("1KiB-2KiB", RandomSize::Range(KIB, 2 * KIB))]
    #[test_case("1KiB", RandomSize::Values(vec![KIB]))]
    #[test_case("1MiB", RandomSize::Values(vec![MIB]))]
    #[test_case("1MiB,2MiB,4MiB", RandomSize::Values(vec![MIB, 2 * MIB, 4 * MIB]))]
    fn parse_random_size_success(input: &str, want: RandomSize) -> anyhow::Result<()> {
        let got = parse_random_size_arg(input)?;
        assert_eq!(got, want);
        Ok(())
    }

    #[test_case("4-2")]
    #[test_case("")]
    fn parse_random_size_error(input: &str) -> anyhow::Result<()> {
        let got = parse_random_size_arg(input);
        assert!(got.is_err(), "{got:?}");
        Ok(())
    }
}
