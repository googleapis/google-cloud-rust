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

use regex::Regex;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let checker = Checker::new()?;

    let (_success, errors): (Vec<_>, Vec<_>) = std::env::args()
        .skip(1)
        .map(|filename| {
            let r = checker.verify(&filename);
            (filename, r)
        })
        .partition(|(_, b)| b.is_ok());

    for (filename, result) in errors.iter() {
        eprintln!("Error searching for copyright boiler plate in {filename}: {result:?}");
    }
    let _ = errors
        .into_iter()
        .map(|(_, b)| b)
        .collect::<Result<Vec<()>, _>>()?;
    Ok(())
}

struct Checker {
    copyright: Regex,
    boilerplate: Vec<String>,
}

impl Checker {
    fn new() -> Result<Checker, Box<dyn Error>> {
        const COPYRIGHT_RE: &str = "^ Copyright [0-9]{4} Google LLC$";

        let boilerplate = Self::load_boilerplate(file!())?;
        // Just panic on failures to compile the RE.
        let copyright = Regex::new(COPYRIGHT_RE).unwrap();
        Ok(Self {
            boilerplate,
            copyright,
        })
    }

    fn verify(&self, filename: &str) -> Result<(), Box<dyn Error>> {
        let found = Self::load_boilerplate(filename)?;
        let mut lines = found.into_iter();
        if let Some(first) = lines.next() {
            if !self.copyright.is_match(&first) {
                return Err(format!("Missing copyright in first line, found={first}"))?;
            }
        } else {
            return Err("Could not read any boilerplate lines".to_string())?;
        }

        let first_mismatch = lines
            .zip(self.boilerplate[1..].iter())
            // Humans prefer "line 1" vs. "line 0", and we already consumed the first line.
            .zip(2..)
            .filter_map(|((found, expected), lineno)| {
                if &found == expected {
                    None
                } else {
                    Some((lineno, found, expected))
                }
            })
            .map(|(lineno, found, expected)| {
                format!(
                    "Mismatched boilerplate in line {lineno}, found={found}, expected={expected}"
                )
            })
            .nth(0);
        if let Some(msg) = first_mismatch {
            return Err(msg)?;
        }

        Ok(())
    }

    fn load_boilerplate(filename: &str) -> Result<Vec<String>, Box<dyn Error>> {
        let prefix = Self::comment_prefix(filename);
        use std::io::BufRead;
        let file = std::fs::File::open(filename)?;
        let lines = std::io::BufReader::new(file)
            .lines()
            .take(13)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(lines
            .iter()
            .map(|line| line.strip_prefix(&prefix).unwrap_or(""))
            .map(str::to_string)
            .collect::<Vec<_>>())
    }

    fn comment_prefix(filename: &str) -> String {
        const KNOWN_EXTENSIONS: &[(&str, &str); 5] = &[
            (".rs", "//"),
            (".go", "//"),
            (".yaml", "#"),
            (".yml", "#"),
            (".toml", "#"),
        ];

        for &(extension, prefix) in KNOWN_EXTENSIONS {
            if filename.ends_with(extension) {
                return prefix.to_string();
            }
        }
        String::new()
    }
}
