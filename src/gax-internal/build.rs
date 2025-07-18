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

use std::fs::File;
use std::io::Write;
use std::path::Path;

fn main() {
    let out_dir = std::env::var_os("OUT_DIR").expect("OUT_DIR not specified");
    let out_path = Path::new(&out_dir).to_owned();

    let rust_version = rustc_version::version().expect("Could not retrieve rustc version");
    let mut f =
        File::create(out_path.join("build_env.rs")).expect("Could not create build environment");
    f.write_all(format!("pub(crate) const RUSTC_VERSION: &str = \"{rust_version}\";").as_bytes())
        .expect("Unable to write rust version");
    f.flush().expect("failed to flush");
}
