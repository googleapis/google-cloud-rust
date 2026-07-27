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

use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// The name of the bucket to use for the benchmark.
    #[arg(long, env = "GOOGLE_CLOUD_RUST_BENCHMARKS_BUCKET")]
    pub bucket_name: String,

    /// The size of the object to append.
    #[arg(long, default_value_t = 104_857_600)] // 100 MiB default
    pub object_size: usize,

    /// The size of each append chunk.
    #[arg(long, default_value_t = 262_144)] // 256 KiB default
    pub chunk_size: usize,
}
