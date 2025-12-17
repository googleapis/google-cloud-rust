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

fn main() {
    #[cfg(feature = "_generate-protos")]
    {
        let mut config = prost_build::Config::default();
        config.disable_comments(["."]);
        tonic_prost_build::configure()
            .out_dir("src/generated/protos")
            .compile_with_config(
                config,
                &["protos/google/pubsub/v1/pubsub.proto"],
                &["protos"],
            )
            .expect("error compiling protos");
    }
}
