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
            .disable_comments([
                "google.iam.v1.IAMPolicy",
                "google.iam.v1.IAMPolicy.GetIamPolicy",
                "google.iam.v1.IAMPolicy.SetIamPolicy",
                "google.iam.v1.IAMPolicy.TestIamPermissions",
            ])
            .disable_comments([
                "google.storage.v2.Storage",
                "google.storage.v2.Storage.DeleteBucket",
                "google.storage.v2.Storage.GetBucket",
                "google.storage.v2.Storage.CreateBucket",
                "google.storage.v2.Storage.ListBuckets",
                "google.storage.v2.Storage.LockBucketRetentionPolicy",
                "google.storage.v2.Storage.GetIamPolicy",
                "google.storage.v2.Storage.SetIamPolicy",
                "google.storage.v2.Storage.TestIamPermissions",
                "google.storage.v2.Storage.UpdateBucket",
                "google.storage.v2.Storage.ComposeObject",
                "google.storage.v2.Storage.DeleteObject",
                "google.storage.v2.Storage.RestoreObject",
                "google.storage.v2.Storage.CancelResumableWrite",
                "google.storage.v2.Storage.GetObject",
                "google.storage.v2.Storage.ReadObject",
                "google.storage.v2.Storage.BidiReadObject",
                "google.storage.v2.Storage.UpdateObject",
                "google.storage.v2.Storage.WriteObject",
                "google.storage.v2.Storage.BidiWriteObject",
                "google.storage.v2.Storage.ListObjects",
                "google.storage.v2.Storage.RewriteObject",
                "google.storage.v2.Storage.StartResumableWrite",
                "google.storage.v2.Storage.QueryWriteStatus",
                "google.storage.v2.Storage.MoveObject",
            ])
            .out_dir("src/generated/protos")
            .compile_with_config(
                config,
                &["protos/google/storage/v2/storage.proto"],
                &["protos"],
            )
            .expect("error compiling protos");
    }
}
