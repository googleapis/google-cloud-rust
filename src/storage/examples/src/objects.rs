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

pub mod change_file_storage_class;
pub mod compose_file;
pub mod delete_file;
pub mod download_byte_range;
pub mod download_encrypted_file;
pub mod download_file;
pub mod file_download_into_memory;
pub mod file_upload_from_memory;
pub mod generate_encryption_key;
pub mod get_metadata;
pub mod list_files;
pub mod list_files_with_prefix;
pub mod release_event_based_hold;
pub mod release_temporary_hold;
pub mod set_event_based_hold;
pub mod set_metadata;
pub mod set_temporary_hold;
pub mod stream_file_download;
pub mod stream_file_upload;
pub mod upload_encrypted_file;
pub mod upload_file;
pub mod upload_with_kms_key;
