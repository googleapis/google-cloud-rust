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

#[allow(dead_code)]
pub mod add_file_owner;
pub mod change_file_storage_class;
pub mod compose_file;
pub mod copy_file;
pub mod copy_file_archived_generation;
pub mod delete_file;
pub mod delete_file_archived_generation;
pub mod download_byte_range;
pub mod download_encrypted_file;
pub mod download_file;
pub mod download_public_file;
pub mod file_download_into_memory;
pub mod file_upload_from_memory;
pub mod generate_encryption_key;
pub mod get_kms_key;
pub mod get_metadata;
pub mod get_object_contexts;
pub mod list_file_archived_generations;
pub mod list_files;
pub mod list_files_with_prefix;
pub mod list_object_contexts;
#[allow(dead_code)]
pub mod make_public;
pub mod move_file;
pub mod object_csek_to_cmek;
pub mod print_file_acl;
pub mod print_file_acl_for_user;
pub mod release_event_based_hold;
pub mod release_temporary_hold;
#[allow(dead_code)]
pub mod remove_file_owner;
pub mod rotate_encryption_key;
pub mod set_event_based_hold;
pub mod set_metadata;
pub mod set_object_contexts;
pub mod set_object_retention_policy;
pub mod set_temporary_hold;
pub mod stream_file_download;
pub mod stream_file_upload;
pub mod upload_encrypted_file;
pub mod upload_file;
pub mod upload_with_kms_key;
