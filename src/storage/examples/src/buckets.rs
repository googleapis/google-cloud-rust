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

pub mod add_bucket_conditional_iam_binding;
pub mod add_bucket_iam_member;
pub mod add_bucket_owner;
pub mod change_default_storage_class;
pub mod cors_configuration;
pub mod create_bucket;
pub mod create_bucket_class_location;
pub mod create_bucket_dual_region;
pub mod create_bucket_hierarchical_namespace;
pub mod define_bucket_website_configuration;
pub mod delete_bucket;
pub mod disable_bucket_lifecycle_management;
pub mod disable_default_event_based_hold;
pub mod disable_versioning;
pub mod enable_bucket_lifecycle_management;
pub mod enable_default_event_based_hold;
pub mod enable_versioning;
pub mod get_autoclass;
pub mod get_bucket_metadata;
pub mod get_default_event_based_hold;
pub mod get_public_access_prevention;
pub mod get_retention_policy;
pub mod list_buckets;
pub mod lock_retention_policy;
pub mod print_bucket_acl;
pub mod print_bucket_acl_for_user;
pub mod print_bucket_website_configuration;
pub mod remove_bucket_owner;
pub mod remove_cors_configuration;
pub mod remove_retention_policy;
pub mod set_autoclass;
#[allow(dead_code)]
pub mod set_bucket_public_iam;
pub mod set_lifecycle_abort_multipart_upload;
pub mod set_public_access_prevention_enforced;
pub mod set_public_access_prevention_inherited;
pub mod set_public_access_prevention_unspecified;
pub mod set_retention_policy;
pub mod view_bucket_iam_members;
pub mod view_lifecycle_management_configuration;
pub mod view_versioning_status;
