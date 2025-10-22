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

use crate::model::{
    InstancesBulkInsertOperationMetadata, SetCommonInstanceMetadataOperationMetadata,
};

impl crate::model::Operation {
    pub fn to_result(self) -> std::result::Result<Self, OperationError> {
        if self.error.is_some()
            || self.http_error_status_code.is_some()
            || self.http_error_message.is_some()
        {
            let error = GenericOperationError::new();
            let error = self.error.into_iter().fold(error, |e, v| e.set_details(v));
            let error = self
                .http_error_status_code
                .into_iter()
                .fold(error, |e, v| e.set_status_code(v));
            let error = self
                .http_error_message
                .into_iter()
                .fold(error, |e, v| e.set_message(v));
            return Err(OperationError::Generic(error));
        }
        if let Some(metadata) = self.instances_bulk_insert_operation_metadata.as_ref() {
            let found = metadata
                .per_location_status
                .iter()
                .any(|(_, v)| v.failed_to_create_vm_count.is_some_and(|c| c != 0));
            if found {
                let error = InstanceBulkInsertOperationError::new()
                    .set_metadata(self.instances_bulk_insert_operation_metadata.unwrap());
                return Err(OperationError::InstancesBulkInsert(error));
            }
        }
        if let Some(metadata) = self
            .set_common_instance_metadata_operation_metadata
            .as_ref()
        {
            let found = metadata
                .per_location_operations
                .iter()
                .any(|(_, v)| v.error.is_some());
            if found {
                let error = SetCommonInstanceMetadataOperationError::new().set_metadata(
                    self.set_common_instance_metadata_operation_metadata
                        .unwrap(),
                );
                return Err(OperationError::SetCommonInstanceMetadata(error));
            }
        }
        Ok(self)
    }
}

/// Possible errors returned by an operation.
///
/// The Compute API long running operations return different errors depending on
/// the operation being called.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum OperationError {
    /// A HTTP error with additional details.
    Generic(GenericOperationError),
    /// A partial failure when inserting instances (VMs) in bulk.
    InstancesBulkInsert(InstanceBulkInsertOperationError),
    /// A partial failure when setting the common metadata for all instances.
    SetCommonInstanceMetadata(SetCommonInstanceMetadataOperationError),
}

impl std::fmt::Display for OperationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Generic(d) => write!(f, "the long-running operation failed with {d:?}"),
            Self::InstancesBulkInsert(d) => {
                write!(
                    f,
                    "the long-running operation to insert instances in bulk failed with {d:?}"
                )
            }
            Self::SetCommonInstanceMetadata(d) => write!(
                f,
                "the long-running operation to set the common instance metadata fialed with {d:?}"
            ),
        }
    }
}

impl std::error::Error for OperationError {}

/// Details about a generic long-running operation error.
#[derive(Clone, Debug, Default, PartialEq)]
#[non_exhaustive]
pub struct GenericOperationError {
    /// The HTTP error message.
    pub message: Option<String>,

    /// The HTTP error status code.
    pub status_code: Option<i32>,

    /// The errors generated while processing the operation.
    pub details: Option<crate::model::operation::Error>,
}

impl GenericOperationError {
    /// Create a new instance.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the [message][Self::message] field.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_compute_v1::errors::GenericOperationError;
    /// let error = GenericOperationError::new().set_message("useful in mocks");
    /// ```
    pub fn set_message<V: Into<String>>(mut self, v: V) -> Self {
        self.message = Some(v.into());
        self
    }

    /// Set the [status_code][Self::status_code] field.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_compute_v1::errors::GenericOperationError;
    /// let error = GenericOperationError::new().set_status_code(503);
    /// ```
    pub fn set_status_code(mut self, v: i32) -> Self {
        self.status_code = Some(v);
        self
    }

    /// Set the [details][Self::details] field.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_compute_v1::errors::GenericOperationError;
    /// use google_cloud_compute_v1::model::operation::{Error, error::Errors};
    /// let error = GenericOperationError::new().set_details(
    ///     Error::new().set_errors([
    ///         Errors::new()
    ///             .set_code("MOCK_ERROR_CODE")
    ///             .set_location("some_field")
    ///             .set_message("a mocked error"),
    ///         ]),
    /// );
    /// ```
    pub fn set_details<V: Into<crate::model::operation::Error>>(mut self, v: V) -> Self {
        self.details = Some(v.into());
        self
    }
}

/// Details about an [instance bulk insert] long-runing operation error.
///
/// [instance bulk insert]: crate::client::Instances::bulk_insert
#[derive(Clone, Debug, Default, PartialEq)]
#[non_exhaustive]
pub struct InstanceBulkInsertOperationError {
    /// The information about all zonal actions and their state.
    pub metadata: InstancesBulkInsertOperationMetadata,
}

impl InstanceBulkInsertOperationError {
    /// Create a new instance.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the [metadata][Self::metadata] field.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_compute_v1::errors::InstanceBulkInsertOperationError;
    /// use google_cloud_compute_v1::model::{BulkInsertOperationStatus, InstancesBulkInsertOperationMetadata};
    /// let error = InstanceBulkInsertOperationError::new().set_metadata(
    ///     InstancesBulkInsertOperationMetadata::new().set_per_location_status([
    ///         ("zones/us-central1-a", BulkInsertOperationStatus::new()
    ///                 .set_failed_to_create_vm_count(42))
    ///     ])
    /// );
    /// ```
    pub fn set_metadata(mut self, v: InstancesBulkInsertOperationMetadata) -> Self {
        self.metadata = v;
        self
    }
}

/// Details about an [set common instance metadata] long-runing operation error.
///
/// [set common instance metadata]: crate::client::Projects::set_common_instance_metadata
#[derive(Clone, Debug, Default, PartialEq)]
#[non_exhaustive]
pub struct SetCommonInstanceMetadataOperationError {
    /// The information about all zonal actions and their state.
    pub metadata: SetCommonInstanceMetadataOperationMetadata,
}

impl SetCommonInstanceMetadataOperationError {
    /// Create a new instance.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the [metadata][Self::metadata] field.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_compute_v1::errors::SetCommonInstanceMetadataOperationError;
    /// use google_cloud_compute_v1::model::{
    ///     SetCommonInstanceMetadataOperationMetadata, SetCommonInstanceMetadataOperationMetadataPerLocationOperationInfo,
    ///     Status, set_common_instance_metadata_operation_metadata_per_location_operation_info::State};
    /// let error = SetCommonInstanceMetadataOperationError::new().set_metadata(
    ///     SetCommonInstanceMetadataOperationMetadata::new().set_per_location_operations([
    ///         ("zones/us-central1-a", SetCommonInstanceMetadataOperationMetadataPerLocationOperationInfo::new()
    ///             .set_state(State::Abandoned)
    ///             .set_error(Status::new().set_message("NOT FOUND")))
    ///     ])
    /// );
    /// ```
    pub fn set_metadata(mut self, v: SetCommonInstanceMetadataOperationMetadata) -> Self {
        self.metadata = v;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{
        BulkInsertOperationStatus, Operation,
        SetCommonInstanceMetadataOperationMetadataPerLocationOperationInfo, operation::Error,
    };

    #[test]
    fn to_result() {
        let operation = Operation::new().set_client_operation_id("abc");
        let got = operation.clone().to_result();
        assert!(matches!(got, Ok(ref o) if o == &operation), "{got:?}");

        let operation = Operation::new().set_http_error_message("uh-oh");
        let got = operation.clone().to_result();
        assert!(
            matches!(got, Err(OperationError::Generic(ref e)) if e == &GenericOperationError::new().set_message("uh-oh")),
            "{got:?}"
        );

        let operation = Operation::new().set_http_error_status_code(503);
        let got = operation.clone().to_result();
        assert!(
            matches!(got, Err(OperationError::Generic(ref e)) if e == &GenericOperationError::new().set_status_code(503)),
            "{got:?}"
        );

        let operation = Operation::new().set_error(Error::new());
        let got = operation.clone().to_result();
        assert!(
            matches!(got, Err(OperationError::Generic(ref e)) if e == &GenericOperationError::new().set_details(Error::new())),
            "{got:?}"
        );
    }

    #[test]
    fn to_result_instances_bulk_insert() {
        let metadata = InstancesBulkInsertOperationMetadata::new();
        let operation = Operation::new().set_instances_bulk_insert_operation_metadata(metadata);
        let got = operation.clone().to_result();
        assert!(matches!(got, Ok(ref op) if op == &operation), "{got:?}");

        let metadata = InstancesBulkInsertOperationMetadata::new().set_per_location_status([(
            "zones/us-central1-a",
            BulkInsertOperationStatus::new().set_created_vm_count(42),
        )]);
        let operation = Operation::new().set_instances_bulk_insert_operation_metadata(metadata);
        let got = operation.clone().to_result();
        assert!(matches!(got, Ok(ref op) if op == &operation), "{got:?}");

        let metadata = InstancesBulkInsertOperationMetadata::new().set_per_location_status([
            (
                "zones/us-central1-a",
                BulkInsertOperationStatus::new().set_created_vm_count(42),
            ),
            (
                "zones/us-central1-f",
                BulkInsertOperationStatus::new().set_failed_to_create_vm_count(42),
            ),
        ]);
        let operation =
            Operation::new().set_instances_bulk_insert_operation_metadata(metadata.clone());
        let got = operation.to_result();
        assert!(
            matches!(got, Err(OperationError::InstancesBulkInsert(ref e)) if e.metadata == metadata),
            "{got:?}"
        );
    }

    #[test]
    fn to_result_set_common_instances_metadata() {
        use crate::model::Status;
        use crate::model::set_common_instance_metadata_operation_metadata_per_location_operation_info::State;

        let metadata = SetCommonInstanceMetadataOperationMetadata::new();
        let operation =
            Operation::new().set_set_common_instance_metadata_operation_metadata(metadata);
        let got = operation.clone().to_result();
        assert!(matches!(got, Ok(ref op) if op == &operation), "{got:?}");

        let metadata = SetCommonInstanceMetadataOperationMetadata::new()
            .set_per_location_operations([(
                "zones/us-central1-a",
                SetCommonInstanceMetadataOperationMetadataPerLocationOperationInfo::new()
                    .set_state(State::Done),
            )]);
        let operation =
            Operation::new().set_set_common_instance_metadata_operation_metadata(metadata);
        let got = operation.clone().to_result();
        assert!(matches!(got, Ok(ref op) if op == &operation), "{got:?}");

        let metadata = SetCommonInstanceMetadataOperationMetadata::new()
            .set_per_location_operations([
                (
                    "zones/us-central1-a",
                    SetCommonInstanceMetadataOperationMetadataPerLocationOperationInfo::new()
                        .set_state(State::Done),
                ),
                (
                    "zones/us-central1-f",
                    SetCommonInstanceMetadataOperationMetadataPerLocationOperationInfo::new()
                        .set_state(State::Abandoned)
                        .set_error(Status::new().set_message("uh-oh")),
                ),
            ]);
        let operation =
            Operation::new().set_set_common_instance_metadata_operation_metadata(metadata.clone());
        let got = operation.to_result();
        assert!(
            matches!(got, Err(OperationError::SetCommonInstanceMetadata(ref e)) if e.metadata == metadata),
            "{got:?}"
        );
    }

    #[test]
    fn display() {
        let input =
            OperationError::Generic(GenericOperationError::new().set_message("test-message"));
        let got = input.to_string();
        assert!(got.contains("test-message"), "{input:?} => {got}");

        let input = OperationError::InstancesBulkInsert(
            InstanceBulkInsertOperationError::new().set_metadata(
                InstancesBulkInsertOperationMetadata::new().set_per_location_status([(
                    "zones/us-central1-a",
                    BulkInsertOperationStatus::new().set_created_vm_count(123456),
                )]),
            ),
        );
        let got = input.to_string();
        assert!(got.contains("zones/us-central1-a"), "{input:?} => {got}");
        assert!(got.contains("123456"), "{input:?} => {got}");

        let input = OperationError::SetCommonInstanceMetadata(
            SetCommonInstanceMetadataOperationError::new().set_metadata(
                SetCommonInstanceMetadataOperationMetadata::new().set_per_location_operations([(
                    "zones/us-central1-a",
                    SetCommonInstanceMetadataOperationMetadataPerLocationOperationInfo::new()
                        .set_error(crate::model::Status::new().set_message("error-message")),
                )]),
            ),
        );
        let got = input.to_string();
        assert!(got.contains("zones/us-central1-a"), "{input:?} => {got}");
        assert!(got.contains("error-message"), "{input:?} => {got}");
    }

    #[test]
    fn generic_operation_setters() {
        use crate::model::LocalizedMessage;
        use crate::model::operation::{Error, error::Errors, error::errors::ErrorDetails};
        let got = GenericOperationError::new().set_message("abc");
        assert_eq!(got.message.as_deref(), Some("abc"));

        let got = GenericOperationError::new().set_status_code(123);
        assert_eq!(got.status_code, Some(123));

        let details =
            Error::new().set_errors([Errors::new()
                .set_error_details([ErrorDetails::new().set_localized_message(
                    LocalizedMessage::new().set_locale("C").set_message("uh-oh"),
                )])]);
        let got = GenericOperationError::new().set_details(details.clone());
        assert_eq!(got.details, Some(details));
    }

    #[test]
    fn instances_bulk_insert_setters() {
        let metadata = InstancesBulkInsertOperationMetadata::new().set_per_location_status([(
            "zones/us-central1-a",
            BulkInsertOperationStatus::new().set_created_vm_count(123456),
        )]);
        let got = InstanceBulkInsertOperationError::new().set_metadata(metadata.clone());
        assert_eq!(got.metadata, metadata);
    }

    #[test]
    fn set_common_instance_metadata_setters() {
        let metadata = SetCommonInstanceMetadataOperationMetadata::new()
            .set_per_location_operations([(
                "zones/us-central1-a",
                SetCommonInstanceMetadataOperationMetadataPerLocationOperationInfo::new()
                    .set_error(crate::model::Status::new().set_message("error-message")),
            )]);
        let got = SetCommonInstanceMetadataOperationError::new().set_metadata(metadata.clone());
        assert_eq!(got.metadata, metadata);
    }
}
