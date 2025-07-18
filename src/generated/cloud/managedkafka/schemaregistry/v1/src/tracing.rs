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
//
// Code generated by sidekick. DO NOT EDIT.
use crate::Result;

/// Implements a [ManagedSchemaRegistry](super::stub::ManagedSchemaRegistry) decorator for logging and tracing.
#[derive(Clone, Debug)]
pub struct ManagedSchemaRegistry<T>
where
    T: super::stub::ManagedSchemaRegistry + std::fmt::Debug + Send + Sync,
{
    inner: T,
}

impl<T> ManagedSchemaRegistry<T>
where
    T: super::stub::ManagedSchemaRegistry + std::fmt::Debug + Send + Sync,
{
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T> super::stub::ManagedSchemaRegistry for ManagedSchemaRegistry<T>
where
    T: super::stub::ManagedSchemaRegistry + std::fmt::Debug + Send + Sync,
{
    #[tracing::instrument(ret)]
    async fn get_schema_registry(
        &self,
        req: crate::model::GetSchemaRegistryRequest,
        options: gax::options::RequestOptions,
    ) -> Result<gax::response::Response<crate::model::SchemaRegistry>> {
        self.inner.get_schema_registry(req, options).await
    }

    #[tracing::instrument(ret)]
    async fn list_schema_registries(
        &self,
        req: crate::model::ListSchemaRegistriesRequest,
        options: gax::options::RequestOptions,
    ) -> Result<gax::response::Response<crate::model::ListSchemaRegistriesResponse>> {
        self.inner.list_schema_registries(req, options).await
    }

    #[tracing::instrument(ret)]
    async fn create_schema_registry(
        &self,
        req: crate::model::CreateSchemaRegistryRequest,
        options: gax::options::RequestOptions,
    ) -> Result<gax::response::Response<crate::model::SchemaRegistry>> {
        self.inner.create_schema_registry(req, options).await
    }

    #[tracing::instrument(ret)]
    async fn delete_schema_registry(
        &self,
        req: crate::model::DeleteSchemaRegistryRequest,
        options: gax::options::RequestOptions,
    ) -> Result<gax::response::Response<()>> {
        self.inner.delete_schema_registry(req, options).await
    }

    #[tracing::instrument(ret)]
    async fn get_context(
        &self,
        req: crate::model::GetContextRequest,
        options: gax::options::RequestOptions,
    ) -> Result<gax::response::Response<crate::model::Context>> {
        self.inner.get_context(req, options).await
    }

    #[tracing::instrument(ret)]
    async fn list_contexts(
        &self,
        req: crate::model::ListContextsRequest,
        options: gax::options::RequestOptions,
    ) -> Result<gax::response::Response<api::model::HttpBody>> {
        self.inner.list_contexts(req, options).await
    }

    #[tracing::instrument(ret)]
    async fn get_schema(
        &self,
        req: crate::model::GetSchemaRequest,
        options: gax::options::RequestOptions,
    ) -> Result<gax::response::Response<crate::model::Schema>> {
        self.inner.get_schema(req, options).await
    }

    #[tracing::instrument(ret)]
    async fn get_raw_schema(
        &self,
        req: crate::model::GetSchemaRequest,
        options: gax::options::RequestOptions,
    ) -> Result<gax::response::Response<api::model::HttpBody>> {
        self.inner.get_raw_schema(req, options).await
    }

    #[tracing::instrument(ret)]
    async fn list_schema_versions(
        &self,
        req: crate::model::ListSchemaVersionsRequest,
        options: gax::options::RequestOptions,
    ) -> Result<gax::response::Response<api::model::HttpBody>> {
        self.inner.list_schema_versions(req, options).await
    }

    #[tracing::instrument(ret)]
    async fn list_schema_types(
        &self,
        req: crate::model::ListSchemaTypesRequest,
        options: gax::options::RequestOptions,
    ) -> Result<gax::response::Response<api::model::HttpBody>> {
        self.inner.list_schema_types(req, options).await
    }

    #[tracing::instrument(ret)]
    async fn list_subjects(
        &self,
        req: crate::model::ListSubjectsRequest,
        options: gax::options::RequestOptions,
    ) -> Result<gax::response::Response<api::model::HttpBody>> {
        self.inner.list_subjects(req, options).await
    }

    #[tracing::instrument(ret)]
    async fn list_subjects_by_schema_id(
        &self,
        req: crate::model::ListSubjectsBySchemaIdRequest,
        options: gax::options::RequestOptions,
    ) -> Result<gax::response::Response<api::model::HttpBody>> {
        self.inner.list_subjects_by_schema_id(req, options).await
    }

    #[tracing::instrument(ret)]
    async fn delete_subject(
        &self,
        req: crate::model::DeleteSubjectRequest,
        options: gax::options::RequestOptions,
    ) -> Result<gax::response::Response<api::model::HttpBody>> {
        self.inner.delete_subject(req, options).await
    }

    #[tracing::instrument(ret)]
    async fn lookup_version(
        &self,
        req: crate::model::LookupVersionRequest,
        options: gax::options::RequestOptions,
    ) -> Result<gax::response::Response<crate::model::SchemaVersion>> {
        self.inner.lookup_version(req, options).await
    }

    #[tracing::instrument(ret)]
    async fn get_version(
        &self,
        req: crate::model::GetVersionRequest,
        options: gax::options::RequestOptions,
    ) -> Result<gax::response::Response<crate::model::SchemaVersion>> {
        self.inner.get_version(req, options).await
    }

    #[tracing::instrument(ret)]
    async fn get_raw_schema_version(
        &self,
        req: crate::model::GetVersionRequest,
        options: gax::options::RequestOptions,
    ) -> Result<gax::response::Response<api::model::HttpBody>> {
        self.inner.get_raw_schema_version(req, options).await
    }

    #[tracing::instrument(ret)]
    async fn list_versions(
        &self,
        req: crate::model::ListVersionsRequest,
        options: gax::options::RequestOptions,
    ) -> Result<gax::response::Response<api::model::HttpBody>> {
        self.inner.list_versions(req, options).await
    }

    #[tracing::instrument(ret)]
    async fn create_version(
        &self,
        req: crate::model::CreateVersionRequest,
        options: gax::options::RequestOptions,
    ) -> Result<gax::response::Response<crate::model::CreateVersionResponse>> {
        self.inner.create_version(req, options).await
    }

    #[tracing::instrument(ret)]
    async fn delete_version(
        &self,
        req: crate::model::DeleteVersionRequest,
        options: gax::options::RequestOptions,
    ) -> Result<gax::response::Response<api::model::HttpBody>> {
        self.inner.delete_version(req, options).await
    }

    #[tracing::instrument(ret)]
    async fn list_referenced_schemas(
        &self,
        req: crate::model::ListReferencedSchemasRequest,
        options: gax::options::RequestOptions,
    ) -> Result<gax::response::Response<api::model::HttpBody>> {
        self.inner.list_referenced_schemas(req, options).await
    }

    #[tracing::instrument(ret)]
    async fn check_compatibility(
        &self,
        req: crate::model::CheckCompatibilityRequest,
        options: gax::options::RequestOptions,
    ) -> Result<gax::response::Response<crate::model::CheckCompatibilityResponse>> {
        self.inner.check_compatibility(req, options).await
    }

    #[tracing::instrument(ret)]
    async fn get_schema_config(
        &self,
        req: crate::model::GetSchemaConfigRequest,
        options: gax::options::RequestOptions,
    ) -> Result<gax::response::Response<crate::model::SchemaConfig>> {
        self.inner.get_schema_config(req, options).await
    }

    #[tracing::instrument(ret)]
    async fn update_schema_config(
        &self,
        req: crate::model::UpdateSchemaConfigRequest,
        options: gax::options::RequestOptions,
    ) -> Result<gax::response::Response<crate::model::SchemaConfig>> {
        self.inner.update_schema_config(req, options).await
    }

    #[tracing::instrument(ret)]
    async fn delete_schema_config(
        &self,
        req: crate::model::DeleteSchemaConfigRequest,
        options: gax::options::RequestOptions,
    ) -> Result<gax::response::Response<crate::model::SchemaConfig>> {
        self.inner.delete_schema_config(req, options).await
    }

    #[tracing::instrument(ret)]
    async fn get_schema_mode(
        &self,
        req: crate::model::GetSchemaModeRequest,
        options: gax::options::RequestOptions,
    ) -> Result<gax::response::Response<crate::model::SchemaMode>> {
        self.inner.get_schema_mode(req, options).await
    }

    #[tracing::instrument(ret)]
    async fn update_schema_mode(
        &self,
        req: crate::model::UpdateSchemaModeRequest,
        options: gax::options::RequestOptions,
    ) -> Result<gax::response::Response<crate::model::SchemaMode>> {
        self.inner.update_schema_mode(req, options).await
    }

    #[tracing::instrument(ret)]
    async fn delete_schema_mode(
        &self,
        req: crate::model::DeleteSchemaModeRequest,
        options: gax::options::RequestOptions,
    ) -> Result<gax::response::Response<crate::model::SchemaMode>> {
        self.inner.delete_schema_mode(req, options).await
    }

    #[tracing::instrument(ret)]
    async fn list_locations(
        &self,
        req: location::model::ListLocationsRequest,
        options: gax::options::RequestOptions,
    ) -> Result<gax::response::Response<location::model::ListLocationsResponse>> {
        self.inner.list_locations(req, options).await
    }

    #[tracing::instrument(ret)]
    async fn get_location(
        &self,
        req: location::model::GetLocationRequest,
        options: gax::options::RequestOptions,
    ) -> Result<gax::response::Response<location::model::Location>> {
        self.inner.get_location(req, options).await
    }

    #[tracing::instrument(ret)]
    async fn list_operations(
        &self,
        req: longrunning::model::ListOperationsRequest,
        options: gax::options::RequestOptions,
    ) -> Result<gax::response::Response<longrunning::model::ListOperationsResponse>> {
        self.inner.list_operations(req, options).await
    }

    #[tracing::instrument(ret)]
    async fn get_operation(
        &self,
        req: longrunning::model::GetOperationRequest,
        options: gax::options::RequestOptions,
    ) -> Result<gax::response::Response<longrunning::model::Operation>> {
        self.inner.get_operation(req, options).await
    }

    #[tracing::instrument(ret)]
    async fn delete_operation(
        &self,
        req: longrunning::model::DeleteOperationRequest,
        options: gax::options::RequestOptions,
    ) -> Result<gax::response::Response<()>> {
        self.inner.delete_operation(req, options).await
    }

    #[tracing::instrument(ret)]
    async fn cancel_operation(
        &self,
        req: longrunning::model::CancelOperationRequest,
        options: gax::options::RequestOptions,
    ) -> Result<gax::response::Response<()>> {
        self.inner.cancel_operation(req, options).await
    }
}
