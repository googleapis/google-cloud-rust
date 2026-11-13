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

//! Compile-time canary for `gaxi::prost::ToProto` and `FromProto`.
//!
//! Crate versions already published on crates.io call `.to_proto()` and
//! `.cnv()` without naming the target type. They compile only while each
//! source type has exactly one matching impl. Applications pick up new
//! `google-cloud-gax-internal` 0.7.x releases automatically, so adding a second
//! impl upstream (in `gaxi`, or in a crate that defines a shared model type)
//! would break those published versions.
//!
//! The tests in this crate mirror the published call sites. They fail to
//! compile (`E0283` or `E0284`, "type annotations needed") if such an impl is
//! added.
//!
//! Keep the dependencies minimal. Do not depend on any crate that implements
//! `ToProto` or `FromProto` for the types checked here (e.g. storage,
//! logging-v2, aiplatform-v1). Those impls are legitimate, and depending on
//! them would make this canary fail.
//!
//! The canary only sees impls added by crates in its own dependency graph. So
//! do depend on every crate that a published converter depends on and that can
//! reach `gaxi::prost`, today or by enabling `_internal-grpc-client` (e.g.
//! `google-cloud-lro`, `google-cloud-longrunning`, `google-cloud-iam-v1`,
//! `google-cloud-location`). If such a crate defines none of the checked
//! types, also load it with `extern crate` below.
//!
//! # If this canary fails
//!
//! The change adds a second impl that published crate versions cannot resolve.
//! Those versions stop compiling once users pick up the new release of the
//! crate that adds the impl. That is not automatically a blocker. It is a
//! judgment call: weigh how recent and widely used the affected versions are
//! against the cost of the alternatives (avoid the impl, ship it in a
//! semver-breaking release, or ship as-is and ask affected customers to
//! upgrade). Whatever we decide, document it in the release notes (the affected
//! crate versions and the upgrade path), then update this canary so it keeps
//! guarding the rest.
//!
//! See <https://github.com/googleapis/google-cloud-rust/issues/7284>.

// rustc only loads dependencies that the source references, and only sees
// impls from loaded crates. These crates define none of the checked types, so
// load them explicitly.
#[cfg(test)]
extern crate google_cloud_location as _;
#[cfg(test)]
extern crate google_cloud_lro as _;

/// External model types that published crates convert with their own
/// `ToProto` impl.
///
/// For example, aiplatform-v1 1.18.0 implements `ToProto` for
/// `google_cloud_rpc::model::Status`, targeting its own prost type, and calls
/// `.to_proto()` on it unqualified. Each type gets a local
/// `ToProto<LocalProst>` impl, which stands in for the published one. If an
/// upstream crate adds another impl, the unqualified call below no longer
/// compiles.
///
/// This list is frozen. Code generated after #7374 and handwritten code after
/// #7437 qualify these calls, so only crate versions published before those
/// changes need coverage.
#[cfg(test)]
mod external_model_to_proto {
    use gaxi::prost::{ConvertError, ToProto};

    struct LocalProst;

    macro_rules! assert_single_upstream_impl {
        ($($ty:ty),* $(,)?) => {
            $(
                impl ToProto<LocalProst> for $ty {
                    type Output = LocalProst;
                    fn to_proto(self) -> Result<LocalProst, ConvertError> {
                        Ok(LocalProst)
                    }
                }
            )*

            #[test]
            fn no_upstream_impl() -> Result<(), ConvertError> {
                $(
                    let _: LocalProst = <$ty>::default().to_proto()?;
                )*
                Ok(())
            }
        };
    }

    assert_single_upstream_impl!(
        // Converted in aiplatform-v1 1.18.0, apigeeconnect-v1 1.12.0,
        // dialogflow-cx-v3 2.11.0, dialogflow-v2 1.18.0,
        // google-maps-routes-v1 1.1.0, and google-maps-routing-v2 1.2.0.
        google_cloud_rpc::model::Status,
        // Converted in storage 1.21.0 and aiplatform-v1 1.18.0.
        google_cloud_type::model::Date,
        // Converted in storage 1.21.0.
        google_cloud_type::model::Expr,
        // Converted in storage 1.21.0 and datacatalog-lineage-v1 1.15.0.
        google_cloud_type::model::Interval,
        // Converted in aiplatform-v1 1.18.0, dialogflow-cx-v3 2.11.0,
        // dialogflow-v2 1.18.0, google-maps-routes-v1 1.1.0, and
        // google-maps-routing-v2 1.2.0.
        google_cloud_type::model::LatLng,
        // Converted in google-maps-routing-v2 1.2.0.
        google_cloud_type::model::LocalizedText,
        // Converted in google-maps-routes-v1 1.1.0 and
        // google-maps-routing-v2 1.2.0.
        google_cloud_type::model::Money,
        // Converted in aiplatform-v1 1.18.0.
        google_cloud_api::model::HttpBody,
        // Converted in logging-v2 1.15.0.
        google_cloud_api::model::MonitoredResource,
        google_cloud_logging_type::model::HttpRequest,
        google_cloud_logging_type::model::LogSeverity,
        // Converted in storage 1.21.0.
        google_cloud_iam_v1::model::AuditConfig,
        google_cloud_iam_v1::model::AuditConfigDelta,
        google_cloud_iam_v1::model::AuditLogConfig,
        google_cloud_iam_v1::model::Binding,
        google_cloud_iam_v1::model::BindingDelta,
        google_cloud_iam_v1::model::GetIamPolicyRequest,
        google_cloud_iam_v1::model::GetPolicyOptions,
        google_cloud_iam_v1::model::Policy,
        google_cloud_iam_v1::model::PolicyDelta,
        google_cloud_iam_v1::model::SetIamPolicyRequest,
        google_cloud_iam_v1::model::TestIamPermissionsRequest,
        google_cloud_iam_v1::model::TestIamPermissionsResponse,
        google_cloud_iam_v1::model::audit_config_delta::Action,
        google_cloud_iam_v1::model::audit_log_config::LogType,
        google_cloud_iam_v1::model::binding_delta::Action,
        // Converted in storage 1.21.0. The `Operation` impl is handwritten, in
        // `src/control/convert.rs`.
        google_cloud_longrunning::model::CancelOperationRequest,
        google_cloud_longrunning::model::DeleteOperationRequest,
        google_cloud_longrunning::model::GetOperationRequest,
        google_cloud_longrunning::model::ListOperationsRequest,
        google_cloud_longrunning::model::ListOperationsResponse,
        google_cloud_longrunning::model::Operation,
        google_cloud_longrunning::model::OperationInfo,
        google_cloud_longrunning::model::WaitOperationRequest,
    );
}

/// Types that `gaxi` itself converts with `ToProto`.
///
/// Published code calls `.to_proto()` on these types unqualified, e.g. storage
/// 1.21.0 `generated/convert/control/convert.rs:62`:
/// `create_time: self.create_time.map(|v| v.to_proto())`.
///
/// A local impl would itself be a second impl, so these checks have none.
/// Nothing in the call constrains the `ToProto<T>` parameter, so each call
/// compiles only while `gaxi` provides exactly one impl.
#[cfg(test)]
mod gaxi_to_proto {
    use gaxi::prost::{ConvertError, ToProto};

    macro_rules! assert_single_impl {
        ($($ty:ty),* $(,)?) => {
            #[test]
            fn single_impl() -> Result<(), ConvertError> {
                $(
                    let _ = <$ty>::default().to_proto()?;
                )*
                Ok(())
            }
        };
    }

    assert_single_impl!(
        google_cloud_wkt::Duration,
        google_cloud_wkt::FieldMask,
        google_cloud_wkt::Timestamp,
        google_cloud_wkt::Struct,
        google_cloud_wkt::Value,
        google_cloud_wkt::ListValue,
        google_cloud_wkt::NullValue,
        google_cloud_wkt::Empty,
        (),
        bool,
        f32,
        i32,
        u32,
        f64,
        i64,
        u64,
        String,
        bytes::Bytes,
    );
}

/// Prost types that `gaxi` converts with `FromProto`.
///
/// Published code passes the result of an unqualified `.cnv()` to a setter
/// that accepts any `Into<Model>`, e.g. storage 1.21.0
/// `generated/convert/control/convert.rs:75`:
/// `.set_or_clear_create_time(self.create_time.map(|v| v.cnv()).transpose()?)`.
///
/// `set_or_clear()` has the same shape as that setter, and the calls below
/// copy the `map(...).transpose()?` expression. The `Into` bound does not pin
/// the `FromProto<T>` parameter, so each call compiles only while `gaxi`
/// provides exactly one impl.
#[cfg(test)]
mod gaxi_from_proto {
    use gaxi::prost::{ConvertError, FromProto};

    fn set_or_clear<M, T: Into<M>>(_: Option<T>) {}

    macro_rules! assert_single_impl {
        ($($prost:ty => $model:ty),* $(,)?) => {
            #[test]
            fn single_impl() -> Result<(), ConvertError> {
                $(
                    set_or_clear::<$model, _>(
                        Some(<$prost>::default()).map(|v| v.cnv()).transpose()?,
                    );
                )*
                Ok(())
            }
        };
    }

    assert_single_impl!(
        prost_types::Duration => google_cloud_wkt::Duration,
        prost_types::FieldMask => google_cloud_wkt::FieldMask,
        prost_types::Timestamp => google_cloud_wkt::Timestamp,
        prost_types::Struct => google_cloud_wkt::Struct,
        prost_types::Value => google_cloud_wkt::Value,
        prost_types::ListValue => google_cloud_wkt::ListValue,
        prost_types::NullValue => google_cloud_wkt::NullValue,
        gaxi::prost::Empty => google_cloud_wkt::Empty,
        () => (),
        bool => bool,
        f32 => f32,
        i32 => i32,
        u32 => u32,
        f64 => f64,
        i64 => i64,
        u64 => u64,
        String => String,
        bytes::Bytes => bytes::Bytes,
    );
}
