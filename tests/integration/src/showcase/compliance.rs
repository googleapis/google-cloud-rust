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

use crate::Result;
use gax::error::binding::{BindingError, SubstitutionFail};
use showcase::model::compliance_data::LifeKingdom;
use showcase::model::*;
use std::error::Error as _;

pub async fn run() -> Result<()> {
    let client = showcase::client::Compliance::builder()
        .with_endpoint("http://localhost:7469")
        .with_credentials(auth::credentials::anonymous::Builder::new().build())
        .with_retry_policy(gax::retry_policy::NeverRetry)
        .with_tracing()
        .build()
        .await?;

    repeat_data_bootstrap(&client).await?;
    repeat_data_body(&client).await?;
    repeat_data_body_info(&client).await?;
    repeat_data_query(&client).await?;
    repeat_data_simple_path(&client).await?;
    repeat_data_path_resource(&client).await?;
    repeat_data_path_trailing_resource(&client).await?;
    repeat_data_body_put(&client).await?;
    repeat_data_body_patch(&client).await?;
    unknown_enum(&client).await?;
    binding_success(&client).await?;
    binding_error_unset(&client).await?;
    binding_error_unset_expecting(&client).await?;
    binding_error_mismatch_expecting(&client).await?;
    additional_binding_used(&client).await?;
    binding_error_has_all_failed_paths(&client).await?;
    Ok(())
}

async fn repeat_data_bootstrap(client: &showcase::client::Compliance) -> Result<()> {
    let request = RepeatRequest::new()
        .set_f_int32(1)
        .set_f_int64(2)
        .set_f_double(3)
        .set_p_int32(4)
        .set_p_int64(5)
        .set_p_double(6.5);
    let response = client
        .repeat_data_body()
        .with_request(request.clone())
        .send()
        .await?;
    tracing::info!("response: {response:?}");
    assert_eq!(response.binding_uri, "/v1beta1/repeat:body");
    assert_eq!(response.request, Some(request));

    Ok(())
}

async fn repeat_data_body(client: &showcase::client::Compliance) -> Result<()> {
    let request = new_request();
    let response = client
        .repeat_data_body()
        .with_request(request.clone())
        .send()
        .await?;
    tracing::info!("response: {response:?}");
    assert_eq!(response.binding_uri, "/v1beta1/repeat:body");
    assert_eq!(response.request, Some(request));

    Ok(())
}

async fn repeat_data_body_info(client: &showcase::client::Compliance) -> Result<()> {
    let request = new_request();
    let response = client
        .repeat_data_body_info()
        .with_request(request.clone())
        .send()
        .await?;
    tracing::info!("response: {response:?}");
    assert_eq!(response.binding_uri, "/v1beta1/repeat:bodyinfo");
    assert_eq!(response.request, Some(request));
    Ok(())
}

async fn repeat_data_query(client: &showcase::client::Compliance) -> Result<()> {
    let request = new_request();
    let response = client
        .repeat_data_query()
        .with_request(request.clone())
        .send()
        .await?;
    tracing::info!("response: {response:?}");
    assert_eq!(response.binding_uri, "/v1beta1/repeat:query");
    let got = workaround_bug_2198(response);
    assert_eq!(got, request);
    Ok(())
}

async fn repeat_data_simple_path(client: &showcase::client::Compliance) -> Result<()> {
    let request = new_request();
    let response = client
        .repeat_data_simple_path()
        .with_request(request.clone())
        .send()
        .await?;
    tracing::info!("response: {response:?}");
    assert!(
        response.binding_uri.starts_with("/v1beta1/repeat/"),
        "{}",
        response.binding_uri
    );
    assert!(
        response.binding_uri.ends_with(":simplepath"),
        "{}",
        response.binding_uri
    );
    let got = workaround_bug_2198(response);
    assert_eq!(got, request);
    Ok(())
}

async fn repeat_data_path_resource(client: &showcase::client::Compliance) -> Result<()> {
    let mut request = new_request();
    request.info = request.info.map(|i| i.set_f_string("first/f-string-value"));
    let response = client
        .repeat_data_path_resource()
        .with_request(request.clone())
        .send()
        .await?;
    tracing::info!("response: {response:?}");
    assert_eq!(
        response.binding_uri,
        "/v1beta1/repeat/{info.f_string=first/*}/{info.f_child.f_string=second/*}/bool/{info.f_bool}:pathresource"
    );
    let got = workaround_bug_2198(response);
    assert_eq!(got, request);
    Ok(())
}

async fn repeat_data_path_trailing_resource(client: &showcase::client::Compliance) -> Result<()> {
    let mut request = new_request();
    request.info = request.info.map(|i| i.set_f_string("first/f-string-value"));
    let response = client
        .repeat_data_path_trailing_resource()
        .with_request(request.clone())
        .send()
        .await?;
    tracing::info!("response: {response:?}");
    assert_eq!(
        response.binding_uri,
        "/v1beta1/repeat/{info.f_string=first/*}/{info.f_child.f_string=second/**}:pathtrailingresource"
    );
    let got = workaround_bug_2198(response);
    assert_eq!(got, request);
    Ok(())
}

async fn repeat_data_body_put(client: &showcase::client::Compliance) -> Result<()> {
    let request = new_request();
    let response = client
        .repeat_data_body_put()
        .with_request(request.clone())
        .send()
        .await?;
    tracing::info!("response: {response:?}");
    assert_eq!(response.binding_uri, "/v1beta1/repeat:bodyput");
    assert_eq!(response.request, Some(request));
    Ok(())
}

async fn repeat_data_body_patch(client: &showcase::client::Compliance) -> Result<()> {
    let request = new_request();
    let response = client
        .repeat_data_body_patch()
        .with_request(request.clone())
        .send()
        .await?;
    tracing::info!("response: {response:?}");
    assert_eq!(response.binding_uri, "/v1beta1/repeat:bodypatch");
    assert_eq!(response.request, Some(request));
    Ok(())
}

async fn unknown_enum(client: &showcase::client::Compliance) -> Result<()> {
    let response = client.get_enum().set_unknown_enum(true).send().await?;
    tracing::info!("response: {response:?}");
    let verify = client
        .verify_enum()
        .with_request(response.clone())
        .send()
        .await?;
    tracing::info!("verify: {verify:?}");
    assert_eq!(verify.continent, response.continent);
    Ok(())
}

async fn binding_success(client: &showcase::client::Compliance) -> Result<()> {
    let request = RepeatRequest::new().set_info(
        ComplianceData::new()
            .set_f_string("string")
            .set_f_int32(1)
            .set_f_double(-2.5)
            .set_f_bool(true)
            .set_f_kingdom(LifeKingdom::Animalia),
    );
    let response = client
        .repeat_data_simple_path()
        .with_request(request.clone())
        .send()
        .await?;
    tracing::info!("response: {response:?}");
    assert_eq!(
        response.binding_uri,
        "/v1beta1/repeat/{info.f_string}/{info.f_int32}/{info.f_double}/{info.f_bool}/{info.f_kingdom}:simplepath"
    );
    let got = response
        .request
        .expect("the response should echo `request`");
    assert_eq!(got, request);
    Ok(())
}

async fn binding_error_unset(client: &showcase::client::Compliance) -> Result<()> {
    let request = RepeatRequest::new(); // `info` is unset
    let e = client
        .repeat_data_simple_path()
        .with_request(request)
        .send()
        .await
        .expect_err("Binding validation should fail");

    assert!(e.is_binding(), "{e:?}");
    assert!(e.source().is_some(), "{e:?}");
    let b = e
        .source()
        .and_then(|e| e.downcast_ref::<BindingError>())
        .expect("should be a BindingError");
    let missing_fields: Vec<&str> = b.paths[0].subs.iter().map(|sub| sub.field_name).collect();
    assert_eq!(
        missing_fields,
        vec![
            "info.f_string",
            "info.f_int32",
            "info.f_double",
            "info.f_bool",
            "info.f_kingdom"
        ]
    );
    Ok(())
}

async fn binding_error_unset_expecting(client: &showcase::client::Compliance) -> Result<()> {
    let request = RepeatRequest::new().set_info(ComplianceData::new()); // `info` is set, but `info.f_string` is empty.
    let e = client
        .repeat_data_simple_path()
        .with_request(request)
        .send()
        .await
        .expect_err("Binding validation should fail");

    assert!(e.is_binding(), "{e:?}");
    assert!(e.source().is_some(), "{e:?}");
    let b = e
        .source()
        .and_then(|e| e.downcast_ref::<BindingError>())
        .expect("should be a BindingError");
    let sub = &b.paths[0].subs[0];
    assert!(
        matches!(sub.problem, SubstitutionFail::UnsetExpecting("*")),
        "{:?}",
        sub.problem
    );
    Ok(())
}

async fn binding_error_mismatch_expecting(client: &showcase::client::Compliance) -> Result<()> {
    let request =
        RepeatRequest::new().set_info(ComplianceData::new().set_f_string("invalid/format")); // `info.f_string` is set, but invalid.
    let e = client
        .repeat_data_simple_path()
        .with_request(request)
        .send()
        .await
        .expect_err("Binding validation should fail");

    assert!(e.is_binding(), "{e:?}");
    assert!(e.source().is_some(), "{e:?}");
    let b = e
        .source()
        .and_then(|e| e.downcast_ref::<BindingError>())
        .expect("should be a BindingError");
    let sub = &b.paths[0].subs[0];
    assert!(
        matches!(&sub.problem, SubstitutionFail::MismatchExpecting(actual, "*") if actual == "invalid/format"),
        "{:?}",
        sub.problem
    );
    Ok(())
}

async fn additional_binding_used(client: &showcase::client::Compliance) -> Result<()> {
    let request = RepeatRequest::new().set_info(
        ComplianceData::new()
            .set_f_string("second/b")
            .set_f_child(ComplianceDataChild::new().set_f_string("first/a")),
    );
    let response = client
        .repeat_data_path_resource()
        .with_request(request.clone())
        .send()
        .await?;
    tracing::info!("response: {response:?}");
    // Note the different verb in the response, when compared with the
    // `repeat_data_path_resource()` test.
    assert_eq!(
        response.binding_uri,
        "/v1beta1/repeat/{info.f_child.f_string=first/*}/{info.f_string=second/*}/bool/{info.f_bool}:childfirstpathresource"
    );
    let got = response
        .request
        .expect("the response should echo `request`");
    assert_eq!(got, request);
    Ok(())
}

async fn binding_error_has_all_failed_paths(client: &showcase::client::Compliance) -> Result<()> {
    let request = RepeatRequest::new();
    let e = client
        .repeat_data_path_resource()
        .with_request(request)
        .send()
        .await
        .expect_err("Binding validation should fail");

    assert!(e.is_binding(), "{e:?}");
    assert!(e.source().is_some(), "{e:?}");
    let b = e
        .source()
        .and_then(|e| e.downcast_ref::<BindingError>())
        .expect("should be a BindingError");
    assert!(b.paths.len() >= 2, "{}", b.paths.len());
    Ok(())
}

fn workaround_bug_2198(response: RepeatResponse) -> RepeatRequest {
    // TODO(#2198) - fix encoding of `bytes` fields in query parameters.
    let mut got = response
        .request
        .expect("the response should echo `request`");
    got.info = got.info.map(|i| i.set_f_bytes(bytes_payload()));
    got
}

fn bytes_payload() -> bytes::Bytes {
    bytes::Bytes::from_static(b"How vexingly quick daft zebras jump!")
}

fn new_request() -> RepeatRequest {
    let grandchild = ComplianceDataGrandchild::new()
        .set_f_double(8.125)
        .set_f_bool(true);

    let child = ComplianceDataChild::new()
        .set_f_float(1.5)
        .set_f_double(2.5)
        .set_f_bool(true)
        .set_f_continent(Continent::Europe)
        .set_f_child(
            grandchild
                .clone()
                .set_f_string("grandchild-in-f-child-field"),
        )
        .set_p_string(concat!(
            "Benjamín pidió una bebida de kiwi y fresa. ",
            "Noé, sin vergüenza, la más exquisita champaña del menú"
        ))
        .set_p_float(4.75)
        .set_p_double(16.25)
        .set_p_bool(false)
        .set_p_continent(Continent::Australia)
        .set_p_child(
            grandchild
                .clone()
                .set_f_string("grandchild-in-p-child-field"),
        );

    RepeatRequest::new()
        .set_f_int32(1)
        .set_f_int64(2)
        .set_f_double(3)
        .set_p_int32(4)
        .set_p_int64(5)
        .set_p_double(6.5)
        .set_info(
            ComplianceData::new()
                .set_f_string("the quick brown fox jumps over the lazy dog")
                .set_f_int32(1)
                .set_f_sint32(2)
                .set_f_sfixed32(3)
                .set_f_uint32(4_u32)
                .set_f_fixed32(5_u32)
                .set_f_int64(6)
                .set_f_sint64(7)
                .set_f_sfixed64(8)
                .set_f_uint64(9_u64)
                .set_f_fixed64(10_u64)
                .set_f_double(11.25_f64)
                .set_f_float(12.5_f32)
                .set_f_bool(true)
                .set_f_bytes(bytes_payload())
                .set_f_kingdom(LifeKingdom::Fungi)
                .set_f_child(child.clone().set_f_string("second/child-in-f-child-field"))
                .set_p_string(
                    "Answer to the Ultimate Question of Life, the Universe, and Everything"
                        .to_string(),
                )
                .set_p_int32(42)
                .set_p_double(42.42)
                .set_p_bool(false)
                .set_p_kingdom(LifeKingdom::Eubacteria)
                .set_p_child(child.clone().set_f_string("child-in-p-child-field")),
        )
}
