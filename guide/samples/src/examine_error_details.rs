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

//! An example showing how to examine error details.

// [START rust_examine_error_details] ANCHOR: examine-error-details
use google_cloud_gax::error::rpc::StatusDetails;
use google_cloud_language_v2::client::LanguageService;
use google_cloud_language_v2::model::Document;
use google_cloud_language_v2::model::document::Type;

pub async fn sample() -> anyhow::Result<()> {
    // [START rust_examine_error_details_client] ANCHOR: examine-error-details-client
    let client = LanguageService::builder().build().await?;
    // [END rust_examine_error_details_client] ANCHOR_END: examine-error-details-client

    // [START rust_examine_error_details_request] ANCHOR: examine-error-details-request
    let result = client
        .analyze_sentiment()
        .set_document(
            Document::new()
                // Missing document contents
                // .set_content("Hello World!")
                .set_type(Type::PlainText),
        )
        .send()
        .await;
    // [END rust_examine_error_details_request] ANCHOR_END: examine-error-details-request

    // [START rust_examine_error_details_print] ANCHOR: examine-error-details-print
    let err = result.expect_err("the request should have failed");
    println!("\nrequest failed with error {err:#?}");
    // [END rust_examine_error_details_print] ANCHOR_END: examine-error-details-print

    // [START rust_examine_error_details_service_error] ANCHOR: examine-error-details-service-error
    if let Some(status) = err.status() {
        println!(
            "  status.code={}, status.message={}",
            status.code, status.message,
        );
        // [END rust_examine_error_details_service_error] ANCHOR_END: examine-error-details-service-error
        // [START rust_examine_error_details_iterate] ANCHOR: examine-error-details-iterate
        for detail in status.details.iter() {
            match detail {
                // [END rust_examine_error_details_iterate] ANCHOR_END: examine-error-details-iterate
                // [START rust_examine_error_details_bad_request] ANCHOR: examine-error-details-bad-request
                StatusDetails::BadRequest(bad) => {
                    // [END rust_examine_error_details_bad_request] ANCHOR_END: examine-error-details-bad-request
                    // [START rust_examine_error_details_each_field] ANCHOR: examine-error-details-each-field
                    for f in bad.field_violations.iter() {
                        println!(
                            "  the request field {} has a problem: \"{}\"",
                            f.field, f.description
                        );
                    }
                    // [END rust_examine_error_details_each_field] ANCHOR_END: examine-error-details-each-field
                }
                _ => {
                    println!("  additional error details: {detail:?}");
                }
            }
        }
    }

    Ok(())
}
// [END rust_examine_error_details] ANCHOR_END: examine-error-details
