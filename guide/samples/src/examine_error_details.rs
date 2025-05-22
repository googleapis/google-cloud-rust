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

//! Examples showing how to examine error details.

// ANCHOR: examine-error-details
pub async fn examine_error_details() -> crate::Result<()> {
    // ANCHOR: examine-error-details-client
    use google_cloud_language_v2 as lang;
    let client = lang::client::LanguageService::builder().build().await?;
    // ANCHOR_END: examine-error-details-client

    // ANCHOR: examine-error-details-request
    let result = client
        .analyze_sentiment()
        .set_document(
            lang::model::Document::new()
                // Missing document contents
                // .set_content("Hello World!")
                .set_type(lang::model::document::Type::PlainText),
        )
        .send()
        .await;
    // ANCHOR_END: examine-error-details-request

    // ANCHOR: examine-error-details-print
    let err = result.expect_err("the request should have failed");
    println!("\nrequest failed with error {err:#?}");
    // ANCHOR_END: examine-error-details-print

    // ANCHOR: examine-error-details-service-error
    if let Some(status) = err.status() {
        println!(
            "  status.code={}, status.message={}",
            status.code, status.message,
        );
        // ANCHOR_END: examine-error-details-service-error
        // ANCHOR: examine-error-details-iterate
        for detail in status.details.iter() {
            use google_cloud_gax::error::rpc::StatusDetails;
            match detail {
                // ANCHOR_END: examine-error-details-iterate
                // ANCHOR: examine-error-details-bad-request
                StatusDetails::BadRequest(bad) => {
                    // ANCHOR_END: examine-error-details-bad-request
                    // ANCHOR: examine-error-details-each-field
                    for f in bad.field_violations.iter() {
                        println!(
                            "  the request field {} has a problem: \"{}\"",
                            f.field, f.description
                        );
                    }
                    // ANCHOR_END: examine-error-details-each-field
                }
                _ => {
                    println!("  additional error details: {detail:?}");
                }
            }
        }
    }

    Ok(())
}
// ANCHOR_END: examine-error-details
