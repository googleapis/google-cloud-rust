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

//! Highlights the observability features in the Rust SDK.

const DESCRIPTION: &str = concat!(
    "The demo highlights the observability features in the Rust SDK.",
    "\n\n",
    "This application is a web service that picks an image at random and asks Gemini to describe it.",
    "\n\n",
    "To pick the image the application uses Cloud Storage.",
    " To describe the image the application uses Vertex AI.",
    " The application converts the markdown output from Gemini to HTML.",
    "\n\n",
    "Each request to Cloud Storage and Vertex AI are traced, its latency is measured, and any",
    " errors are logged in a format that Cloud Logging can consume.",
    "\n"
);

// A public bucket.
//
// The images/ prefix includes a number of images that can be sent to Gemini.
const BUCKET: &str = "generativeai-downloads";

mod args;
mod error;
mod logs;
mod observability;
mod state;

use args::Args;
use axum::Router;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::Html;
use axum::routing;
use clap::Parser;
use error::AppError;
use google_cloud_aiplatform_v1::client::PredictionService;
use google_cloud_aiplatform_v1::model::part::Data;
use google_cloud_aiplatform_v1::model::{Content, FileData, Part};
use google_cloud_auth::credentials::Builder as CredentialsBuilder;
use google_cloud_gax::options::RequestOptionsBuilder;
use google_cloud_gax::paginator::ItemPaginator as _;
use google_cloud_storage::client::StorageControl;
use google_cloud_storage::model::Object;
use opentelemetry_http::HeaderExtractor;
use state::AppState;
use std::time::Duration;
use tokio::net::TcpListener;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let credentials = CredentialsBuilder::default()
        .build()
        .inspect_err(|e| tracing::error!("Cannot initialize credentials: {e:#?}"))?;
    observability::exporters(&args, credentials.clone()).await?;
    tracing::info!("configuration: {args:?}");

    let state = AppState::new(args.clone(), credentials.clone()).await?;
    let app = Router::new()
        .route("/", routing::get(handler))
        .route("/ok", routing::get(ok))
        .route("/predict", routing::get(predict))
        .with_state(state);
    let addr = format!("0.0.0.0:{}", args.port);
    let listener = TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn ok() -> &'static str {
    "OK\n"
}

async fn handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Html<String>, AppError> {
    let extractor = HeaderExtractor(&headers);
    let remote_context =
        opentelemetry::global::get_text_map_propagator(|propagator| propagator.extract(&extractor));
    let span = tracing::info_span!(
        "handling / request",
        "otel.status_code" = tracing::field::Empty
    );
    let _ = span
        .set_parent(remote_context)
        .inspect_err(|e| tracing::warn!("cannot set context: {e:?}"));

    let object = random_image(state.storage_control())
        .instrument(span.clone())
        .await?;
    let prediction = call_model(state.prediction_service(), state.model(), &object)
        .instrument(span.clone())
        .await?;
    let _enter = span.entered();
    let description = markdown::to_html(&prediction);
    let path = format!("{BUCKET}/{}", object.name);
    let body = format!(
        r#"
<!DOCTYPE html><html><body>
<h1>Rust SDK Demo: Vertex AI Prediction</h1>
<p>
<img src="https://storage.googleapis.com/{path}" alt="a stock image">
</p>
<p>
<b>Gemini Response:</b><br>
{description}
</p>
</body></html>
"#,
    );
    Ok(Html::from(body))
}

async fn predict(State(state): State<AppState>, headers: HeaderMap) -> Result<String, AppError> {
    let extractor = HeaderExtractor(&headers);
    let remote_context =
        opentelemetry::global::get_text_map_propagator(|propagator| propagator.extract(&extractor));
    let span = tracing::info_span!(
        "handling /predict request",
        "otel.status_code" = tracing::field::Empty
    );
    let _ = span
        .set_parent(remote_context)
        .inspect_err(|e| tracing::warn!("cannot set context: {e:?}"));

    let object = random_image(state.storage_control())
        .instrument(span.clone())
        .await?;
    let prediction = call_model(state.prediction_service(), state.model(), &object)
        .instrument(span.clone())
        .await?;
    Ok(prediction)
}

async fn random_image(storage_control: &StorageControl) -> Result<Object, AppError> {
    let bucket = format!("projects/_/buckets/{BUCKET}");
    let mut items = storage_control
        .list_objects()
        .set_parent(&bucket)
        .set_prefix("images/")
        .by_item();
    // Implements Jeffrey Vitter's "Algorithm R" for a reservoir of size 1:
    //     https://en.wikipedia.org/wiki/Reservoir_sampling
    let mut object = None;
    let mut count = 0_usize;
    while let Some(o) = items.next().await.transpose().map_err(AppError::Backend)? {
        count += 1;
        if rand::random_range(0..count) == 0 {
            object = Some(o);
        }
    }
    object.ok_or_else(|| AppError::BadResponseFormat(format!("cannot find image in {bucket}")))
}

async fn call_model(
    prediction_service: &PredictionService,
    model: &str,
    object: &Object,
) -> Result<String, AppError> {
    let response = prediction_service
        .generate_content()
        .set_model(model)
        .set_contents([Content::new().set_role("user").set_parts([
            Part::new().set_file_data(
                FileData::new()
                    .set_mime_type(&object.content_type)
                    .set_file_uri(format!("gs://{BUCKET}/{}", object.name)),
            ),
            Part::new().set_text("Describe this picture."),
        ])])
        .with_attempt_timeout(Duration::from_secs(15))
        .send()
        .await;

    let response = response
        .inspect_err(|e| {
            tracing::error!("response error: {e:?}");
        })
        .map_err(AppError::Backend)?;
    let Some(Data::Text(data)) = response
        .candidates
        .into_iter()
        .filter_map(|candidate| candidate.content)
        .flat_map(|content| content.parts.into_iter())
        .filter_map(|part| part.data)
        .next()
    else {
        return Err(AppError::BadResponseFormat(
            "missing Data::Text element".into(),
        ));
    };
    Ok(data)
}
