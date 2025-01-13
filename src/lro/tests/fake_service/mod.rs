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

use axum::extract::State;
use axum::http::StatusCode;
use std::sync::{Arc, Mutex};
use tokio::task::JoinHandle;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub struct ServerState {
    pub create: std::collections::VecDeque<(StatusCode, String)>,
    pub poll: std::collections::VecDeque<(StatusCode, String)>,
}

type SharedServerState = Arc<Mutex<ServerState>>;

pub async fn start(initial_state: ServerState) -> Result<(String, JoinHandle<()>)> {
    let state = Arc::new(Mutex::new(initial_state));
    let app = axum::Router::new()
        .route("/create", axum::routing::post(create_handler))
        .route("/poll", axum::routing::get(poll_handler))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let server = tokio::spawn(async {
        axum::serve(listener, app).await.unwrap();
    });

    Ok((format!("http://{}:{}", addr.ip(), addr.port()), server))
}

async fn create_handler(State(state): State<SharedServerState>) -> (StatusCode, String) {
    let mut state = state.lock().expect("shared state is poisoned");
    state.create.pop_front().unwrap_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            "exhausted create responses".to_string(),
        )
    })
}

async fn poll_handler(State(state): State<SharedServerState>) -> (StatusCode, String) {
    let mut state = state.lock().expect("shared state is poisoned");
    state
        .poll
        .pop_front()
        .unwrap_or_else(|| (StatusCode::BAD_REQUEST, "exhausted poll data".to_string()))
}
