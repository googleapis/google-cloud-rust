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

use anyhow::Result;
use httptest::http::{Response, StatusCode};
use httptest::{matchers::*, Expectation, Server};
use std::sync::{Arc, Mutex};

pub struct ServerState {
    pub create: std::collections::VecDeque<(StatusCode, String)>,
    pub poll: std::collections::VecDeque<(StatusCode, String)>,
}

type SharedServerState = Arc<Mutex<ServerState>>;

pub fn start(initial_state: ServerState) -> Result<(String, Server)> {    
    let num_create = initial_state.create.len() + 1;
    let num_poll = initial_state.poll.len() + 1;
    let state: SharedServerState = Arc::new(Mutex::new(initial_state));
    let server = Server::run();

    let create_state = Arc::clone(&state);    
    server.expect(
        Expectation::matching(all_of![
            request::method("POST"),
            request::path("/create")
        ])
        .times(0..num_create)
        .respond_with(move || {
            let mut state = create_state.lock().expect("shared state is poisoned");
            let (status, body) = state.create.pop_front().unwrap_or_else(|| {
                (
                    StatusCode::BAD_REQUEST,
                    "exhausted create responses".to_string(),
                )
            });
            Response::builder().status(status).body(body.into_bytes()).unwrap()
        }),
    );

    let poll_state = Arc::clone(&state);
    server.expect(
        Expectation::matching(all_of![
            request::method("GET"),
            request::path("/poll")
        ])
        .times(0..num_poll)
        .respond_with(move || {
            let mut state = poll_state.lock().expect("shared state is poisoned");
            let (status, body) = state.poll.pop_front().unwrap_or_else(|| {
                (
                    StatusCode::BAD_REQUEST,
                    "exhausted poll data".to_string(),
                )
            });
            Response::builder().status(status).body(body.into_bytes()).unwrap()
        }),
    );

    let endpoint = format!("http://{}",server.addr());
    Ok((endpoint, server))
}