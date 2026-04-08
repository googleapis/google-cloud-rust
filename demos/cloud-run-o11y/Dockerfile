# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM rust:1.93-slim-bookworm AS builder

WORKDIR /usr/src/workspace
COPY . .
RUN env RUSTFLAGS="--cfg google_cloud_unstable_tracing" cargo build -p demo-cloud-run-o11y --release

# Use a lightweight Debian image for the runtime
FROM debian:bookworm-slim

# Install CA certificates needed for HTTPS/GCP API calls
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

# Copy the compiled binary from the builder stage
COPY --from=builder /usr/src/workspace/target/release/demo-cloud-run-o11y /usr/local/bin/demo-cloud-run-o11y

# Expose standard Cloud Run port
EXPOSE 8080

# Run the application
ENTRYPOINT ["/usr/local/bin/demo-cloud-run-o11y"]
