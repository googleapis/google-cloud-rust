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

/// The behavior on shutdown.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ShutdownBehavior {
    /// The subscriber stops reading from the underlying gRPC stream.
    ///
    /// The subscriber continues to accept acknowledgements for messages it has
    /// delivered to the application. The subscriber continues to extend leases
    /// for these messages while it waits on the application to ack/nack them.
    ///
    /// The shutdown is complete when all message handles delivered to the
    /// application have been consumed, and all pending ack/nack RPCs have
    /// completed.
    WaitForProcessing,

    /// The subscriber stops reading from the underlying gRPC stream.
    ///
    /// The subscriber stops accepting acknowledgements from the application.
    /// The subscriber sends all pending acknowledgements to the server. The
    /// subscriber nacks all other messages under lease management.
    ///
    /// The shutdown is complete when all pending ack/nack RPCs have completed.
    NackImmediately,
}
