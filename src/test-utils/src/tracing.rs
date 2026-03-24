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

use std::io::Write;
use std::sync::{Arc, Mutex};
use tracing_subscriber::fmt::format::FmtSpan;

/// Enables tracing for the application.
pub fn enable_tracing() -> ::tracing::subscriber::DefaultGuard {
    #[cfg(feature = "log-integration-tests")]
    let max_level = tracing::Level::INFO;
    #[cfg(not(feature = "log-integration-tests"))]
    let max_level = tracing::Level::WARN;
    let builder = tracing_subscriber::fmt()
        .with_level(true)
        .with_thread_ids(true)
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .with_max_level(max_level);
    let subscriber = builder.finish();

    tracing::subscriber::set_default(subscriber)
}

/// A helper type to capture traces
#[derive(Clone, Debug, Default)]
pub struct Buffer(Arc<Mutex<Vec<u8>>>);

impl Buffer {
    pub fn captured(&self) -> Vec<u8> {
        let guard = self.0.lock().expect("never poisoned");
        guard.clone()
    }
}

impl Write for Buffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut guard = self.0.lock().expect("never poisoned");
        guard.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn check_default() {
        let _guard = enable_tracing();
        let default = tracing::Dispatch::default();
        assert!(
            default.is::<tracing_subscriber::FmtSubscriber>(),
            "{default:?}"
        );
    }

    #[test]
    fn buffer() -> anyhow::Result<()> {
        const TEXT: &str = "The quick brown fox jumps over the lazy dog";
        let mut buffer = Buffer::default();
        writeln!(buffer, "{}", TEXT)?;
        writeln!(buffer, "{}", TEXT)?;
        writeln!(buffer, "{}", TEXT)?;
        buffer.flush()?;
        let captured = buffer.captured();
        let contents = String::from_utf8(captured)?;
        let want = format!("{}\n{}\n{}\n", TEXT, TEXT, TEXT);
        assert_eq!(contents, want);
        Ok(())
    }
}
