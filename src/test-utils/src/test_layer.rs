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

use std::collections::HashMap;
use std::sync::{Mutex, Once};
use tracing::{Subscriber, field, span};
use tracing_subscriber::{self, Layer, layer::Context, prelude::*, registry::SpanRef};

/// Represents a captured tracing span with its attributes.
#[derive(Debug, Clone)]
pub struct CapturedSpan {
    /// The unique ID of the span.
    pub id: span::Id,
    /// The name of the span.
    pub name: String,
    /// A map of attribute keys to their string representations.
    pub attributes: HashMap<String, String>,
    /// The test ID associated with this span, if captured via `TestLayer::initialize`.
    pub test_id: Option<String>,
}

/// A `tracing::field::Visit` implementation to extract attribute key-value pairs from span events.
///
/// This visitor is used by the `TestLayer` to populate the `attributes` map
/// in a `CapturedSpan`. It converts various field types (str, debug, i64, u64, bool)
/// into String representations.
struct TestVisitor<'a>(&'a mut HashMap<String, String>);

impl<'a> field::Visit for TestVisitor<'a> {
    fn record_str(&mut self, field: &field::Field, value: &str) {
        self.0.insert(field.name().to_string(), value.to_string());
    }

    fn record_debug(&mut self, field: &field::Field, value: &dyn std::fmt::Debug) {
        self.0
            .insert(field.name().to_string(), format!("{:?}", value));
    }

    fn record_i64(&mut self, field: &field::Field, value: i64) {
        self.0.insert(field.name().to_string(), value.to_string());
    }

    fn record_u64(&mut self, field: &field::Field, value: u64) {
        self.0.insert(field.name().to_string(), value.to_string());
    }

    fn record_bool(&mut self, field: &field::Field, value: bool) {
        self.0.insert(field.name().to_string(), value.to_string());
    }
}

/// A thread-safe log to store `CapturedSpan` instances.
///
/// This struct holds the spans captured by the global `TestLayer`.
/// It uses a Mutex to ensure safe concurrent access.
struct CapturedSpanLog {
    spans: Mutex<Vec<CapturedSpan>>,
}

impl CapturedSpanLog {
    const fn new() -> Self {
        CapturedSpanLog {
            spans: Mutex::new(Vec::new()),
        }
    }

    /// Adds a `CapturedSpan` to the log.
    fn push(&self, span: CapturedSpan) {
        self.spans.lock().unwrap().push(span);
    }

    /// Retrieves and removes all spans associated with a given `test_id`.
    ///
    /// Spans with the name "test_layer" are excluded.
    fn take_by_test_id(&self, test_id: &str) -> Vec<CapturedSpan> {
        let mut spans = self.spans.lock().unwrap();
        let mut taken = Vec::new();
        let mut i = 0;
        while i < spans.len() {
            if spans[i].test_id.as_deref() == Some(test_id) && spans[i].name != "test_layer" {
                taken.push(spans.remove(i));
            } else {
                i += 1;
            }
        }
        taken
    }

    /// Removes all spans associated with a given `test_id`.
    fn clear_by_test_id(&self, test_id: &str) {
        self.spans
            .lock()
            .unwrap()
            .retain(|s| s.test_id.as_deref() != Some(test_id));
    }
}

static SPAN_LOG: CapturedSpanLog = CapturedSpanLog::new();
static INIT: Once = Once::new();

/// A wrapper type to store the test ID in span extensions.
#[derive(Clone, Debug)]
struct TestId(String);

/// Finds the test ID associated with a span by traversing up the span tree.
///
/// It looks for a parent span named "test_layer" and extracts the `TestId` from its extensions.
fn find_test_id<S: Subscriber + for<'b> tracing_subscriber::registry::LookupSpan<'b>>(
    mut span_ref: SpanRef<'_, S>,
    _ctx: &Context<'_, S>,
) -> Option<String> {
    loop {
        if span_ref.name() == "test_layer" {
            return span_ref.extensions().get::<TestId>().map(|t| t.0.clone());
        }
        if let Some(parent) = span_ref.parent() {
            span_ref = parent;
        } else {
            return None;
        }
    }
}

/// A tracing layer for capturing and inspecting spans within tests.
///
/// This layer is designed to be installed as a global subscriber during
/// test execution. It isolates captured spans based on a unique `test_id`
/// provided during initialization, allowing tests to run in parallel without
/// interfering with each other's captured trace data.
///
/// # Usage
///
/// 1.  **Initialize:** In each test function, call `TestLayer::initialize()`
///     with a unique `TEST_ID` string. This returns an RAII guard. The
///     layer will capture spans only while this guard is in scope.
/// 2.  **Execute Code:** Run the code you want to test, which emits tracing spans.
/// 3.  **Capture Spans:** Use `TestLayer::capture()` with the same `TEST_ID`
///     to retrieve all spans captured during the test.
/// 4.  **Assert:** Make assertions on the captured `CapturedSpan` data.
///
/// # Example
///
/// ```rust
/// use google_cloud_test_utils::test_layer::*;
/// use tracing::info_span;
///
/// #[tokio::test]
/// async fn my_tracing_test() {
///     const TEST_ID: &str = "my_tracing_test";
///     let _guard = TestLayer::initialize(TEST_ID);
///
///     // Code under test that emits spans
///     info_span!("my_operation", foo = "bar").in_scope(|| {
///         tracing::info!("Doing something important");
///     });
///
///     let captured = TestLayer::capture(TEST_ID);
///     assert_eq!(captured.len(), 1);
///     let span = &captured[0];
///     assert_eq!(span.name, "my_operation");
///     assert_eq!(span.attributes.get("foo"), Some(&"bar".to_string()));
/// }
/// ```
#[derive(Clone, Default)]
pub struct TestLayer;

impl TestLayer {
    /// Initializes the TestLayer for the current test scope.
    ///
    /// Installs the `TestLayer` as a global subscriber if it hasn't been already.
    /// It clears any previously captured spans for the given `test_id`.
    ///
    /// Returns an RAII guard (`tracing::span::EnteredSpan`). The layer will
    /// only capture spans associated with this `test_id` while this guard is in scope.
    pub fn initialize(test_id: &'static str) -> tracing::span::EnteredSpan {
        INIT.call_once(|| {
            let layer = TestLayer;
            let subscriber = tracing_subscriber::registry().with(layer);
            tracing::subscriber::set_global_default(subscriber)
                .expect("Failed to set global default subscriber");
        });
        SPAN_LOG.clear_by_test_id(test_id);
        tracing::span!(tracing::Level::INFO, "test_layer", test_id = test_id).entered()
    }

    /// Retrieves all spans captured for the given `test_id`.
    ///
    /// This method consumes the captured spans from the log, so subsequent
    /// calls for the same `test_id` within the same test run (without re-initializing)
    /// will return an empty vector unless new spans are emitted.
    pub fn capture(test_id: &str) -> Vec<CapturedSpan> {
        SPAN_LOG.take_by_test_id(test_id)
    }
}

impl<S> Layer<S> for TestLayer
where
    S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    /// Called when a new span is created.
    ///
    /// Captures the span's attributes and associates it with the current `test_id`.
    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, S>) {
        let mut span_map = HashMap::new();
        let mut visitor = TestVisitor(&mut span_map);
        attrs.record(&mut visitor);

        let span_ref = ctx.span(id).expect("Span not found in registry");
        let name = span_ref.name().to_string();
        let test_id = if name == "test_layer" {
            span_map.get("test_id").cloned().inspect(|id_str| {
                span_ref.extensions_mut().insert(TestId(id_str.clone()));
            })
        } else {
            find_test_id(span_ref, &ctx)
        };

        let captured_span = CapturedSpan {
            id: id.clone(),
            name,
            attributes: span_map,
            test_id,
        };
        SPAN_LOG.push(captured_span);
    }

    /// Called when an event is recorded within a span.
    ///
    /// Updates the attributes of the existing captured span.
    fn on_record(&self, id: &span::Id, values: &span::Record<'_>, _ctx: Context<'_, S>) {
        let mut spans = SPAN_LOG.spans.lock().unwrap();
        if let Some(captured_span) = spans.iter_mut().find(|s| s.id == *id) {
            let mut visitor = TestVisitor(&mut captured_span.attributes);
            values.record(&mut visitor);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use tracing::info_span;

    /// Helper to create a dummy CapturedSpan for testing the log.
    fn dummy_span(id: u64, name: &str, test_id: Option<&str>) -> CapturedSpan {
        CapturedSpan {
            id: tracing::Id::from_u64(id),
            name: name.to_string(),
            attributes: HashMap::new(),
            test_id: test_id.map(String::from),
        }
    }

    /// Tests the basic push and take functionality of `CapturedSpanLog`.
    #[test]
    fn test_captured_span_log_push_take() {
        let log = CapturedSpanLog::new();
        log.push(dummy_span(1, "span1", Some("test1")));
        log.push(dummy_span(2, "span2", Some("test2")));
        log.push(dummy_span(3, "span3", Some("test1")));

        let test1_spans = log.take_by_test_id("test1");
        assert_eq!(test1_spans.len(), 2);
        assert!(test1_spans.iter().any(|s| s.name == "span1"));
        assert!(test1_spans.iter().any(|s| s.name == "span3"));

        let test2_spans = log.take_by_test_id("test2");
        assert_eq!(test2_spans.len(), 1);
        assert_eq!(test2_spans[0].name, "span2");

        let remaining_spans = log.spans.lock().unwrap();
        assert!(remaining_spans.is_empty());
    }

    /// Tests the clear functionality of `CapturedSpanLog`.
    #[test]
    fn test_captured_span_log_clear() {
        let log = CapturedSpanLog::new();
        log.push(dummy_span(1, "span1", Some("test1")));
        log.push(dummy_span(2, "span2", Some("test2")));
        log.clear_by_test_id("test1");

        let spans = log.spans.lock().unwrap();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].name, "span2");
    }

    /// Tests taking from an empty or non-matching test ID in `CapturedSpanLog`.
    #[test]
    fn test_captured_span_log_take_empty() {
        let log = CapturedSpanLog::new();
        log.push(dummy_span(1, "span1", Some("test1")));
        let taken = log.take_by_test_id("nonexistent");
        assert!(taken.is_empty());
        assert_eq!(log.spans.lock().unwrap().len(), 1);
    }

    /// Tests that spans created within the `TestLayer::initialize` guard are assigned the correct test ID.
    #[tokio::test]
    async fn test_layer_test_id_assignment() {
        const TEST_ID: &str = "test_layer_test_id_assignment";
        let _guard = TestLayer::initialize(TEST_ID);

        info_span!("outer_span").in_scope(|| {
            info_span!("inner_span").in_scope(|| {
                tracing::info!("deep inside");
            });
        });

        let captured = TestLayer::capture(TEST_ID);
        assert_eq!(captured.len(), 2);

        for span in captured {
            assert_eq!(span.test_id.as_deref(), Some(TEST_ID));
            assert!(span.name == "outer_span" || span.name == "inner_span");
        }
    }

    /// Tests that `TestLayer` can handle multiple initializations with different test IDs.
    #[tokio::test]
    async fn test_layer_multiple_inits() {
        const TEST_ID_A: &str = "test_layer_multiple_inits_A";
        const TEST_ID_B: &str = "test_layer_multiple_inits_B";

        {
            let _guard = TestLayer::initialize(TEST_ID_A);
            info_span!("span_a").in_scope(|| {});
        }
        {
            let _guard = TestLayer::initialize(TEST_ID_B);
            info_span!("span_b").in_scope(|| {});
        }

        let captured_a = TestLayer::capture(TEST_ID_A);
        assert_eq!(captured_a.len(), 1);
        assert_eq!(captured_a[0].name, "span_a");

        let captured_b = TestLayer::capture(TEST_ID_B);
        assert_eq!(captured_b.len(), 1);
        assert_eq!(captured_b[0].name, "span_b");
    }

    /// Tests that spans are only captured for a test ID while the guard from `TestLayer::initialize` is in scope.
    #[tokio::test]
    async fn test_layer_guard_drops() {
        const TEST_ID: &str = "test_layer_guard_drops";
        {
            let _guard = TestLayer::initialize(TEST_ID);
            tracing::info_span!("span_inside_guard").in_scope(|| {
                tracing::info!("This is inside the guard");
            });
        } // _guard is dropped here

        tracing::info_span!("span_outside_guard").in_scope(|| {
            tracing::info!("This is outside the guard");
        });

        let captured = TestLayer::capture(TEST_ID);
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].name, "span_inside_guard");
        assert_eq!(captured[0].test_id.as_deref(), Some(TEST_ID));

        // Check that the outside span was NOT captured with this TEST_ID
        let all_spans = SPAN_LOG.spans.lock().unwrap();
        assert!(
            !all_spans
                .iter()
                .any(|s| s.name == "span_outside_guard" && s.test_id.as_deref() == Some(TEST_ID))
        );
    }

    /// Tests that attributes added via `span.record()` are captured.
    #[tokio::test]
    async fn test_layer_on_record() {
        const TEST_ID: &str = "test_layer_on_record";
        let _guard = TestLayer::initialize(TEST_ID);

        let span = info_span!(
            "my_span",
            initial_attr = "initial_value",
            dynamic_attr = field::Empty,
            number_attr = field::Empty,
            bool_attr = field::Empty,
            debug_attr = field::Empty
        );
        span.in_scope(|| {
            // Record attributes within the span's context
            span.record("dynamic_attr", "dynamic_value");
            span.record("number_attr", 123i64);
            span.record("bool_attr", true);
            span.record("debug_attr", field::debug(&vec![1, 2, 3]));
        });

        let captured = TestLayer::capture(TEST_ID);
        assert_eq!(captured.len(), 1);
        let span = &captured[0];
        assert_eq!(span.name, "my_span");

        let expected_attributes: HashMap<String, String> = [
            ("initial_attr".to_string(), "initial_value".to_string()),
            ("dynamic_attr".to_string(), "dynamic_value".to_string()),
            ("number_attr".to_string(), "123".to_string()),
            ("bool_attr".to_string(), "true".to_string()),
            ("debug_attr".to_string(), "[1, 2, 3]".to_string()),
        ]
        .into_iter()
        .collect();
        assert_eq!(span.attributes, expected_attributes);
    }

    /// Tests that TestVisitor correctly converts various field types to strings.
    #[tokio::test]
    async fn test_visitor_type_conversions() {
        const TEST_ID: &str = "test_visitor_type_conversions";
        let _guard = TestLayer::initialize(TEST_ID);

        let _span = info_span!(
            "type_test_span",
            my_str = "hello",
            my_i64 = -123i64,
            my_u64 = 456u64,
            my_bool = true,
            my_debug = field::debug(&("test", 789))
        );

        let captured = TestLayer::capture(TEST_ID);
        assert_eq!(captured.len(), 1);
        let span = &captured[0];

        let expected_attributes: HashMap<String, String> = [
            ("my_str", "hello"),
            ("my_i64", "-123"),
            ("my_u64", "456"),
            ("my_bool", "true"),
            ("my_debug", "(\"test\", 789)"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        assert_eq!(span.attributes, expected_attributes);
    }
}
