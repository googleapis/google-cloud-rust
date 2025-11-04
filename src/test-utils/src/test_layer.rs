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
use uuid::Uuid;

/// RAII guard for the TestLayer.
///
/// Contains the unique test ID and the tracing subscriber guard.
pub struct TestLayerGuard {
    test_id: String,
    _guard: tracing::span::EnteredSpan,
}

/// Represents the value of a captured tracing attribute.
#[derive(Debug, Clone)]
pub enum AttributeValue {
    String(String),
    StaticString(&'static str),
    Int64(i64),
    UInt64(u64),
    Boolean(bool),
    Double(f64),
}

impl PartialEq for AttributeValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            // Compare the string values, regardless of storage (String vs StaticString)
            (AttributeValue::String(a), AttributeValue::String(b)) => a == b,
            (AttributeValue::String(a), AttributeValue::StaticString(b)) => a == *b,
            (AttributeValue::StaticString(a), AttributeValue::String(b)) => *a == b,
            (AttributeValue::StaticString(a), AttributeValue::StaticString(b)) => a == b,
            (AttributeValue::Int64(a), AttributeValue::Int64(b)) => a == b,
            (AttributeValue::UInt64(a), AttributeValue::UInt64(b)) => a == b,
            (AttributeValue::Boolean(a), AttributeValue::Boolean(b)) => a == b,
            (AttributeValue::Double(a), AttributeValue::Double(b)) => a == b,
            _ => false,
        }
    }
}

impl From<&str> for AttributeValue {
    fn from(s: &str) -> Self {
        AttributeValue::String(s.to_string())
    }
}

impl From<String> for AttributeValue {
    fn from(s: String) -> Self {
        AttributeValue::String(s)
    }
}

impl From<i64> for AttributeValue {
    fn from(i: i64) -> Self {
        AttributeValue::Int64(i)
    }
}

impl From<u64> for AttributeValue {
    fn from(u: u64) -> Self {
        AttributeValue::UInt64(u)
    }
}

impl From<bool> for AttributeValue {
    fn from(b: bool) -> Self {
        AttributeValue::Boolean(b)
    }
}

impl From<f64> for AttributeValue {
    fn from(f: f64) -> Self {
        AttributeValue::Double(f)
    }
}

impl AttributeValue {
    /// Helper to get the string value if the variant is String or StaticString.
    pub fn as_string(&self) -> Option<String> {
        match self {
            AttributeValue::String(s) => Some(s.clone()),
            AttributeValue::StaticString(s) => Some(s.to_string()),
            _ => None,
        }
    }
    /// Helper to get the i64 value if the variant is Int64.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            AttributeValue::Int64(i) => Some(*i),
            _ => None,
        }
    }
    // Add other as_ type helpers as needed
}

/// Represents a captured tracing span with its attributes.
#[derive(Debug, Clone)]
pub struct CapturedSpan {
    /// The unique ID of the span.
    pub id: span::Id,
    /// The name of the span.
    pub name: String,
    /// A map of attribute keys to their typed values.
    pub attributes: HashMap<String, AttributeValue>,
    /// The test ID associated with this span, if captured via `TestLayer::initialize`.
    pub test_id: Option<String>,
}

/// A `tracing::field::Visit` implementation to extract attribute key-value pairs from span events.
///
/// This visitor is used by the `TestLayer` to populate the `attributes` map
/// in a `CapturedSpan`. It converts various field types (str, debug, i64, u64, bool)
/// into String representations.
struct TestVisitor<'a>(&'a mut HashMap<String, AttributeValue>);

impl<'a> field::Visit for TestVisitor<'a> {
    fn record_str(&mut self, field: &field::Field, value: &str) {
        self.0
            .insert(field.name().to_string(), AttributeValue::from(value));
    }

    fn record_i64(&mut self, field: &field::Field, value: i64) {
        self.0
            .insert(field.name().to_string(), AttributeValue::from(value));
    }

    fn record_u64(&mut self, field: &field::Field, value: u64) {
        self.0
            .insert(field.name().to_string(), AttributeValue::from(value));
    }

    fn record_bool(&mut self, field: &field::Field, value: bool) {
        self.0
            .insert(field.name().to_string(), AttributeValue::from(value));
    }

    fn record_f64(&mut self, field: &field::Field, value: f64) {
        self.0
            .insert(field.name().to_string(), AttributeValue::from(value));
    }

    fn record_debug(&mut self, field: &field::Field, value: &dyn std::fmt::Debug) {
        self.0.insert(
            field.name().to_string(),
            AttributeValue::String(format!("{:?}", value)),
        );
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
    /// Returns a `TestLayerGuard` which must be kept in scope for the duration of the test.
    pub fn initialize() -> TestLayerGuard {
        INIT.call_once(|| {
            let layer = TestLayer;
            let subscriber = tracing_subscriber::registry().with(layer);
            tracing::subscriber::set_global_default(subscriber)
                .expect("Failed to set global default subscriber");
        });
        let test_id = Uuid::new_v4().to_string();
        SPAN_LOG.clear_by_test_id(&test_id);
        let _guard =
            tracing::span!(tracing::Level::INFO, "test_layer", test_id = test_id).entered();
        TestLayerGuard { test_id, _guard }
    }

    /// Retrieves all spans captured for the given `TestLayerGuard`.
    ///
    /// This method consumes the captured spans from the log, so subsequent
    /// calls for the same `test_id` within the same test run (without re-initializing)
    /// will return an empty vector unless new spans are emitted.
    pub fn capture(guard: &TestLayerGuard) -> Vec<CapturedSpan> {
        SPAN_LOG.take_by_test_id(&guard.test_id)
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
            span_map
                .get("test_id")
                .and_then(|v| v.as_string())
                .inspect(|id_str| {
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

    /// Tests that `TestLayer` can handle multiple initializations with different test IDs.
    #[tokio::test]
    async fn test_layer_multiple_inits() {
        let guard_a = TestLayer::initialize();
        info_span!("span_a").in_scope(|| {});

        let guard_b = TestLayer::initialize();
        info_span!("span_b").in_scope(|| {});

        let captured_a = TestLayer::capture(&guard_a);
        assert_eq!(captured_a.len(), 1);
        assert_eq!(captured_a[0].name, "span_a");

        let captured_b = TestLayer::capture(&guard_b);
        assert_eq!(captured_b.len(), 1);
        assert_eq!(captured_b[0].name, "span_b");
    }

    /// Tests that attributes added via `span.record()` are captured.
    #[tokio::test]
    async fn test_layer_on_record() {
        let guard = TestLayer::initialize();

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
            span.record("number_attr", 123_i64);
            span.record("bool_attr", true);
            span.record("debug_attr", field::debug(&vec![1, 2, 3]));
        });

        let captured = TestLayer::capture(&guard);
        assert_eq!(captured.len(), 1);
        let span = &captured[0];
        assert_eq!(span.name, "my_span");

        let expected_attributes: HashMap<String, AttributeValue> = [
            ("initial_attr", "initial_value".into()),
            ("dynamic_attr", "dynamic_value".into()),
            ("number_attr", 123_i64.into()),
            ("bool_attr", true.into()),
            ("debug_attr", "[1, 2, 3]".into()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
        assert_eq!(span.attributes, expected_attributes);
    }

    /// Tests that TestVisitor correctly converts various field types to strings.
    #[tokio::test]
    async fn test_visitor_type_conversions() {
        let guard = TestLayer::initialize();

        let _span = info_span!(
            "type_test_span",
            my_str = "hello",
            my_i64 = -123_i64,
            my_u64 = 456_u64,
            my_bool = true,
            my_f64 = 1.23_f64,
            my_debug = field::debug(&("test", 789))
        );

        let captured = TestLayer::capture(&guard);
        assert_eq!(captured.len(), 1);
        let span = &captured[0];

        let expected_attributes: HashMap<String, AttributeValue> = [
            ("my_str", "hello".into()),
            ("my_i64", (-123_i64).into()),
            ("my_u64", 456_u64.into()),
            ("my_bool", true.into()),
            ("my_f64", 1.23_f64.into()),
            ("my_debug", "(\"test\", 789)".into()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
        assert_eq!(span.attributes, expected_attributes);
    }

    #[test]
    fn test_attribute_value_from() {
        assert_eq!(
            AttributeValue::from("hello"),
            AttributeValue::String("hello".to_string())
        );
        assert_eq!(
            AttributeValue::from("hello".to_string()),
            AttributeValue::String("hello".to_string())
        );
        assert_eq!(AttributeValue::from(123_i64), AttributeValue::Int64(123));
        assert_eq!(AttributeValue::from(456_u64), AttributeValue::UInt64(456));
        assert_eq!(AttributeValue::from(true), AttributeValue::Boolean(true));
        assert_eq!(AttributeValue::from(1.23_f64), AttributeValue::Double(1.23));
    }

    #[test]
    fn test_attribute_value_as_string() {
        assert_eq!(
            AttributeValue::from("hello").as_string(),
            Some("hello".to_string())
        );
        assert_eq!(
            AttributeValue::StaticString("hello").as_string(),
            Some("hello".to_string())
        );
        assert_eq!(AttributeValue::from(123_i64).as_string(), None);
    }

    #[test]
    fn test_attribute_value_as_i64() {
        assert_eq!(AttributeValue::from(123_i64).as_i64(), Some(123));
        assert_eq!(AttributeValue::from("hello").as_i64(), None);
    }

    #[test]
    fn test_attribute_value_partial_eq() {
        let string_foo = AttributeValue::String("foo".to_string());
        let static_foo = AttributeValue::StaticString("foo");
        let static_foo_2 = AttributeValue::StaticString("foo");
        let string_bar = AttributeValue::String("bar".to_string());
        let static_bar = AttributeValue::StaticString("bar");

        assert_eq!(
            string_foo, static_foo,
            "String should equal StaticString with same value"
        );
        assert_eq!(
            static_foo, string_foo,
            "StaticString should equal String with same value"
        );
        assert_eq!(
            static_foo, static_foo_2,
            "StaticString should equal StaticString with same value"
        );
        assert_ne!(
            string_foo, string_bar,
            "String should not equal String with different value"
        );
        assert_ne!(
            static_foo, static_bar,
            "StaticString should not equal StaticString with different value"
        );
        assert_ne!(
            string_foo, static_bar,
            "String should not equal StaticString with different value"
        );
        assert_ne!(
            AttributeValue::Int64(123),
            AttributeValue::UInt64(123),
            "Int64 should not equal UInt64 even with same value"
        );
    }
}
