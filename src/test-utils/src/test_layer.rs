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

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use tracing::{
    Event, Metadata, Subscriber, field,
    level_filters::LevelFilter,
    span,
    subscriber::{DefaultGuard, Interest},
};
use tracing_subscriber::{self, Layer, layer::Context, prelude::*};

/// RAII guard for the [`TestLayer`].
///
/// Keeps the per-test subscriber active on the current thread while in scope,
/// and holds the `CapturedSpanLog` for this test.
pub struct TestLayerGuard {
    _subscriber_guard: DefaultGuard,
    log: Arc<CapturedSpanLog>,
}

impl TestLayerGuard {
    /// Returns a no-op span for backwards compatibility with callers that
    /// have not yet removed `.instrument(guard.span())`.
    pub fn span(&self) -> tracing::Span {
        tracing::Span::none()
    }
}

/// Represents the value of a captured tracing attribute.
#[derive(Debug, Clone, PartialEq)]
pub enum AttributeValue {
    String(Cow<'static, str>),
    Int64(i64),
    UInt64(u64),
    Boolean(bool),
    Double(f64),
}

impl From<&'static str> for AttributeValue {
    fn from(s: &'static str) -> Self {
        AttributeValue::String(Cow::Borrowed(s))
    }
}

impl From<String> for AttributeValue {
    fn from(s: String) -> Self {
        AttributeValue::String(Cow::Owned(s))
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
    /// Helper to get the string value if the variant is String.
    pub fn as_string(&self) -> Option<String> {
        match self {
            AttributeValue::String(s) => Some(s.to_string()),
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

    /// Helper to get the bool value if the variant is Boolean.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            AttributeValue::Boolean(b) => Some(*b),
            _ => None,
        }
    }
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
    /// The count of events in the span.
    pub events: usize,
}

/// A `tracing::field::Visit` implementation to extract attribute key-value pairs from span events.
///
/// This visitor is used by the `TestLayer` to populate the `attributes` map
/// in a `CapturedSpan`. It converts various field types (str, debug, i64, u64, bool)
/// into String representations.
struct TestVisitor<'a>(&'a mut HashMap<String, AttributeValue>);

impl<'a> field::Visit for TestVisitor<'a> {
    fn record_str(&mut self, field: &field::Field, value: &str) {
        self.0.insert(
            field.name().to_string(),
            AttributeValue::String(Cow::Owned(value.to_string())),
        );
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
            AttributeValue::String(Cow::Owned(format!("{:?}", value))),
        );
    }
}

/// A thread-safe log to store `CapturedSpan` instances for a single test guard.
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

    /// Retrieves and removes all captured spans from the log.
    fn take_all(&self) -> Vec<CapturedSpan> {
        std::mem::take(&mut *self.spans.lock().unwrap())
    }

    /// Updates an existing `CapturedSpan` in the log by its `span::Id`.
    fn update_span<F: FnOnce(&mut CapturedSpan)>(&self, id: &span::Id, f: F) {
        let mut spans = self.spans.lock().unwrap();
        if let Some(captured_span) = spans.iter_mut().rfind(|s| s.id == *id) {
            f(captured_span);
        }
    }
}

/// A lightweight no-op subscriber registered once in `tracing_core`'s
/// dispatcher list (without setting a global default subscriber) so that
/// `STATIC_MAX_LEVEL` remains `LevelFilter::TRACE` and callsites never cache
/// `Interest::never()` when another test installs a lower-level subscriber.
struct MaxLevelTraceSubscriber;

impl Subscriber for MaxLevelTraceSubscriber {
    fn register_callsite(&self, _metadata: &'static Metadata<'static>) -> Interest {
        Interest::sometimes()
    }

    fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
        false
    }

    fn max_level_hint(&self) -> Option<LevelFilter> {
        Some(LevelFilter::TRACE)
    }

    fn new_span(&self, _span: &span::Attributes<'_>) -> span::Id {
        span::Id::from_u64(1)
    }

    fn record(&self, _span: &span::Id, _values: &span::Record<'_>) {}

    fn record_follows_from(&self, _span: &span::Id, _follows: &span::Id) {}

    fn event(&self, _event: &Event<'_>) {}

    fn enter(&self, _span: &span::Id) {}

    fn exit(&self, _span: &span::Id) {}
}

/// A tracing layer for capturing and inspecting spans within tests.
///
/// Each call to [`TestLayer::initialize()`] installs an isolated thread-local
/// `tracing_subscriber::Registry` and span buffer via
/// `tracing::dispatcher::set_default`, avoiding any global `Registry` or global
/// span log contention across parallel tests.
///
/// # Usage
///
/// 1.  **Initialize:** In each test function, call `TestLayer::initialize()`.
///     This returns an RAII [`TestLayerGuard`]. The layer will capture spans on
///     the current thread while this guard is in scope.
/// 2.  **Execute Code:** Run the code you want to test, which emits tracing spans.
/// 3.  **Capture Spans:** Use `TestLayer::capture(&guard)` to retrieve all spans
///     captured during the test.
/// 4.  **Assert:** Make assertions on the captured [`CapturedSpan`] data.
///
/// # Example
///
/// ```rust
/// use google_cloud_test_utils::test_layer::*;
/// use tracing::info_span;
///
/// #[tokio::test]
/// async fn my_tracing_test() {
///     let guard = TestLayer::initialize();
///
///     // Code under test that emits spans
///     info_span!("my_operation", foo = "bar").in_scope(|| {
///         tracing::info!("Doing something important");
///     });
///
///     let captured = TestLayer::capture(&guard);
///     assert_eq!(captured.len(), 1);
///     let span = &captured[0];
///     assert_eq!(span.name, "my_operation");
///     assert_eq!(span.attributes.get("foo"), Some(&"bar".into()));
/// }
/// ```
pub struct TestLayer {
    log: Arc<CapturedSpanLog>,
}

impl TestLayer {
    /// Ensures `tracing_core`'s global `STATIC_MAX_LEVEL` is permanently kept at
    /// `LevelFilter::TRACE` and callsites never cache `Interest::never()`,
    /// without installing a global default subscriber.
    pub fn ensure_max_level_trace() {
        static MAX_LEVEL_DISPATCH: OnceLock<tracing::Dispatch> = OnceLock::new();
        MAX_LEVEL_DISPATCH.get_or_init(|| tracing::Dispatch::new(MaxLevelTraceSubscriber));
    }

    /// Initializes an isolated [`TestLayer`] for the current thread's test scope.
    ///
    /// Returns a [`TestLayerGuard`] which must be kept in scope for the duration of the test.
    pub fn initialize() -> TestLayerGuard {
        Self::ensure_max_level_trace();
        let log = Arc::new(CapturedSpanLog::new());
        let layer = TestLayer {
            log: Arc::clone(&log),
        };
        let subscriber = tracing_subscriber::registry().with(layer);
        let subscriber_guard = tracing::subscriber::set_default(subscriber);
        TestLayerGuard {
            _subscriber_guard: subscriber_guard,
            log,
        }
    }

    /// Retrieves all spans captured for the given `TestLayerGuard`.
    ///
    /// This method consumes the captured spans from the guard's log, so subsequent
    /// calls for the same guard will return an empty vector unless new spans are emitted.
    pub fn capture(guard: &TestLayerGuard) -> Vec<CapturedSpan> {
        guard.log.take_all()
    }
}

impl<S> Layer<S> for TestLayer
where
    S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    /// Called when a new span is created.
    ///
    /// Captures the span's attributes in this layer's private log.
    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, S>) {
        let mut span_map = HashMap::new();
        let mut visitor = TestVisitor(&mut span_map);
        attrs.record(&mut visitor);

        let span_ref = ctx.span(id).expect("Span not found in registry");
        let name = span_ref.name().to_string();

        let captured_span = CapturedSpan {
            id: id.clone(),
            name,
            attributes: span_map,
            events: 0_usize,
        };
        self.log.push(captured_span);
    }

    /// Updates the attributes of the existing captured span.
    fn on_record(&self, id: &span::Id, values: &span::Record<'_>, _ctx: Context<'_, S>) {
        self.log.update_span(id, |captured_span| {
            let mut visitor = TestVisitor(&mut captured_span.attributes);
            values.record(&mut visitor);
        });
    }

    /// Called before `on_event`, to determine if `on_event` should be called.
    fn event_enabled(&self, _event: &tracing::Event<'_>, _ctx: Context<'_, S>) -> bool {
        true
    }

    /// Notifies this layer that an event has occurred.
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let Some(id) = event.parent() else {
            return;
        };
        self.log.update_span(id, |captured_span| {
            captured_span.events += 1;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use tracing::info_span;

    /// Helper to create a dummy CapturedSpan for testing the log.
    fn dummy_span(id: u64, name: &str) -> CapturedSpan {
        CapturedSpan {
            id: tracing::Id::from_u64(id),
            name: name.to_string(),
            attributes: HashMap::new(),
            events: 0_usize,
        }
    }

    /// Tests the basic push and take_all functionality of `CapturedSpanLog`.
    #[test]
    fn test_captured_span_log_push_take() {
        let log = CapturedSpanLog::new();
        log.push(dummy_span(1, "span1"));
        log.push(dummy_span(2, "span2"));
        log.push(dummy_span(3, "span3"));

        let spans = log.take_all();
        assert_eq!(spans.len(), 3);
        assert_eq!(spans[0].name, "span1");
        assert_eq!(spans[1].name, "span2");
        assert_eq!(spans[2].name, "span3");

        let remaining_spans = log.spans.lock().unwrap();
        assert!(remaining_spans.is_empty(), "{remaining_spans:?}");
    }

    /// Tests taking from an empty `CapturedSpanLog`.
    #[test]
    fn test_captured_span_log_take_empty() {
        let log = CapturedSpanLog::new();
        let taken = log.take_all();
        assert!(taken.is_empty(), "{taken:?}");
    }

    /// Tests updating an existing span in `CapturedSpanLog`.
    #[test]
    fn test_captured_span_log_update_span() {
        let log = CapturedSpanLog::new();
        log.push(dummy_span(1, "span1"));
        log.push(dummy_span(2, "span2"));

        log.update_span(&tracing::Id::from_u64(2), |span| {
            span.events += 5;
            span.attributes.insert("key".to_string(), "val".into());
        });

        let spans = log.take_all();
        assert_eq!(spans[0].events, 0);
        assert_eq!(spans[1].events, 5);
        assert_eq!(spans[1].attributes.get("key"), Some(&"val".into()));
    }

    /// Tests that `TestLayer` can handle multiple nested initializations on the same thread.
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

    #[tokio::test]
    async fn test_layer_on_event() {
        let guard = TestLayer::initialize();

        let span = info_span!("my_span");
        ::tracing::event!(parent: &span, tracing::Level::INFO, seq = 1);
        ::tracing::event!(parent: &span, tracing::Level::INFO, seq = 2);
        ::tracing::event!(parent: &span, tracing::Level::INFO, seq = 3);

        let captured = TestLayer::capture(&guard);
        let got = captured
            .iter()
            .find(|s| s.name == "my_span")
            .unwrap_or_else(|| panic!("missing `my_span` in captured spans: {captured:?}"));
        assert_eq!(got.events, 3);
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
            AttributeValue::String(Cow::Borrowed("hello"))
        );
        assert_eq!(
            AttributeValue::from("hello".to_string()),
            AttributeValue::String(Cow::Owned("hello".to_string()))
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
            AttributeValue::String(Cow::Borrowed("hello")).as_string(),
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
    fn test_layer_isolated_across_threads_and_registries() {
        let guard = TestLayer::initialize();
        info_span!("main_span").in_scope(|| {});

        // Spans on an unrelated thread without a TestLayerGuard are not captured
        // and cannot corrupt this guard's Registry.
        std::thread::spawn(|| {
            let _unrelated = info_span!("unrelated_thread_span").entered();
            let sub =
                tracing_subscriber::registry().with(tracing_subscriber::layer::Identity::new());
            let _local = tracing::subscriber::set_default(sub);
            let parent = info_span!("local_parent");
            let _entered = parent.enter();
            let _child = info_span!("local_child");
        })
        .join()
        .expect("worker thread should not panic");

        let captured = TestLayer::capture(&guard);
        assert_eq!(captured.len(), 1, "{captured:?}");
        assert_eq!(captured[0].name, "main_span");
    }
}
