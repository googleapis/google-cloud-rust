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

/// Tests to validate the generated JSON encoding code using types from
/// package:google_cloud_language and package:google_cloud_rpc.
library;

import 'dart:convert';

import 'package:google_cloud_language_v2/language.dart';
import 'package:google_cloud_protobuf/protobuf.dart';
import 'package:google_cloud_rpc/rpc.dart';
import 'package:test/test.dart';

void main() {
  // simple fields
  test('LocalizedMessage', () {
    final expected = LocalizedMessage(locale: 'en-US', message: 'Lorem ipsum.');
    final actual = LocalizedMessage.fromJson(encodeDecode(expected.toJson()));

    expect(actual.locale, expected.locale);
    expect(actual.message, expected.message);
  });

  // simple fields
  test('HttpHeader', () {
    final expected =
        HttpHeader(key: 'Accept-Language', value: 'en-US,en;q=0.5');
    final actual = HttpHeader.fromJson(encodeDecode(expected.toJson()));

    expect(actual.key, expected.key);
    expect(actual.value, expected.value);
  });

  // enum
  test('Code', () {
    final expected = Code.unauthenticated;
    final actual = Code.fromJson(encodeDecode(expected.toJson()));

    expect(actual, expected);
  });

  // fields and primitive maps
  test('ErrorInfo', () {
    var expected = ErrorInfo(
      reason: 'LOREM_IPSUM',
      domain: 'cheese.com',
    );
    var actual = ErrorInfo.fromJson(encodeDecode(expected.toJson()));

    expect(actual.reason, expected.reason);
    expect(actual.domain, expected.domain);
    expect(actual.metadata, isNull);

    expected = ErrorInfo(
      reason: 'LOREM_IPSUM',
      domain: 'cheese.com',
      metadata: {'instanceLimitPerRequest': '100'},
    );
    actual = ErrorInfo.fromJson(encodeDecode(expected.toJson()));

    expect(actual.reason, expected.reason);
    expect(actual.domain, expected.domain);
    expect(actual.metadata, hasLength(1));
    expect(actual.metadata!['instanceLimitPerRequest'], '100');
  });

  // fields using custom encoding
  test('RetryInfo', () {
    final expected = RetryInfo(retryDelay: Duration(seconds: 100, nanos: 1000));
    final actual = RetryInfo.fromJson(encodeDecode(expected.toJson()));

    expect(actual.retryDelay, isNotNull);
    final retry = actual.retryDelay!;
    expect(retry.seconds, 100);
    expect(retry.nanos, 1000);
  });

  // primitive lists
  test('DebugInfo', () {
    final expected = DebugInfo(
        stackEntries: ['one', 'two', 'three'], detail: 'Lorem ipsum.');
    final actual = DebugInfo.fromJson(encodeDecode(expected.toJson()));

    expect(actual.stackEntries, hasLength(3));
    expect(actual.stackEntries![0], 'one');
    expect(actual.stackEntries![1], 'two');
    expect(actual.stackEntries![2], 'three');
    expect(actual.detail, expected.detail);
  });

  // message lists
  test('QuotaFailure', () {
    final expected = QuotaFailure(
      violations: [
        QuotaFailure_Violation(
          subject: 'project:foo',
          description: 'Limit exceeded',
        ),
        QuotaFailure_Violation(
          subject: 'clientip:1.2.3.4',
          description: 'Service disabled',
        ),
      ],
    );
    final actual = QuotaFailure.fromJson(encodeDecode(expected.toJson()));

    expect(actual.violations, hasLength(2));
    expect(
      jsonEncode(actual.violations![0].toJson()),
      jsonEncode(expected.violations![0].toJson()),
    );
    expect(
      jsonEncode(actual.violations![1].toJson()),
      jsonEncode(expected.violations![1].toJson()),
    );
  });

  // Status
  test('Status', () {
    var expected = Status(code: 5, message: 'Not found');
    var actual = Status.fromJson(encodeDecode(expected.toJson()));

    expect(actual.code, expected.code);
    expect(actual.message, expected.message);

    // For now, we're not testing round-tripping the 'Any' type.
    expected = Status(code: 5, message: 'Not found', details: []);
    actual = Status.fromJson(encodeDecode(expected.toJson()));

    expect(actual.code, expected.code);
    expect(actual.message, expected.message);
    expect(actual.details, isEmpty);
  });

  // doubles
  test('Sentiment', () {
    // ints
    var expected = Sentiment(magnitude: 123, score: 0);
    var actual = Sentiment.fromJson(encodeDecode(expected.toJson()));
    expect(actual.magnitude, 123.0);
    expect(actual.score, 0.0);

    // doubles
    expected = Sentiment(magnitude: 1.5, score: 0.5);
    actual = Sentiment.fromJson(encodeDecode(expected.toJson()));
    expect(actual.magnitude, 1.5);
    expect(actual.score, 0.5);

    // Make sure we handle ints in JSON as doubles.
    actual = Sentiment.fromJson(jsonDecode('{"magnitude":123,"score":0}'));
    expect(actual.magnitude, 123);
    expect(actual.score, 0);
  });
}

/// Encode the given object to a JSON string, then return the results of JSON
/// decoding that string.
dynamic encodeDecode(Object json) => jsonDecode(jsonEncode(json));
