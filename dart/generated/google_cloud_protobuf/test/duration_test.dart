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

import 'package:google_cloud_protobuf/protobuf.dart';
import 'package:test/test.dart';

void main() {
  const secondsInDay = 24 * 60 * 60;
  const secondsInYear = 365 * secondsInDay + secondsInDay ~/ 4;

  void testCase(int seconds, int nanos, String encoding) {
    test('test case $encoding', () {
      // test DurationExtension.encode()
      final duration = Duration(seconds: seconds, nanos: nanos);
      expect(duration.toJson(), encoding);

      // test DurationExtension.decode()
      final copy = Duration.fromJson(encoding);
      expect(copy.seconds, seconds);
      expect(copy.nanos, nanos);
    });
  }

  // encode and decode tests
  testCase(0, 0, '0s');
  testCase(1, 0, '1s');
  testCase(0, 1, '0.000000001s');
  testCase(1, 1, '1.000000001s');
  testCase(60, 1_000_000, '60.001s');
  testCase(10_000 * secondsInYear, 0, '315576000000s');
  testCase(10_000 * secondsInYear, 999_999_999, '315576000000.999999999s');
  testCase(-10_000 * secondsInYear, -999_999_999, '-315576000000.999999999s');
  testCase(0, 999_999_999, '0.999999999s');
  testCase(0, -999_999_999, '-0.999999999s');

  // Verify durations can roundtrip from String -> Duration -> String.
  void roundTrip(String name, String encoding) {
    test('round trip $name ($encoding)', () {
      final duration = Duration.fromJson(encoding);
      expect(duration.toJson(), encoding);
    });
  }

  roundTrip('zero', '0s');
  roundTrip('2ns', '0.000000002s');
  roundTrip('200ms', '0.2s');
  roundTrip('round positive seconds', '12s');
  roundTrip('positive seconds and nanos', '12.000000123s');
  roundTrip('positive seconds and micros', '12.000123s');
  roundTrip('positive seconds and millis', '12.123s');
  roundTrip('positive seconds and full nanos', '12.123456789s');
  roundTrip('round negative seconds', '-12s');
  roundTrip('negative seconds and nanos', '-12.000000123s');
  roundTrip('negative seconds and micros', '-12.000123s');
  roundTrip('negative seconds and millis', '-12.123s');
  roundTrip('negative seconds and full nanos', '-12.123456789s');
  roundTrip('range edge start', '-315576000000.999999999s');
  roundTrip('range edge end', '315576000000.999999999s');

  // bad format tests
  test('bad number', () {
    expect(() => Duration.fromJson('20u10s'), throwsFormatException);
  });

  test('bad format', () {
    expect(() => Duration.fromJson('2.00001'), throwsFormatException);
  });

  test('bad format - too many periods', () {
    expect(() => Duration.fromJson('1.2.3.4s'), throwsFormatException);
  });

  test('too many positive nanoseconds', () {
    expect(
        () => Duration(seconds: 0, nanos: 1_000_000_000), throwsArgumentError);
  });

  test('too many negative nanoseconds', () {
    expect(
        () => Duration(seconds: 0, nanos: -1_000_000_000), throwsArgumentError);
  });

  test('mismatched sign case 1', () {
    expect(() => Duration(seconds: 1, nanos: -1), throwsArgumentError);
  });

  test('mismatched sign case 2', () {
    expect(() => Duration(seconds: -1, nanos: 1), throwsArgumentError);
  });
}
