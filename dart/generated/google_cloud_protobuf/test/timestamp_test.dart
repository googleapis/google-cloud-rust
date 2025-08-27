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
  test('test zero seconds, zero nanos', () {
    const encoding = '1970-01-01T00:00:00Z';
    // test Timestamp.encode()
    final timestamp = Timestamp(seconds: 0, nanos: 0);
    expect(timestamp.toJson(), encoding);

    // test Timestamp.decode()
    final copy = Timestamp.fromJson(encoding);
    expect(copy.seconds, 0);
    expect(copy.nanos, 0);
  });

  test('min seconds', () {
    final timestamp = Timestamp.fromJson('0001-01-01T00:00:00Z');
    expect(timestamp.seconds, TimestampExtension.minSeconds);
    expect(timestamp.nanos, 0);
  });

  test('max seconds', () {
    final timestamp = Timestamp.fromJson('9999-12-31T23:59:59Z');
    expect(timestamp.seconds, TimestampExtension.maxSeconds);
    expect(timestamp.nanos, 0);
  });

  // Verify timestamps can roundtrip from String -> Timestamp -> String.
  void roundTrip(String rfc3339) {
    test('round trip $rfc3339', () {
      final timestamp = Timestamp.fromJson(rfc3339);
      final encoded = timestamp.toJson();
      expect(encoded, rfc3339);
    });
  }

  roundTrip('0001-01-01T00:00:00.123456789Z');
  roundTrip('0001-01-01T00:00:00.123456Z');
  roundTrip('0001-01-01T00:00:00.123Z');
  roundTrip('0001-01-01T00:00:00Z');
  roundTrip('1960-01-01T00:00:00.123456789Z');
  roundTrip('1960-01-01T00:00:00.123456Z');
  roundTrip('1960-01-01T00:00:00.123Z');
  roundTrip('1960-01-01T00:00:00Z');
  roundTrip('1970-01-01T00:00:00.123456789Z');
  roundTrip('1970-01-01T00:00:00.123456Z');
  roundTrip('1970-01-01T00:00:00.123Z');
  roundTrip('1970-01-01T00:00:00Z');
  roundTrip('9999-12-31T23:59:59.999999999Z');
  roundTrip('9999-12-31T23:59:59.123456789Z');
  roundTrip('9999-12-31T23:59:59.123456Z');
  roundTrip('9999-12-31T23:59:59.123Z');
  roundTrip('2024-10-19T12:34:56Z');
  roundTrip('2024-10-19T12:34:56.789Z');
  roundTrip('2024-10-19T12:34:56.789123456Z');

  void validate(String rfc3339, int seconds, int nanos) {
    test('validate $rfc3339', () {
      final timestamp = Timestamp.fromJson(rfc3339);
      expect(timestamp.seconds, seconds);
      expect(timestamp.nanos, nanos);
      expect(timestamp.toJson(), rfc3339);
    });
  }

  // Validate that a given RFC3339 gives the expected seconds and nanos.
  validate(
    '0001-01-01T00:00:00.123456789Z',
    TimestampExtension.minSeconds,
    123_456_789,
  );
  validate(
    '0001-01-01T00:00:00.123456Z',
    TimestampExtension.minSeconds,
    123_456_000,
  );
  validate(
    '0001-01-01T00:00:00.123Z',
    TimestampExtension.minSeconds,
    123_000_000,
  );
  validate('0001-01-01T00:00:00Z', TimestampExtension.minSeconds, 0);
  validate('1970-01-01T00:00:00.123456789Z', 0, 123_456_789);
  validate('1970-01-01T00:00:00.123456Z', 0, 123_456_000);
  validate('1970-01-01T00:00:00.123Z', 0, 123_000_000);
  validate('1970-01-01T00:00:00Z', 0, 0);
  validate(
    '9999-12-31T23:59:59.123456789Z',
    TimestampExtension.maxSeconds,
    123_456_789,
  );
  validate(
    '9999-12-31T23:59:59.123456Z',
    TimestampExtension.maxSeconds,
    123_456_000,
  );
  validate(
    '9999-12-31T23:59:59.123Z',
    TimestampExtension.maxSeconds,
    123_000_000,
  );
  validate('9999-12-31T23:59:59Z', TimestampExtension.maxSeconds, 0);

  // bad format tests
  test('bad format', () {
    expect(() => Timestamp.fromJson('2024-10-19T12:'), throwsFormatException);
  });

  test('seconds below range', () {
    expect(
      () => Timestamp(seconds: TimestampExtension.minSeconds - 1, nanos: 0),
      throwsArgumentError,
    );
  });

  test('seconds above range', () {
    expect(
      () => Timestamp(seconds: TimestampExtension.maxSeconds + 1, nanos: 0),
      throwsArgumentError,
    );
  });

  test('nanos below range', () {
    expect(() => Timestamp(seconds: 0, nanos: -1), throwsArgumentError);
  });

  test('nanos above range', () {
    expect(
      () => Timestamp(seconds: 0, nanos: 1_000_000_000),
      throwsArgumentError,
    );
  });
}
