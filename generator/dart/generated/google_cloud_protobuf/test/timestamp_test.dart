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
  void testCase(int seconds, int nanos, String encoding) {
    test('test case $encoding', () {
      // test Timestamp.encode()
      final timestamp = Timestamp(seconds: seconds, nanos: nanos);
      expect(timestamp.toJson(), encoding);

      // test Timestamp.decode()
      final copy = Timestamp.fromJson(encoding);
      expect(copy.seconds, seconds);
      expect(copy.nanos, nanos);
    });
  }

  // encode and decode tests
  testCase(0, 0, '1970-01-01T00:00:00Z');
  test('min seconds', () {
    final timestamp = TimestampHelper.decode('0001-01-01T00:00:00Z');
    expect(timestamp.seconds, -62135596800);
    expect(timestamp.nanos, 0);
  });
  test('max seconds', () {
    final timestamp = TimestampHelper.decode('9999-12-31T23:59:59Z');
    expect(timestamp.seconds, 253402300799);
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

  roundTrip('0001-01-01T00:00:00Z');
  roundTrip('9999-12-31T23:59:59.999999999Z');
  roundTrip('2024-10-19T12:34:56.789Z');
  roundTrip('2024-10-19T12:34:56.780Z');
  roundTrip('2024-10-19T12:34:56.780123Z');
  roundTrip('2024-10-19T12:34:56.780120Z');
  roundTrip('2024-10-19T12:34:56.789123456Z');
  roundTrip('2024-10-19T12:34:56.789123450Z');

  // bad format tests
  test('bad format', () {
    expect(() => Timestamp.fromJson('2024-10-19T12:'), throwsFormatException);
  });
  test('seconds below range', () {
    expect(
      () => Timestamp(seconds: -62135596800 - 1, nanos: 0),
      throwsArgumentError,
    );
  });
  test('seconds above range', () {
    expect(
      () => Timestamp(seconds: 253402300799 + 1, nanos: 0),
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
