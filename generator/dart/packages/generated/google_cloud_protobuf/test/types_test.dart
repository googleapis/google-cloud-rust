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
  group('FieldMask', () {
    test('encode empty', () {
      final fieldMask = FieldMask(paths: []);
      expect(fieldMask.encode(), '');
    });

    test('encode single', () {
      final fieldMask = FieldMask(paths: ['one']);
      expect(fieldMask.encode(), 'one');
    });

    test('encode multiple', () {
      final fieldMask = FieldMask(paths: ['one', 'two']);
      expect(fieldMask.encode(), 'one,two');
    });

    test('decode empty', () {
      final fieldMask = FieldMaskExtension.decode('');
      final actual = fieldMask.paths!.join('|');
      expect(actual, '');
    });

    test('decode single', () {
      final fieldMask = FieldMaskExtension.decode('one');
      final actual = fieldMask.paths!.join('|');
      expect(actual, 'one');
    });

    test('decode multiple', () {
      final fieldMask = FieldMaskExtension.decode('one,two');
      final actual = fieldMask.paths!.join('|');
      expect(actual, 'one|two');
    });
  });

  group('Duration', () {
    final testCases = [
      (Duration(seconds: 0, nanos: 0), '0s'),
      (Duration(seconds: 1, nanos: 0), '1s'),
      (Duration(seconds: 0, nanos: 1), '0.000000001s'),
      (Duration(seconds: 1, nanos: 1), '1.000000001s'),
      (Duration(seconds: 60, nanos: 1_000_000), '60.001s'),
    ];

    // encode tests
    for (final testCase in testCases) {
      test('encode ${testCase.$2}', () {
        expect(testCase.$1.encode(), testCase.$2);
      });
    }

    // decode tests
    for (final testCase in testCases) {
      test('decode ${testCase.$2}', () {
        final expected = testCase.$1;
        final actual = DurationExtension.decode(testCase.$2);

        expect(actual.seconds, expected.seconds);
        expect(actual.nanos, expected.nanos);
      });
    }

    // bad format tests
    test('bad number', () {
      expect(() => DurationExtension.decode('20u10s'), throwsFormatException);
    });

    test('bad format', () {
      expect(() => DurationExtension.decode('2.00001'), throwsFormatException);
    });
  });
}
