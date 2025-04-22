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

import 'dart:convert';
import 'dart:typed_data';

import 'package:google_cloud_protobuf/protobuf.dart';
import 'package:test/test.dart';

void main() {
  test('BoolValue', () {
    var expected = BoolValue(value: true);
    var actual = BoolValue.fromJson(encodeDecode(expected.toJson()));
    expect(actual.value, expected.value);
  });

  test('FloatValue', () {
    var expected = FloatValue(value: 0.5);
    var actual = FloatValue.fromJson(encodeDecode(expected.toJson()));
    expect(actual.value, expected.value);

    expect(FloatValue.fromJson(1).value, 1.0);
  });

  test('DoubleValue', () {
    var expected = DoubleValue(value: 0.5);
    var actual = DoubleValue.fromJson(encodeDecode(expected.toJson()));
    expect(actual.value, expected.value);

    expect(DoubleValue.fromJson(1).value, 1.0);
  });

  group('NaN and Infinity', () {
    test('FloatValue', () {
      expect(FloatValue.fromJson('NaN').value, isNaN);
      expect(FloatValue.fromJson('Infinity').value, double.infinity);
      expect(FloatValue.fromJson('-Infinity').value, double.negativeInfinity);

      // don't allow arbitrary strings for floats
      expect(() => FloatValue.fromJson('1.0').value, throwsFormatException);

      expect(FloatValue(value: double.nan).toJson(), 'NaN');
      expect(FloatValue(value: double.infinity).toJson(), 'Infinity');
      expect(FloatValue(value: double.negativeInfinity).toJson(), '-Infinity');
    });

    test('DoubleValue', () {
      expect(DoubleValue.fromJson('NaN').value, isNaN);
      expect(DoubleValue.fromJson('Infinity').value, double.infinity);
      expect(DoubleValue.fromJson('-Infinity').value, double.negativeInfinity);

      // don't allow arbitrary strings for doubles
      expect(() => DoubleValue.fromJson('1.0').value, throwsFormatException);

      expect(DoubleValue(value: double.nan).toJson(), 'NaN');
      expect(DoubleValue(value: double.infinity).toJson(), 'Infinity');
      expect(DoubleValue(value: double.negativeInfinity).toJson(), '-Infinity');
    });
  });

  test('Int64Value', () {
    var expected = Int64Value(value: 5_000);
    expect(expected.toJson(), isA<String>());

    var actual = Int64Value.fromJson(encodeDecode(expected.toJson()));
    expect(actual.value, expected.value);

    expect(Int64Value.fromJson('123').value, 123);
    expect(Int64Value.fromJson(123).value, 123);
  });

  test('Uint64Value', () {
    var expected = Uint64Value(value: 5_000);
    expect(expected.toJson(), isA<String>());

    var actual = Uint64Value.fromJson(encodeDecode(expected.toJson()));
    expect(actual.value, expected.value);

    expect(Uint64Value.fromJson('123').value, 123);
    expect(Uint64Value.fromJson(123).value, 123);
  });

  test('Int32Value', () {
    var expected = Int32Value(value: 500);
    var actual = Int32Value.fromJson(encodeDecode(expected.toJson()));
    expect(actual.value, expected.value);
  });

  test('StringValue', () {
    var expected = StringValue(value: 'foobar');
    var actual = StringValue.fromJson(encodeDecode(expected.toJson()));
    expect(actual.value, expected.value);
  });

  test('BytesValue', () {
    var expected = BytesValue(value: Uint8List.fromList([1, 2, 3]));
    var actual = BytesValue.fromJson(encodeDecode(expected.toJson()));
    var encoded = actual.value!.map((item) => '$item').join(',');
    expect(encoded, '1,2,3');
  });

  test('Uint32Value', () {
    var expected = Uint32Value(value: 500);
    var actual = Uint32Value.fromJson(encodeDecode(expected.toJson()));
    expect(actual.value, expected.value);
  });
}

/// Encode the given object to a JSON string, then return the results of JSON
/// decoding that string.
dynamic encodeDecode(Object json) => jsonDecode(jsonEncode(json));
