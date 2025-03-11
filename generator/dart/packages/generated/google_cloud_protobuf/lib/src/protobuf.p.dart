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

part of '../protobuf.dart';

extension FieldMaskExtension on FieldMask {
  /// Encode the field mask as a single comma-separated string.
  String encode() {
    return paths?.join(',') ?? '';
  }

  /// Decode the field mask from a single comma-separated string.
  static FieldMask decode(String format) {
    return FieldMask(paths: format.split(','));
  }
}

extension DurationExtension on Duration {
  // Called from the Duration constructor to validate the construction
  // parameters.
  void _validate() {
    // For durations of one second or more, a non-zero value for the `nanos`
    // field must be of the same sign as the `seconds` field.
    if ((seconds! < 0 && nanos! > 0) || (seconds! > 0 && nanos! < 0)) {
      throw ArgumentError('seconds and nanos must have the same sign');
    }

    // Nanos must be from -999,999,999 to +999,999,999 inclusive.
    if (nanos! < -999_999_999 || nanos! > 999_999_999) {
      throw ArgumentError('nanos out of range');
    }
  }

  /// Encode into a decimal representation of the seconds and nanos, suffixed
  /// with 's'.
  ///
  /// E.g., 3 seconds with 0 nanoseconds would be '3s'; 3 seconds with 70
  /// nanosecond would be '3.00000007s'.
  String encode() {
    if (nanos == 0) {
      return '${seconds}s';
    } else {
      final rhs = nanos!.abs().toString().padLeft(9, '0');

      var duration =
          seconds == 0 ? '${nanos! < 0 ? '-' : ''}0.$rhs' : '${seconds}.$rhs';
      while (duration.endsWith('0')) {
        duration = duration.substring(0, duration.length - 1);
      }

      return '${duration}s';
    }
  }

  /// Decode a string representation of the duration.
  ///
  /// This is a decimal value suffixed with 's'. 3 seconds with 0 nanoseconds
  /// would be '3s'; 3 seconds with 70 nanosecond would be '3.00000007s'.
  static Duration decode(String format) {
    if (!format.endsWith('s')) {
      throw FormatException("duration value should end in 's'");
    }

    // '-123.456s'
    format = format.substring(0, format.length - 1);
    final negative = format.startsWith('-');

    final parts = format.split('.');
    if (parts.length > 2) {
      throw FormatException('too many periods');
    }

    final seconds = int.parse(parts[0]);
    if (parts.length == 1) {
      return Duration(seconds: seconds, nanos: 0);
    }

    final nanos = int.parse(parts[1].padRight(9, '0'));
    return Duration(seconds: seconds, nanos: negative ? -nanos : nanos);
  }
}
