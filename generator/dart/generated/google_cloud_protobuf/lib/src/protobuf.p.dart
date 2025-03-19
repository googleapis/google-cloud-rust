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

/// `Any` contains an arbitrary serialized message along with a URL that
/// describes the type of the serialized message.
class Any extends Message {
  static const String fullyQualifiedName = 'google.protobuf.Any';

  static const Set<String> _customEncodedTypes = {
    'google.protobuf.Duration',
    'google.protobuf.FieldMask'
  };

  /// The raw JSON encoding of the underlying value.
  final Map<String, dynamic> json;

  Any({Map<String, dynamic>? json})
      : this.json = json ?? {},
        super(fullyQualifiedName);

  /// Create an [Any] from an existing [message].
  Any.from(Message message)
      : json = {},
        super(fullyQualifiedName) {
    packInto(message);
  }

  factory Any.fromJson(Map<String, dynamic> json) {
    return Any(json: json);
  }

  /// '@type' will be something like
  /// `type.googleapis.com/google.protobuf.Duration`, or
  /// `type.googleapis.com/google.rpc.ErrorInfo`.
  String get _type => json['@type'];

  /// Return the fully qualified name of the contained type.
  ///
  /// For example, `google.protobuf.Duration`, or `google.rpc.ErrorInfo`.
  String get typeName {
    const prefix = 'type.googleapis.com/';

    final type = _type;

    // Only extract the type name if we recognize the prefix.
    if (type.startsWith(prefix)) {
      return type.substring(prefix.length);
    } else {
      return type;
    }
  }

  /// Returns whether the type represented by this `Any` is the same as [name].
  ///
  /// [name] should be a fully qualified type name, for example,
  /// `google.protobuf.Duration` or `google.rpc.ErrorInfo`.
  bool isType(String name) => typeName == name;

  /// Deserialize a message from this `Any` object.
  ///
  /// For most message types, you should pass the `<type>.fromJson` constructor
  /// into this method. Eg.:
  ///
  /// ```dart
  /// if (any.isType(Status.fullyQualifiedName)) {
  ///   final status = any.unpackFrom(Status.fromJson);
  ///   ...
  /// }
  /// ```
  T unpackFrom<T, S>(T Function(S) decoder) {
    final name = typeName;

    if (_customEncodedTypes.contains(name)) {
      // Handle custom types:
      //   { "@type": "type.googl...obuf.Duration", "value": "1.212s" }
      return decoder(json['value'] as S);
    } else {
      return decoder(json as S);
    }
  }

  /// Serialize the given message into this `Any` instance.
  void packInto(Message message) {
    final qualifiedName = message.qualifiedName;

    // @type
    json['@type'] = 'type.googleapis.com/$qualifiedName';

    // values
    final encoded = message.toJson();
    if (_customEncodedTypes.contains(qualifiedName)) {
      json['value'] = encoded;
    } else {
      for (final key in (encoded as Map).keys) {
        json[key] = encoded[key];
      }
    }
  }

  @override
  Map<String, dynamic> toJson() => json;

  @override
  String toString() => 'Any($typeName)';
}

/// Called from the Duration constructor to validate the construction
/// parameters.
extension DurationExtension on Duration {
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
}

class DurationHelper {
  /// Encode into a decimal representation of the seconds and nanos, suffixed
  /// with 's'.
  ///
  /// E.g., 3 seconds with 0 nanoseconds would be '3s'; 3 seconds with 70
  /// nanosecond would be '3.00000007s'.
  static String encode(Duration duration) {
    if (duration.nanos == 0) {
      return '${duration.seconds}s';
    } else {
      final rhs = duration.nanos!.abs().toString().padLeft(9, '0');

      var result = duration.seconds == 0
          ? '${duration.nanos! < 0 ? '-' : ''}0.$rhs'
          : '${duration.seconds}.$rhs';
      while (result.endsWith('0')) {
        result = result.substring(0, result.length - 1);
      }

      return '${result}s';
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

class FieldMaskHelper {
  /// Encode the field mask as a single comma-separated string.
  static String encode(FieldMask fieldMask) {
    return fieldMask.paths?.join(',') ?? '';
  }

  /// Decode the field mask from a single comma-separated string.
  static FieldMask decode(String format) {
    return FieldMask(paths: format.split(','));
  }
}
