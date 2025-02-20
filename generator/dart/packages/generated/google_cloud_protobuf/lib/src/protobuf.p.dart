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
