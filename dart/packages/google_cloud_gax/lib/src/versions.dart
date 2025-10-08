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

export 'web.dart' if (dart.library.io) 'vm.dart' show clientDartVersion;

/// The version of this package.
//
// Keep these versions in sync:
// * CHANGELOG.yaml
// * pubspec.yaml
// * lib/src/versions.dart
const gaxVersion = '0.1.0';
