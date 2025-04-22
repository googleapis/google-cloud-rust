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

part of '../rpc.dart';

typedef _MessageDecoder = ProtoMessage Function(Map<String, dynamic> json);

// A map from message IDs to decoder functions.
const Map<String, _MessageDecoder> _decoders = {
  BadRequest.fullyQualifiedName: BadRequest.fromJson,
  DebugInfo.fullyQualifiedName: DebugInfo.fromJson,
  ErrorInfo.fullyQualifiedName: ErrorInfo.fromJson,
  Help.fullyQualifiedName: Help.fromJson,
  LocalizedMessage.fullyQualifiedName: LocalizedMessage.fromJson,
  PreconditionFailure.fullyQualifiedName: PreconditionFailure.fromJson,
  QuotaFailure.fullyQualifiedName: QuotaFailure.fromJson,
  RequestInfo.fullyQualifiedName: RequestInfo.fromJson,
  ResourceInfo.fullyQualifiedName: ResourceInfo.fromJson,
  RetryInfo.fullyQualifiedName: RetryInfo.fromJson,
};

/// Extend [Status] to add custom handling for [Status.details].
extension StatusExtension on Status {
  /// A utility method to return any [ErrorInfo] instance from the [details]
  /// list.
  ///
  /// All error responses are expected to include an [ErrorInfo] object.
  ErrorInfo? get errorInfo {
    if (details == null) return null;

    for (final any in details!) {
      if (any.typeName == ErrorInfo.fullyQualifiedName) {
        return any.unpackFrom(ErrorInfo.fromJson);
      }
    }

    return null;
  }

  /// A utility method to return any [LocalizedMessage] instance from the
  /// [details] list.
  LocalizedMessage? get localizedMessage {
    if (details == null) return null;

    for (final any in details!) {
      if (any.typeName == LocalizedMessage.fullyQualifiedName) {
        return any.unpackFrom(LocalizedMessage.fromJson);
      }
    }

    return null;
  }

  /// Return the list of [details] with the list elements converted to
  /// [ProtoMessage] instances.
  ///
  /// If an element isn't a known error detail type then [Any] is returned for
  /// that element.
  ///
  /// The known message error types are:
  ///
  /// - [BadRequest]
  /// - [DebugInfo]
  /// - [ErrorInfo]
  /// - [Help]
  /// - [LocalizedMessage]
  /// - [PreconditionFailure]
  /// - [QuotaFailure]
  /// - [RequestInfo]
  /// - [ResourceInfo]
  /// - [RetryInfo]
  ///
  /// For more information see https://google.aip.dev/193 and
  /// https://github.com/googleapis/googleapis/blob/master/google/rpc/error_details.proto.
  List<ProtoMessage> get detailsAsMessages {
    return (details ?? []).map((any) {
      final decoder = _decoders[any.typeName];
      return decoder == null ? any : any.unpackFrom(decoder);
    }).toList();
  }
}
