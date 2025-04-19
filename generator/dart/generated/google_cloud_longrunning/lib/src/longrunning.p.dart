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

part of '../longrunning.dart';

/// This resource represents a long-running operation that is the result of a
/// network API call.
class Operation<T extends ProtoMessage, S extends ProtoMessage>
    extends ProtoMessage {
  static const String fullyQualifiedName = 'google.longrunning.Operation';

  /// The server-assigned name, which is only unique within the same service that
  /// originally returns it. If you use the default HTTP mapping, the
  /// `name` should be a resource name ending with `operations/{unique_id}`.
  final String? name;

  /// Service-specific metadata associated with the operation. See also
  /// [metadataAsMessage] for the typed version of this property.
  ///
  /// It typically
  /// contains progress information and common metadata such as create time.
  /// Some services might not provide such metadata.  Any method that returns a
  /// long-running operation should document the metadata type, if any.
  final Any? metadata;

  /// If the value is `false`, it means the operation is still in progress.
  /// If `true`, the operation is completed, and either `error` or `response` is
  /// available.
  final bool? done;

  /// The error result of the operation in case of failure or cancellation.
  final Status? error;

  /// The normal, successful response of the operation. See also
  /// [responseAsMessage] for the typed version of this property.
  ///
  /// If the original
  /// method returns no data on success, such as `Delete`, the response is
  /// `google.protobuf.Empty`.  If the original method is standard
  /// `Get`/`Create`/`Update`, the response should be the resource.  For other
  /// methods, the response should have the type `XxxResponse`, where `Xxx`
  /// is the original method name.  For example, if the original method name
  /// is `TakeSnapshot()`, the inferred response type is
  /// `TakeSnapshotResponse`.
  final Any? response;

  /// Internal plumbing used to implement [responseAsMessage] and
  /// [metadataAsMessage].
  final OperationHelper<T, S>? operationHelper;

  Operation({
    this.name,
    this.metadata,
    this.done,
    this.error,
    this.response,
    this.operationHelper,
  }) : super(fullyQualifiedName);

  factory Operation.fromJson(
    Map<String, dynamic> json, [
    OperationHelper<T, S>? helper,
  ]) {
    return Operation(
      name: json['name'],
      metadata: decode(json['metadata'], Any.fromJson),
      done: json['done'],
      error: decode(json['error'], Status.fromJson),
      response: decode(json['response'], Any.fromJson),
      operationHelper: helper,
    );
  }

  /// The normal, successful response of the operation.
  ///
  /// See also [response] for the untyped version of this property.
  T? get responseAsMessage {
    if (operationHelper == null) return null;
    return response?.unpackFrom(operationHelper!.responseDecoder);
  }

  /// Service-specific metadata associated with the operation.
  ///
  /// See also [metadata] for the untyped version of this property.
  S? get metadataAsMessage {
    if (operationHelper?.metadataDecoder == null) return null;
    return metadata?.unpackFrom(operationHelper!.metadataDecoder!);
  }

  @override
  Object toJson() {
    return {
      if (name != null) 'name': name,
      if (metadata != null) 'metadata': metadata!.toJson(),
      if (done != null) 'done': done,
      if (error != null) 'error': error!.toJson(),
      if (response != null) 'response': response!.toJson(),
    };
  }

  @override
  String toString() {
    final contents = [
      if (name != null) 'name=$name',
      if (done != null) 'done=$done',
    ].join(',');
    return 'Operation($contents)';
  }
}

/// This helper class is used to pull typed messages out of the
/// [Operation.response] and [Operation.metadata] fields in order to populate
/// the typed [Operation.responseAsMessage] and [Operation.metadataAsMessage]
/// fields.
final class OperationHelper<T extends ProtoMessage, S extends ProtoMessage> {
  final T Function(Map<String, dynamic>) responseDecoder;
  final S Function(Map<String, dynamic>)? metadataDecoder;

  OperationHelper(this.responseDecoder, [this.metadataDecoder]);
}
