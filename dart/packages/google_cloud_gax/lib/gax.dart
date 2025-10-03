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

import 'package:google_cloud_gax/src/versions.dart';
import 'package:google_cloud_rpc/rpc.dart';
import 'package:http/http.dart' as http;

export 'dart:typed_data' show Uint8List;

const String _clientKey = 'x-goog-api-client';

final String _clientName = 'gl-dart/$clientDartVersion gax/$gaxVersion';

const String _contentTypeKey = 'content-type';
const String _typeJson = 'application/json';

/// An abstract class that can return a JSON representation of itself.
///
/// Classes that implement [JsonEncodable] will often have a `fromJson()`
/// constructor.
abstract class JsonEncodable {
  Object? toJson();
}

/// The abstract common superclass of all messages.
abstract class ProtoMessage implements JsonEncodable {
  /// The fully qualified name of this message, i.e., `google.protobuf.Duration`
  /// or `google.rpc.ErrorInfo`.
  final String qualifiedName;

  ProtoMessage(this.qualifiedName);
}

/// The abstract common superclass of all enum values.
abstract class ProtoEnum implements JsonEncodable {
  final String value;

  const ProtoEnum(this.value);

  @override
  String toJson() => value;

  @override
  bool operator ==(Object other) {
    return other.runtimeType == runtimeType &&
        value == (other as ProtoEnum).value;
  }

  @override
  int get hashCode => value.hashCode;
}

class ServiceClient {
  final http.Client client;

  ServiceClient({required this.client});

  Future<Map<String, dynamic>> get(Uri url) => _makeRequest(url, 'GET');

  Stream<Map<String, dynamic>> getStreaming(Uri url) =>
      _makeStreamingRequest(url, 'GET');

  Future<Map<String, dynamic>> delete(Uri url) => _makeRequest(url, 'DELETE');

  Stream<Map<String, dynamic>> deleteStreaming(Uri url) =>
      _makeStreamingRequest(url, 'DELETE');

  Future<Map<String, dynamic>> patch(Uri url, {JsonEncodable? body}) =>
      _makeRequest(url, 'PATCH', body);

  Stream<Map<String, dynamic>> patchStreaming(Uri url, {JsonEncodable? body}) =>
      _makeStreamingRequest(url, 'PATCH', body);

  Future<Map<String, dynamic>> post(Uri url, {JsonEncodable? body}) =>
      _makeRequest(url, 'POST', body);

  Stream<Map<String, dynamic>> postStreaming(Uri url, {JsonEncodable? body}) =>
      _makeStreamingRequest(url, 'POST', body);

  Future<Map<String, dynamic>> put(Uri url, {JsonEncodable? body}) =>
      _makeRequest(url, 'PUT', body);

  Stream<Map<String, dynamic>> putStreaming(Uri url, {JsonEncodable? body}) =>
      _makeStreamingRequest(url, 'PUT', body);

  /// Closes the client and cleans up any resources associated with it.
  ///
  /// Once [close] is called, no other methods should be called.
  void close() => client.close();

  Future<Map<String, dynamic>> _makeRequest(
    Uri url,
    method, [
    JsonEncodable? requestBody,
  ]) async {
    final request = http.Request(method, url);
    if (requestBody != null) {
      request.body = requestBody._asEncodedJson;
    }
    request.headers.addAll({
      _clientKey: _clientName,
      if (requestBody != null) _contentTypeKey: _typeJson,
    });

    final response = await client.send(request);
    final responseBody = await response.stream.bytesToString();
    final statusOK = response.statusCode >= 200 && response.statusCode < 300;
    if (!statusOK) {
      _throwException(response.statusCode, response.reasonPhrase, responseBody);
    }
    return responseBody.isEmpty ? {} : jsonDecode(responseBody);
  }

  /// Make a request that streams its results using
  /// [Server-sent events](https://html.spec.whatwg.org/multipage/server-sent-events.html).
  ///
  /// NOTE: most Google APIs do not support Server-sent events.
  Stream<Map<String, dynamic>> _makeStreamingRequest(
    Uri url,
    String method, [
    JsonEncodable? requestBody,
  ]) async* {
    final request = http.Request(method, _makeUrlStreaming(url));
    if (requestBody != null) {
      request.body = requestBody._asEncodedJson;
    }
    request.headers.addAll({
      _clientKey: _clientName,
      if (requestBody != null) _contentTypeKey: _typeJson,
    });

    final response = await client.send(request);
    final statusOK = response.statusCode >= 200 && response.statusCode < 300;
    if (!statusOK) {
      _throwException(
        response.statusCode,
        response.reasonPhrase,
        await response.stream.bytesToString(),
      );
    }

    final lines = response.stream.toStringStream().transform(LineSplitter());
    await for (final line in lines) {
      // Google APIs only generate "data" events.
      // The SSE specification does not require a space after the colon but
      // Google APIs always generate one.
      const dataPrefix = 'data: ';
      if (line.startsWith(dataPrefix)) {
        final jsonText = line.substring(dataPrefix.length);
        final json = jsonDecode(jsonText) as Map<String, dynamic>;
        yield json;
      }
    }
  }

  static Uri _makeUrlStreaming(Uri url) {
    final query = Map.of(url.queryParameters);
    query['alt'] = 'sse';
    return url.replace(queryParameters: query);
  }

  Never _throwException(
    int statusCode,
    String? reasonPhrase,
    String responseBody,
  ) {
    Status status;

    try {
      final json = jsonDecode(responseBody);
      status = Status.fromJson(json['error']);
    } catch (_) {
      // Return a general HTTP exception if we can't parse the Status response.
      throw http.ClientException('$statusCode: $reasonPhrase');
    }

    throw status;
  }
}

extension on JsonEncodable {
  String get _asEncodedJson => jsonEncode(toJson());
}
