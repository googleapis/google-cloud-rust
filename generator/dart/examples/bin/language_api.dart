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

/// A simple end-to-end example showing an API call through the
/// `google_cloud_language` package with authentication via
/// `package:googleapis_auth`.
library;

import 'dart:io';

import 'package:google_cloud_language_v2/language.dart';
import 'package:google_cloud_rpc/rpc.dart';
import 'package:googleapis_auth/auth_io.dart' as auth;

void main(List<String> args) async {
  if (args.isEmpty) {
    print('usage: dart bin/language_api.dart <api-key>');
    exit(1);
  }

  final apiKey = args[0];

  final client = auth.clientViaApiKey(apiKey);
  final service = LanguageService(client: client);
  final document = Document(
    content: 'Hello, world!',
    type: Document_Type.plainText,
  );

  try {
    final result = await service.analyzeSentiment(
      AnalyzeSentimentRequest(document: document),
    );

    print('result: $result');
    print('documentSentiment: ${result.documentSentiment}');

    for (final sentence in result.sentences!) {
      print('');
      print('sentence:');
      print('  text: ${sentence.text}');
      print('  sentiment: ${sentence.sentiment}');
    }
  } on Status catch (error) {
    print('error: $error');
    for (final detail in error.detailsAsMessages) {
      print('  detail: $detail');
    }
  }

  client.close();
}
