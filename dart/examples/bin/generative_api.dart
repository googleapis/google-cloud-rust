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

/// A simple end-to-end example showing an API call through to the
/// `google_cloud_ai_generativelanguage_v1` package with authentication via
/// `package:googleapis_auth`.
library;

import 'dart:io';

import 'package:google_cloud_ai_generativelanguage_v1/generativelanguage.dart';
import 'package:google_cloud_rpc/rpc.dart';
import 'package:googleapis_auth/auth_io.dart' as auth;

void main(List<String> args) async {
  if (args.length != 2) {
    print('usage: dart bin/generative_api.dart <api-key> <prompt>');
    exit(1);
  }

  final apiKey = args[0];
  final prompt = args[1];

  final client = auth.clientViaApiKey(apiKey);
  final service = GenerativeService(client: client);
  final request = GenerateContentRequest(
    model: 'models/gemini-2.5-flash',
    contents: [
      Content(parts: [Part(text: prompt)]),
    ],
  );

  try {
    final result = await service.generateContent(request);
    final textResponse = result.candidates?[0].content?.parts?[0].text;
    if (textResponse == null) {
      print('<No textual response>');
    } else {
      print(textResponse);
    }
  } on Status catch (error) {
    print('error: $error');
    for (final detail in error.detailsAsMessages) {
      print('  detail: $detail');
    }
  }

  client.close();
}
