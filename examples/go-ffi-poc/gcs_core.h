/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* C ABI of gcs_core v3 (hand-written; keep in sync with gcs_core/src/*.rs).
 *
 * Used by the Go SDK via cgo. Nothing in it is Go-specific (plain C types, JSON payloads, opaque
 * handles, completion callbacks), so other language SDKs can bind to the same core:
 *
 *  - gcs_call:   every unary RPC of google.storage.v2.Storage. `method` is the RPC name
 *                ("GetObject", "ListObjects", "UpdateBucket", ...), `req` the proto3-JSON request,
 *                the result the proto3-JSON response. FieldMask fields (`readMask`,
 *                `updateMask`) may also be JSON arrays of raw paths: proto3 JSON cannot carry
 *                the "*" wildcard or map-key paths such as "metadata.<key>".
 *  - gcs_read_*: streaming reads; request = proto3-JSON ReadObjectRequest.
 *  - gcs_write*: streaming uploads; request = proto3-JSON StartResumableWriteRequest shape
 *                {"writeObjectSpec", "commonObjectRequestParams", "objectChecksums"}.
 *  - `opts` (nullable JSON) carries only caller policy: retry {policy, maxAttempts,
 *    maxRetryDurationMs, backoff, predicate}, userProject, attemptTimeoutMs, headers, and
 *    read/write tuning (decompress, verifyCrc32c, prefetchBytes, metadataOnly, chunkSize).
 *    Retries, idempotency, uploads, checksums and error classification run inside the core.
 *
 * Ownership rules:
 *  - Strings in GcsStatus and strings returned by gcs_core are owned by the caller after the
 *    call and must be released with gcs_string_free.
 *  - Async functions return 0 (completed synchronously), 1 (pending: the GcsDoneCb fires exactly
 *    once) or -1 (failed synchronously: *out_status is set, no callback).
 *  - Memory lent to a pending operation must stay valid (pinned) until its callback fires.
 */
#ifndef GCS_CORE_V3_H
#define GCS_CORE_V3_H
#include <stddef.h>
#include <stdint.h>

typedef struct GcsClient GcsClient;
typedef struct GcsReader GcsReader;
typedef struct GcsWriter GcsWriter;

enum { GCS_OK = 0, GCS_ERR = 1, GCS_PANIC = 2, GCS_INVALID = 3, GCS_CANCELLED = 4 };
enum { GCS_AUTH_ADC = 0, GCS_AUTH_ANONYMOUS = 1, GCS_AUTH_JSON = 2, GCS_AUTH_TOKEN_CALLBACK = 3 };

typedef struct GcsStatus {
  int32_t code;      /* GCS_* */
  int32_t http_code; /* HTTP status (derived from rpc_code for gRPC calls), 0 if none */
  int32_t rpc_code;  /* google.rpc.Code (derived from http_code for HTTP calls), -1 if none */
  char *message;
  char *headers; /* "Name: value\n" lines */
} GcsStatus;

typedef void (*GcsDoneCb)(uintptr_t user, GcsStatus status, uint64_t n);
typedef int32_t (*GcsTokenCb)(uintptr_t user, char **out_token, int64_t *out_expiry_unix, char **out_err);
/* Evaluates a caller retry predicate (opts.retry.predicate); non-zero = retry. Owns status. */
typedef int32_t (*GcsRetryCb)(uintptr_t predicate, GcsStatus status, uint32_t attempt);
/* Called exactly once after the core's last use of a host value it was given (token_user, a
 * retry predicate id), also when the operation fails synchronously. */
typedef void (*GcsReleaseCb)(uintptr_t user);

typedef struct GcsClientConfig {
  const char *endpoint;      /* JSON/data path, NULL = https://storage.googleapis.com */
  const char *grpc_endpoint; /* control plane (gRPC), NULL = default */
  const char *universe_domain;
  const char *credentials_json;
  const char *quota_project;
  const char *user_agent;
  const char *custom_headers; /* "Name: value\n" lines, data path */
  int32_t auth_mode;
  GcsTokenCb token_cb;
  uintptr_t token_user;
  GcsRetryCb retry_cb; /* may be NULL */
  GcsReleaseCb release_cb; /* may be NULL */
} GcsClientConfig;

uint32_t gcs_abi_version(void);
int32_t gcs_runtime_configure(uint32_t worker_threads);
void gcs_string_free(char *s);
void gcs_op_cancel(uint64_t op_id);

GcsStatus gcs_client_new(const GcsClientConfig *cfg, GcsClient **out);
void gcs_client_free(GcsClient *c);

/* Unary RPCs. Completion n = (char *) proto3-JSON response; free with gcs_string_free. */
int32_t gcs_call(const GcsClient *c, const char *method, const uint8_t *req, size_t req_len,
                 const uint8_t *opts, size_t opts_len, GcsDoneCb cb, uintptr_t user,
                 uint64_t *out_op, GcsStatus *out_status);

/* Reads. Open completion n = (GcsReader *). Metadata JSON:
 * {"metadata": <v2 Object>, "readOffset": n, "remaining": n (-1 = unknown), "decompressed": b} */
int32_t gcs_read_open(const GcsClient *c, const uint8_t *req, size_t req_len, const uint8_t *opts,
                      size_t opts_len, GcsDoneCb cb, uintptr_t user, uint64_t *out_op,
                      GcsStatus *out_status);
const char *gcs_reader_metadata(const GcsReader *r); /* borrowed until gcs_reader_close */
int32_t gcs_reader_read(const GcsReader *r, uint8_t *buf, size_t cap, GcsDoneCb cb, uintptr_t user,
                        size_t *out_n, GcsStatus *out_status);
void gcs_reader_cancel(const GcsReader *r);
void gcs_reader_close(GcsReader *r);

/* Writes. Close completion n = (char *) proto3-JSON Object; free with gcs_string_free. */
int32_t gcs_write_open(const GcsClient *c, const uint8_t *req, size_t req_len, const uint8_t *opts,
                       size_t opts_len, GcsWriter **out, GcsStatus *out_status);
int32_t gcs_writer_write(const GcsWriter *w, const uint8_t *data, size_t len, GcsDoneCb cb,
                         uintptr_t user, GcsStatus *out_status);
uint64_t gcs_writer_accepted(const GcsWriter *w);
int32_t gcs_writer_close(const GcsWriter *w, GcsDoneCb cb, uintptr_t user, GcsStatus *out_status);
void gcs_writer_abort(const GcsWriter *w);
void gcs_writer_free(GcsWriter *w);

/* Microbenchmarks of the boundary itself. */
uint64_t gcs_noop(uint64_t x);
int32_t gcs_async_noop(GcsDoneCb cb, uintptr_t user);

#endif /* GCS_CORE_V3_H */
