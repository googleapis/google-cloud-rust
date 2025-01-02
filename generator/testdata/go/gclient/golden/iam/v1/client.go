// Copyright 2024 Google LLC
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
//
// Code generated by sidekick. DO NOT EDIT.

package iam

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"cloud.google.com/go/auth"
	"github.com/google-cloud-rust/generator/testdata/go/gclient/golden/typez"
	"github.com/google-cloud-rust/generator/testdata/go/gclient/golden/wkt"
)

const defaultHost = "https://iam-meta-api.googleapis.com"

type Options struct {
	Credentials *auth.Credentials
	Endpoint    string
}

// Client used to talk to services.
type Client struct {
	hc        *http.Client
	opts      *Options
	iampolicy *IAMPolicy
}

// NewClient returns a new Client.
func NewClient(opts *Options) *Client {
	return &Client{hc: http.DefaultClient, opts: opts}
}

func (c *Client) addAuthorizationHeader(ctx context.Context, h http.Header) error {
	tok, err := c.token(ctx)
	if err != nil {
		return err
	}
	h.Set("Authorization", "Bearer "+tok)
	return nil
}

func (c *Client) token(ctx context.Context) (string, error) {
	tok, err := c.opts.Credentials.Token(ctx)
	if err != nil {
		return "", err
	}
	return tok.Value, nil
}

// API Overview
//
// Manages Identity and Access Management (IAM) policies.
//
// Any implementation of an API that offers access control features
// implements the google.iam.v1.IAMPolicy interface.
//
// ## Data model
//
// Access control is applied when a principal (user or service account), takes
// some action on a resource exposed by a service. Resources, identified by
// URI-like names, are the unit of access control specification. Service
// implementations can choose the granularity of access control and the
// supported permissions for their resources.
// For example one database service may allow access control to be
// specified only at the Table level, whereas another might allow access control
// to also be specified at the Column level.
//
// ## Policy Structure
//
// # See google.iam.v1.Policy
//
// This is intentionally not a CRUD style API because access control policies
// are created and deleted implicitly with the resources to which they are
// attached.
type IAMPolicy struct {
	client  *Client
	baseURL string
}

// API Overview
//
// Manages Identity and Access Management (IAM) policies.
//
// Any implementation of an API that offers access control features
// implements the google.iam.v1.IAMPolicy interface.
//
// ## Data model
//
// Access control is applied when a principal (user or service account), takes
// some action on a resource exposed by a service. Resources, identified by
// URI-like names, are the unit of access control specification. Service
// implementations can choose the granularity of access control and the
// supported permissions for their resources.
// For example one database service may allow access control to be
// specified only at the Table level, whereas another might allow access control
// to also be specified at the Column level.
//
// ## Policy Structure
//
// # See google.iam.v1.Policy
//
// This is intentionally not a CRUD style API because access control policies
// are created and deleted implicitly with the resources to which they are
// attached.
func (c *Client) IAMPolicy() *IAMPolicy {
	return &IAMPolicy{client: c, baseURL: defaultHost}
}

// Sets the access control policy on the specified resource. Replaces any
// existing policy.
//
// Can return `NOT_FOUND`, `INVALID_ARGUMENT`, and `PERMISSION_DENIED` errors.
func (s *IAMPolicy) SetIamPolicy(ctx context.Context, req *SetIamPolicyRequest) (*Policy, error) {
	out := new(Policy)
	reqBody, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	baseURL, err := url.Parse(s.baseURL)
	if err != nil {
		return nil, err
	}
	baseURL.Path += fmt.Sprintf("/v1/%s:setIamPolicy", req.Resource)
	params := url.Values{}
	params.Add("$alt", "json")
	baseURL.RawQuery = params.Encode()
	httpReq, err := http.NewRequestWithContext(ctx, "POST", baseURL.String(), bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	if err := s.client.addAuthorizationHeader(ctx, httpReq.Header); err != nil {
		return nil, err
	}
	respBody, err := doRequest(s.client.hc, httpReq)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(respBody, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// Gets the access control policy for a resource.
// Returns an empty policy if the resource exists and does not have a policy
// set.
func (s *IAMPolicy) GetIamPolicy(ctx context.Context, req *GetIamPolicyRequest) (*Policy, error) {
	out := new(Policy)
	reqBody, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	baseURL, err := url.Parse(s.baseURL)
	if err != nil {
		return nil, err
	}
	baseURL.Path += fmt.Sprintf("/v1/%s:getIamPolicy", req.Resource)
	params := url.Values{}
	params.Add("$alt", "json")
	baseURL.RawQuery = params.Encode()
	httpReq, err := http.NewRequestWithContext(ctx, "POST", baseURL.String(), bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	if err := s.client.addAuthorizationHeader(ctx, httpReq.Header); err != nil {
		return nil, err
	}
	respBody, err := doRequest(s.client.hc, httpReq)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(respBody, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// Returns permissions that a caller has on the specified resource.
// If the resource does not exist, this will return an empty set of
// permissions, not a `NOT_FOUND` error.
//
// Note: This operation is designed to be used for building permission-aware
// UIs and command-line tools, not for authorization checking. This operation
// may "fail open" without warning.
func (s *IAMPolicy) TestIamPermissions(ctx context.Context, req *TestIamPermissionsRequest) (*TestIamPermissionsResponse, error) {
	out := new(TestIamPermissionsResponse)
	reqBody, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	baseURL, err := url.Parse(s.baseURL)
	if err != nil {
		return nil, err
	}
	baseURL.Path += fmt.Sprintf("/v1/%s:testIamPermissions", req.Resource)
	params := url.Values{}
	params.Add("$alt", "json")
	baseURL.RawQuery = params.Encode()
	httpReq, err := http.NewRequestWithContext(ctx, "POST", baseURL.String(), bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	if err := s.client.addAuthorizationHeader(ctx, httpReq.Header); err != nil {
		return nil, err
	}
	respBody, err := doRequest(s.client.hc, httpReq)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(respBody, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func doRequest(client *http.Client, req *http.Request) ([]byte, error) {
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// Request message for `SetIamPolicy` method.
type SetIamPolicyRequest struct {

	// REQUIRED: The resource for which the policy is being specified.
	// See the operation documentation for the appropriate value for this field.
	Resource string `json:"resource,omitempty"`

	// REQUIRED: The complete policy to be applied to the `resource`. The size of
	// the policy is limited to a few 10s of KB. An empty policy is a
	// valid policy but certain Cloud Platform services (such as Projects)
	// might reject them.
	Policy *Policy `json:"policy,omitempty"`

	// OPTIONAL: A FieldMask specifying which fields of the policy to modify. Only
	// the fields in the mask will be modified. If no mask is provided, the
	// following default mask is used:
	//
	// `paths: "bindings, etag"`
	UpdateMask *wkt.FieldMask `json:"updateMask,omitempty"`
}

// Request message for `GetIamPolicy` method.
type GetIamPolicyRequest struct {

	// REQUIRED: The resource for which the policy is being requested.
	// See the operation documentation for the appropriate value for this field.
	Resource string `json:"resource,omitempty"`

	// OPTIONAL: A `GetPolicyOptions` object for specifying options to
	// `GetIamPolicy`.
	Options *GetPolicyOptions `json:"options,omitempty"`
}

// Request message for `TestIamPermissions` method.
type TestIamPermissionsRequest struct {

	// REQUIRED: The resource for which the policy detail is being requested.
	// See the operation documentation for the appropriate value for this field.
	Resource string `json:"resource,omitempty"`

	// The set of permissions to check for the `resource`. Permissions with
	// wildcards (such as '*' or 'storage.*') are not allowed. For more
	// information see
	// [IAM Overview](https://cloud.google.com/iam/docs/overview#permissions).
	Permissions string `json:"permissions,omitempty"`
}

// Response message for `TestIamPermissions` method.
type TestIamPermissionsResponse struct {

	// A subset of `TestPermissionsRequest.permissions` that the caller is
	// allowed.
	Permissions string `json:"permissions,omitempty"`
}

// Encapsulates settings provided to GetIamPolicy.
type GetPolicyOptions struct {

	// Optional. The maximum policy version that will be used to format the
	// policy.
	//
	// Valid values are 0, 1, and 3. Requests specifying an invalid value will be
	// rejected.
	//
	// Requests for policies with any conditional role bindings must specify
	// version 3. Policies with no conditional role bindings may specify any valid
	// value or leave the field unset.
	//
	// The policy in the response might use the policy version that you specified,
	// or it might use a lower policy version. For example, if you specify version
	// 3, but the policy has no conditional role bindings, the response uses
	// version 1.
	//
	// To learn which resources support conditions in their IAM policies, see the
	// [IAM
	// documentation](https://cloud.google.com/iam/help/conditions/resource-policies).
	RequestedPolicyVersion int32 `json:"requestedPolicyVersion,omitempty"`
}

// An Identity and Access Management (IAM) policy, which specifies access
// controls for Google Cloud resources.
//
// A `Policy` is a collection of `bindings`. A `binding` binds one or more
// `members`, or principals, to a single `role`. Principals can be user
// accounts, service accounts, Google groups, and domains (such as G Suite). A
// `role` is a named list of permissions; each `role` can be an IAM predefined
// role or a user-created custom role.
//
// For some types of Google Cloud resources, a `binding` can also specify a
// `condition`, which is a logical expression that allows access to a resource
// only if the expression evaluates to `true`. A condition can add constraints
// based on attributes of the request, the resource, or both. To learn which
// resources support conditions in their IAM policies, see the
// [IAM
// documentation](https://cloud.google.com/iam/help/conditions/resource-policies).
//
// **JSON example:**
//
// ```
//
//	{
//	  "bindings": [
//	    {
//	      "role": "roles/resourcemanager.organizationAdmin",
//	      "members": [
//	        "user:mike@example.com",
//	        "group:admins@example.com",
//	        "domain:google.com",
//	        "serviceAccount:my-project-id@appspot.gserviceaccount.com"
//	      ]
//	    },
//	    {
//	      "role": "roles/resourcemanager.organizationViewer",
//	      "members": [
//	        "user:eve@example.com"
//	      ],
//	      "condition": {
//	        "title": "expirable access",
//	        "description": "Does not grant access after Sep 2020",
//	        "expression": "request.time <
//	        timestamp('2020-10-01T00:00:00.000Z')",
//	      }
//	    }
//	  ],
//	  "etag": "BwWWja0YfJA=",
//	  "version": 3
//	}
//
// ```
//
// **YAML example:**
//
// ```
//
//	bindings:
//	- members:
//	  - user:mike@example.com
//	  - group:admins@example.com
//	  - domain:google.com
//	  - serviceAccount:my-project-id@appspot.gserviceaccount.com
//	  role: roles/resourcemanager.organizationAdmin
//	- members:
//	  - user:eve@example.com
//	  role: roles/resourcemanager.organizationViewer
//	  condition:
//	    title: expirable access
//	    description: Does not grant access after Sep 2020
//	    expression: request.time < timestamp('2020-10-01T00:00:00.000Z')
//	etag: BwWWja0YfJA=
//	version: 3
//
// ```
//
// For a description of IAM and its features, see the
// [IAM documentation](https://cloud.google.com/iam/docs/).
type Policy struct {

	// Specifies the format of the policy.
	//
	// Valid values are `0`, `1`, and `3`. Requests that specify an invalid value
	// are rejected.
	//
	// Any operation that affects conditional role bindings must specify version
	// `3`. This requirement applies to the following operations:
	//
	// * Getting a policy that includes a conditional role binding
	// * Adding a conditional role binding to a policy
	// * Changing a conditional role binding in a policy
	// * Removing any role binding, with or without a condition, from a policy
	//   that includes conditions
	//
	// **Important:** If you use IAM Conditions, you must include the `etag` field
	// whenever you call `setIamPolicy`. If you omit this field, then IAM allows
	// you to overwrite a version `3` policy with a version `1` policy, and all of
	// the conditions in the version `3` policy are lost.
	//
	// If a policy does not include any conditions, operations on that policy may
	// specify any valid version or leave the field unset.
	//
	// To learn which resources support conditions in their IAM policies, see the
	// [IAM
	// documentation](https://cloud.google.com/iam/help/conditions/resource-policies).
	Version int32 `json:"version,omitempty"`

	// Associates a list of `members`, or principals, with a `role`. Optionally,
	// may specify a `condition` that determines how and when the `bindings` are
	// applied. Each of the `bindings` must contain at least one principal.
	//
	// The `bindings` in a `Policy` can refer to up to 1,500 principals; up to 250
	// of these principals can be Google groups. Each occurrence of a principal
	// counts towards these limits. For example, if the `bindings` grant 50
	// different roles to `user:alice@example.com`, and not to any other
	// principal, then you can add another 1,450 principals to the `bindings` in
	// the `Policy`.
	Bindings *Binding `json:"bindings,omitempty"`

	// Specifies cloud audit logging configuration for this policy.
	AuditConfigs *AuditConfig `json:"auditConfigs,omitempty"`

	// `etag` is used for optimistic concurrency control as a way to help
	// prevent simultaneous updates of a policy from overwriting each other.
	// It is strongly suggested that systems make use of the `etag` in the
	// read-modify-write cycle to perform policy updates in order to avoid race
	// conditions: An `etag` is returned in the response to `getIamPolicy`, and
	// systems are expected to put that etag in the request to `setIamPolicy` to
	// ensure that their change will be applied to the same version of the policy.
	//
	// **Important:** If you use IAM Conditions, you must include the `etag` field
	// whenever you call `setIamPolicy`. If you omit this field, then IAM allows
	// you to overwrite a version `3` policy with a version `1` policy, and all of
	// the conditions in the version `3` policy are lost.
	Etag []byte `json:"etag,omitempty"`
}

// Associates `members`, or principals, with a `role`.
type Binding struct {

	// Role that is assigned to the list of `members`, or principals.
	// For example, `roles/viewer`, `roles/editor`, or `roles/owner`.
	Role string `json:"role,omitempty"`

	// Specifies the principals requesting access for a Google Cloud resource.
	// `members` can have the following values:
	//
	// * `allUsers`: A special identifier that represents anyone who is
	//    on the internet; with or without a Google account.
	//
	// * `allAuthenticatedUsers`: A special identifier that represents anyone
	//    who is authenticated with a Google account or a service account.
	//
	// * `user:{emailid}`: An email address that represents a specific Google
	//    account. For example, `alice@example.com` .
	//
	//
	// * `serviceAccount:{emailid}`: An email address that represents a service
	//    account. For example, `my-other-app@appspot.gserviceaccount.com`.
	//
	// * `group:{emailid}`: An email address that represents a Google group.
	//    For example, `admins@example.com`.
	//
	// * `deleted:user:{emailid}?uid={uniqueid}`: An email address (plus unique
	//    identifier) representing a user that has been recently deleted. For
	//    example, `alice@example.com?uid=123456789012345678901`. If the user is
	//    recovered, this value reverts to `user:{emailid}` and the recovered user
	//    retains the role in the binding.
	//
	// * `deleted:serviceAccount:{emailid}?uid={uniqueid}`: An email address (plus
	//    unique identifier) representing a service account that has been recently
	//    deleted. For example,
	//    `my-other-app@appspot.gserviceaccount.com?uid=123456789012345678901`.
	//    If the service account is undeleted, this value reverts to
	//    `serviceAccount:{emailid}` and the undeleted service account retains the
	//    role in the binding.
	//
	// * `deleted:group:{emailid}?uid={uniqueid}`: An email address (plus unique
	//    identifier) representing a Google group that has been recently
	//    deleted. For example, `admins@example.com?uid=123456789012345678901`. If
	//    the group is recovered, this value reverts to `group:{emailid}` and the
	//    recovered group retains the role in the binding.
	//
	//
	// * `domain:{domain}`: The G Suite domain (primary) that represents all the
	//    users of that domain. For example, `google.com` or `example.com`.
	//
	//
	Members string `json:"members,omitempty"`

	// The condition that is associated with this binding.
	//
	// If the condition evaluates to `true`, then this binding applies to the
	// current request.
	//
	// If the condition evaluates to `false`, then this binding does not apply to
	// the current request. However, a different role binding might grant the same
	// role to one or more of the principals in this binding.
	//
	// To learn which resources support conditions in their IAM policies, see the
	// [IAM
	// documentation](https://cloud.google.com/iam/help/conditions/resource-policies).
	Condition *typez.Expr `json:"condition,omitempty"`
}

// Specifies the audit configuration for a service.
// The configuration determines which permission types are logged, and what
// identities, if any, are exempted from logging.
// An AuditConfig must have one or more AuditLogConfigs.
//
// If there are AuditConfigs for both `allServices` and a specific service,
// the union of the two AuditConfigs is used for that service: the log_types
// specified in each AuditConfig are enabled, and the exempted_members in each
// AuditLogConfig are exempted.
//
// Example Policy with multiple AuditConfigs:
//
//	{
//	  "audit_configs": [
//	    {
//	      "service": "allServices",
//	      "audit_log_configs": [
//	        {
//	          "log_type": "DATA_READ",
//	          "exempted_members": [
//	            "user:jose@example.com"
//	          ]
//	        },
//	        {
//	          "log_type": "DATA_WRITE"
//	        },
//	        {
//	          "log_type": "ADMIN_READ"
//	        }
//	      ]
//	    },
//	    {
//	      "service": "sampleservice.googleapis.com",
//	      "audit_log_configs": [
//	        {
//	          "log_type": "DATA_READ"
//	        },
//	        {
//	          "log_type": "DATA_WRITE",
//	          "exempted_members": [
//	            "user:aliya@example.com"
//	          ]
//	        }
//	      ]
//	    }
//	  ]
//	}
//
// For sampleservice, this policy enables DATA_READ, DATA_WRITE and ADMIN_READ
// logging. It also exempts `jose@example.com` from DATA_READ logging, and
// `aliya@example.com` from DATA_WRITE logging.
type AuditConfig struct {

	// Specifies a service that will be enabled for audit logging.
	// For example, `storage.googleapis.com`, `cloudsql.googleapis.com`.
	// `allServices` is a special value that covers all services.
	Service string `json:"service,omitempty"`

	// The configuration for logging of each type of permission.
	AuditLogConfigs *AuditLogConfig `json:"auditLogConfigs,omitempty"`
}

// Provides the configuration for logging a type of permissions.
// Example:
//
//	{
//	  "audit_log_configs": [
//	    {
//	      "log_type": "DATA_READ",
//	      "exempted_members": [
//	        "user:jose@example.com"
//	      ]
//	    },
//	    {
//	      "log_type": "DATA_WRITE"
//	    }
//	  ]
//	}
//
// This enables 'DATA_READ' and 'DATA_WRITE' logging, while exempting
// jose@example.com from DATA_READ logging.
type AuditLogConfig struct {

	// The log type that this config enables.
	LogType AuditLogConfig_LogType `json:"logType,omitempty"`

	// Specifies the identities that do not cause logging for this type of
	// permission.
	// Follows the same format of
	// [Binding.members][google.iam.v1.Binding.members].
	ExemptedMembers string `json:"exemptedMembers,omitempty"`
}

type AuditLogConfig_LogType int32

const (
	// Default case. Should never be this.
	AuditLogConfig_LOG_TYPE_UNSPECIFIED AuditLogConfig_LogType = 0
	// Admin reads. Example: CloudIAM getIamPolicy
	AuditLogConfig_ADMIN_READ AuditLogConfig_LogType = 1
	// Data writes. Example: CloudSQL Users create
	AuditLogConfig_DATA_WRITE AuditLogConfig_LogType = 2
	// Data reads. Example: CloudSQL Users list
	AuditLogConfig_DATA_READ AuditLogConfig_LogType = 3
)

// The difference delta between two policies.
type PolicyDelta struct {

	// The delta for Bindings between two policies.
	BindingDeltas *BindingDelta `json:"bindingDeltas,omitempty"`

	// The delta for AuditConfigs between two policies.
	AuditConfigDeltas *AuditConfigDelta `json:"auditConfigDeltas,omitempty"`
}

// One delta entry for Binding. Each individual change (only one member in each
// entry) to a binding will be a separate entry.
type BindingDelta struct {

	// The action that was performed on a Binding.
	// Required
	Action BindingDelta_Action `json:"action,omitempty"`

	// Role that is assigned to `members`.
	// For example, `roles/viewer`, `roles/editor`, or `roles/owner`.
	// Required
	Role string `json:"role,omitempty"`

	// A single identity requesting access for a Google Cloud resource.
	// Follows the same format of Binding.members.
	// Required
	Member string `json:"member,omitempty"`

	// The condition that is associated with this binding.
	Condition *typez.Expr `json:"condition,omitempty"`
}

type BindingDelta_Action int32

const (
	// Unspecified.
	BindingDelta_ACTION_UNSPECIFIED BindingDelta_Action = 0
	// Addition of a Binding.
	BindingDelta_ADD BindingDelta_Action = 1
	// Removal of a Binding.
	BindingDelta_REMOVE BindingDelta_Action = 2
)

// One delta entry for AuditConfig. Each individual change (only one
// exempted_member in each entry) to a AuditConfig will be a separate entry.
type AuditConfigDelta struct {

	// The action that was performed on an audit configuration in a policy.
	// Required
	Action AuditConfigDelta_Action `json:"action,omitempty"`

	// Specifies a service that was configured for Cloud Audit Logging.
	// For example, `storage.googleapis.com`, `cloudsql.googleapis.com`.
	// `allServices` is a special value that covers all services.
	// Required
	Service string `json:"service,omitempty"`

	// A single identity that is exempted from "data access" audit
	// logging for the `service` specified above.
	// Follows the same format of Binding.members.
	ExemptedMember string `json:"exemptedMember,omitempty"`

	// Specifies the log_type that was be enabled. ADMIN_ACTIVITY is always
	// enabled, and cannot be configured.
	// Required
	LogType string `json:"logType,omitempty"`
}

type AuditConfigDelta_Action int32

const (
	// Unspecified.
	AuditConfigDelta_ACTION_UNSPECIFIED AuditConfigDelta_Action = 0
	// Addition of an audit configuration.
	AuditConfigDelta_ADD AuditConfigDelta_Action = 1
	// Removal of an audit configuration.
	AuditConfigDelta_REMOVE AuditConfigDelta_Action = 2
)