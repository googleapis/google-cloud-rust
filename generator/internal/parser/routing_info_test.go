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

package parser

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/googleapis/google-cloud-rust/generator/internal/api"
)

func TestExamples(t *testing.T) {
	tests := []struct {
		methodID string
		want     []*api.RoutingInfo
	}{
		{
			".test.TestService.Example1",
			[]*api.RoutingInfo{{
				Name: "app_profile_id",
				Variants: []*api.RoutingInfoVariant{{
					FieldPath: []string{"app_profile_id"},
					Matching:  api.RoutingPathSpec{Segments: []string{"**"}},
				}},
			}},
		},
		{
			".test.TestService.Example2",
			[]*api.RoutingInfo{{
				Name: "routing_id",
				Variants: []*api.RoutingInfoVariant{{
					FieldPath: []string{"app_profile_id"},
					Matching:  api.RoutingPathSpec{Segments: []string{"**"}},
				}},
			}},
		},
		{
			".test.TestService.Example3a",
			[]*api.RoutingInfo{{
				Name: "table_name",
				Variants: []*api.RoutingInfoVariant{{
					FieldPath: []string{"table_name"},
					Matching:  api.RoutingPathSpec{Segments: []string{"projects", "*", "instances", "*", "**"}},
				}},
			}},
		},
		{
			".test.TestService.Example3b",
			[]*api.RoutingInfo{{
				Name: "table_name",
				Variants: []*api.RoutingInfoVariant{{
					FieldPath: []string{"table_name"},
					Matching:  api.RoutingPathSpec{Segments: []string{"regions", "*", "zones", "*", "**"}},
				}},
			}},
		},
		{
			".test.TestService.Example3c",
			[]*api.RoutingInfo{{
				Name: "table_name",
				Variants: []*api.RoutingInfoVariant{
					{
						FieldPath: []string{"table_name"},
						Matching:  api.RoutingPathSpec{Segments: []string{"projects", "*", "instances", "*", "**"}},
					},
					{
						FieldPath: []string{"table_name"},
						Matching:  api.RoutingPathSpec{Segments: []string{"regions", "*", "zones", "*", "**"}},
					},
				},
			}},
		},
		{
			".test.TestService.Example4",
			[]*api.RoutingInfo{{
				Name: "routing_id",
				Variants: []*api.RoutingInfoVariant{{
					FieldPath: []string{"table_name"},
					Matching:  api.RoutingPathSpec{Segments: []string{"projects", "*"}},
					Suffix:    api.RoutingPathSpec{Segments: []string{"**"}},
				}},
			}},
		},
		{
			".test.TestService.Example5",
			[]*api.RoutingInfo{{
				Name: "routing_id",
				Variants: []*api.RoutingInfoVariant{
					{
						FieldPath: []string{"table_name"},
						Matching:  api.RoutingPathSpec{Segments: []string{"projects", "*", "instances", "*"}},
						Suffix:    api.RoutingPathSpec{Segments: []string{"**"}},
					},
					{
						FieldPath: []string{"table_name"},
						Matching:  api.RoutingPathSpec{Segments: []string{"projects", "*"}},
						Suffix:    api.RoutingPathSpec{Segments: []string{"**"}},
					},
				},
			}},
		},
		{
			".test.TestService.Example6a",
			[]*api.RoutingInfo{
				{
					Name: "instance_id",
					Variants: []*api.RoutingInfoVariant{{
						FieldPath: []string{"table_name"},
						Prefix:    api.RoutingPathSpec{Segments: []string{"projects", "*"}},
						Matching:  api.RoutingPathSpec{Segments: []string{"instances", "*"}},
						Suffix:    api.RoutingPathSpec{Segments: []string{"**"}},
					}},
				},
				{
					Name: "project_id",
					Variants: []*api.RoutingInfoVariant{{
						FieldPath: []string{"table_name"},
						Matching:  api.RoutingPathSpec{Segments: []string{"projects", "*"}},
						Suffix:    api.RoutingPathSpec{Segments: []string{"instances", "*", "**"}},
					}},
				},
			},
		},
		{
			".test.TestService.Example6b",
			[]*api.RoutingInfo{
				{
					Name: "instance_id",
					Variants: []*api.RoutingInfoVariant{{
						FieldPath: []string{"table_name"},
						Prefix:    api.RoutingPathSpec{Segments: []string{"projects", "*"}},
						Matching:  api.RoutingPathSpec{Segments: []string{"instances", "*"}},
						Suffix:    api.RoutingPathSpec{Segments: []string{"**"}},
					}},
				},
				{
					Name: "project_id",
					Variants: []*api.RoutingInfoVariant{{
						FieldPath: []string{"table_name"},
						Matching:  api.RoutingPathSpec{Segments: []string{"projects", "*"}},
						Suffix:    api.RoutingPathSpec{Segments: []string{"**"}},
					}},
				},
			},
		},
		{
			".test.TestService.Example7",
			[]*api.RoutingInfo{
				{
					Name: "project_id",
					Variants: []*api.RoutingInfoVariant{{
						FieldPath: []string{"table_name"},
						Matching:  api.RoutingPathSpec{Segments: []string{"projects", "*"}},
						Suffix:    api.RoutingPathSpec{Segments: []string{"**"}},
					}},
				},
				{
					Name: "routing_id",
					Variants: []*api.RoutingInfoVariant{{
						FieldPath: []string{"app_profile_id"},
						Matching:  api.RoutingPathSpec{Segments: []string{"**"}},
					}},
				},
			},
		},
		{
			".test.TestService.Example8",
			[]*api.RoutingInfo{
				{
					Name: "routing_id",
					Variants: []*api.RoutingInfoVariant{
						{
							FieldPath: []string{"app_profile_id"},
							Matching:  api.RoutingPathSpec{Segments: []string{"**"}},
						},
						{
							FieldPath: []string{"table_name"},
							Matching:  api.RoutingPathSpec{Segments: []string{"regions", "*"}},
							Suffix:    api.RoutingPathSpec{Segments: []string{"**"}},
						},
						{
							FieldPath: []string{"table_name"},
							Matching:  api.RoutingPathSpec{Segments: []string{"projects", "*"}},
							Suffix:    api.RoutingPathSpec{Segments: []string{"**"}},
						},
					},
				},
			},
		},
		{
			".test.TestService.Example9",
			[]*api.RoutingInfo{
				{
					Name: "routing_id",
					Variants: []*api.RoutingInfoVariant{
						{
							FieldPath: []string{"app_profile_id"},
							Prefix:    api.RoutingPathSpec{Segments: []string{"profiles"}},
							Matching:  api.RoutingPathSpec{Segments: []string{"*"}},
						},
						{
							FieldPath: []string{"app_profile_id"},
							Matching:  api.RoutingPathSpec{Segments: []string{"**"}},
						},
						{
							FieldPath: []string{"table_name"},
							Matching:  api.RoutingPathSpec{Segments: []string{"projects", "*"}},
							Suffix:    api.RoutingPathSpec{Segments: []string{"**"}},
						},
					},
				},
				{
					Name: "table_location",
					Variants: []*api.RoutingInfoVariant{
						{
							FieldPath: []string{"table_name"},
							Matching:  api.RoutingPathSpec{Segments: []string{"regions", "*", "zones", "*"}},
							Suffix:    api.RoutingPathSpec{Segments: []string{"tables", "*"}},
						},
						{
							FieldPath: []string{"table_name"},
							Prefix:    api.RoutingPathSpec{Segments: []string{"projects", "*"}},
							Matching:  api.RoutingPathSpec{Segments: []string{"instances", "*"}},
							Suffix:    api.RoutingPathSpec{Segments: []string{"tables", "*"}},
						},
					},
				},
			},
		},
	}

	test := makeAPIForProtobuf(nil, newTestCodeGeneratorRequest(t, "routing_info.proto"))
	for _, tc := range tests {
		t.Run(tc.methodID, func(t *testing.T) {
			got, ok := test.State.MethodByID[tc.methodID]
			if !ok {
				t.Fatalf("Cannot find method %s in API State", tc.methodID)
			}
			if diff := cmp.Diff(got.Routing, tc.want); diff != "" {
				t.Errorf("routing mismatch (-want, +got):\n%s", diff)
			}
		})
	}
}

func TestParsePathTemplateSuccess(t *testing.T) {
	tests := []struct {
		fieldPath string
		path      string
		want      api.RoutingInfo
	}{
		{
			"default",
			"",
			api.RoutingInfo{
				Name: "default",
				Variants: []*api.RoutingInfoVariant{{
					FieldPath: []string{"default"},
					Matching:  api.RoutingPathSpec{Segments: []string{"**"}},
				}},
			},
		},
		{
			// AIP-4222: An empty google.api.routing annotation is acceptable.
			// It means that no routing headers should be generated for the RPC,
			// when they otherwise would be e.g. implicitly from the
			// google.api.http annotation.
			"",
			"",
			api.RoutingInfo{
				Name: "",
				Variants: []*api.RoutingInfoVariant{{
					FieldPath: []string{},
					Matching:  api.RoutingPathSpec{Segments: []string{}},
				}},
			},
		},
		{
			// AIP-4222: It is acceptable to omit the pattern in the resource ID
			// segment, `{parent}` for example, is equivalent to `{parent=*}`.
			"parent",
			"projects/{parent}",
			api.RoutingInfo{
				Name: "parent",
				Variants: []*api.RoutingInfoVariant{{
					FieldPath: []string{"parent"},
					Prefix:    api.RoutingPathSpec{Segments: []string{"projects"}},
					Matching:  api.RoutingPathSpec{Segments: []string{"*"}},
				}},
			},
		},
		{
			// AIP-4222: It is acceptable to omit the path_template field
			// altogether. An omitted path_template is equivalent to a
			// path_template with the same resource ID name as the field and
			// the pattern `**`.
			"parent",
			"",
			api.RoutingInfo{
				Name: "parent",
				Variants: []*api.RoutingInfoVariant{{
					FieldPath: []string{"parent"},
					Matching:  api.RoutingPathSpec{Segments: []string{"**"}},
				}},
			},
		},
		{
			"field.child",
			"",
			api.RoutingInfo{
				Name: "field.child",
				Variants: []*api.RoutingInfoVariant{{
					FieldPath: []string{"field", "child"},
					Matching:  api.RoutingPathSpec{Segments: []string{"**"}},
				}},
			},
		},
		{
			"default",
			"{**}",
			api.RoutingInfo{
				Name: "default",
				Variants: []*api.RoutingInfoVariant{
					{
						FieldPath: []string{"default"},
						Matching:  api.RoutingPathSpec{Segments: []string{"**"}},
					},
				},
			},
		},
		{
			"default",
			"{routing=**}",
			api.RoutingInfo{
				Name: "routing",
				Variants: []*api.RoutingInfoVariant{
					{
						FieldPath: []string{"default"},
						Matching:  api.RoutingPathSpec{Segments: []string{"**"}},
					},
				},
			},
		},
		{
			"default",
			"{routing=a/*/b/**}",
			api.RoutingInfo{
				Name: "routing",
				Variants: []*api.RoutingInfoVariant{
					{
						FieldPath: []string{"default"},
						Matching:  api.RoutingPathSpec{Segments: []string{"a", "*", "b", "**"}},
					},
				},
			},
		},
		{
			"default",
			"p/*/q/*/{routing=a/*/b/**}",
			api.RoutingInfo{
				Name: "routing",
				Variants: []*api.RoutingInfoVariant{
					{
						FieldPath: []string{"default"},
						Matching:  api.RoutingPathSpec{Segments: []string{"a", "*", "b", "**"}},
						Prefix:    api.RoutingPathSpec{Segments: []string{"p", "*", "q", "*"}},
					},
				},
			},
		},
		{
			"default",
			"p/*/q/*/{routing=a/*/b/*}/s/*/u/*/v/**",
			api.RoutingInfo{
				Name: "routing",
				Variants: []*api.RoutingInfoVariant{
					{
						FieldPath: []string{"default"},
						Matching:  api.RoutingPathSpec{Segments: []string{"a", "*", "b", "*"}},
						Prefix:    api.RoutingPathSpec{Segments: []string{"p", "*", "q", "*"}},
						Suffix:    api.RoutingPathSpec{Segments: []string{"s", "*", "u", "*", "v", "**"}},
					},
				},
			},
		},
		{
			"default",
			"p/*/q/*/{routing=a/*/b/**}/s/*/u/*/v/*",
			api.RoutingInfo{
				Name: "routing",
				Variants: []*api.RoutingInfoVariant{
					{
						FieldPath: []string{"default"},
						Matching:  api.RoutingPathSpec{Segments: []string{"a", "*", "b", "**"}},
						Prefix:    api.RoutingPathSpec{Segments: []string{"p", "*", "q", "*"}},
						Suffix:    api.RoutingPathSpec{Segments: []string{"s", "*", "u", "*", "v", "*"}},
					},
				},
			},
		},
		{
			"field.sub_field.child",
			"p/*/q/*/{routing=a/*/b/*}/s/*/u/*/v/**",
			api.RoutingInfo{
				Name: "routing",
				Variants: []*api.RoutingInfoVariant{
					{
						FieldPath: []string{"field", "sub_field", "child"},
						Matching:  api.RoutingPathSpec{Segments: []string{"a", "*", "b", "*"}},
						Prefix:    api.RoutingPathSpec{Segments: []string{"p", "*", "q", "*"}},
						Suffix:    api.RoutingPathSpec{Segments: []string{"s", "*", "u", "*", "v", "**"}},
					}},
			},
		},
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("%s:%s", tc.fieldPath, tc.path), func(t *testing.T) {
			got, err := parseRoutingPathTemplate(tc.fieldPath, tc.path)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(got, &tc.want); diff != "" {
				t.Errorf("segments mismatch (-want, +got):\n%s\n", diff)
			}
		})
	}
}

func TestParsePathTemplateFailures(t *testing.T) {
	tests := []string{
		"projects/*",
		"projects/*/{routing_id=**}/**",
		"projects/*}",
		"projects/*/}",
		"projects/*/{",
		"projects/*/{{",
		"projects/*/{a/b/c=**}",
		"projects/*/{routing_id=**}foo",
		// AIP-4222: A multi-segment wildcard must only appear as the final
		// segment or make up the entire path_template.
		"projects/**/{a}",
		"projects/**/b/{a}",
		"projects/*/{**/a}",
		"projects/*/{a/**/b}",
		"projects/*/{a/**/b/*}",
		"projects/*/{a}/**/b",
		"projects/*/{a}/*/b/**/c",
	}

	for _, path := range tests {
		t.Run(path, func(t *testing.T) {
			got, err := parseRoutingPathTemplate("default", path)
			if err == nil {
				t.Errorf("expected error for %q, got=%v", path, got)
			}
		})
	}
}

func TestParseVariableSuccess(t *testing.T) {
	tests := []struct {
		path        string
		wantName    string
		wantSpec    api.RoutingPathSpec
		wantTrailer string
	}{
		{"**", "default", api.RoutingPathSpec{Segments: []string{"**"}}, ""},
		{"routing=**", "routing", api.RoutingPathSpec{Segments: []string{"**"}}, ""},
		{"routing=a/*/b/**", "routing", api.RoutingPathSpec{Segments: []string{"a", "*", "b", "**"}}, ""},
		{"routing=a/*/b/**}", "routing", api.RoutingPathSpec{Segments: []string{"a", "*", "b", "**"}}, "}"},
		{"routing=a/*/b/**}/c/*", "routing", api.RoutingPathSpec{Segments: []string{"a", "*", "b", "**"}}, "}/c/*"},
	}

	for _, tc := range tests {
		t.Run(tc.path, func(t *testing.T) {
			gotName, gotSpec, width, err := parseRoutingVariable("default", tc.path)
			if err != nil {
				t.Fatal(err)
			}
			if gotName != tc.wantName {
				t.Errorf("mismatched variable names, want=%s, got=%s", tc.wantName, gotName)
			}
			if diff := cmp.Diff(gotSpec, tc.wantSpec); diff != "" {
				t.Errorf("segments mismatch (-want, +got):\n%s\n", diff)
			}
			if tc.path[width:] != tc.wantTrailer {
				t.Errorf("trailer segment mismatch, want=%s, got=%s", tc.wantTrailer, tc.path[width:])
			}
		})
	}
}

func TestParseRoutingVariableError(t *testing.T) {
	tests := []string{"=**", "a/b=**"}

	for _, path := range tests {
		t.Run(path, func(t *testing.T) {
			gotName, gotSpec, _, err := parseRoutingVariable("default", path)
			if err == nil {
				t.Errorf("expected error for %q, gotName=%s, gotSpec=%v", path, gotName, gotSpec)
			}
		})
	}
}

func TestParseRoutingPathSpecSuccess(t *testing.T) {
	tests := []struct {
		path         string
		wantTrailer  string
		wantSegments []string
	}{
		{"**", "", []string{"**"}},
		{"a/b/c", "", []string{"a", "b", "c"}},
		{"a/*/b/*/c/**", "", []string{"a", "*", "b", "*", "c", "**"}},
		{"a/*/b/*}/c/**", "}/c/**", []string{"a", "*", "b", "*"}},
		{"a=b/*/c/*", "=b/*/c/*", []string{"a"}},
		{"a/b=b/*/c/*", "=b/*/c/*", []string{"a", "b"}},
	}

	for _, tc := range tests {
		t.Run(tc.path, func(t *testing.T) {
			got, width := parseRoutingPathSpec(tc.path)
			if diff := cmp.Diff(got.Segments, tc.wantSegments); diff != "" {
				t.Errorf("segments mismatch (-want, +got):\n%s\n", diff)
			}
			if tc.path[width:] != tc.wantTrailer {
				t.Errorf("trailer segment mismatch, want=%s, got=%s", tc.wantTrailer, tc.path[width:])
			}
		})
	}
}
