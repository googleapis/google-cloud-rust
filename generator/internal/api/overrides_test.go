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

package api

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestApplyNameOverrides(t *testing.T) {
	tests := []struct {
		name      string
		api       *API
		overrides map[string]string
		want      *API
	}{
		{
			name: "single override",
			api: &API{
				State: &APIState{
					MessageByID: map[string]*Message{
						"foo.bar.Baz": {ID: "foo.bar.Baz", Name: "Baz"},
					},
				},
			},
			overrides: map[string]string{
				"foo.bar.Baz": "NewBaz",
			},
			want: &API{
				State: &APIState{
					MessageByID: map[string]*Message{
						"foo.bar.Baz": {ID: "foo.bar.Baz", Name: "NewBaz"},
					},
				},
			},
		},
		{
			name: "multiple overrides",
			api: &API{
				State: &APIState{
					MessageByID: map[string]*Message{
						"foo.bar.Baz":           {ID: "foo.bar.Baz", Name: "Baz"},
						"foo.bar.Qux":           {ID: "foo.bar.Qux", Name: "Qux"},
						"foo.bar.NotOverridden": {ID: "foo.bar.NotOverridden", Name: "NotOverridden"},
					},
				},
			},
			overrides: map[string]string{
				"foo.bar.Baz": "NewBaz",
				"foo.bar.Qux": "NewQux",
			},
			want: &API{
				State: &APIState{
					MessageByID: map[string]*Message{
						"foo.bar.Baz":           {ID: "foo.bar.Baz", Name: "NewBaz"},
						"foo.bar.Qux":           {ID: "foo.bar.Qux", Name: "NewQux"},
						"foo.bar.NotOverridden": {ID: "foo.bar.NotOverridden", Name: "NotOverridden"},
					},
				},
			},
		},
		{
			name: "no overrides",
			api: &API{
				State: &APIState{
					MessageByID: map[string]*Message{
						"foo.bar.Baz": {ID: "foo.bar.Baz", Name: "Baz"},
					},
				},
			},
			overrides: map[string]string{},
			want: &API{
				State: &APIState{
					MessageByID: map[string]*Message{
						"foo.bar.Baz": {ID: "foo.bar.Baz", Name: "Baz"},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ApplyNameOverrides(tt.api, tt.overrides)
			if diff := cmp.Diff(tt.want, tt.api); diff != "" {
				t.Errorf("ApplyNameOverrides(%+v, %+v) returned unexpected diff (-want +got):\n%s", tt.api, tt.overrides, diff)
			}
		})
	}
}
