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

package dart

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/googleapis/google-cloud-rust/generator/internal/api"
	"github.com/googleapis/google-cloud-rust/generator/internal/sample"
)

func TestAnnotateModel(t *testing.T) {
	model := api.NewTestAPI([]*api.Message{}, []*api.Enum{}, []*api.Service{})
	model.PackageName = "test"
	annotate := newAnnotateModel(model)
	_, err := annotate.annotateModel(map[string]string{})
	if err != nil {
		t.Fatal(err)
	}

	codec := model.Codec.(*modelAnnotations)

	if diff := cmp.Diff("google_cloud_test", codec.PackageName); diff != "" {
		t.Errorf("mismatch in Codec.PackageName (-want, +got)\n:%s", diff)
	}
	if diff := cmp.Diff("test", codec.MainFileName); diff != "" {
		t.Errorf("mismatch in Codec.MainFileName (-want, +got)\n:%s", diff)
	}
}

func TestAnnotateModel_Options(t *testing.T) {
	model := api.NewTestAPI([]*api.Message{}, []*api.Enum{}, []*api.Service{})
	annotate := newAnnotateModel(model)
	_, err := annotate.annotateModel(map[string]string{
		"version":   "1.0.0",
		"part-file": "src/test.p.dart",
	})
	if err != nil {
		t.Fatal(err)
	}

	codec := model.Codec.(*modelAnnotations)

	if diff := cmp.Diff("1.0.0", codec.PackageVersion); diff != "" {
		t.Errorf("mismatch in Codec.PackageVersion (-want, +got)\n:%s", diff)
	}
	if diff := cmp.Diff("src/test.p.dart", codec.PartFileReference); diff != "" {
		t.Errorf("mismatch in Codec.PartFileReference (-want, +got)\n:%s", diff)
	}
}

func TestAnnotateMethod(t *testing.T) {
	method := sample.MethodListSecretVersions()
	service := &api.Service{
		Name:          sample.ServiceName,
		Documentation: sample.APIDescription,
		DefaultHost:   sample.DefaultHost,
		Methods:       []*api.Method{method},
		Package:       sample.Package,
	}
	model := api.NewTestAPI(
		[]*api.Message{sample.ListSecretVersionsRequest(), sample.ListSecretVersionsResponse(),
			sample.Secret(), sample.SecretVersion(), sample.Replication(), sample.Automatic(),
			sample.CustomerManagedEncryption()},
		[]*api.Enum{sample.EnumState()},
		[]*api.Service{service},
	)
	api.Validate(model)
	annotate := newAnnotateModel(model)
	_, err := annotate.annotateModel(map[string]string{})
	if err != nil {
		t.Fatal(err)
	}

	annotate.annotateMethod(method)
	codec := method.Codec.(*methodAnnotation)

	got := codec.Name
	want := "listSecretVersions"
	if got != want {
		t.Errorf("mismatched name, got=%q, want=%q", got, want)
	}

	got = codec.RequestType
	want = "ListSecretVersionRequest"
	if got != want {
		t.Errorf("mismatched type, got=%q, want=%q", got, want)
	}

	got = codec.ResponseType
	want = "ListSecretVersionsResponse"
	if got != want {
		t.Errorf("mismatched type, got=%q, want=%q", got, want)
	}
}

func TestCalculateDependencies(t *testing.T) {
	for _, test := range []struct {
		name    string
		imports []string
		want    []string
	}{
		{name: "empty", imports: []string{}, want: []string{}},
		{name: "dart import", imports: []string{typedDataImport}, want: []string{}},
		{name: "package import", imports: []string{httpImport}, want: []string{"http"}},
		{name: "dart and package imports", imports: []string{typedDataImport, httpImport}, want: []string{"http"}},
		{name: "package imports", imports: []string{
			httpImport,
			"package:google_cloud_foo/foo.dart",
		}, want: []string{"google_cloud_foo", "http"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			deps := map[string]string{}
			for _, imp := range test.imports {
				deps[imp] = imp
			}
			gotFull := calculateDependencies(deps)

			got := []string{}
			for _, dep := range gotFull {
				got = append(got, dep.Name)
			}

			if diff := cmp.Diff(test.want, got); diff != "" {
				t.Errorf("mismatch in calculateDependencies (-want, +got)\n:%s", diff)
			}
		})
	}
}

func TestCalculateImports(t *testing.T) {
	for _, test := range []struct {
		name    string
		imports []string
		want    []string
	}{
		{name: "dart import", imports: []string{typedDataImport}, want: []string{
			"import 'dart:typed_data';",
		}},
		{name: "package import", imports: []string{httpImport}, want: []string{
			"import 'package:http/http.dart' as http;",
		}},
		{name: "dart and package imports", imports: []string{typedDataImport, httpImport}, want: []string{
			"import 'dart:typed_data';",
			"",
			"import 'package:http/http.dart' as http;",
		}},
		{name: "package imports", imports: []string{
			httpImport,
			"package:google_cloud_foo/foo.dart",
		}, want: []string{
			"import 'package:google_cloud_foo/foo.dart';",
			"import 'package:http/http.dart' as http;",
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			deps := map[string]string{}
			for _, imp := range test.imports {
				deps[imp] = imp
			}
			got := calculateImports(deps)

			if diff := cmp.Diff(test.want, got); diff != "" {
				t.Errorf("mismatch in calculateImports (-want, +got)\n:%s", diff)
			}
		})
	}
}

func TestAnnotateMessageToString(t *testing.T) {
	model := api.NewTestAPI(
		[]*api.Message{sample.Secret(), sample.SecretVersion(), sample.Replication(),
			sample.Automatic(), sample.CustomerManagedEncryption()},
		[]*api.Enum{sample.EnumState()},
		[]*api.Service{},
	)
	annotate := newAnnotateModel(model)
	annotate.annotateModel(map[string]string{})

	for _, test := range []struct {
		message  *api.Message
		expected int
	}{
		// Expect the number of fields less the number of message fields.
		{message: sample.Secret(), expected: 1},
		{message: sample.SecretVersion(), expected: 2},
		{message: sample.Replication(), expected: 0},
		{message: sample.Automatic(), expected: 0},
	} {
		t.Run(test.message.Name, func(t *testing.T) {
			imports := map[string]string{}
			annotate.annotateMessage(test.message, imports)

			codec := test.message.Codec.(*messageAnnotation)
			actual := codec.ToStringLines

			if len(actual) != test.expected {
				t.Errorf("Expected list of length %d, got %d", test.expected, len(actual))
			}
		})
	}
}

func TestCalculateRequiredFields(t *testing.T) {
	service := &api.Service{
		Name:          sample.ServiceName,
		Documentation: sample.APIDescription,
		DefaultHost:   sample.DefaultHost,
		Methods:       []*api.Method{sample.MethodListSecretVersions()},
		Package:       sample.Package,
	}
	model := api.NewTestAPI(
		[]*api.Message{sample.ListSecretVersionsRequest(), sample.ListSecretVersionsResponse(),
			sample.Secret(), sample.SecretVersion(), sample.Replication()},
		[]*api.Enum{sample.EnumState()},
		[]*api.Service{service},
	)
	api.Validate(model)

	// Test that field annotations correctly calculate their required state; this
	// uses the method's PathInfo.
	requiredFields := calculateRequiredFields(model)

	got := map[string]string{}
	for key, value := range requiredFields {
		got[key] = value.Name
	}

	want := map[string]string{
		"..Secret.parent": "parent",
	}

	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatch in TestCalculateRequiredFields (-want, +got)\n:%s", diff)
	}
}
