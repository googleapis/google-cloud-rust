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
	"github.com/google/go-cmp/cmp"
	"testing"
)

func Test_RoutingCombos(t *testing.T) {
	va1 := &RoutingInfoVariant{
		FieldPath: []string{"va1"},
	}
	va2 := &RoutingInfoVariant{
		FieldPath: []string{"va2"},
	}
	va3 := &RoutingInfoVariant{
		FieldPath: []string{"va3"},
	}
	a := &RoutingInfo{
		Name:     "a",
		Variants: []*RoutingInfoVariant{va1, va2, va3},
	}

	vb1 := &RoutingInfoVariant{
		FieldPath: []string{"vb1"},
	}
	vb2 := &RoutingInfoVariant{
		FieldPath: []string{"vb2"},
	}
	b := &RoutingInfo{
		Name:     "b",
		Variants: []*RoutingInfoVariant{vb1, vb2},
	}

	vc1 := &RoutingInfoVariant{
		FieldPath: []string{"vc1"},
	}
	c := &RoutingInfo{
		Name:     "c",
		Variants: []*RoutingInfoVariant{vc1},
	}

	method := Method{
		Routing: []*RoutingInfo{a, b, c},
	}

	got := method.RoutingCombos()

	make_combo := func(va *RoutingInfoVariant, vb *RoutingInfoVariant, vc *RoutingInfoVariant) *RoutingInfoCombo {
		return &RoutingInfoCombo{
			Items: []*RoutingInfoComboItem{
				{
					Name:    "a",
					Variant: va,
				},
				{
					Name:    "b",
					Variant: vb,
				},
				{
					Name:    "c",
					Variant: vc,
				},
			},
		}
	}
	want := []*RoutingInfoCombo{
		make_combo(va1, vb1, vc1),
		make_combo(va1, vb2, vc1),
		make_combo(va2, vb1, vc1),
		make_combo(va2, vb2, vc1),
		make_combo(va3, vb1, vc1),
		make_combo(va3, vb2, vc1),
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("Incorrect routing combos (-want, +got):\n%s", diff)
	}
}
