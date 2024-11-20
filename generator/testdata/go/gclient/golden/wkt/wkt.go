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

package wkt

import (
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// `FieldMask` represents a set of symbolic field paths, for example:
type FieldMask struct {
	fieldmaskpb.FieldMask
}

func (f *FieldMask) UnmarshalJSON(b []byte) error {
	return protojson.Unmarshal(b, &f.FieldMask)
}

func (f *FieldMask) MarshalJSON() ([]byte, error) {
	return protojson.Marshal(&f.FieldMask)
}
