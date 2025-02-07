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

import "fmt"

// CrossReference fills out the cross-references in `model` that the parser(s)
// missed.
//
// The parsers cannot always cross-reference all elements because the
// elements are built incrementally, and may not be available until the parser
// has completed all the work.
//
// This function is called after the parser has completed its work but before
// the codecs run. It populates links between the parsed elements that the
// codecs need. For example, the `oneof` fields use the containing `OneOf` to
// reference any types or names of the `OneOf` during their generation.
func CrossReference(model *API) error {
	for _, m := range model.State.MessageByID {
		for _, o := range m.OneOfs {
			for _, f := range o.Fields {
				f.Group = o
			}
		}
	}
	for _, m := range model.State.MethodByID {
		input, ok := model.State.MessageByID[m.InputTypeID]
		if !ok {
			return fmt.Errorf("cannot find input type %s for method %s", m.InputTypeID, m.ID)
		}
		output, ok := model.State.MessageByID[m.OutputTypeID]
		if !ok {
			return fmt.Errorf("cannot find output type %s for method %s", m.OutputTypeID, m.ID)
		}
		m.InputType = input
		m.OutputType = output
		if m.OperationInfo != nil {
			m.OperationInfo.Method = m
		}
	}
	for _, s := range model.State.ServiceByID {
		s.Model = model
		for _, m := range s.Methods {
			m.Model = model
			m.Service = s
		}
	}
	return nil
}
