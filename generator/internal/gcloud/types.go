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

package gcloud

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

type RefString string

func (r RefString) MarshalYAML() (interface{}, error) {
	node := &yaml.Node{
		Kind:  yaml.ScalarNode,
		Tag:   "!REF",
		Value: string(r),
	}
	return node, nil
}

func (r *RefString) UnmarshalYAML(node *yaml.Node) error {
	if node.Tag == "!REF" || node.Tag == "!" {
		*r = RefString(node.Value)
		return nil
	}
	return fmt.Errorf("unexpected tag for RefString: %s", node.Tag)
}
