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
	"fmt"
	"strings"

	"github.com/googleapis/google-cloud-rust/generator/internal/config"
)

func PatchDocumentation(model *API, config *config.Config) error {
	for _, override := range config.CommentOverrides {
		id := override.ID
		if msg, ok := model.State.MessageByID[id]; ok {
			if err := patchElementDocs(&msg.Documentation, &override); err != nil {
				return err
			}
			continue
		}
		if enu, ok := model.State.EnumByID[id]; ok {
			if err := patchElementDocs(&enu.Documentation, &override); err != nil {
				return err
			}
			continue
		}
		if svc, ok := model.State.ServiceByID[id]; ok {
			if err := patchElementDocs(&svc.Documentation, &override); err != nil {
				return err
			}
			continue
		}
		idx := strings.LastIndex(id, ".")
		if idx == -1 {
			return fmt.Errorf("cannot find element %s to apply comment overrides", id)
		}
		parentId := id[0:idx]
		childId := id[idx+1:]
		if msg, ok := model.State.MessageByID[parentId]; ok {
			if err := patchFieldDocs(msg, childId, &override); err != nil {
				return err
			}
			continue
		}
		if enu, ok := model.State.EnumByID[parentId]; ok {
			if err := patchEnumValueDocs(enu, childId, &override); err != nil {
				return err
			}
			continue
		}
		if svc, ok := model.State.ServiceByID[parentId]; ok {
			if err := patchMethodDocs(svc, childId, &override); err != nil {
				return err
			}
			continue
		}
		return fmt.Errorf("cannot find element %s to apply comment overrides, only searched for messages, enums and services", id)
	}
	return nil
}

func patchFieldDocs(msg *Message, fieldName string, override *config.DocumentationOverride) error {
	for _, field := range msg.Fields {
		if field.Name != fieldName {
			continue
		}
		if err := patchElementDocs(&field.Documentation, override); err != nil {
			return err
		}
		return nil
	}
	return fmt.Errorf("cannot find field %s in message %s to apply comment override", fieldName, msg.ID)
}

func patchEnumValueDocs(enu *Enum, name string, override *config.DocumentationOverride) error {
	for _, v := range enu.Values {
		if v.Name != name {
			continue
		}
		if err := patchElementDocs(&v.Documentation, override); err != nil {
			return err
		}
		return nil
	}
	return fmt.Errorf("cannot find field %s in message %s to apply comment override", name, enu.ID)
}

func patchMethodDocs(svc *Service, name string, override *config.DocumentationOverride) error {
	for _, m := range svc.Methods {
		if m.Name != name {
			continue
		}
		if err := patchElementDocs(&m.Documentation, override); err != nil {
			return err
		}
		return nil
	}
	return fmt.Errorf("cannot find field %s in message %s to apply comment override", name, svc.ID)
}

func patchElementDocs(documentation *string, override *config.DocumentationOverride) error {
	new := strings.Replace(*documentation, override.Match, override.Replace, -1)
	if *documentation == new {
		return fmt.Errorf("comment override for %s did not match", override.ID)
	}
	*documentation = new
	return nil
}
