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

package api

import "fmt"

// Element is an interface for all visitable API elements.
type Element interface {
	Accept(v *Visitor) error
}

// Visitor is an interface for visiting API elements, with each visitable element having its own Visit* method.
//
// Each node is visited before its children. Parent/child hierarchy is documented in api.API.
//
// Traversing the API with a visitor, even the NoOpVisitor, has the side effect of assigning the *.Parent and *.API
// fields to the visited elements. This side effect is idempotent.
//
// Note that as of now, not all API elements are visitable.
// This was done mostly because there hasn't been a use case for it yet.
type Visitor interface {
	VisitAPI(a *API) error
	VisitMessage(m *Message) error
	VisitField(f *Field) error
	VisitOneOf(o *OneOf) error
	VisitEnum(e *Enum) error
	VisitService(s *Service) error
	VisitMethod(m *Method) error
	VisitPathInfo(p *PathInfo) error
	VisitPathSegment(s *PathSegment) error
}

func (a *API) Accept(v Visitor) error {
	err := v.VisitAPI(a)
	if err != nil {
		return err
	}

	// First we visit the local types, as they are direct children of the API being visited.
	for _, m := range a.Messages {
		m.API = a
		err = m.Accept(v)
		if err != nil {
			return err
		}
	}
	// Then we visit the mixin types, as the easiest way to distinguish
	// them from the local types is that their API field should remain nil.
	for _, m := range a.State.MessageByID {
		if m.API == nil {
			err = m.Accept(v)
			if err != nil {
				return err
			}
		}
	}

	for _, e := range a.Enums {
		e.API = a
		err = e.Accept(v)
		if err != nil {
			return err
		}
	}
	for _, e := range a.State.EnumByID {
		if e.API == nil {
			err = e.Accept(v)
			if err != nil {
				return err
			}
		}
	}

	for _, s := range a.Services {
		s.API = a
		err = s.Accept(v)
		if err != nil {
			return err
		}
	}
	for _, s := range a.State.ServiceByID {
		if s.API == nil {
			err = s.Accept(v)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *Message) Accept(v Visitor) error {
	err := v.VisitMessage(m)
	if err != nil {
		return err
	}

	for _, f := range m.Fields {
		f.Parent = m
		err = f.Accept(v)
		if err != nil {
			return err
		}
	}

	for _, o := range m.OneOfs {
		o.Parent = m
		err = o.Accept(v)
		if err != nil {
			return err
		}
	}

	for _, nestedEnum := range m.Enums {
		nestedEnum.Parent = m
		nestedEnum.API = m.API // Nested types inherit the value of the API field from their parent. Even if it's nil.
		err = nestedEnum.Accept(v)
		if err != nil {
			return err
		}
	}

	for _, nestedMessage := range m.Messages {
		nestedMessage.Parent = m
		nestedMessage.API = m.API
		err = nestedMessage.Accept(v)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *Enum) Accept(v Visitor) error {
	return v.VisitEnum(e)
}

func (f *Field) Accept(v Visitor) error {
	return v.VisitField(f)
}

func (o *OneOf) Accept(v Visitor) error {
	return v.VisitOneOf(o)
}

func (s *Service) Accept(v Visitor) error {
	err := v.VisitService(s)
	if err != nil {
		return err
	}
	for _, m := range s.Methods {
		m.Parent = s
		err = m.Accept(v)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *Method) Accept(v Visitor) error {
	err := v.VisitMethod(m)
	if err != nil {
		return err
	}
	if m.PathInfo != nil {
		m.PathInfo.Method = m
		err = m.PathInfo.Accept(v)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *PathInfo) Accept(v Visitor) error {
	err := v.VisitPathInfo(p)
	if err != nil {
		return err
	}
	for _, s := range p.PathTemplate {
		s.Parent = p
		err = s.Accept(v)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *PathSegment) Accept(v Visitor) error {
	return v.VisitPathSegment(s)
}

// NoOpVisitor is a default Visitor implementation with no behavior.
type NoOpVisitor struct {
}

func (n NoOpVisitor) VisitAPI(a *API) error {
	return nil
}

func (n NoOpVisitor) VisitMessage(m *Message) error {
	return nil
}

func (n NoOpVisitor) VisitField(f *Field) error {
	return nil
}

func (n NoOpVisitor) VisitOneOf(o *OneOf) error {
	return nil
}

func (n NoOpVisitor) VisitEnum(e *Enum) error {
	return nil
}

func (n NoOpVisitor) VisitService(s *Service) error {
	return nil
}

func (n NoOpVisitor) VisitMethod(m *Method) error {
	return nil
}

func (n NoOpVisitor) VisitPathInfo(p *PathInfo) error {
	return nil
}

func (n NoOpVisitor) VisitPathSegment(s *PathSegment) error {
	return nil
}

// CrossReferencingVisitor is a Visitor used cross-reference API elements.
// This visitor needs to be invoked through the Traverse method, as it needs to keep track of the API being visited.
type CrossReferencingVisitor struct {
	NoOpVisitor
	API *API
}

func (v CrossReferencingVisitor) Traverse(a *API) error {
	v.API = a
	return a.Accept(v)
}

func (v CrossReferencingVisitor) VisitMethod(m *Method) error {
	var ok bool
	m.InputType, ok = v.API.State.MessageByID[m.InputTypeID]
	if !ok {
		return fmt.Errorf("unable to lookup input type %s", m.InputTypeID)
	}
	m.OutputType, ok = v.API.State.MessageByID[m.OutputTypeID]
	if !ok {
		return fmt.Errorf("unable to lookup output type %s", m.OutputTypeID)
	}
	return nil
}

func (v CrossReferencingVisitor) VisitMessage(m *Message) error {
	m.Elements = make(map[string]*MessageElement)
	for _, f := range m.Fields {
		if f.Typez == MESSAGE_TYPE {
			m.Elements[f.Name] = &MessageElement{Message: v.API.State.MessageByID[f.TypezID], Parent: m}
		} else if f.Typez == ENUM_TYPE {
			m.Elements[f.Name] = &MessageElement{Enum: v.API.State.EnumByID[f.TypezID], Parent: m}
		} else {
			m.Elements[f.Name] = &MessageElement{Field: f, Parent: m}
		}
	}
	for _, e := range m.Enums {
		m.Elements[e.Name] = &MessageElement{Enum: e, Parent: m}
	}
	for _, msg := range m.Messages {
		m.Elements[msg.Name] = &MessageElement{Message: msg, Parent: m}
	}
	for _, o := range m.OneOfs {
		m.Elements[o.Name] = &MessageElement{OneOf: o, Parent: m}
	}
	return nil
}

func (v CrossReferencingVisitor) VisitPathInfo(p *PathInfo) error {
	for _, segment := range p.PathTemplate {
		if segment.FieldPath != nil {
			// Every FieldPath starts pointing to the root input type Message
			ref := &MessageElement{
				Message: p.Method.InputType,
			}
			for _, component := range segment.FieldPath.Components {
				if ref.Message == nil {
					return fmt.Errorf("could not initialize FieldPath (%s), as the component (%s) does not map to any field in the enclosing type: %v", segment.FieldPath.String(), component.Identifier, ref)
				}
				enclosingType := ref.Message
				ref = enclosingType.Elements[component.Identifier]
				if ref == nil {
					return fmt.Errorf("could not find field \"%s\" in message %s", component.Identifier, enclosingType.Name)
				}
				component.Reference = ref
			}
		}
	}
	return nil
}
