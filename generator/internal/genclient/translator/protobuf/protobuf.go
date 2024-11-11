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

package protobuf

import (
	"github.com/googleapis/google-cloud-rust/generator/internal/genclient"
	"github.com/googleapis/google-cloud-rust/generator/internal/genclient/language"
	parser "github.com/googleapis/google-cloud-rust/generator/internal/genclient/parser/protobuf"
	"google.golang.org/genproto/googleapis/api/serviceconfig"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/pluginpb"
)

type Options struct {
	Language string
	// Only used for local testing
	OutDir        string
	TemplateDir   string
	ServiceConfig *serviceconfig.Service
}

// Translate translates proto representation into a [genclienGenerateRequest].
func Translate(req *pluginpb.CodeGeneratorRequest, opts *Options) (*genclient.GenerateRequest, error) {
	codec, err := language.NewCodec(opts.Language)
	if err != nil {
		return nil, err
	}
	return &genclient.GenerateRequest{
		API:         parser.MakeAPI(opts.ServiceConfig, req),
		Codec:       codec,
		OutDir:      opts.OutDir,
		TemplateDir: opts.TemplateDir,
	}, nil
}

func NewCodeGeneratorResponse(_ *genclient.Output, err error) *pluginpb.CodeGeneratorResponse {
	resp := &pluginpb.CodeGeneratorResponse{}
	if err != nil {
		resp.Error = proto.String(err.Error())
	}
	return resp
}
