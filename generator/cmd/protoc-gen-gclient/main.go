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

package main

import (
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/googleapis/google-cloud-rust/generator/internal/genclient"
	"github.com/googleapis/google-cloud-rust/generator/internal/genclient/translator/protobuf"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/pluginpb"
)

func main() {
	inputPath := flag.String("input-path", "", "the path to a binary input file in the format of pluginpb.CodeGeneratorRequest")
	outDir := flag.String("out-dir", "", "the path to the output directory")
	templateDir := flag.String("template-dir", "templates/", "the path to the template directory")
	flag.Parse()

	if err := run(*inputPath, *outDir, *templateDir); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
	slog.Info("Generation Completed Successfully")
}

func run(inputPath, outDir, templateDir string) error {
	var reqBytes []byte
	var err error
	if inputPath == "" {
		reqBytes, err = io.ReadAll(os.Stdin)
		if err != nil {
			return err
		}
	} else {
		reqBytes, err = os.ReadFile(inputPath)
		if err != nil {
			return err
		}
	}

	genReq := &pluginpb.CodeGeneratorRequest{}
	if err := proto.Unmarshal(reqBytes, genReq); err != nil {
		return err
	}

	opts, err := parseOpts(genReq.GetParameter())
	if err != nil {
		return err
	}

	if opts.CaptureInput {
		// Remove capture-input param from the captured input
		ss := slices.DeleteFunc(strings.Split(genReq.GetParameter(), ","), func(s string) bool {
			return strings.Contains(s, "capture-input")
		})
		genReq.Parameter = proto.String(strings.Join(ss, ","))
		reqBytes, err = proto.Marshal(genReq)
		if err != nil {
			return err
		}
		if err := os.WriteFile(fmt.Sprintf("sample-input-%s.bin", time.Now().Format(time.RFC3339)), reqBytes, 0644); err != nil {
			return err
		}
	}

	req, err := protobuf.NewTranslator(&protobuf.Options{
		Request:     genReq,
		OutDir:      outDir,
		Language:    opts.Language,
		TemplateDir: templateDir,
	}).Translate()
	if err != nil {
		return err
	}

	resp := protobuf.NewCodeGeneratorResponse(genclient.Generate(req))
	outBytes, err := proto.Marshal(resp)
	if err != nil {
		return err
	}
	if _, err := os.Stdout.Write(outBytes); err != nil {
		return err
	}

	return nil
}

type protobufOptions struct {
	CaptureInput bool
	Language     string
}

func parseOpts(optStr string) (*protobufOptions, error) {
	opts := &protobufOptions{}
	for _, s := range strings.Split(strings.TrimSpace(optStr), ",") {
		if s == "" {
			slog.Warn("empty option string, skipping")
			continue
		}
		sp := strings.Split(s, "=")
		if len(sp) > 2 {
			slog.Warn("too many `=` in option string, skipping", "option", s)
			continue
		}
		switch sp[0] {
		case "capture-input":
			b, err := strconv.ParseBool(sp[1])
			if err != nil {
				slog.Error("invalid bool in option string, skipping", "option", s)
				return nil, err
			}
			opts.CaptureInput = b
		case "language":
			opts.Language = strings.ToLower(strings.TrimSpace(sp[1]))
		default:
			slog.Warn("unknown option", "option", s)
		}
	}
	return opts, nil
}
