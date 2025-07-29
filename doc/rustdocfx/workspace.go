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

package main

import (
	"encoding/json"
	"fmt"
	//"log/slog"
	"os"
	//"strings"
	"io/ioutil"
)

type crate struct {
	Name     string
	Version  string
	Location string
	Rustdoc  string
	Root     uint32
	Index    map[string]item
}

type item struct {
	Id   uint32
	Docs string
}

func getWorkspaceCrates(jsonBytes []byte) ([]crate, error) {
	var crates []crate
	err := json.Unmarshal(jsonBytes, &crates)
	if err != nil {
		return nil, fmt.Errorf("workspace crate unmarshal error: %v", err)
	}

	return crates, nil
}

func unmarshalRustdoc(crate *crate) {

	jsonFile, err := os.Open(crate.Rustdoc)
	if err != nil {
		// TODO: Exit early.
		fmt.Println(err)
	}
	// defer the closing of our jsonFile so that we can parse it later on
	defer jsonFile.Close()

	jsonBytes, _ := ioutil.ReadAll(jsonFile)

	// TODO(NOW): Handle error
	json.Unmarshal(jsonBytes, &crate)
	//if err != nil {
	//return nil, fmt.Errorf("workspace crate unmarshal error: %v", err)
	//}
}
