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

package parser

import "github.com/googleapis/google-cloud-rust/generator/internal/api"

const (
	pageSize      = "pageSize"
	maxResults    = "maxResults"
	pageToken     = "pageToken"
	nextPageToken = "nextPageToken"
)

// updateMethodPagination marks all methods that conform to
// [AIP-4233](https://google.aip.dev/client-libraries/4233) as pageable.
func updateMethodPagination(a *api.API) {
	for _, m := range a.State.MethodByID {
		if m.InputTypeID == "" || m.OutputTypeID == "" {
			continue
		}

		reqMsg := a.State.MessageByID[m.InputTypeID]
		if reqMsg == nil {
			continue
		}
		var hasPageSize bool
		var hasPageToken *api.Field
		for _, f := range reqMsg.Fields {
			// Some legacy services (e.g. sqladmin.googleapis.com)
			// predate AIP-4233 and use `maxResults` instead of
			// `pageSize` for the field name.
			// Furthermore, some of these services use both
			// `uint32` and `int32` for the `maxResults` field type.
			switch f.JSONName {
			case pageSize:
				if f.Typez == api.INT32_TYPE {
					hasPageSize = true
				}
			case maxResults:
				if f.Typez == api.INT32_TYPE || f.Typez == api.UINT32_TYPE {
					hasPageSize = true
				}
			}
			if f.JSONName == pageToken && f.Typez == api.STRING_TYPE {
				hasPageToken = f
			}
			if hasPageSize && hasPageToken != nil {
				break
			}
		}
		if !(hasPageSize && hasPageToken != nil) {
			continue
		}

		respMsg := a.State.MessageByID[m.OutputTypeID]
		if respMsg == nil {
			continue
		}
		var hasNextPageToken bool
		var hasRepeatedItem bool
		info := api.PaginationInfo{}
		for _, f := range respMsg.Fields {
			if f.JSONName == nextPageToken && f.Typez == api.STRING_TYPE {
				hasNextPageToken = true
				info.NextPageToken = f
			}
			if f.Repeated && f.Typez == api.MESSAGE_TYPE {
				hasRepeatedItem = true
				info.PageableItem = f
			}
			if hasNextPageToken && hasRepeatedItem {
				break
			}
		}
		if !(hasNextPageToken && hasRepeatedItem) {
			continue
		}
		m.Pagination = hasPageToken
		respMsg.Pagination = &info
	}
}
