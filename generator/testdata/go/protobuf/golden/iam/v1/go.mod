module github.com/google-cloud-rust/generator/testdata/go/protobuf/golden/iam/v1

go 1.23.2

replace github.com/google-cloud-rust/generator/testdata/go/protobuf/golden/typez => ../../typez

replace github.com/google-cloud-rust/generator/testdata/go/protobuf/golden/wkt => ../../wkt

require (
	cloud.google.com/go/auth v0.14.0
	github.com/google-cloud-rust/generator/testdata/go/protobuf/golden/typez v0.0.0-00010101000000-000000000000
	github.com/google-cloud-rust/generator/testdata/go/protobuf/golden/wkt v0.0.0-00010101000000-000000000000
)

require (
	cloud.google.com/go/compute/metadata v0.6.0 // indirect
	github.com/googleapis/gax-go/v2 v2.14.0 // indirect
	golang.org/x/sys v0.28.0 // indirect
	google.golang.org/protobuf v1.36.0 // indirect
)
