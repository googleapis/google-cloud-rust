install:
	cargo install --path .

fmt:
	cd ../storage; cargo fmt

gen:
	discogen -i ./resources/test/storage-api.json -o ../storage/src

all: install gen fmt