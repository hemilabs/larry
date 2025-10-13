# Copyright (c) 2025 Hemi Labs, Inc.
# Use of this source code is governed by the MIT License,
# which can be found in the LICENSE file.

.PHONY: all clean deps tidy build lint lint-deps test

all: tidy build lint test

clean:
	rm -rf ./bin/

deps: tidy
	go mod download
	go mod verify

tidy:
	go mod tidy

build:
	go build -trimpath -ldflags "-s -w $(GO_LDFLAGS)" ./larry

define LICENSE_HEADER
Copyright (c) {{.year}} Hemi Labs, Inc.
Use of this source code is governed by the MIT License,
which can be found in the LICENSE file.
endef
export LICENSE_HEADER

# Borrowed code
LICENSE_EXCLUDE := **/larry/**vendor**

lint:
	$(shell go env GOPATH)/bin/golangci-lint fmt ./...
	$(shell go env GOPATH)/bin/golangci-lint run --fix ./...
	$(shell go env GOPATH)/bin/golicenser -tmpl="$$LICENSE_HEADER" -author="Hemi Labs, Inc." -year-mode=git-range --exclude "$(LICENSE_EXCLUDE)" --fix ./...

lint-deps:
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.5
	go install github.com/joshuasing/golicenser/cmd/golicenser@v0.3

test:
	go test -test.timeout=20m ./...

race:
	go test -race -test.timeout=20m ./...
