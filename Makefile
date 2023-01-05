PKG=github.com/cyverse/irods-rule-async-exec-cmd
VERSION=v0.2.6
GIT_COMMIT?=$(shell git rev-parse HEAD)
BUILD_DATE?=$(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS?="-X '${PKG}/commons.releaseVersion=${VERSION}' -X '${PKG}/commons.gitCommit=${GIT_COMMIT}' -X '${PKG}/commons.buildDate=${BUILD_DATE}'"
GO111MODULE=on
GOPROXY=direct
GOPATH=$(shell go env GOPATH)

.EXPORT_ALL_VARIABLES:

.PHONY: build
build:
	mkdir -p bin
	CGO_ENABLED=0 GOOS=linux go build -ldflags=${LDFLAGS} -o bin/irods-rule-async-exec-cmd ./client-cmd/
	CGO_ENABLED=0 GOOS=linux go build -ldflags=${LDFLAGS} -o bin/irods-rule-async-exec-cmd-svc ./server-cmd/

.PHONY: release
release: build
	mkdir -p release
	mkdir -p release/bin
	cp bin/irods-rule-async-exec-cmd release/bin
	cp bin/irods-rule-async-exec-cmd-svc release/bin
	cd release && tar zcvf ../irods-rule-async-exec-cmd.tar.gz *