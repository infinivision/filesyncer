#!/usr/bin/env bash

DIRS="cmd/faceclient cmd/faceserver cmd/server"
PKG_VERSION="github.com/infinivision/filesyncer/pkg/version"

GIT_SHA=$(git rev-parse --short HEAD || echo "GitNotFound")
BUILD_TIME=$(date --iso-8601=seconds)
for dir in ${DIRS}; do
	pushd ${dir}
    go build -ldflags "-X ${PKG_VERSION}.GitSHA=${GIT_SHA} -X ${PKG_VERSION}.BuildTime=${BUILD_TIME}"
  	popd
done
