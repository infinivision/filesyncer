#!/usr/bin/env bash

DIRS="cmd/faceserver cmd/server cmd/fetchImgs cmd/cpRedisQue cmd/replayVisits"
DIRS_ARM="cmd/faceclient"
PKG_VERSION="github.com/infinivision/filesyncer/pkg/version"

GIT_SHA=$(git rev-parse --short HEAD || echo "GitNotFound")
BUILD_TIME=$(date --iso-8601=seconds)
for dir in ${DIRS}; do
    pushd ${dir}
    GO111MODULE=on go build -ldflags "-X ${PKG_VERSION}.GitSHA=${GIT_SHA} -X ${PKG_VERSION}.BuildTime=${BUILD_TIME}"
    popd
done

for dir in ${DIRS_ARM}; do
    pushd ${dir}
    GO111MODULE=on GOOS=linux GOARCH=arm64 GOARM=7 go build -ldflags "-X ${PKG_VERSION}.GitSHA=${GIT_SHA} -X ${PKG_VERSION}.BuildTime=${BUILD_TIME}"
    popd
done
