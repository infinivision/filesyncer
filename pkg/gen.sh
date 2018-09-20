#!/bin/bash
#
# Generate all elasticell protobuf bindings.
# Run from repository root.
#
set -e

PRJ="filesyncer"

# directories containing protos to be built
DIRS="./pb"

GOGOPROTO_ROOT="$(go env GOPATH)/src/github.com/gogo/protobuf"
GOGOPROTO_PATH="${GOGOPROTO_ROOT}:${GOGOPROTO_ROOT}/protobuf"
INFINIVISION_PB_PATH="$(go env GOPATH)/src/github.com/infinivision/${PRJ}/pkg"

for dir in ${DIRS}; do
	pushd ${dir}
		protoc --gofast_out=plugins=grpc:. -I=.:"${GOGOPROTO_PATH}":"${INFINIVISION_PB_PATH}":"$(go env GOPATH)/src" *.proto
		sed -i.bak -E 's/import _ \"gogoproto\"//g' *.pb.go
		rm -f *.bak
		goimports -w *.pb.go
	popd
done
