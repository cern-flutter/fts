#!/bin/bash
#
# Generate protocol buffers code from the .proto files
#

# Validate installation of protocol buffers
protoc=`which protoc`
if [[ $? -ne 0 ]]; then
    echo "Can't find protoc on the PATH"
    exit 1
fi

version=`${protoc} --version | cut -d' ' -f 2`
if [[ "$version" != 3.* ]]; then
    echo "Require protoc version 3, got ${version}"
    exit 1;
fi

echo "Using ${protoc} version ${version}"

# Generate code for Python
mkdir -p "messages"
${protoc} -I="protobuf" --go_out="messages" protobuf/*.proto
