#!/bin/bash
set -ev

sudo apt install python3-pip
sudo pip3 install protobuf
sudo pip3 install tensorflow
PROTOBUF_VERSION=3.6.0
PROTOC_FILENAME=protoc-${PROTOBUF_VERSION}-linux-x86_64.zip
pushd /home/travis
wget https://github.com/google/protobuf/releases/download/v${PROTOBUF_VERSION}/${PROTOC_FILENAME}
unzip ${PROTOC_FILENAME}
bin/protoc --version
popd
