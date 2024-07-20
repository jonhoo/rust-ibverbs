#!/bin/bash

set -euo pipefail

echo "Install libibverbs dependencies"
sudo apt-get update
sudo apt-get install libibverbs1 libibverbs-dev

echo "Install clang for rdma-core to be happy"
sudo apt-get install clang

echo "Move Cargo target dir to avoid super long paths"
mkdir /tmp/cargo

echo "Tell Cargo to use clang and target dir"
cat >.cargo <<EOF
[build]
target-dir = "/tmp/cargo"

[env]
CC = "clang"
CXX = "clang++"
LD = "clang"
EOF
