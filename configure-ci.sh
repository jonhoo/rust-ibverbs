#!/bin/bash

set -euo pipefail

echo "Install libibverbs dependencies"
sudo apt-get update
# librdmacm is needed to build and link the optional `rdmacm` feature.
sudo apt-get install libibverbs1 libibverbs-dev librdmacm1 librdmacm-dev

echo "Install clang (for bindgen) and cmake (to generate the vendored rdma-core headers)"
sudo apt-get install clang cmake

echo "Move Cargo target dir to avoid super long paths"
mkdir /tmp/cargo

# Generate the bindings against the vendored rdma-core submodule (the headers we actually ship) but
# link against the system libibverbs. rdma-core publishes its header tree at cmake *configure* time,
# so configuring the submodule (without building it) is enough to produce the include tree. This
# keeps us testing the real submodule headers while avoiding building the vendored shared library,
# whose link step is sensitive to the host toolchain and breaks under some clang/cmake versions.
echo "Configure the vendored rdma-core to generate its headers"
cmake -S ibverbs-sys/vendor/rdma-core -B ibverbs-sys/vendor/rdma-core/build \
    -DNO_MAN_PAGES=1 -DCMAKE_INSTALL_PREFIX=/usr

echo "Tell Cargo to use clang and the target dir, with vendored headers and the system library"
mkdir .cargo
cat >.cargo/config.toml <<EOF
[build]
target-dir = "/tmp/cargo"

[env]
CC = "clang"
CXX = "clang++"
LD = "clang"
RDMA_CORE_INCLUDE_DIR = "$(pwd)/ibverbs-sys/vendor/rdma-core/build/include"
RDMA_CORE_LIB_DIR = "/usr/lib/x86_64-linux-gnu"
EOF
