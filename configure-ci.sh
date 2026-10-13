#!/bin/bash

set -euo pipefail

case "${1:-}" in
    "") vendored_libs=false ;;
    --vendored-libs) vendored_libs=true ;;
    *) echo "Usage: $0 [--vendored-libs]" >&2; exit 1 ;;
esac
if (( $# > 1 )); then
    echo "Usage: $0 [--vendored-libs]" >&2
    exit 1
fi

echo "Install libibverbs, librdmacm, clang, and cmake"
sudo apt-get update
packages=(libibverbs1 libibverbs-dev librdmacm1 librdmacm-dev clang cmake)
if "$vendored_libs"; then
    packages+=(build-essential pkg-config libnl-3-dev libnl-route-3-dev)
fi
sudo apt-get install -y --no-install-recommends "${packages[@]}"

if "$vendored_libs"; then
    echo "Build matching libibverbs, libmlx5, libefa, and librdmacm from vendored rdma-core"
    build_dir="$(pwd)/rdma-core-build"
    cmake -S ibverbs-sys/vendor/rdma-core -B "$build_dir" \
        -DIN_PLACE=1 -DNO_MAN_PAGES=1 -DNO_PYVERBS=1 -DCMAKE_BUILD_TYPE=Release
    cmake --build "$build_dir" -j"$(nproc)"
    lib_dir="$build_dir/lib"
    ls "$lib_dir"/libibverbs.so* "$lib_dir"/libmlx5.so* "$lib_dir"/libefa.so* \
        "$lib_dir"/librdmacm.so* "$lib_dir"/librxe-rdmav*.so
    export LD_LIBRARY_PATH="$lib_dir${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
    if [[ -n "${GITHUB_ENV:-}" ]]; then
        printf 'RDMA_CORE_LIB_DIR=%s\nLD_LIBRARY_PATH=%s\n' \
            "$lib_dir" "$LD_LIBRARY_PATH" >> "$GITHUB_ENV"
    fi
else
    echo "Configure vendored rdma-core headers for the system libraries"
    build_dir="$(pwd)/ibverbs-sys/vendor/rdma-core/build"
    cmake -S ibverbs-sys/vendor/rdma-core -B "$build_dir" \
        -DNO_MAN_PAGES=1 -DNO_PYVERBS=1 -DCMAKE_INSTALL_PREFIX=/usr
    lib_dir=/usr/lib/x86_64-linux-gnu
fi

echo "Configure Cargo with clang and a short target path"
mkdir -p /tmp/cargo .cargo
cat >.cargo/config.toml <<EOF
[build]
target-dir = "/tmp/cargo"

[env]
CC = "clang"
CXX = "clang++"
LD = "clang"
RDMA_CORE_INCLUDE_DIR = "$build_dir/include"
RDMA_CORE_LIB_DIR = "$lib_dir"
EOF
if "$vendored_libs"; then
    printf 'LD_LIBRARY_PATH = "%s"\n' "$LD_LIBRARY_PATH" >> .cargo/config.toml
fi
