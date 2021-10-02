#!/usr/bin/env bash

set -euxETo pipefail

declare build_dir
build_dir="$(readlink --canonicalize-existing "$(dirname "${0}")")"
declare -r build_dir

pushd "${build_dir}"
docker buildx build --tag=rust_ibverbs_image_debian "${build_dir}"
docker buildx build -f Dockerfile.alpine --tag=rust_ibverbs_image_alpine "${build_dir}"
popd

