#!/usr/bin/env bash

set -euxETo pipefail

declare build_dir
build_dir="$(readlink --canonicalize-existing "$(dirname "${0}")")"
declare -r build_dir

pushd "${build_dir}"
"${build_dir}/image-definition/make-container.sh"
"${build_dir}/image-builder/make-container.sh"
popd

# TODO: determine if this container really needs to be privileged.
docker run \
 --interactive \
 --privileged \
 --rm \
 --tty \
 --mount "type=tmpfs,destination=/tmp" \
 --mount "type=bind,source=${build_dir},destination=/image" \
 --mount "type=bind,source=/var/run/docker.sock,destination=/var/run/docker.sock" \
 --name="rust_ibverbs_image_builder" \
 rust_ibverbs_image_builder
