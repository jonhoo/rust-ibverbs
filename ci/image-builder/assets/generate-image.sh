#!/usr/bin/env bash

set -euxETo pipefail

log_msg() {
  printf >&2 -- "%s\n" "${*}"
}

declare -r OUTPUT_LOCATION="/image/rust_ibverbs.img.zst"

# Get the current time (to the second) in iso-8601 format to use as a tag for
# this particular build
declare BUILD_TIME
BUILD_TIME="$(date --iso-8601=s --utc)"
declare -r BUILD_TIME

# Make a new raw disk file in memory to hold the generated image
declare IMAGE_FILE
IMAGE_FILE="$(mktemp -t --suffix=".rust_ibverbs.${BUILD_TIME}.img")"
declare -r IMAGE_FILE
trap 'rm "${IMAGE_FILE}" || log_msg "Failed to clean up!"' EXIT

# Adjust the image size as needed
declare -i CONTAINER_IMAGE_SIZE=0
CONTAINER_IMAGE_SIZE="$(docker image inspect rust_ibverbs_image --format='{{ .Size }}')"
declare -ri CONTAINER_IMAGE_SIZE
declare -ri BLOCK_SIZE=$((4 * 1024 ** 2))    # 4MiB block sizes
declare -ri SPARE_SPACE=$((4096 * 1024 ** 2)) # 4096MiB spare disc space in drive
declare -ri DISC_IMAGE_SIZE=$((CONTAINER_IMAGE_SIZE + SPARE_SPACE))
declare -ri IMAGE_BLOCK_COUNT=$((DISC_IMAGE_SIZE / BLOCK_SIZE))

# Allocate a block device to store the disc image in
truncate --size="${DISC_IMAGE_SIZE}" "${IMAGE_FILE}"
# we zero fill the image to make it compress nicely at the end
dd if=/dev/zero of="${IMAGE_FILE}" status=progress bs="${BLOCK_SIZE}" count="${IMAGE_BLOCK_COUNT}"

# Partition, format, and then mount the raw image
declare LOOP
LOOP="$(losetup --find --show "${IMAGE_FILE}")"
declare -r LOOP
# Update our exit trap to detach the loopback device before we rm it.
trap 'losetup --detach "${LOOP}" && rm "${IMAGE_FILE}" || log_msg "Failed to clean up!"' EXIT
parted \
  --script \
  --align optimal \
  "${LOOP}" \
    -- \
    mklabel gpt \
    mkpart primary 1049kB 2MB \
    name 1 BIOS \
    set 1 bios_grub on \
    mkpart primary 2MB 250MB \
    name 2 BOOT \
    set 2 esp on \
    set 2 boot on \
    mkpart primary 250MB -1 \
    name 3 ROOT \
    align-check opt 1 \
    align-check opt 2 \
    align-check opt 3

# refresh the kernel's partition table to make sure it has the changes we just made in parted
partprobe --summary "${LOOP}"
# sync all outstanding writes.  This shouldn't be needed but I don't like tempting fate.
sync

declare -r BOOT="${LOOP}p2"
declare -r ROOT="${LOOP}p3"

mkfs.fat -F32 "${BOOT}"
fatlabel "${BOOT}" BOOT
mkfs.ext4 "${ROOT}"
e2label "${ROOT}" ROOT
sync

# set up a root filesystem in a tmpfs to store our operating system
declare IMAGE_ROOT
IMAGE_ROOT="$(mktemp -t --directory --suffix=".rust_ibverbs.${BUILD_TIME}.rootfs")"
declare -r IMAGE_ROOT

# Mount our loopback device partitions in the needed locations in the rootfs
mount "${ROOT}" "${IMAGE_ROOT}"
mkdir --parent "${IMAGE_ROOT}/boot"
mount "${BOOT}" "${IMAGE_ROOT}/boot"

# Extract our image into the rootfs mount
declare CONTAINER_ID
CONTAINER_ID="$(docker create rust_ibverbs_image /bin/true)"
declare -r CONTAINER_ID
# now dump the operating system from the image-definition container into the root
docker export "${CONTAINER_ID}" | tar --extract --directory="${IMAGE_ROOT}"
log_msg "Syncing"
sync
docker rm "${CONTAINER_ID}"

# Docker messes with /etc/hosts when it creates and runs containers, so we need to replace it with what we need here, it
# can't be done from the image-definition Dockerfile.
cat <<EOF > "${IMAGE_ROOT}/etc/hosts"
127.0.0.1 localhost
::1 localhost
EOF

# Clean up
sync
umount --lazy --recursive "${IMAGE_ROOT}"
sync
rmdir "${IMAGE_ROOT}"

zstd --force --compress --sparse "${IMAGE_FILE}" -o "${OUTPUT_LOCATION}"
