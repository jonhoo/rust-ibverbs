#!/usr/bin/env bash
# Install the generated image to the EBS volume

set -euxETo pipefail

declare -rx DEBIAN_FRONTEND="noninteractive"

declare -r IMAGE_DEVICE="/dev/xvdf"

sudo apt-get update
sudo apt-get install --yes --no-install-recommends \
  coreutils `#needed for chroot` \
  e2fsprogs `#needed to resize root filesystem` \
  gdisk     `#needed to resize root partition` \
  parted    `#needed to partprobe image after transfer` \
  zstd      `#needed to decompress system image`
sudo zstd --decompress --force -o "${IMAGE_DEVICE}" /tmp/rust_ibverbs.img.zst
sync

sudo partprobe --summary

declare -ri ROOT_DEVICE_PARTITION_NUMBER=3
declare -ri BOOT_DEVICE_PARTITION_NUMBER=2
declare -r ROOT_DEVICE="${IMAGE_DEVICE}${ROOT_DEVICE_PARTITION_NUMBER}"
declare -r BOOT_DEVICE="${IMAGE_DEVICE}${BOOT_DEVICE_PARTITION_NUMBER}"

# Resize the root partition to take all available space.
sync
sudo sgdisk --move-second-header "${IMAGE_DEVICE}"
sync
sudo sgdisk --delete="${ROOT_DEVICE_PARTITION_NUMBER}" "${IMAGE_DEVICE}"
sync
sudo sgdisk --largest-new="${ROOT_DEVICE_PARTITION_NUMBER}" "${IMAGE_DEVICE}"
sync
sudo sgdisk --change-name="${ROOT_DEVICE_PARTITION_NUMBER}":root "${IMAGE_DEVICE}"
sync
sudo sgdisk --move-second-header "${IMAGE_DEVICE}"
sync
sudo partprobe --summary
sync
sudo e2fsck -v -f "${ROOT_DEVICE}" || true
sync
sudo e2fsck -v -f "${ROOT_DEVICE}"
sync
sudo resize2fs "${ROOT_DEVICE}"
sync
sudo partprobe --summary
sync

declare CHROOT
CHROOT="$(sudo mktemp -t --directory --suffix=".rust_ibverbs.rootfs")"
declare -r CHROOT

sudo mount "${ROOT_DEVICE}" "${CHROOT}"
sudo mount "${BOOT_DEVICE}" "${CHROOT}/boot"

# Prepare our chroot with necessary bind mounts
sudo mount -t proc /proc "${CHROOT}"/proc
sudo mount --rbind /sys "${CHROOT}"/sys
sudo mount --rbind /dev/ "${CHROOT}"/dev
sudo mount --make-rslave "${CHROOT}"
sudo mount -t tmpfs -o size=128M tmpfs "${CHROOT}/tmp"

sudo cp /tmp/provision-image.sh "${CHROOT}/tmp/provision-image.sh"
sudo mv "${CHROOT}/etc/resolv.conf" "${CHROOT}/etc/resolv.conf.orig"
sudo cp /etc/resolv.conf "${CHROOT}/etc/resolv.conf"
sudo chmod +x "${CHROOT}/tmp/provision-image.sh"

sudo chroot "${CHROOT}" /tmp/provision-image.sh "${IMAGE_DEVICE}"
sync
sudo mv "${CHROOT}/etc/resolv.conf.orig" "${CHROOT}/etc/resolv.conf"
sync