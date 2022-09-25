#!/usr/bin/env bash

set -euxETo pipefail

declare -x DEBIAN_FRONTEND=noninteractive
declare -r IMAGE_DEVICE="${1}"

apt-get update

# Do not configure grub during package install
printf 'grub-pc grub-pc/install_devices_empty select true\n' | debconf-set-selections
printf 'grub-pc grub-pc/install_devices select\n' | debconf-set-selections

# Install various packages needed for a booting system
apt-get install --yes --no-install-recommends \
	grub2 \
	locales

# Set the locale to en_US.UTF-8
locale-gen --purge en_US.UTF-8
printf 'LANG="en_US.UTF-8"\nLANGUAGE="en_US:en"\n' > /etc/default/locale
locale-gen

# Install GRUB (can't currently seem to do UEFI in AWS)
grub-probe /
grub-install "${IMAGE_DEVICE}"

# Configure and update GRUB
mkdir -p /etc/default/grub.d
cat <<EOF > /etc/default/grub.d/50-aws-settings.cfg
GRUB_RECORDFAIL_TIMEOUT=0
GRUB_TIMEOUT=0
GRUB_CMDLINE_LINUX_DEFAULT="root=LABEL=ROOT rw console=tty0 earlyprintk=tty0 console=ttyS0,115200 earlyprintk=ttyS0,115200 scsi_mod.use_blk_mq=Y intel_iommu=on iommu=pt"
GRUB_TERMINAL=console
EOF

update-grub

# Set options for the default interface
cat <<EOF >> /etc/network/interfaces
auto eth0
iface eth0 inet dhcp
EOF
