#!/usr/bin/env bash
# This is the script run by CI.

set -euxETo pipefail

# We need to install linux-modules-extra for our current kernel or else we don't have access to the rdma_rxe module in
# CI.
sudo apt-get install --yes linux-modules-extra-"$(uname --kernel-release)"
sudo ./scripts/make-rdma-loopback.sh "${RXE_INTERFACE_NAME}"
