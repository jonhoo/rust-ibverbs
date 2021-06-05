#!/usr/bin/env bash
# This is the script run by CI.

set -euxETo pipefail

# Define the name of the SoftRoCE device we would like to create for integration testing.
declare -r RXE_INTERFACE_NAME="rust_ibverbs"

# We need to install linux-modules-extra for our current kernel or else we don't have access to the rdma_rxe module in
# CI.
sudo apt-get install --yes linux-modules-extra-"$(uname --kernel-release)"
sudo ./scripts/make-rdma-loopback.sh "${RXE_INTERFACE_NAME}"