#!/usr/bin/env bash
# This script sets up a simple SoftRoCE device to facilitate testing.

set -euxETo pipefail

# Configuration

# Define the name of the SoftRoCE device we would like to create for integration testing.
declare -r RXE_INTERFACE_NAME="rust_ibverbs"

# Print an error message
log() {
  >&2 printf "%s\n" "${*}"
}

# Determine if netns state of rdma devices is shared or exclusive
get_rdma_netns_state() {
  # Note: for some reason rdma system does not seem to support json output at the moment.
  # Thus we need to parse the output, can't use jq
  rdma system show | grep netns | awk '{print $NF}'
}

# Check that our rdma devices are available across network namespaces
confirm_rdma_netns_shared() {
  declare access
  access="$(get_rdma_netns_state)"
  declare -r access
  if [[ "${access}" != "shared" ]]; then
    log "rdma netns state is not shared: current state ${access}"
    return 1
  fi
}

# Set up a SoftRoCE loopback device.  To do this we create a dummy device and then hang a macvlan in bridge mode off of
# that dummy device.  This strategy allows all ordinary network traffic transmitted to the macvlan to be sent back to
# that macvlan.  We then add a rxe device to the macvlan so that RDMA traffic can be exchanged as well.
set_up_soft_roce_loopback_device() {
  declare -r DEVICE="${1}"
  modprobe rdma_rxe
  ip link add root type dummy
  ip link add link root name "${DEVICE}" type macvlan mode bridge
  rdma link add "${DEVICE}" type rxe netdev "${DEVICE}"
  log "Added SoftRoCE link ${DEVICE}"
  ip link set dev root up
  ip link set dev "${DEVICE}" up
}

# Get state of RDMA device supplied in first argument.
get_rdma_device_state() {
  rdma --json link | jq --raw-output --arg device "${1}" '.[] | select(.ifname == $device).state'
}

wait_for_rdma_device_to_be_ready() {
  declare -r DEVICE="${1}"
  declare -ri MAX_RETRY_COUNT=100
  declare -i RETRY_COUNT=0
  until [[ "$(get_rdma_device_state "${DEVICE}")" == "ACTIVE" ]]; do
    log "waiting for ${DEVICE} RDMA device to be active"
    RETRY_COUNT+=1
    if [[ "${RETRY_COUNT}" -gt "${MAX_RETRY_COUNT}" ]]; then
      log "Timed out waiting for RDMA device ${DEVICE} to be ready"
      return 1
    fi
    sleep 0.1
  done
}

main() {
  confirm_rdma_netns_shared
  set_up_soft_roce_loopback_device "${RXE_INTERFACE_NAME}"
  wait_for_rdma_device_to_be_ready "${RXE_INTERFACE_NAME}"
}

main