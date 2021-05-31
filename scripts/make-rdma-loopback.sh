#!/usr/bin/env bash

set -euxETo pipefail

declare -r RXE_INTERFACE_NAME="rust_ibverbs"

# Print an error message
log_err() {
  >&2 echo "${*}"
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
    log_err "rdma netns state is not shared: current state ${access}"
    return 1
  fi
}

confirm_rdma_netns_shared
ip link add root type dummy
ip link add link root name "${RXE_INTERFACE_NAME}" type macvlan mode bridge
rdma link add "${RXE_INTERFACE_NAME}" type rxe netdev "${RXE_INTERFACE_NAME}"
ip link set group default up