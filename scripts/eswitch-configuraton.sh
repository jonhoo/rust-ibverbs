#!/usr/bin/env bash

set -euxETo pipefail

# Configuration

# Apply a function to each line of stdin
map() {
  declare -i escape_output=0
  while [[ ${#} -gt 0 ]]; do
    case "${1}" in
    --escape-output | -e)
      escape_output=1
      shift
      ;;
    --)
      shift
      break
      ;;
    *)
      break
      ;;
    esac
  done
  declare -ri escape_output
  declare -ar input_args=("${@}")
  if [[ "${escape_output}" -eq 0 ]]; then
    __mapfile_adapter__() {
      "${input_args[@]}" "${@:2}"
    }
  else
    __mapfile_adapter__() {
      "${input_args[@]}" "${@:2}" | jq --slurp --raw-input 'rtrimstr("\n")'
    }
  fi
  # shellcheck disable=SC2034
  mapfile -t -c 1 -C __mapfile_adapter__
}

# convert a newline delimited list to a json array
to_json() {
  jq --slurp --raw-input 'rtrimstr("\n") | split("\n")'
}

# convert a json array to a newline delimited list
to_lines() {
  jq --raw-output '.[] | tostring'
}

# convert a newline delimited list to a json sequence
lines_json_seq() {
  jq --raw-input --seq
}

# convert a json sequence to a newline delimited list
json_seq_to_lines() {
  jq --seq --slurp --raw-input 'split("\n")[:-1]'
}

# apply a transform to each line of stdin and form a json object associating each input to its output
associate() {
  declare -ar args=("${@}")
  declare inputs
  inputs="$(cat)"
  declare -r inputs
  map --escape-output "${args[@]}" <<<"${inputs}" |
    jq --slurp --slurpfile input <(to_json <<<"${inputs}") \
      '[$input[], .] | transpose | map({(.[0] | tostring): .[1]}) | add'
}

list_devlink_devs() {
  devlink --json --no-nice-names dev show | jq --compact-output '.dev | keys'
}

get_interfaces_for_devlink() {
  devlink --no-nice-names --json --verbose port show |
    jq --arg devlink_dev "${1}" --compact-output '
        .port
        | to_entries
        | map(select(.value | has("netdev"))
        | select(.key | test($devlink_dev)))
        | from_entries
        | {($devlink_dev): .}
      '
}

name_interfaces_by_switch_port() {
  ip -detail -json address |
    jq --raw-output --compact-output '
        .[]
        | select(has("phys_switch_id"))
        | {current_name: .ifname, desired_name: .phys_port_name}
      ' |
    map rename_interface
}

group_interfaces_by_devlink() {
  list_devlink_devs |
    jq --raw-output '.[]' |
    map get_interfaces_for_devlink |
    jq --compact-output --slurp 'add'
}

rename_interface() {
  declare -r input="${*}"
  declare desired_name
  desired_name="$(jq --compact-output --raw-output '.desired_name' <<<"${input}")"
  declare -r desired_name
  declare current_name
  current_name="$(jq --compact-output --raw-output '.current_name' <<<"${input}")"
  declare -r current_name
  ip link set dev "${current_name}" down
  ip link set dev "${current_name}" name "${desired_name}"
  ip link property add altname "${current_name}" "${desired_name}"
  ip link set dev "${desired_name}" up
}

group_interfaces_by_phys_switch_id() {
  ip -detail -json address | jq '
    [
      .[]
      |
      select(has("phys_switch_id"))
    ]
    |
    [
      group_by(.phys_switch_id)[]
      |
      {(.[0].phys_switch_id): .}
    ]
    |
    add
  '
}

rebind_switchdev() {
  group_interfaces_by_phys_switch_id |
    jq --raw-output 'keys[]' |
    map ip netns add
}

list_interfaces_by_devlink() {
  group_interfaces_by_devlink | jq '
    [
      to_entries[]
      | {key: (.key), value: (.value | to_entries[].value.netdev)}
    ]
    | group_by(.key)[] | {(.[0].key): map(.value)}
  ' | jq --slurp 'add'
}

list_interfaces_by_phys_switch_id() {
  group_interfaces_by_phys_switch_id |
    jq '
      to_entries[]
      | {(.value[0].phys_switch_id): [.value[].ifname]}
    ' |
    jq --slurp add
}

group_devlink_devs_by_phys_switch_id() {
  declare iface_to_devlink
  iface_to_devlink="$(
    list_interfaces_by_devlink |
      jq '[to_entries[] | {(.value[]): (.key)} | to_entries[]] | from_entries'
  )"
  declare -r iface_to_devlink
  declare iface_to_phys_switch_id
  iface_to_phys_switch_id="$(
    ip -detail -json addr |
      jq '[.[] | {key: (.ifname), value: (.phys_switch_id)} | select(.value!=null)] | from_entries'
  )"
  declare -r iface_to_phys_switch_id
  jq --null-input \
    --slurpfile iface_to_devlink <(echo "${iface_to_devlink}") \
    --slurpfile iface_to_phys_switch_id <(echo "${iface_to_phys_switch_id}") \
    '
      $iface_to_devlink[]
      | to_entries[]
      | {key: (.key), value: {devlink: (.value), phys_switch_id: ($iface_to_phys_switch_id[0][(.key)])}}
    ' |
    jq --slurp 'group_by(.value.phys_switch_id)[] | {(.[0].value.phys_switch_id): map(.value.devlink) | unique}'
}

list_phys_switch_ids() {
  ip -detail -json addr |
    jq '[.[].phys_switch_id | select(.!=null)] | unique'
}

# decide if arg 2 is contained in the path described by arg 1.
contains() {
  declare -r parent="${1}"
  declare -r possible_child="${2}"
  declare possible_child_dirname
  possible_child_dirname="$(dirname "${possible_child}")"
  declare -r possible_child_dirname
  if [[ ${possible_child_dirname} == "${parent}" ]]; then
    return 0
  elif [[ "$(dirname "${possible_child}")" != "/" ]]; then
    contains "${parent}" "${possible_child_dirname}"
  else
    return 1
  fi
}

list_pci_drivers() {
  for file in "/sys/bus/pci/drivers/${driver}"/*; do
    [[ -d ${file} ]] && basename "${file}"
  done | to_json
}

list_pci_devices_for_driver() {
  declare -r driver="${1}"
  if [[ ! -d "/sys/bus/pci/drivers/${driver}" ]]; then
    echo >&2 "driver ${driver} not listed under /sys/bus/pci/drivers"
    return 1
  fi

  for file in "/sys/bus/pci/drivers/${driver}"/*; do
    [[ -L ${file} ]] &&
      contains /sys/devices "$(readlink --canonicalize-existing "${file}")" &&
      basename "${file}" || true
  done | to_json
}

cpu_info() {
  lscpu --json | jq '.lscpu[] | {key: .field, value: .data}' | jq --slurp 'from_entries'
}

activate_switchdev_mode() {
  declare -r device="${1}"
  devlink dev eswitch set "${device}" mode switchdev encap-mode basic
}

deactivate_switchdev_mode() {
  declare -r device="${1}"
  devlink dev eswitch set "${device}" mode legacy
}

set_number_of_sriov_devices_for_interface() {
  declare -ri number_of_devices="${1}"
  declare -r physical_function="${2}"
  echo "${number_of_devices}" >"/sys/class/net/${physical_function}/device/sriov_numvfs"
}

set_number_of_sriov_devices_for_pci() {
  declare -r driver="${1}"
  declare -ri number_of_devices="${2}"
  declare -r pci_address="${3}"
  echo "${number_of_devices}" >"/sys/bus/pci/drivers/${driver}/${pci_address}/sriov_numvfs"
}

unbind_sriov_device_from_driver() {
  declare -r device="${1}"
  declare driver_path
  driver_path="$(readlink --canonicalize-existing "/sys/class/net/${device}/device/driver")"
  declare -r driver_path
  declare pci_address
  pci_address="$(basename "$(readlink --canonicalize-existing "/sys/class/net/${device}/device")")"
  declare -r pci_address
  echo "${pci_address}" >"${driver_path}/unbind"
}

unbind_pci_address_from_driver() {
  declare -r driver="${1}"
  declare -r pci_address="${2}"
  echo "${pci_address}" >"/sys/bus/pci/drivers/${driver}/unbind"
}

bind_sriov_device_to_mlx5_driver() {
  declare -r pci_address="${1}"
  echo "${pci_address}" >"/sys/bus/pci/drivers/mlx5_core/bind"
}

pci_device_to_numa_node() {
  declare -r PCI_DEVICE="${1}"
  cat "/sys/bus/pci/devices/${PCI_DEVICE}/numa_node"
}

pci_device_to_network_interfaces() {
  declare -r PCI_DEVICE="${1}"
  for nic in "/sys/bus/pci/devices/${PCI_DEVICE}/net"/*; do
    basename "${nic}"
  done | to_json
}

enable_hw_tc_offload() {
  declare -r device="${1}"
  ethtool --offload "${device}" hw-tc-offload on
}

wait_for_sriov_devices_unbind() {
  declare -r pcie_address="${1}"
  declare -r sys_symlink_file="/sys/bus/pci/drivers/mlx5_core/${pcie_address}"
  until [[ ! -L "${sys_symlink_file}" ]]; do
    inotifywait --event delete_self --timeout 1 "${sys_symlink_file}" || true
  done
}

main() {
  udevadm --debug settle --timeout=90
  sync --file-system /sys

  # Get pcie devices attached to mlx5_core driver
  declare MLX5_PHYSICAL_DEVICES_PCI_ADDRESSES
  MLX5_PHYSICAL_DEVICES_PCI_ADDRESSES="$(
    list_pci_devices_for_driver mlx5_core
  )"
  declare -r MLX5_PHYSICAL_DEVICES_PCI_ADDRESSES

  # Enable SR-IOV interfaces
  to_lines <<<"${MLX5_PHYSICAL_DEVICES_PCI_ADDRESSES}" | map set_number_of_sriov_devices_for_pci mlx5_core 1
  sync --file-system /sys
  udevadm settle --timeout=30

  # Find the SR-IOV (semi physical) devices we just created.
  declare MLX5_SRIOV_DEVICES_PCI_ADDRESSES
  MLX5_SRIOV_DEVICES_PCI_ADDRESSES="$(
    list_pci_devices_for_driver mlx5_core |
      to_lines |
      associate pci_device_to_numa_node |
      jq --argjson physical_devs "${MLX5_PHYSICAL_DEVICES_PCI_ADDRESSES}" '[to_entries[].key] - $physical_devs'
  )"
  declare -r MLX5_SRIOV_DEVICES_PCI_ADDRESSES

  declare MLX5_PHYSICAL_NETWORK_INTERFACES
  MLX5_PHYSICAL_NETWORK_INTERFACES="$(
    to_lines <<<"${MLX5_PHYSICAL_DEVICES_PCI_ADDRESSES}" |
      map pci_device_to_network_interfaces |
      to_lines |
      to_json
  )"
  declare -r MLX5_PHYSICAL_NETWORK_INTERFACES

  # Unbind the SR-IOV devices from the mlx5_core driver.  Needed to set up "lag" on the eswitch (including ecmp routes).
  to_lines <<<"${MLX5_SRIOV_DEVICES_PCI_ADDRESSES}" | map unbind_pci_address_from_driver mlx5_core
  to_lines <<<"${MLX5_SRIOV_DEVICES_PCI_ADDRESSES}" | map wait_for_sriov_devices_unbind
  sync --file-system /sys
  udevadm settle --timeout=30

  # set the cards to 'switchdev'
  to_lines <<<"${MLX5_PHYSICAL_DEVICES_PCI_ADDRESSES}" | map printf "pci/%s\n" | map activate_switchdev_mode

  wait_for_card_to_enter_switchdev_mode() {
    declare -r pcie_address="${1}"
    declare -i retry_count=0
    until [[ \
      "$(
        devlink --json dev eswitch show "pci/${pcie_address}" |
          jq --arg dev "pci/${pcie_address}" --raw-output '.dev[$dev].mode' 2>/dev/null
      )" == "switchdev" ]]; do
      printf >&2 -- "%s\n" "waiting for pci/${pcie_address} to enter switchdev mode"
      sleep 1
      if [[ "${retry_count}" -gt 20 ]]; then
        printf >&2 -- "%s\n" "Failed to enter switchdev mode before timeout!"
        return 1
      fi
      retry_count=$((retry_count + 1))
    done
  }

  # Wait for the cards to be in switchdev mode.
  to_lines <<<"${MLX5_PHYSICAL_DEVICES_PCI_ADDRESSES}" | map wait_for_card_to_enter_switchdev_mode
  sync --file-system /sys
  udevadm settle --timeout=30

  # We now have four 'port representors: two 'virtual PRs' which will be connected
  #  to the SR-IOV devices (sriovs are not visible until the network card drivers are
  #  rebound to the NICs), and two 'physical port representors' named for the native
  #  ports. To take advantage of the two external paths, we will create a LAG of
  #  the physical port representors. See also: equal cost multipathing. We must
  #  create the LAG before we rebind the ports to the MLX driver or it gets mad.

  # Create nonsense route to form an ECMP "LAG" (this is Mellanox's terminology, not mine)
  # shellcheck disable=SC2046
  ip route replace multicast 0.0.0.0/32 metric 9999 \
    $(to_lines <<<"${MLX5_PHYSICAL_NETWORK_INTERFACES}" | map echo nexthop dev)
  sync --file-system /sys
  udevadm settle --timeout=30

  # now we can give the devices back to the mlx5_core driver
  to_lines <<<"${MLX5_SRIOV_DEVICES_PCI_ADDRESSES}" | map bind_sriov_device_to_mlx5_driver

  wait_for_sriov_devices_bind() {
    declare -r driver_dir="/sys/bus/pci/drivers/mlx5_core"
    declare -r pcie_address="${1}"
    declare -i retry_count=0
    until [[ -L "${driver_dir}/${pcie_address}" ]]; do
      inotifywait --recursive --timeout 1 "${driver_dir}" || true
      sync --file-system /sys
      udevadm settle --timeout=30
      if [[ "${retry_count}" -gt 20 ]]; then
        printf >&2 -- "%s\n" "Failed to enter switchdev mode before timeout!"
        return 1
      fi
      retry_count=$((retry_count + 1))
      sleep 1
    done
  }
  to_lines <<<"${MLX5_SRIOV_DEVICES_PCI_ADDRESSES}" | map wait_for_sriov_devices_bind
  sync --file-system /sys
  udevadm settle --timeout=30

  # Listing the physical devices now also includes the port representors since the eswitch is active
  to_lines <<<"${MLX5_PHYSICAL_DEVICES_PCI_ADDRESSES}" |
    map pci_device_to_network_interfaces | to_lines | map enable_hw_tc_offload
  to_lines <<<"${MLX5_SRIOV_DEVICES_PCI_ADDRESSES}" |
    map pci_device_to_network_interfaces | to_lines | map enable_hw_tc_offload
  udevadm settle --timeout=30
  sync --file-system /sys

  to_lines <<<"${MLX5_PHYSICAL_DEVICES_PCI_ADDRESSES}" |
    map pci_device_to_network_interfaces | to_lines | map ip link set up
  to_lines <<<"${MLX5_SRIOV_DEVICES_PCI_ADDRESSES}" |
    map pci_device_to_network_interfaces | to_lines | map ip link set up
}

main
