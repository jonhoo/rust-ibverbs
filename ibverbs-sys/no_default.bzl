# Generated types for which an all-zero value is invalid.
#
# bindgen zero-fills any struct or union it cannot `#[derive(Default)]`. With `--default-enum-style
# rust`, a field whose enum has no zero-valued variant (`ibv_mtu`, `ibv_qp_type`, `ibv_node_type`,
# ...) then holds an invalid value, which is undefined behavior at `assume_init` (see "Invalid
# values" in the Rust reference), whether or not the caller overwrites the field afterwards. Structs
# containing such a struct, directly, nested, or in an array, inherit the problem. These get no
# generated `Default`; `src/lib.rs` hand-writes one for the types the safe crate constructs.
#
# Both bindgen invocations consume this list: `BUILD.bazel` loads it, `build.rs` reads it as text,
# so keep it a plain list of one quoted name per line. It is the union across features; a name
# absent from the current feature set is inert. `tests/no_default.rs` checks it against the
# generated bindings.
NO_DEFAULT_TYPES = [
    "ibv_device",
    "ibv_flow_spec",
    "ibv_flow_spec__bindgen_ty_1",
    "ibv_flow_spec__bindgen_ty_1__bindgen_ty_1",
    "ibv_flow_spec_action_drop",
    "ibv_flow_spec_action_handle",
    "ibv_flow_spec_action_tag",
    "ibv_flow_spec_counter_action",
    "ibv_flow_spec_esp",
    "ibv_flow_spec_eth",
    "ibv_flow_spec_gre",
    "ibv_flow_spec_ipv4",
    "ibv_flow_spec_ipv4_ext",
    "ibv_flow_spec_ipv6",
    "ibv_flow_spec_mpls",
    "ibv_flow_spec_tcp_udp",
    "ibv_flow_spec_tunnel",
    "ibv_mw",
    "ibv_port_attr",
    "ibv_qp",
    "ibv_qp_attr",
    "ibv_qp_ex",
    "ibv_qp_init_attr",
    "ibv_qp_init_attr_ex",
    "ibv_qp_open_attr",
    "rdma_cm_id",
]
