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
#
# The list is empty since the enums are generated as newtypes, which every value is valid for.
NO_DEFAULT_TYPES = [
]
