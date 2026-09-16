//! `no_default.bzl` against the generated bindings.
//!
//! bindgen zero-fills any struct or union it cannot `#[derive(Default)]`; for a type that contains
//! an enum with no zero-valued variant that is an invalid value, so undefined behavior. The
//! detector below finds such types in bindgen output. The fixture tests pin down what it
//! recognizes; the tests on the real bindings check that no generated `Default` lands on such a
//! type and that the shared exclusion list is complete and free of stale entries for the current
//! feature set.

use std::collections::{BTreeMap, BTreeSet};

const BINDINGS: &str = include_str!(concat!(env!("OUT_DIR"), "/bindings.rs"));
const NO_DEFAULT_BZL: &str = include_str!("../no_default.bzl");

/// The plain type name a zeroed field must be valid for: a path without generic arguments or the
/// element type of an array. `None` is a field that is valid when zeroed whatever it names: a
/// pointer, an `Option<fn>`, or bindgen's `__IncompleteArrayField<T>`.
fn field_type_name(ty: &syn::Type) -> Option<String> {
    match ty {
        syn::Type::Path(p) => {
            let last = p.path.segments.last()?;
            matches!(last.arguments, syn::PathArguments::None).then(|| last.ident.to_string())
        }
        syn::Type::Array(a) => field_type_name(&a.elem),
        _ => None,
    }
}

fn discriminant_value(expr: &syn::Expr) -> i128 {
    match expr {
        syn::Expr::Lit(syn::ExprLit {
            lit: syn::Lit::Int(int),
            ..
        }) => int.base10_parse().expect("integer discriminant"),
        syn::Expr::Unary(syn::ExprUnary {
            op: syn::UnOp::Neg(_),
            expr,
            ..
        }) => -discriminant_value(expr),
        _ => panic!("enum discriminant is not an integer literal"),
    }
}

/// The structs and unions in `src` whose zeroed value must not be handed out.
///
/// A struct is invalid when any field is: directly, through a nested struct, through an array, or
/// through a type alias, it holds an enum with no zero-valued variant. A union's bytes carry no
/// validity requirement of their own; reading a member does. So a zeroed union is excluded only
/// when every member is invalid, since then no member of the default can ever be read.
fn types_without_valid_zero(src: &str) -> BTreeSet<String> {
    let file = syn::parse_file(src).expect("valid Rust");

    let mut enums_without_zero = BTreeSet::new();
    let mut aliases: BTreeMap<String, String> = BTreeMap::new();
    let mut fields_by_type: BTreeMap<String, Vec<Option<String>>> = BTreeMap::new();
    let mut unions = BTreeSet::new();
    for item in &file.items {
        match item {
            syn::Item::Enum(e) => {
                let mut next = 0i128;
                let mut has_zero = false;
                for variant in &e.variants {
                    let value = match &variant.discriminant {
                        Some((_, expr)) => discriminant_value(expr),
                        None => next,
                    };
                    has_zero |= value == 0;
                    next = value + 1;
                }
                if !has_zero {
                    enums_without_zero.insert(e.ident.to_string());
                }
            }
            syn::Item::Type(t) => {
                if let Some(target) = field_type_name(&t.ty) {
                    aliases.insert(t.ident.to_string(), target);
                }
            }
            syn::Item::Struct(s) => {
                let fields = s.fields.iter().map(|f| field_type_name(&f.ty));
                fields_by_type.insert(s.ident.to_string(), fields.collect());
            }
            syn::Item::Union(u) => {
                let fields = u.fields.named.iter().map(|f| field_type_name(&f.ty));
                fields_by_type.insert(u.ident.to_string(), fields.collect());
                unions.insert(u.ident.to_string());
            }
            _ => {}
        }
    }

    let resolve = |mut name: String| {
        let mut hops = 0;
        while let Some(target) = aliases.get(&name) {
            name = target.clone();
            hops += 1;
            assert!(hops <= aliases.len(), "alias cycle at {name}");
        }
        name
    };
    let fields_by_type: BTreeMap<String, Vec<Option<String>>> = fields_by_type
        .into_iter()
        .map(|(name, fields)| (name, fields.into_iter().map(|f| f.map(resolve)).collect()))
        .collect();

    let mut tainted = enums_without_zero;
    loop {
        let before = tainted.len();
        for (name, fields) in &fields_by_type {
            let field_invalid =
                |f: &Option<String>| f.as_ref().is_some_and(|f| tainted.contains(f));
            let invalid = if unions.contains(name) {
                !fields.is_empty() && fields.iter().all(field_invalid)
            } else {
                fields.iter().any(field_invalid)
            };
            if invalid {
                tainted.insert(name.clone());
            }
        }
        if tainted.len() == before {
            break;
        }
    }
    tainted
        .into_iter()
        .filter(|name| fields_by_type.contains_key(name))
        .collect()
}

/// The structs and unions in `src` that carry a `Default`, derived or implemented.
fn types_with_default(src: &str) -> BTreeSet<String> {
    let file = syn::parse_file(src).expect("valid Rust");
    let derives_default = |attrs: &[syn::Attribute]| {
        attrs.iter().any(|attr| {
            attr.path().is_ident("derive") && attr.to_token_stream_string().contains("Default")
        })
    };
    let mut out = BTreeSet::new();
    for item in &file.items {
        match item {
            syn::Item::Struct(s) if derives_default(&s.attrs) => {
                out.insert(s.ident.to_string());
            }
            syn::Item::Union(u) if derives_default(&u.attrs) => {
                out.insert(u.ident.to_string());
            }
            syn::Item::Impl(i) => {
                let is_default = i.trait_.as_ref().is_some_and(|(_, path, _)| {
                    path.segments.last().is_some_and(|s| s.ident == "Default")
                });
                if let (true, syn::Type::Path(p)) = (is_default, &*i.self_ty) {
                    out.insert(p.path.segments.last().expect("type path").ident.to_string());
                }
            }
            _ => {}
        }
    }
    out
}

trait TokenString {
    fn to_token_stream_string(&self) -> String;
}

impl TokenString for syn::Attribute {
    fn to_token_stream_string(&self) -> String {
        match &self.meta {
            syn::Meta::List(list) => list.tokens.to_string(),
            _ => String::new(),
        }
    }
}

/// The structs and unions defined in `src`.
fn defined_types(src: &str) -> BTreeSet<String> {
    let file = syn::parse_file(src).expect("valid Rust");
    file.items
        .iter()
        .filter_map(|item| match item {
            syn::Item::Struct(s) => Some(s.ident.to_string()),
            syn::Item::Union(u) => Some(u.ident.to_string()),
            _ => None,
        })
        .collect()
}

/// The quoted names in `no_default.bzl`, the way `build.rs` reads them.
fn listed() -> BTreeSet<String> {
    NO_DEFAULT_BZL
        .lines()
        .map(str::trim)
        .filter_map(|line| line.strip_prefix('"'))
        .map(|line| line.trim_end_matches(',').trim_end_matches('"').to_string())
        .collect()
}

fn names(items: &[&str]) -> BTreeSet<String> {
    items.iter().map(|s| s.to_string()).collect()
}

#[test]
fn enum_with_zero_variant_is_valid() {
    let src = r"
        #[repr(u32)] pub enum state { NOP = 0, DOWN = 1 }
        pub struct holder { pub s: state }
    ";
    assert!(types_without_valid_zero(src).is_empty());
}

#[test]
fn enum_without_zero_variant_taints_direct_field() {
    let src = r"
        #[repr(u32)] pub enum mtu { M256 = 1, M512 = 2 }
        pub struct attr { pub max_mtu: mtu, pub lid: u16 }
        pub struct clean { pub lid: u16 }
    ";
    assert_eq!(types_without_valid_zero(src), names(&["attr"]));
}

#[test]
fn implicit_and_negative_discriminants() {
    let src = r"
        #[repr(u32)] pub enum implicit_from_zero { A, B, C }
        #[repr(i32)] pub enum implicit_after_one { A = 1, B, C }
        #[repr(i32)] pub enum negative_only { UNKNOWN = -1, CA = 1 }
        #[repr(i32)] pub enum negative_then_zero { UNKNOWN = -1, NONE = 0 }
        pub struct a { pub f: implicit_from_zero }
        pub struct b { pub f: implicit_after_one }
        pub struct c { pub f: negative_only }
        pub struct d { pub f: negative_then_zero }
    ";
    assert_eq!(types_without_valid_zero(src), names(&["b", "c"]));
}

#[test]
fn taint_propagates_through_nesting_arrays_and_aliases() {
    let src = r"
        #[repr(u32)] pub enum qp_type { RC = 2, UC = 3 }
        pub type qp_type_t = qp_type;
        pub type qp_type_tt = qp_type_t;
        pub struct inner { pub t: qp_type_tt }
        pub struct nested { pub i: inner }
        pub struct arrayed { pub ts: [qp_type; 4] }
        pub struct nested_array { pub is: [[inner; 2]; 3] }
        pub struct clean_ptr { pub p: *mut inner }
        pub struct clean_fn { pub f: Option<unsafe extern fn(t: qp_type)> }
        pub struct clean_flex { pub tail: __IncompleteArrayField<inner> }
    ";
    assert_eq!(
        types_without_valid_zero(src),
        names(&["arrayed", "inner", "nested", "nested_array"])
    );
}

#[test]
fn union_is_invalid_only_when_every_member_is() {
    let src = r"
        #[repr(u32)] pub enum qp_type { RC = 2, UC = 3 }
        pub struct inner { pub t: qp_type }
        pub struct other { pub t: qp_type, pub n: u32 }
        pub union some_member_valid { pub i: inner, pub n: u32 }
        pub struct holds_valid_union { pub u: some_member_valid }
        pub union no_member_valid { pub i: inner, pub o: other, pub a: [inner; 2] }
        pub struct holds_invalid_union { pub u: no_member_valid }
        pub union pointer_member_valid { pub i: inner, pub p: *mut inner }
        pub union callback_member_valid { pub i: inner, pub f: Option<unsafe extern fn()> }
        pub union empty {}
    ";
    assert_eq!(
        types_without_valid_zero(src),
        names(&["holds_invalid_union", "inner", "no_member_valid", "other"])
    );
}

#[test]
fn defaults_are_found_whether_derived_or_implemented() {
    let src = r"
        #[derive(Debug, Default, Copy, Clone)] pub struct derived { pub x: u32 }
        #[derive(Debug, Copy, Clone)] pub struct implemented { pub x: u32 }
        impl Default for implemented { fn default() -> Self { implemented { x: 0 } } }
        #[derive(Debug, Copy, Clone)] pub struct neither { pub x: u32 }
        #[derive(Default)] pub union u { pub x: u32 }
    ";
    assert_eq!(
        types_with_default(src),
        names(&["derived", "implemented", "u"])
    );
}

#[test]
fn generated_bindings_have_no_default_on_a_type_without_a_valid_zero() {
    let tainted = types_without_valid_zero(BINDINGS);
    let with_default = types_with_default(BINDINGS);
    let invalid: Vec<_> = tainted.intersection(&with_default).collect();
    assert!(
        invalid.is_empty(),
        "generated `Default` would zero an enum with no zero variant: {invalid:?}"
    );
}

#[test]
fn exclusion_list_covers_every_generated_type_without_a_valid_zero() {
    let tainted = types_without_valid_zero(BINDINGS);
    let listed = listed();
    let missing: Vec<_> = tainted.difference(&listed).collect();
    assert!(missing.is_empty(), "add to no_default.bzl: {missing:?}");
}

#[test]
fn exclusion_list_has_no_stale_entry_for_the_current_feature_set() {
    let tainted = types_without_valid_zero(BINDINGS);
    let present = defined_types(BINDINGS);
    let stale: Vec<_> = listed()
        .into_iter()
        .filter(|name| present.contains(name) && !tainted.contains(name))
        .collect();
    assert!(
        stale.is_empty(),
        "no_default.bzl names types whose zero value is valid: {stale:?}"
    );
}
