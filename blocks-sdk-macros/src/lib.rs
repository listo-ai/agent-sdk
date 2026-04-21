//! Proc-macros for the Rust blocks-sdk.
//!
//! The centrepiece is [`NodeKind`], which wires a block-author struct
//! to its declarative YAML manifest. See the
//! [`blocks_sdk`](../blocks_sdk/index.html) crate for the
//! user-facing documentation and examples.
//!
//! ## Why a derive, not a builder?
//!
//! The YAML manifest is the single source of truth: the Studio renders
//! from it, the installer validates capabilities against it, the
//! process-block gRPC adapter describes kinds with it. Hand-building a
//! `KindManifest` in Rust would let the two drift. The derive reads the
//! YAML at compile time, validates it parses, and emits the glue.
//!
//! ## Path resolution
//!
//! The `manifest = "..."` attribute is **always resolved relative to
//! `CARGO_MANIFEST_DIR`** — i.e. the root of the crate containing the
//! derive, not the Rust source file. Convention: keep manifests under
//! `manifests/<kind>.yaml` in each crate.

mod attrs;
mod manifest;
mod node_kind;

use proc_macro::TokenStream;

/// Derive an [`blocks_sdk::NodeKind`](../blocks_sdk/trait.NodeKind.html)
/// impl for a struct from a declarative YAML manifest.
///
/// # Attributes
///
/// Required:
///
/// - `kind = "sys.domain.name"` — the reverse-DNS kind id (must match
///   the `kind` field in the YAML).
/// - `manifest = "manifests/name.yaml"` — path to the manifest YAML,
///   **relative to `CARGO_MANIFEST_DIR`**.
/// - `behavior = "none" | "custom"` — whether this kind has a runtime
///   [`NodeBehavior`](../blocks_sdk/trait.NodeBehavior.html) impl.
///   `"none"` marks it as a manifest-only (container) kind; `"custom"`
///   means the author provides their own impl. Omitting the attribute
///   is a compile error — the forcing function keeps behaviour-less
///   kinds from silently reading as no-op behaviour kinds.
///
/// # Example (manifest-only container)
///
/// ```ignore
/// use blocks_sdk::NodeKind;
///
/// #[derive(NodeKind)]
/// #[node(
///     kind = "sys.core.folder",
///     manifest = "manifests/folder.yaml",
///     behavior = "none"
/// )]
/// pub struct Folder;
/// ```
#[proc_macro_derive(NodeKind, attributes(node))]
pub fn derive_node_kind(input: TokenStream) -> TokenStream {
    node_kind::expand(input.into())
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}
