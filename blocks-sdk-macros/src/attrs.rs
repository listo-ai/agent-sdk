//! Parser for `#[node(...)]` attributes on a `#[derive(NodeKind)]` struct.

use syn::{Attribute, Expr, ExprLit, Lit, Meta, Result, Token};

#[derive(Debug)]
pub(crate) struct NodeAttrs {
    pub kind: String,
    pub manifest_path: String,
    pub behavior: Behavior,
    pub span: proc_macro2::Span,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Behavior {
    /// Manifest-only kind (containers). No `NodeBehavior` impl expected.
    None,
    /// Author provides their own `impl NodeBehavior`. Not verified by
    /// the derive — the derive can't see sibling impls — but declared
    /// here so omission surfaces as a compile error.
    Custom,
}

impl NodeAttrs {
    pub(crate) fn parse(attrs: &[Attribute]) -> Result<Self> {
        let mut kind: Option<(String, proc_macro2::Span)> = None;
        let mut manifest_path: Option<(String, proc_macro2::Span)> = None;
        let mut behavior: Option<(Behavior, proc_macro2::Span)> = None;
        let mut outer_span: Option<proc_macro2::Span> = None;

        for attr in attrs {
            if !attr.path().is_ident("node") {
                continue;
            }
            outer_span = Some(attr.path().segments[0].ident.span());
            let nested = attr.parse_args_with(
                syn::punctuated::Punctuated::<Meta, Token![,]>::parse_terminated,
            )?;
            for meta in nested {
                let Meta::NameValue(nv) = meta else {
                    return Err(syn::Error::new_spanned(
                        meta,
                        "expected `key = \"value\"` inside `#[node(...)]`",
                    ));
                };
                let Expr::Lit(ExprLit {
                    lit: Lit::Str(s), ..
                }) = &nv.value
                else {
                    return Err(syn::Error::new_spanned(
                        &nv.value,
                        "expected a string literal",
                    ));
                };
                let value = s.value();
                let span = s.span();
                if nv.path.is_ident("kind") {
                    kind = Some((value, span));
                } else if nv.path.is_ident("manifest") {
                    manifest_path = Some((value, span));
                } else if nv.path.is_ident("behavior") {
                    let b = match value.as_str() {
                        "none" => Behavior::None,
                        "custom" => Behavior::Custom,
                        other => {
                            return Err(syn::Error::new(
                                span,
                                format!(
                                    "unknown behavior `{other}` — use `\"none\"` for \
                                     container kinds or `\"custom\"` when providing a \
                                     NodeBehavior impl"
                                ),
                            ));
                        }
                    };
                    behavior = Some((b, span));
                } else {
                    return Err(syn::Error::new_spanned(
                        &nv.path,
                        "unknown attribute — expected one of `kind`, `manifest`, `behavior`",
                    ));
                }
            }
        }

        let span = outer_span.unwrap_or_else(proc_macro2::Span::call_site);
        let (kind, _) = kind
            .ok_or_else(|| syn::Error::new(span, "missing `kind = \"...\"` in `#[node(...)]`"))?;
        let (manifest_path, _) = manifest_path.ok_or_else(|| {
            syn::Error::new(
                span,
                "missing `manifest = \"path.yaml\"` in `#[node(...)]` \
                 (path is relative to CARGO_MANIFEST_DIR)",
            )
        })?;
        let (behavior, _) = behavior.ok_or_else(|| {
            syn::Error::new(
                span,
                "missing `behavior = \"...\"` in `#[node(...)]` — use \
                 `\"none\"` for manifest-only container kinds, or \
                 `\"custom\"` when you provide your own NodeBehavior impl",
            )
        })?;

        Ok(Self {
            kind,
            manifest_path,
            behavior,
            span,
        })
    }
}
