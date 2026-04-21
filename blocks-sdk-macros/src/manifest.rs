//! Compile-time manifest loading + validation.
//!
//! Resolves the `manifest = "..."` path against `CARGO_MANIFEST_DIR`,
//! reads the file, and round-trips it through `serde_yml` →
//! [`spi::KindManifest`] to confirm the schema parses. Returns the
//! absolute path so the expansion can emit `include_str!` against it
//! (cargo tracks the file for incremental rebuilds).

use std::path::PathBuf;

use spi::KindManifest;
use syn::Result;

pub(crate) struct LoadedManifest {
    pub absolute_path: String,
    pub parsed: KindManifest,
}

pub(crate) fn load(rel_path: &str, span: proc_macro2::Span) -> Result<LoadedManifest> {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").map_err(|_| {
        syn::Error::new(
            span,
            "CARGO_MANIFEST_DIR is not set — the NodeKind derive must be \
             invoked from a crate built by cargo",
        )
    })?;
    let absolute = PathBuf::from(&manifest_dir).join(rel_path);
    let yaml = std::fs::read_to_string(&absolute).map_err(|e| {
        syn::Error::new(
            span,
            format!(
                "cannot read manifest `{}` (resolved from CARGO_MANIFEST_DIR \
                 `{}`): {e}",
                absolute.display(),
                manifest_dir,
            ),
        )
    })?;
    let parsed: KindManifest = serde_yml::from_str(&yaml).map_err(|e| {
        syn::Error::new(
            span,
            format!(
                "manifest `{}` does not match the KindManifest schema: {e}",
                absolute.display()
            ),
        )
    })?;
    let absolute_path = absolute
        .to_str()
        .ok_or_else(|| syn::Error::new(span, "manifest path is not valid UTF-8"))?
        .to_string();
    Ok(LoadedManifest {
        absolute_path,
        parsed,
    })
}
