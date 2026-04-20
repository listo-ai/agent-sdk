#![cfg_attr(test, allow(clippy::unwrap_used, clippy::panic))]
//! # `extensions-sdk` — author SDK for node kinds
//!
//! Every node kind in the platform — core native, Wasm, or process
//! plugin — is written against this SDK. One authoring API, three
//! packaging choices via the mutually-exclusive feature flags `native`
//! (default), `wasm`, `process`.
//!
//! Stage 3a-2 wires the **imperative** half end-to-end on the native
//! adapter: real [`NodeCtx`] / [`NodeBehavior`] / [`ResolvedSettings`],
//! plus the [`requires!`] capability-declaration macro. wasm/process
//! adapters surface the same trait shapes but their `GraphAccess` /
//! `EmitSink` impls are stubs until 3b/3c.
//!
//! See `docs/sessions/NODE-SCOPE.md` for the full design and
//! `docs/sessions/STEPS.md` § "Stage 3a-2" for the current scope.

#[cfg(any(
    all(feature = "native", feature = "wasm"),
    all(feature = "native", feature = "process"),
    all(feature = "wasm", feature = "process"),
))]
compile_error!(
    "extensions-sdk: features `native`, `wasm`, and `process` are mutually \
     exclusive — enable exactly one on each consuming crate"
);

pub mod ctx;
pub mod error;
pub mod node;
pub mod prelude;
pub mod requires;
pub mod settings;

#[cfg(feature = "process")]
pub mod process;

#[cfg(feature = "wasm")]
pub mod wasm;

pub use ctx::{
    DynBehavior, EmitSink, GraphAccess, NodeCtx, TimerHandle, TimerScheduler, TypedBehavior,
};
pub use error::NodeError;
pub use extensions_sdk_macros::NodeKind;
pub use node::{InputPort, NodeBehavior, NodeKind};
pub use settings::ResolvedSettings;

// Re-export the SPI surface authors reach through the prelude.
pub use spi::capabilities;
pub use spi::{
    Cardinality, CascadePolicy, ContainmentSchema, Facet, FacetSet, KindId, KindManifest,
    MessageId, Msg, NodeId, NodePath, ParentMatcher, SlotRole, SlotSchema, TriggerPolicy,
};

/// Private re-exports consumed by the `#[derive(NodeKind)]` expansion.
/// Not part of the stable surface — do not reference directly.
#[doc(hidden)]
pub mod __private {
    pub use serde_yml;
    pub use spi;
}
