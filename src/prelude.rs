//! Author-facing prelude.
//!
//! `use extensions_sdk::prelude::*;` brings every name a plugin author
//! typically needs into scope. Kept narrow — contract surface only.

pub use crate::ctx::{
    DynBehavior, EmitSink, GraphAccess, NodeCtx, TimerHandle, TimerScheduler, TypedBehavior,
};
pub use crate::error::NodeError;
pub use crate::node::{InputPort, NodeBehavior, NodeKind};
pub use crate::settings::ResolvedSettings;
pub use crate::{
    Cardinality, CascadePolicy, ContainmentSchema, Facet, FacetSet, KindId, KindManifest,
    MessageId, Msg, NodeId, NodePath, ParentMatcher, SlotRole, SlotSchema, TriggerPolicy,
};
// The derive and the trait share a name — different namespaces, so
// `use extensions_sdk::prelude::*;` gives you both.
pub use extensions_sdk_macros::NodeKind;
