//! Core author traits — `NodeKind` (declarative) and `NodeBehavior`
//! (imperative).
//!
//! Stage 3a-2: behaviour methods take `&self` so per-instance state on
//! the struct is a compile error. State lives in slots, accessed via
//! [`NodeCtx`]. See `docs/sessions/NODE-SCOPE.md` § "Behaviours are
//! stateless — state lives in slots".

use crate::ctx::{NodeCtx, TimerHandle};
use crate::error::NodeError;
use crate::{KindId, KindManifest, Msg};

/// Declarative half of a kind — kind id and manifest. Implemented by
/// `#[derive(NodeKind)]`.
pub trait NodeKind {
    fn kind_id() -> KindId;
    fn manifest() -> KindManifest;
}

/// Port identifier — the named input that fired.
pub type InputPort = String;

/// Imperative half of a kind. Methods take `&self`: a behaviour is
/// **stateless** (the unit struct typical of authoring), and any
/// per-instance state must live in slots accessed through [`NodeCtx`].
///
/// Manifest-only (container) kinds do not implement this trait.
pub trait NodeBehavior: Send + Sync {
    type Config: serde::de::DeserializeOwned + Send + 'static;

    fn on_init(&self, _ctx: &NodeCtx, _cfg: &Self::Config) -> Result<(), NodeError> {
        Ok(())
    }

    fn on_message(&self, ctx: &NodeCtx, port: InputPort, msg: Msg) -> Result<(), NodeError>;

    /// Fired by the runtime when a timer scheduled via
    /// [`NodeCtx::schedule`] elapses. Default: no-op. Kinds that never
    /// call `schedule` (like `sys.compute.count`) leave it that way;
    /// `sys.logic.trigger` overrides to emit its delayed payload.
    fn on_timer(&self, _ctx: &NodeCtx, _handle: TimerHandle) -> Result<(), NodeError> {
        Ok(())
    }

    fn on_config_change(&self, _ctx: &NodeCtx, _cfg: &Self::Config) -> Result<(), NodeError> {
        Ok(())
    }

    fn on_shutdown(&self, _ctx: &NodeCtx) -> Result<(), NodeError> {
        Ok(())
    }
}
