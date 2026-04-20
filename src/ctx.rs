//! `NodeCtx` — the runtime handle a behaviour holds while it processes
//! a message.
//!
//! Per `docs/sessions/NODE-SCOPE.md` the rule is: **a behaviour is
//! stateless; per-instance state lives in slots.** `NodeCtx` is the only
//! way a behaviour reads or writes that state, and it is *self-scoped* —
//! `read_status` / `read_config` / `update_status` operate on the current
//! node only. Cross-node communication is by message via [`emit`] →
//! live-wire propagation; no peeking at peers' slots.
//!
//! Stage 3a-2 ships the native surface. wasm/process adapters land in
//! 3b/3c — until then their feature-flag impls of [`GraphAccess`]/
//! [`EmitSink`] are stub-only.

use std::sync::Arc;

use serde_json::Value as JsonValue;
use spi::{KindId, KindManifest, Msg, NodeId, NodePath};

use crate::error::NodeError;
use crate::node::NodeBehavior;

/// The minimum graph surface a [`NodeCtx`] needs. Implemented by the
/// engine over `GraphStore`; tests can mock it without pulling the
/// graph runtime.
pub trait GraphAccess: Send + Sync + 'static {
    fn read_slot(&self, path: &NodePath, slot: &str) -> Result<JsonValue, NodeError>;
    fn write_slot(&self, path: &NodePath, slot: &str, value: JsonValue) -> Result<(), NodeError>;
}

/// Sink for `ctx.emit(port, msg)`. Production wires this to the same
/// graph store (writing to the source's output slot triggers live-wire
/// fan-out); tests can capture emissions for assertion.
pub trait EmitSink: Send + Sync + 'static {
    fn emit(&self, source: NodeId, port: &str, msg: Msg) -> Result<(), NodeError>;
}

/// Opaque identifier for a scheduled timer. Returned by
/// [`NodeCtx::schedule`], accepted by [`NodeCtx::cancel`], and passed
/// back to the behaviour on [`NodeBehavior::on_timer`](crate::NodeBehavior::on_timer)
/// so a single node that maintains multiple pending timers can tell
/// them apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TimerHandle(pub u64);

/// Scheduler seam. Implemented by the engine on the native adapter;
/// wasm/process adapters stub until 3b/3c. Kept behind a trait so
/// tests can mock timers without a tokio runtime.
pub trait TimerScheduler: Send + Sync + 'static {
    fn schedule(&self, node: NodeId, delay_ms: u64) -> Result<TimerHandle, NodeError>;
    fn cancel(&self, handle: TimerHandle);
}

/// Per-dispatch context handed to a [`NodeBehavior`](crate::NodeBehavior).
///
/// Bound to a specific node id at dispatch time. All graph reads/writes
/// scope to that node — the `NodePath` is held internally and not
/// exposed as an arg.
pub struct NodeCtx {
    node_id: NodeId,
    node_path: NodePath,
    kind_id: KindId,
    manifest: Arc<KindManifest>,
    config: JsonValue,
    graph: Arc<dyn GraphAccess>,
    emit_sink: Arc<dyn EmitSink>,
    scheduler: Arc<dyn TimerScheduler>,
}

impl NodeCtx {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeId,
        node_path: NodePath,
        kind_id: KindId,
        manifest: Arc<KindManifest>,
        config: JsonValue,
        graph: Arc<dyn GraphAccess>,
        emit_sink: Arc<dyn EmitSink>,
        scheduler: Arc<dyn TimerScheduler>,
    ) -> Self {
        Self {
            node_id,
            node_path,
            kind_id,
            manifest,
            config,
            graph,
            emit_sink,
            scheduler,
        }
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn node_path(&self) -> &NodePath {
        &self.node_path
    }

    pub fn kind_id(&self) -> &KindId {
        &self.kind_id
    }

    pub fn manifest(&self) -> &KindManifest {
        &self.manifest
    }

    pub fn config(&self) -> &JsonValue {
        &self.config
    }

    /// Read a status slot on this node.
    pub fn read_status(&self, slot: &str) -> Result<JsonValue, NodeError> {
        self.read_self_role(slot, spi::SlotRole::Status)
    }

    /// Read a config slot on this node.
    pub fn read_config(&self, slot: &str) -> Result<JsonValue, NodeError> {
        self.read_self_role(slot, spi::SlotRole::Config)
    }

    /// Read the last `Msg` emitted on an output port — the persisted
    /// value of an output-role slot. A source node uses this instead of
    /// a mirror status slot to recover its previous state across ticks
    /// and restarts (see docs/design/NODE-RED-MODEL.md Stage 4 —
    /// mirror slots deleted; the output slot IS the current value).
    /// Returns `JsonValue::Null` if nothing has been emitted yet.
    pub fn read_output(&self, port: &str) -> Result<JsonValue, NodeError> {
        self.read_self_role(port, spi::SlotRole::Output)
    }

    /// Write a status slot on this node.
    pub fn update_status(&self, slot: &str, value: JsonValue) -> Result<(), NodeError> {
        let schema = self.find_slot(slot)?;
        if schema.role != spi::SlotRole::Status {
            return Err(NodeError::runtime(format!(
                "slot `{slot}` is not a status slot"
            )));
        }
        self.graph.write_slot(&self.node_path, slot, value)
    }

    /// Emit a message on the named output port. The runtime fans out to
    /// every link departing from this output via the live-wire executor.
    pub fn emit(&self, port: &str, msg: Msg) -> Result<(), NodeError> {
        let schema = self.find_slot(port)?;
        if schema.role != spi::SlotRole::Output {
            return Err(NodeError::runtime(format!(
                "port `{port}` is not an output slot"
            )));
        }
        self.emit_sink.emit(self.node_id, port, msg)
    }

    /// Schedule a one-shot timer. After `delay_ms`, the behaviour's
    /// [`NodeBehavior::on_timer`](crate::NodeBehavior::on_timer) fires
    /// with the returned [`TimerHandle`]. Cancel with [`Self::cancel`].
    pub fn schedule(&self, delay_ms: u64) -> Result<TimerHandle, NodeError> {
        self.scheduler.schedule(self.node_id, delay_ms)
    }

    /// Cancel a pending timer. A no-op if the timer has already fired
    /// or been cancelled.
    pub fn cancel(&self, handle: TimerHandle) {
        self.scheduler.cancel(handle);
    }

    fn read_self_role(&self, slot: &str, expected: spi::SlotRole) -> Result<JsonValue, NodeError> {
        let schema = self.find_slot(slot)?;
        if schema.role != expected {
            return Err(NodeError::runtime(format!(
                "slot `{slot}` has role {:?}, expected {:?}",
                schema.role, expected
            )));
        }
        self.graph.read_slot(&self.node_path, slot)
    }

    fn find_slot(&self, name: &str) -> Result<&spi::SlotSchema, NodeError> {
        self.manifest
            .slots
            .iter()
            .find(|s| s.name == name)
            .ok_or_else(|| NodeError::UnknownSlot(name.to_string()))
    }
}

/// Object-safe wrapper around [`NodeBehavior`] so the engine can hold
/// kind→behaviour entries in a homogeneous map. Authors never write
/// this — the blanket impl on [`TypedBehavior`] is what the registry
/// stores.
pub trait DynBehavior: Send + Sync + 'static {
    fn on_init(&self, ctx: &NodeCtx, cfg: &JsonValue) -> Result<(), NodeError>;
    fn on_message(&self, ctx: &NodeCtx, port: String, msg: Msg) -> Result<(), NodeError>;
    fn on_timer(&self, ctx: &NodeCtx, handle: TimerHandle) -> Result<(), NodeError>;
    fn on_shutdown(&self, ctx: &NodeCtx) -> Result<(), NodeError>;
}

pub struct TypedBehavior<B: NodeBehavior>(pub B);

impl<B: NodeBehavior + 'static> DynBehavior for TypedBehavior<B> {
    fn on_init(&self, ctx: &NodeCtx, cfg: &JsonValue) -> Result<(), NodeError> {
        let typed: B::Config = serde_json::from_value(cfg.clone())
            .map_err(|e| NodeError::InvalidConfig(e.to_string()))?;
        self.0.on_init(ctx, &typed)
    }

    fn on_message(&self, ctx: &NodeCtx, port: String, msg: Msg) -> Result<(), NodeError> {
        self.0.on_message(ctx, port, msg)
    }

    fn on_timer(&self, ctx: &NodeCtx, handle: TimerHandle) -> Result<(), NodeError> {
        self.0.on_timer(ctx, handle)
    }

    fn on_shutdown(&self, ctx: &NodeCtx) -> Result<(), NodeError> {
        self.0.on_shutdown(ctx)
    }
}
