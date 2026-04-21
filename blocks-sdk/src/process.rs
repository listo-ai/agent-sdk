//! Process-block runtime (feature `process`).
//!
//! Block authors never hand-write gRPC. They provide identity and
//! register their `NodeKind + NodeBehavior` structs;
//! [`run_process_plugin`] wires up the tonic server on the Unix-domain
//! socket the supervisor passes via the `US_PLUGIN_SOCKET` env var, and
//! dispatches incoming `OnMessage` RPCs to the right behaviour.
//!
//! The minimum viable block:
//!
//! ```ignore
//! use blocks_sdk::process::{BlockIdentity, run_process_plugin};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let mut identity = BlockIdentity::new("com.acme.hello", "0.1.0");
//!     identity.register_kind(Greeter);   // Greeter: NodeKind + NodeBehavior
//!     run_process_plugin(identity).await?;
//!     Ok(())
//! }
//! ```
//!
//! The engine's block-host reads `Describe` to populate its kind
//! registry and then sends `OnMessage` on every trigger-input slot
//! write. The SDK converts that into a `NodeBehavior::on_message` call
//! and returns the emitted output messages.
//!
//! Defaults: `Discover` / `Subscribe` / `Invoke` return `UNIMPLEMENTED`
//! (driver-side concerns, not yet wired). `Health` returns `READY`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};

use std::str::FromStr;

use spi::{KindId, KindManifest, Msg, NodeId, NodePath};
use tokio::net::UnixListener;
use tokio::sync::broadcast;
use tokio_stream::wrappers::{BroadcastStream, UnixListenerStream};
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::StreamExt;
use tonic::{transport::Server, Request, Response, Status};
use transport_grpc::proto::health_response::Status as HStatus;
use transport_grpc::{
    DescribeRequest, DescribeResponse, DiscoverEvent, DiscoverRequest, Extension, ExtensionServer,
    HealthRequest, HealthResponse, InvokeRequest, InvokeResponse, KindDeclaration, OnInitRequest,
    OnInitResponse, OnMessageRequest, OnMessageResponse, OutputEmit, SlotEvent, SubscribeRequest,
};

use crate::ctx::{DynBehavior, EmitSink, GraphAccess, NodeCtx, TimerHandle, TimerScheduler};
use crate::error::NodeError;
use crate::node::{NodeBehavior, NodeKind};
use crate::TypedBehavior;

/// Identity returned by the block's `Describe` RPC.
///
/// `id` must equal the block directory name (the supervisor
/// cross-checks this and refuses on mismatch). Use
/// [`Self::register_kind`] to add a kind — that registers both the
/// manifest and its behaviour in one call so the process SDK can
/// dispatch `OnMessage` without extra bookkeeping.
pub struct BlockIdentity {
    pub id: String,
    pub version: String,
    pub capabilities: Vec<String>,
    kinds: Vec<KindEntry>,
}

struct KindEntry {
    manifest: KindManifest,
    behavior: Arc<dyn DynBehavior>,
}

impl BlockIdentity {
    pub fn new(id: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            version: version.into(),
            capabilities: Vec::new(),
            kinds: Vec::new(),
        }
    }

    /// Register a kind the block owns. Supplies both the declarative
    /// manifest (for the `Describe` RPC → engine's kind registry) and
    /// the imperative behaviour (for the `OnMessage` RPC → dispatch).
    pub fn register_kind<K>(&mut self, kind: K)
    where
        K: NodeKind + NodeBehavior + 'static,
    {
        self.kinds.push(KindEntry {
            manifest: K::manifest(),
            behavior: Arc::new(TypedBehavior(kind)),
        });
    }

    /// Register a manifest without a behaviour. Useful for manifest-only
    /// kinds (pure containers) that the block ships.
    pub fn register_manifest(&mut self, manifest: KindManifest) {
        self.kinds.push(KindEntry {
            manifest,
            behavior: Arc::new(NoopBehavior),
        });
    }
}

/// Convert a public `KindManifest` into the wire-level `KindDeclaration`
/// the supervisor expects on `Describe`. Keeps `transport-grpc` off the
/// block author's dep graph.
fn manifest_to_declaration(m: &KindManifest) -> KindDeclaration {
    let facets: Vec<String> = m
        .facets
        .iter()
        .filter_map(|f| {
            serde_json::to_value(f)
                .ok()
                .and_then(|v| v.as_str().map(str::to_owned))
        })
        .collect();

    KindDeclaration {
        kind_id: m.id.to_string(),
        facets,
        containment_schema_json: serde_json::to_string(&m.containment)
            .expect("ContainmentSchema serialises"),
        slot_schema_json: serde_json::to_string(&m.slots).expect("Vec<SlotSchema> serialises"),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ProcessError {
    #[error("`{0}` env var not set — are you running under the supervisor?")]
    MissingSocketEnv(&'static str),
    #[error("binding UDS `{path}`: {source}")]
    Bind {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("serving gRPC: {0}")]
    Serve(#[from] tonic::transport::Error),
}

/// Name of the env var the supervisor uses to pass the UDS path.
/// Kept in sync with `blocks_host::supervisor::SOCKET_ENV`.
pub const SOCKET_ENV: &str = "US_PLUGIN_SOCKET";

/// Serve the `Extension` gRPC service on the UDS the supervisor
/// provides via [`SOCKET_ENV`]. Blocks until the server shuts down.
pub async fn run_process_plugin(identity: BlockIdentity) -> Result<(), ProcessError> {
    let socket: PathBuf = std::env::var_os(SOCKET_ENV)
        .map(PathBuf::from)
        .ok_or(ProcessError::MissingSocketEnv(SOCKET_ENV))?;

    // Stale socket from a previous run; supervisor usually cleans this
    // but we belt-and-brace here.
    let _ = std::fs::remove_file(&socket);

    let listener = UnixListener::bind(&socket).map_err(|e| ProcessError::Bind {
        path: socket.clone(),
        source: e,
    })?;
    let stream = UnixListenerStream::new(listener);

    let svc = ExtensionServer::new(DefaultPlugin::new(identity));
    Server::builder()
        .add_service(svc)
        .serve_with_incoming(stream)
        .await?;
    Ok(())
}

/// `Extension` impl that dispatches `OnMessage` to registered
/// `NodeBehavior`s. Default `Health` is `READY`; other RPCs are
/// `UNIMPLEMENTED` until driver-side wiring lands.
struct DefaultPlugin {
    id: String,
    version: String,
    capabilities: Vec<String>,
    kinds: Vec<KindManifest>,
    // Resolved at startup so OnMessage dispatch is a HashMap lookup.
    by_kind: HashMap<KindId, KindHandler>,
}

struct KindHandler {
    manifest: Arc<KindManifest>,
    behavior: Arc<dyn DynBehavior>,
}

impl DefaultPlugin {
    fn new(identity: BlockIdentity) -> Self {
        let mut manifests = Vec::with_capacity(identity.kinds.len());
        let mut by_kind = HashMap::with_capacity(identity.kinds.len());
        for entry in identity.kinds {
            let kind_id = entry.manifest.id.clone();
            by_kind.insert(
                kind_id,
                KindHandler {
                    manifest: Arc::new(entry.manifest.clone()),
                    behavior: entry.behavior,
                },
            );
            manifests.push(entry.manifest);
        }
        Self {
            id: identity.id,
            version: identity.version,
            capabilities: identity.capabilities,
            kinds: manifests,
            by_kind,
        }
    }
}

#[tonic::async_trait]
impl Extension for DefaultPlugin {
    async fn describe(
        &self,
        _req: Request<DescribeRequest>,
    ) -> Result<Response<DescribeResponse>, Status> {
        Ok(Response::new(DescribeResponse {
            extension_id: self.id.clone(),
            version: self.version.clone(),
            kinds: self.kinds.iter().map(manifest_to_declaration).collect(),
            capabilities: self.capabilities.clone(),
        }))
    }

    type DiscoverStream =
        Pin<Box<dyn tokio_stream::Stream<Item = Result<DiscoverEvent, Status>> + Send>>;
    async fn discover(
        &self,
        _req: Request<DiscoverRequest>,
    ) -> Result<Response<Self::DiscoverStream>, Status> {
        Err(Status::unimplemented("discover: driver-side, not wired"))
    }

    type SubscribeStream =
        Pin<Box<dyn tokio_stream::Stream<Item = Result<SlotEvent, Status>> + Send>>;
    /// Async back-channel for block-initiated slot writes. Block code
    /// (e.g. the MQTT `sub` kind on an incoming broker packet) publishes
    /// a `SlotEvent` via [`publish_slot_event`]; every open Subscribe
    /// stream receives it and forwards to its agent-side consumer, which
    /// applies the write to the graph.
    ///
    /// This is the only async emit path from a process block today —
    /// `on_init` / `on_message` RPCs only carry *synchronous* emits
    /// captured during dispatch (see `CapturingEmitSink`). Use this
    /// whenever state originates outside the dispatch tick (timers,
    /// subscriptions, external I/O).
    async fn subscribe(
        &self,
        _req: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let rx = slot_event_bus().subscribe();
        // BroadcastStream turns a broadcast::Receiver into a Stream and
        // quietly drops lagged events rather than closing the channel —
        // which is what we want: a slow agent-side consumer should not
        // kill the block's ability to ever emit again.
        // Drop lag-errors silently — a slow agent-side consumer must
        // not shut down the block's emit path. The bus capacity is
        // sized (SLOT_EVENT_BUS_CAP) so this should only trigger under
        // genuine pathology, and the agent-side consumer logs when it
        // reconnects and resumes receiving.
        let stream = BroadcastStream::new(rx).filter_map(|r| match r {
            Ok(ev) => Some(Ok(ev)),
            Err(BroadcastStreamRecvError::Lagged(_)) => None,
        });
        Ok(Response::new(Box::pin(stream) as Self::SubscribeStream))
    }

    async fn invoke(
        &self,
        _req: Request<InvokeRequest>,
    ) -> Result<Response<InvokeResponse>, Status> {
        Err(Status::unimplemented("invoke: driver-side, not wired"))
    }

    async fn on_message(
        &self,
        req: Request<OnMessageRequest>,
    ) -> Result<Response<OnMessageResponse>, Status> {
        let req = req.into_inner();

        let kind_id = KindId::new(req.kind_id.clone());
        let Some(handler) = self.by_kind.get(&kind_id) else {
            return Err(Status::not_found(format!(
                "kind `{}` not registered in this block",
                req.kind_id
            )));
        };

        // Decode the incoming Msg + settings blob.
        let msg: Msg = serde_json::from_str(&req.msg_json)
            .map_err(|e| Status::invalid_argument(format!("msg_json: {e}")))?;
        let cfg: serde_json::Value = if req.config_json.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_str(&req.config_json)
                .map_err(|e| Status::invalid_argument(format!("config_json: {e}")))?
        };
        let node_path = NodePath::from_str(&req.node_path)
            .map_err(|e| Status::invalid_argument(format!("node_path: {e}")))?;

        // Stand up a NodeCtx whose graph/scheduler are no-ops and whose
        // emit sink captures into a Vec. The supervisor on the other end
        // picks the emits up via the response and applies them to the
        // real graph.
        let captured = Arc::new(CapturingEmitSink::default());
        let ctx = NodeCtx::new(
            // NodeId isn't carried across the wire in this first pass —
            // on_message in a process block doesn't have a real id. Use a
            // fresh random value; the capturing emit sink ignores it.
            NodeId::new(),
            node_path,
            kind_id,
            handler.manifest.clone(),
            cfg,
            Arc::new(StubGraph),
            captured.clone() as Arc<dyn EmitSink>,
            Arc::new(StubScheduler),
        );

        // Synchronous dispatch — NodeBehavior is a sync trait by
        // design (stateless, fast, doesn't block).
        match handler.behavior.on_message(&ctx, req.port, msg) {
            Ok(()) => Ok(Response::new(OnMessageResponse {
                ok: true,
                error: String::new(),
                emitted: captured.take(),
            })),
            Err(e) => Ok(Response::new(OnMessageResponse {
                ok: false,
                error: e.to_string(),
                emitted: captured.take(),
            })),
        }
    }

    async fn on_init(
        &self,
        req: Request<OnInitRequest>,
    ) -> Result<Response<OnInitResponse>, Status> {
        let req = req.into_inner();
        let kind_id = KindId::new(req.kind_id.clone());
        let Some(handler) = self.by_kind.get(&kind_id) else {
            return Err(Status::not_found(format!(
                "kind `{}` not registered in this block",
                req.kind_id
            )));
        };
        let cfg: serde_json::Value = if req.config_json.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_str(&req.config_json)
                .map_err(|e| Status::invalid_argument(format!("config_json: {e}")))?
        };
        let node_path = NodePath::from_str(&req.node_path)
            .map_err(|e| Status::invalid_argument(format!("node_path: {e}")))?;

        let captured = Arc::new(CapturingEmitSink::default());
        let ctx = NodeCtx::new(
            NodeId::new(),
            node_path,
            kind_id,
            handler.manifest.clone(),
            cfg.clone(),
            Arc::new(StubGraph),
            captured.clone() as Arc<dyn EmitSink>,
            Arc::new(StubScheduler),
        );

        match handler.behavior.on_init(&ctx, &cfg) {
            Ok(()) => Ok(Response::new(OnInitResponse {
                ok: true,
                error: String::new(),
                emitted: captured.take(),
            })),
            Err(e) => Ok(Response::new(OnInitResponse {
                ok: false,
                error: e.to_string(),
                emitted: captured.take(),
            })),
        }
    }

    async fn health(
        &self,
        _req: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        Ok(Response::new(HealthResponse {
            status: HStatus::Ready as i32,
            detail: String::new(),
        }))
    }
}

// ---------------------------------------------------------------------------
// Stubs for NodeCtx surfaces that aren't wired yet.
// ---------------------------------------------------------------------------

#[derive(Default)]
struct CapturingEmitSink {
    out: Mutex<Vec<OutputEmit>>,
}

impl CapturingEmitSink {
    fn take(&self) -> Vec<OutputEmit> {
        std::mem::take(&mut *self.out.lock().expect("sink mutex"))
    }
}

impl EmitSink for CapturingEmitSink {
    fn emit(&self, _source: NodeId, port: &str, msg: Msg) -> Result<(), NodeError> {
        let msg_json = serde_json::to_string(&msg)
            .map_err(|e| NodeError::runtime(format!("serialise emit: {e}")))?;
        self.out.lock().expect("sink mutex").push(OutputEmit {
            port: port.to_owned(),
            msg_json,
        });
        Ok(())
    }
}

struct StubGraph;
impl GraphAccess for StubGraph {
    fn read_slot(&self, _path: &NodePath, _slot: &str) -> Result<serde_json::Value, NodeError> {
        Err(NodeError::runtime(
            "GraphAccess from a process block is not wired — read slots via settings",
        ))
    }
    fn write_slot(
        &self,
        _path: &NodePath,
        _slot: &str,
        _value: serde_json::Value,
    ) -> Result<(), NodeError> {
        Err(NodeError::runtime(
            "GraphAccess from a process block is not wired — emit on output ports instead",
        ))
    }
}

struct StubScheduler;
impl TimerScheduler for StubScheduler {
    fn schedule(&self, _node: NodeId, _delay_ms: u64) -> Result<TimerHandle, NodeError> {
        Err(NodeError::runtime(
            "timers from a process block are not wired",
        ))
    }
    fn cancel(&self, _handle: TimerHandle) {}
}

// ---------------------------------------------------------------------------
// Async slot-event back-channel (block → agent)
// ---------------------------------------------------------------------------

/// Capacity of the process-wide slot-event broadcast. Large enough that a
/// brief agent-side hiccup doesn't force lag-drops at normal emission
/// rates, small enough to bound memory on a pathological producer.
const SLOT_EVENT_BUS_CAP: usize = 1024;

fn slot_event_bus() -> &'static broadcast::Sender<SlotEvent> {
    static BUS: OnceLock<broadcast::Sender<SlotEvent>> = OnceLock::new();
    BUS.get_or_init(|| broadcast::channel(SLOT_EVENT_BUS_CAP).0)
}

/// Publish a block-initiated slot write. Surfaces on every agent-side
/// `Subscribe` stream this process has open.
///
/// Use when the value's origin is **not** a sync dispatch tick —
/// e.g. an MQTT sub kind pushing a received message onto its `out`
/// port, a driver streaming telemetry, or a long-running timer. For
/// emits that happen *inside* `on_message` / `on_init`, prefer
/// [`crate::ctx::NodeCtx::emit`] — the CapturingEmitSink returns those
/// with the RPC response and avoids the bus entirely.
///
/// `value` is any serde-serialisable payload; for an output-port write
/// that should read as a Node-RED msg downstream, pass a [`Msg`] (or
/// its JSON form). Returns the number of receivers the event reached
/// (0 if no agent has opened a Subscribe stream yet — which is fine;
/// the agent is the authoritative surface, dropping on the floor is
/// correct back-pressure).
pub fn publish_slot_event(
    node_path: &NodePath,
    slot: &str,
    value: &serde_json::Value,
) -> usize {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let ev = SlotEvent {
        node_path: node_path.as_str().to_string(),
        slot: slot.to_string(),
        value_json: value.to_string(),
        timestamp_unix_ms: now_ms,
    };
    slot_event_bus().send(ev).unwrap_or(0)
}

struct NoopBehavior;
impl DynBehavior for NoopBehavior {
    fn on_init(&self, _ctx: &NodeCtx, _cfg: &serde_json::Value) -> Result<(), NodeError> {
        Ok(())
    }
    fn on_message(&self, _ctx: &NodeCtx, _port: String, _msg: Msg) -> Result<(), NodeError> {
        Err(NodeError::runtime(
            "kind is manifest-only — no behaviour registered",
        ))
    }
    fn on_timer(&self, _ctx: &NodeCtx, _handle: TimerHandle) -> Result<(), NodeError> {
        Ok(())
    }
    fn on_shutdown(&self, _ctx: &NodeCtx) -> Result<(), NodeError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tonic::transport::{Endpoint, Uri};
    use transport_grpc::ExtensionClient;

    #[tokio::test]
    async fn default_plugin_responds_to_describe_and_health() {
        let tmp = tempfile::tempdir().unwrap();
        let socket = tmp.path().join("sdk.sock");
        std::env::set_var(SOCKET_ENV, &socket);

        let server = tokio::spawn(run_process_plugin(BlockIdentity::new(
            "com.acme.sdktest",
            "9.9.9",
        )));

        for _ in 0..50 {
            if socket.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(socket.exists(), "socket not bound");

        let sp = socket.clone();
        let channel = Endpoint::try_from("http://[::]:1")
            .unwrap()
            .connect_with_connector(tower::service_fn(move |_: Uri| {
                let p = sp.clone();
                async move {
                    let s = tokio::net::UnixStream::connect(p).await?;
                    Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(s))
                }
            }))
            .await
            .unwrap();
        let mut client = ExtensionClient::new(channel);
        let id = client
            .describe(DescribeRequest {})
            .await
            .unwrap()
            .into_inner();
        assert_eq!(id.extension_id, "com.acme.sdktest");

        let h = client.health(HealthRequest {}).await.unwrap().into_inner();
        assert_eq!(h.status, HStatus::Ready as i32);

        server.abort();
    }
}
