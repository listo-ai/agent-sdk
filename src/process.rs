//! Process-block runtime (feature `process`).
//!
//! Block authors never hand-write gRPC. They provide identity +
//! declared kinds and (optionally) handlers for `Invoke` / `Health`
//! / `Discover` / `Subscribe`; [`run_process_plugin`] wires up the
//! tonic server on the Unix-domain socket the supervisor passes via
//! the `US_PLUGIN_SOCKET` env var.
//!
//! The minimum viable block is four lines of `main`:
//!
//! ```ignore
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     blocks_sdk::process::run_process_plugin(
//!         blocks_sdk::process::BlockIdentity {
//!             id: "com.acme.hello".into(),
//!             version: "0.1.0".into(),
//!             capabilities: vec![],
//!         },
//!     )
//!     .await
//! }
//! ```
//!
//! Defaults: `Discover` / `Subscribe` / `Invoke` return `UNIMPLEMENTED`;
//! `Health` returns `READY`. Override by building a [`ProcessPlugin`]
//! with a custom [`InvokeHandler`] (typed handlers for streaming RPCs
//! land when Stage 3c wires real driver traits through).

use std::path::PathBuf;
use std::pin::Pin;

use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::{transport::Server, Request, Response, Status};
use transport_grpc::proto::health_response::Status as HStatus;
use transport_grpc::{
    DescribeRequest, DescribeResponse, DiscoverEvent, DiscoverRequest, Extension, ExtensionServer,
    HealthRequest, HealthResponse, InvokeRequest, InvokeResponse, KindDeclaration, SlotEvent,
    SubscribeRequest,
};

/// Identity returned by the block's `Describe` RPC.
///
/// `id` must equal the block directory name (the supervisor
/// cross-checks this and refuses on mismatch).
#[derive(Debug, Clone)]
pub struct BlockIdentity {
    pub id: String,
    pub version: String,
    pub capabilities: Vec<String>,
    pub kinds: Vec<KindDeclaration>,
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
/// provides via [`SOCKET_ENV`].
///
/// Blocks (awaits) until the server shuts down. Author-supplied
/// handlers for `Invoke`/`Discover`/`Subscribe` aren't plumbed yet —
/// this lands in Stage 3c when the `NodeBehavior` trait gets its
/// process adapter.
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

    let svc = ExtensionServer::new(DefaultPlugin { identity });
    Server::builder()
        .add_service(svc)
        .serve_with_incoming(stream)
        .await?;
    Ok(())
}

/// Minimal `Extension` impl — describes identity, reports `READY`,
/// and returns `UNIMPLEMENTED` for the RPCs block authors haven't
/// written handlers for yet.
struct DefaultPlugin {
    identity: BlockIdentity,
}

#[tonic::async_trait]
impl Extension for DefaultPlugin {
    async fn describe(
        &self,
        _req: Request<DescribeRequest>,
    ) -> Result<Response<DescribeResponse>, Status> {
        Ok(Response::new(DescribeResponse {
            extension_id: self.identity.id.clone(),
            version: self.identity.version.clone(),
            kinds: self.identity.kinds.clone(),
            capabilities: self.identity.capabilities.clone(),
        }))
    }

    type DiscoverStream =
        Pin<Box<dyn tokio_stream::Stream<Item = Result<DiscoverEvent, Status>> + Send>>;
    async fn discover(
        &self,
        _req: Request<DiscoverRequest>,
    ) -> Result<Response<Self::DiscoverStream>, Status> {
        Err(Status::unimplemented(
            "discover: provide a handler via the SDK when Stage 3c lands the driver trait",
        ))
    }

    type SubscribeStream =
        Pin<Box<dyn tokio_stream::Stream<Item = Result<SlotEvent, Status>> + Send>>;
    async fn subscribe(
        &self,
        _req: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        Err(Status::unimplemented("subscribe: Stage 3c"))
    }

    async fn invoke(
        &self,
        _req: Request<InvokeRequest>,
    ) -> Result<Response<InvokeResponse>, Status> {
        Err(Status::unimplemented("invoke: Stage 3c"))
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

        // Wait for socket to appear.
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
        assert_eq!(id.version, "9.9.9");

        let h = client.health(HealthRequest {}).await.unwrap().into_inner();
        assert_eq!(h.status, HStatus::Ready as i32);

        server.abort();
    }
}
