//! Test fixtures for `faucet-source-grpc` integration tests.
//!
//! Spins up an in-process `EchoService` tonic server backed by the `.proto`
//! compiled in `build.rs`. The same server handles unary and server-streaming
//! RPCs and includes a `fail_after` knob to exercise the source's reconnect
//! path.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, transport::Server};

pub mod pb {
    tonic::include_proto!("faucet.test.echo");
}

use pb::echo_service_server::{EchoService, EchoServiceServer};
use pb::{Event, Item, ListRequest, ListResponse, TailRequest};

/// Path on disk to the descriptor set emitted by `build.rs`.
pub fn descriptor_set_path() -> PathBuf {
    PathBuf::from(env!("ECHO_DESCRIPTOR_PATH"))
}

#[derive(Default)]
pub struct EchoServer {
    /// Counts the number of `Tail` invocations seen — useful for verifying
    /// reconnect behaviour (one connect per attempt).
    pub tail_attempts: Arc<AtomicU32>,
}

#[tonic::async_trait]
impl EchoService for EchoServer {
    async fn list(&self, request: Request<ListRequest>) -> Result<Response<ListResponse>, Status> {
        let count = request.into_inner().count;
        let items = (0..count)
            .map(|i| Item {
                id: i,
                name: format!("item-{i}"),
            })
            .collect();
        Ok(Response::new(ListResponse { items }))
    }

    type TailStream = ReceiverStream<Result<Event, Status>>;

    async fn tail(
        &self,
        request: Request<TailRequest>,
    ) -> Result<Response<Self::TailStream>, Status> {
        let req = request.into_inner();
        let attempt = self.tail_attempts.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        tokio::spawn(async move {
            let mut emitted: u32 = 0;
            while emitted < req.count {
                // Fail-after injection: on the *first* attempt only, close
                // the stream with an UNAVAILABLE status after `fail_after`
                // events. Subsequent attempts run to completion so the test
                // can assert the reconnect produced the full record set.
                if req.fail_after > 0 && attempt == 0 && emitted >= req.fail_after {
                    let _ = tx
                        .send(Err(Status::unavailable("simulated disconnect")))
                        .await;
                    return;
                }
                let event = Event {
                    seq: emitted as u64,
                    payload: format!("event-{emitted}"),
                };
                if tx.send(Ok(event)).await.is_err() {
                    return;
                }
                emitted += 1;
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

/// A running EchoService instance bound to an ephemeral port. Drop the handle
/// to shut down the server.
pub struct ServerHandle {
    #[allow(dead_code)]
    pub addr: SocketAddr,
    pub endpoint: String,
    #[allow(dead_code)]
    pub tail_attempts: Arc<AtomicU32>,
    shutdown: Option<oneshot::Sender<()>>,
    join: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.join.take() {
            // Best-effort: abort if the runtime is still alive. We don't
            // await here to keep `Drop` non-async; the test runtime will
            // reap the task on shutdown.
            handle.abort();
        }
    }
}

/// Start an EchoService server bound to an OS-assigned port. Returns a
/// handle whose `endpoint` field is the `http://127.0.0.1:PORT` URL the
/// `GrpcStream` should connect to.
pub async fn start_server() -> ServerHandle {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let endpoint = format!("http://{addr}");

    let server = EchoServer::default();
    let tail_attempts = server.tail_attempts.clone();

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let join = tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(EchoServiceServer::new(server))
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = shutdown_rx.await;
            })
            .await;
    });

    // Wait for the server to become reachable before returning. tonic's
    // `serve_with_incoming` is ready as soon as `bind` succeeded, but the
    // first connect can still race against axum's routing setup on cold
    // start — a tiny yield is enough.
    tokio::task::yield_now().await;

    ServerHandle {
        addr,
        endpoint,
        tail_attempts,
        shutdown: Some(shutdown_tx),
        join: Some(join),
    }
}
