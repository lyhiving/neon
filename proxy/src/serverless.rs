//! Routers for our serverless APIs
//!
//! Handles both SQL over HTTP and SQL over Websockets.

mod backend;
mod conn_pool;
mod http_util;
mod json;
mod sql_over_http;
mod websocket;

use atomic_take::AtomicTake;
use bytes::Bytes;
pub use conn_pool::GlobalConnPoolOptions;

use anyhow::Context;
use futures::future::{select, Either};
use futures::TryFutureExt;
use http::{Method, Response, StatusCode};
use http_body_util::Full;
use hyper1::body::Incoming;
use hyper_util::rt::TokioExecutor;
use hyper_util::server::conn::auto::Builder;
use rand::rngs::StdRng;
use rand::SeedableRng;
pub use reqwest_middleware::{ClientWithMiddleware, Error};
pub use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use tokio::time::timeout;
use tokio_rustls::TlsAcceptor;
use tokio_util::task::TaskTracker;

use crate::cancellation::CancellationHandlerMain;
use crate::config::ProxyConfig;
use crate::context::RequestMonitoring;
use crate::metrics::{NUM_CLIENT_CONNECTION_GAUGE, TLS_HANDSHAKE_FAILURES};
use crate::protocol2::read_proxy_protocol;
use crate::proxy::run_until_cancelled;
use crate::rate_limiter::EndpointRateLimiter;
use crate::serverless::backend::PoolingBackend;
use crate::serverless::http_util::{api_error_into_response, json_response};

use std::net::{IpAddr, SocketAddr};
use std::pin::pin;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn, Instrument};
use utils::http::error::ApiError;

pub const SERVERLESS_DRIVER_SNI: &str = "api";

pub async fn task_main(
    config: &'static ProxyConfig,
    ws_listener: TcpListener,
    cancellation_token: CancellationToken,
    endpoint_rate_limiter: Arc<EndpointRateLimiter>,
    cancellation_handler: Arc<CancellationHandlerMain>,
) -> anyhow::Result<()> {
    scopeguard::defer! {
        info!("websocket server has shut down");
    }

    let conn_pool = conn_pool::GlobalConnPool::new(&config.http_config);
    {
        let conn_pool = Arc::clone(&conn_pool);
        tokio::spawn(async move {
            conn_pool.gc_worker(StdRng::from_entropy()).await;
        });
    }

    // shutdown the connection pool
    tokio::spawn({
        let cancellation_token = cancellation_token.clone();
        let conn_pool = conn_pool.clone();
        async move {
            cancellation_token.cancelled().await;
            tokio::task::spawn_blocking(move || conn_pool.shutdown())
                .await
                .unwrap();
        }
    });

    let backend = Arc::new(PoolingBackend {
        pool: Arc::clone(&conn_pool),
        config,
    });

    let tls_config = match config.tls_config.as_ref() {
        Some(config) => config,
        None => {
            warn!("TLS config is missing, WebSocket Secure server will not be started");
            return Ok(());
        }
    };
    let mut tls_server_config = rustls::ServerConfig::clone(&tls_config.to_server_config());
    // prefer http2, but support http/1.1
    tls_server_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    let tls_acceptor: tokio_rustls::TlsAcceptor = Arc::new(tls_server_config).into();

    let connections = tokio_util::task::task_tracker::TaskTracker::new();
    connections.close(); // allows `connections.wait to complete`

    let server = Builder::new(hyper_util::rt::TokioExecutor::new());

    while let Some(res) = run_until_cancelled(ws_listener.accept(), &cancellation_token).await {
        let (conn, peer_addr) = res.context("could not accept TCP stream")?;
        if let Err(e) = conn.set_nodelay(true) {
            tracing::error!("could not set nodelay: {e}");
            continue;
        }
        let conn_id = uuid::Uuid::new_v4();
        let http_conn_span = tracing::info_span!("http_conn", ?conn_id);

        connections.spawn(
            connection_handler(
                config,
                backend.clone(),
                connections.clone(),
                cancellation_handler.clone(),
                endpoint_rate_limiter.clone(),
                cancellation_token.clone(),
                server.clone(),
                tls_acceptor.clone(),
                conn,
                peer_addr,
            )
            .instrument(http_conn_span),
        );
    }

    connections.wait().await;

    Ok(())
}

/// Handles the TCP lifecycle.
///
/// 1. Parses PROXY protocol V2
/// 2. Handles TLS handshake
/// 3. Handles HTTP connection
///     1. With graceful shutdowns
///     2. With graceful request cancellation with connection failure
///     3. With websocket upgrade support.
#[allow(clippy::too_many_arguments)]
async fn connection_handler(
    config: &'static ProxyConfig,
    backend: Arc<PoolingBackend>,
    connections: TaskTracker,
    cancellation_handler: Arc<CancellationHandlerMain>,
    endpoint_rate_limiter: Arc<EndpointRateLimiter>,
    cancellation_token: CancellationToken,
    server: Builder<TokioExecutor>,
    tls_acceptor: TlsAcceptor,
    conn: TcpStream,
    peer_addr: SocketAddr,
) {
    let session_id = uuid::Uuid::new_v4();

    let _gauge = NUM_CLIENT_CONNECTION_GAUGE
        .with_label_values(&["http"])
        .guard();

    // handle PROXY protocol
    let (conn, peer) = match read_proxy_protocol(conn).await {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(?session_id, %peer_addr, "failed to accept TCP connection: invalid PROXY protocol V2 header: {e:#}");
            return;
        }
    };

    let peer_addr = peer.unwrap_or(peer_addr).ip();
    info!(?session_id, %peer_addr, "accepted new TCP connection");

    // try upgrade to TLS, but with a timeout.
    let conn = match timeout(config.handshake_timeout, tls_acceptor.accept(conn)).await {
        Ok(Ok(conn)) => {
            info!(?session_id, %peer_addr, "accepted new TLS connection");
            conn
        }
        // The handshake failed
        Ok(Err(e)) => {
            TLS_HANDSHAKE_FAILURES.inc();
            warn!(?session_id, %peer_addr, "failed to accept TLS connection: {e:?}");
            return;
        }
        // The handshake timed out
        Err(e) => {
            TLS_HANDSHAKE_FAILURES.inc();
            warn!(?session_id, %peer_addr, "failed to accept TLS connection: {e:?}");
            return;
        }
    };

    let session_id = AtomicTake::new(session_id);

    // Cancel all current inflight HTTP requests if the HTTP connection is closed.
    let http_cancellation_token = CancellationToken::new();
    let _cancel_connection = http_cancellation_token.clone().drop_guard();

    let conn = server.serve_connection_with_upgrades(
        hyper_util::rt::TokioIo::new(conn),
        hyper1::service::service_fn(move |req: hyper1::Request<Incoming>| {
            // First HTTP request shares the same session ID
            let session_id = session_id.take().unwrap_or_else(uuid::Uuid::new_v4);

            // Cancel the current inflight HTTP request if the requets stream is closed.
            // This is slightly different to `_cancel_connection` in that
            // h2 can cancel individual requests with a `RST_STREAM`.
            let http_request_token = http_cancellation_token.child_token();
            let cancel_request = http_request_token.clone().drop_guard();

            // `request_handler` is not cancel safe. It expects to be cancelled only at specific times.
            // By spawning the future, we ensure it never gets cancelled until it decides to.
            let handler = connections.spawn(
                request_handler(
                    req,
                    config,
                    backend.clone(),
                    connections.clone(),
                    cancellation_handler.clone(),
                    session_id,
                    peer_addr,
                    endpoint_rate_limiter.clone(),
                    http_request_token,
                )
                .in_current_span()
                .map_ok_or_else(api_error_into_response, |r| r),
            );

            async move {
                let res = handler.await;
                cancel_request.disarm();
                res
            }
        }),
    );

    // On cancellation, trigger the HTTP connection handler to shut down.
    let res = match select(pin!(cancellation_token.cancelled()), pin!(conn)).await {
        Either::Left((_cancelled, mut conn)) => {
            conn.as_mut().graceful_shutdown();
            conn.await
        }
        Either::Right((res, _)) => res,
    };

    match res {
        Ok(()) => tracing::info!(%peer_addr, "HTTP connection closed"),
        Err(e) => tracing::warn!(%peer_addr, "HTTP connection error {e}"),
    }
}

#[allow(clippy::too_many_arguments)]
async fn request_handler(
    mut request: hyper1::Request<Incoming>,
    config: &'static ProxyConfig,
    backend: Arc<PoolingBackend>,
    ws_connections: TaskTracker,
    cancellation_handler: Arc<CancellationHandlerMain>,
    session_id: uuid::Uuid,
    peer_addr: IpAddr,
    endpoint_rate_limiter: Arc<EndpointRateLimiter>,
    // used to cancel in-flight HTTP requests. not used to cancel websockets
    http_cancellation_token: CancellationToken,
) -> Result<Response<Full<Bytes>>, ApiError> {
    let host = request
        .headers()
        .get("host")
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.split(':').next())
        .map(|s| s.to_string());

    // Check if the request is a websocket upgrade request.
    if hyper_tungstenite::is_upgrade_request(&request) {
        let ctx = RequestMonitoring::new(session_id, peer_addr, "ws", &config.region);
        let span = ctx.span.clone();
        info!(parent: &span, "performing websocket upgrade");

        let (response, websocket) = hyper_tungstenite::upgrade(&mut request, None)
            .map_err(|e| ApiError::BadRequest(e.into()))?;

        ws_connections.spawn(
            async move {
                if let Err(e) = websocket::serve_websocket(
                    config,
                    ctx,
                    websocket,
                    cancellation_handler,
                    host,
                    endpoint_rate_limiter,
                )
                .await
                {
                    error!("error in websocket connection: {e:#}");
                }
            }
            .instrument(span),
        );

        // Return the response so the spawned future can continue.
        Ok(response)
    } else if request.uri().path() == "/sql" && *request.method() == Method::POST {
        let ctx = RequestMonitoring::new(session_id, peer_addr, "http", &config.region);
        let span = ctx.span.clone();

        sql_over_http::handle(config, ctx, request, backend, http_cancellation_token)
            .instrument(span)
            .await
    } else if request.uri().path() == "/sql" && *request.method() == Method::OPTIONS {
        Response::builder()
            .header("Allow", "OPTIONS, POST")
            .header("Access-Control-Allow-Origin", "*")
            .header(
                "Access-Control-Allow-Headers",
                "Neon-Connection-String, Neon-Raw-Text-Output, Neon-Array-Mode, Neon-Pool-Opt-In, Neon-Batch-Read-Only, Neon-Batch-Isolation-Level",
            )
            .header("Access-Control-Max-Age", "86400" /* 24 hours */)
            .status(StatusCode::OK) // 204 is also valid, but see: https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods/OPTIONS#status_code
            .body(Full::new(Bytes::new()))
            .map_err(|e| ApiError::InternalServerError(e.into()))
    } else {
        json_response(StatusCode::BAD_REQUEST, "query is not supported")
    }
}
