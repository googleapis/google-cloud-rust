// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Google Cloud Storage Rust core exposed as a gRPC server over a Unix Domain Socket (UDS).
//!
//! Supports all three deployment topologies specified in the GCS SDK Consolidation architecture:
//! 1. **Multi-process shared sidecar daemon (`gcs-core-server`):** A single per-UID daemon process
//!    listening on a `0600` Unix Domain Socket with `SO_PEERCRED` peer-UID authentication,
//!    per-caller OAuth2 token-lending sessions bound to `(uid, pid)`, active-connection reference
//!    counting (`TrackedUnixStream`), dead-PID session/handle reaping, idle timeout eviction,
//!    stale-socket recovery, and an optional Linux `memfd` + `SCM_RIGHTS` shared-memory zero-copy
//!    read data plane.
//! 2. **Per-process `1:1` child sidecar (`--pdeathsig=true`):** Requests `PR_SET_PDEATHSIG(SIGTERM)`
//!    on Linux so the sidecar terminates immediately if its spawning parent process exits.
//! 3. **In-process background server (`gcs_core_start_inproc`):** Spawns the identical `GcsCore`
//!    tonic server inside the caller's OS process on a background Tokio runtime thread.

pub mod service;
pub mod session;
pub mod shm;

use clap::Parser;
use std::ffi::{CStr, c_char, c_int};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{UnixListener, UnixStream};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;
use tonic::transport::server::{Connected, UdsConnectInfo};

/// Wrapper around `tokio::net::UnixStream` that decrements `Shared::active_conns`
/// and refreshes `Shared::last_rpc` when a client UDS connection closes (including on `SIGKILL`).
pub struct TrackedUnixStream {
    inner: UnixStream,
    shared: Arc<Shared>,
}

impl Connected for TrackedUnixStream {
    type ConnectInfo = UdsConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.inner.connect_info()
    }
}

impl AsyncRead for TrackedUnixStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for TrackedUnixStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl Drop for TrackedUnixStream {
    fn drop(&mut self) {
        self.shared.active_conns.fetch_sub(1, Ordering::Relaxed);
        self.shared.last_rpc.store(now_ms(), Ordering::Relaxed);
    }
}

#[allow(missing_docs, clippy::all)]
pub mod pb {
    include!("generated/protos/gcscore.v1.rs");
}

/// Configuration parameters for the `gcs-core-server` gRPC-over-UDS daemon.
#[derive(Parser, Debug, Clone)]
#[command(version, about = "Google Cloud Storage Rust core gRPC-over-UDS server")]
pub struct Args {
    /// Unix Domain Socket path. Prefix with `@` on Linux to use the abstract socket namespace.
    #[arg(long, default_value = "/tmp/gcs-core.sock")]
    pub socket: String,

    /// Exit after this many seconds with zero active connections and no RPCs (0 = never).
    #[arg(long, default_value_t = 300)]
    pub idle_secs: u64,

    /// Default upstream GCS transport when a request leaves `transport` unspecified (`json` or `grpc`).
    #[arg(long, default_value = "json")]
    pub default_transport: String,

    /// Number of gRPC subchannels to open in the upstream `Storage` pool (0 = available vCPUs, min 4).
    #[arg(long, default_value_t = 0)]
    pub grpc_subchannels: usize,

    /// Linux 1:1 child mode: request `PR_SET_PDEATHSIG(SIGTERM)` when the spawning parent exits.
    /// Defaults to `false` so a shared multi-process daemon survives when the initial spawner exits.
    #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
    pub pdeathsig: bool,

    /// Enforce `SO_PEERCRED` check rejecting UDS connections from other OS user IDs.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub peer_uid_check: bool,

    /// Default target bytes per streamed `ReadChunk` message.
    #[arg(long, default_value_t = 2 * 1024 * 1024)]
    pub chunk_size: usize,

    /// Optional explicit GCS endpoint URL (e.g. for `storage-grpc-mock` or local testbench).
    #[arg(long)]
    pub endpoint: Option<String>,

    /// Optional additional TCP listen address (`host:port`) for UDS-vs-TCP microbenchmarks.
    #[arg(long)]
    pub tcp: Option<String>,

    /// Reject data RPCs that do not carry a valid per-caller `x-session` token-lending header.
    #[arg(long, default_value_t = false)]
    pub require_session: bool,

    /// Per-client shared-memory region size served on `<socket>.shm` via `SCM_RIGHTS` (0 = disabled).
    #[arg(long, default_value_t = 256 * 1024 * 1024)]
    pub shm_size: usize,

    /// Tokio worker threads serving the core (0 = one per available CPU, Tokio's default).
    #[arg(long, default_value_t = 0)]
    pub worker_threads: usize,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            socket: "/tmp/gcs-core.sock".to_string(),
            idle_secs: 300,
            default_transport: "json".to_string(),
            grpc_subchannels: 0,
            pdeathsig: false,
            peer_uid_check: true,
            chunk_size: 2 * 1024 * 1024,
            endpoint: None,
            tcp: None,
            require_session: false,
            shm_size: 256 * 1024 * 1024,
            worker_threads: 0,
        }
    }
}

/// Builds the multi-threaded Tokio runtime that serves the core, sized by `--worker-threads`.
pub fn build_runtime(args: &Args, thread_name: &str) -> std::io::Result<tokio::runtime::Runtime> {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all().thread_name(thread_name);
    if args.worker_threads > 0 {
        builder.worker_threads(args.worker_threads);
    }
    builder.build()
}

/// Shared server state for activity tracking, connection reference counting, and shutdown coordination.
pub struct Shared {
    pub last_rpc: AtomicI64,
    pub rpcs: AtomicU64,
    pub active_conns: AtomicUsize,
    pub started: Instant,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
}

impl Shared {
    /// Creates a new `Shared` state tracker initialized to the current timestamp.
    pub fn new() -> Arc<Self> {
        let (shutdown_tx, _) = tokio::sync::watch::channel(false);
        Arc::new(Self {
            last_rpc: AtomicI64::new(now_ms()),
            rpcs: AtomicU64::new(0),
            active_conns: AtomicUsize::new(0),
            started: Instant::now(),
            shutdown_tx,
        })
    }

    /// Records an incoming RPC and refreshes the idle timer.
    pub fn touch(&self) {
        self.rpcs.fetch_add(1, Ordering::Relaxed);
        self.last_rpc.store(now_ms(), Ordering::Relaxed);
    }

    /// Signals all server tasks to initiate graceful shutdown.
    pub fn trigger_shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    /// Waits until graceful shutdown has been triggered.
    pub async fn wait_shutdown(&self) {
        let mut rx = self.shutdown_tx.subscribe();
        let _ = rx.wait_for(|&done| done).await;
    }
}

/// Returns the current Unix epoch timestamp in milliseconds.
pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Binds a Unix Domain Socket listener (supporting both filesystem paths with stale-socket
/// cleanup + `0600` permissions and Linux `@` abstract socket names).
pub fn bind_uds_listener(
    sock: &str,
) -> Result<UnixListener, Box<dyn std::error::Error + Send + Sync>> {
    if let Some(name) = sock.strip_prefix('@') {
        #[cfg(target_os = "linux")]
        {
            use std::os::linux::net::SocketAddrExt;
            let addr = std::os::unix::net::SocketAddr::from_abstract_name(name.as_bytes())?;
            let std_l = std::os::unix::net::UnixListener::bind_addr(&addr)?;
            std_l.set_nonblocking(true)?;
            Ok(UnixListener::from_std(std_l)?)
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = name;
            Err("abstract sockets are Linux-only".into())
        }
    } else {
        let p = PathBuf::from(sock);
        if let Ok(meta) = std::fs::symlink_metadata(&p) {
            #[cfg(unix)]
            {
                use std::os::unix::fs::FileTypeExt;
                if !meta.file_type().is_socket() {
                    return Err(format!("refusing to remove non-socket file {sock}").into());
                }
            }
            match std::os::unix::net::UnixStream::connect(&p) {
                Ok(_) => {
                    return Err(format!("another server is already listening on {sock}").into());
                }
                Err(_) => {
                    tracing::warn!("removing stale socket {sock}");
                    std::fs::remove_file(&p)?;
                }
            }
        }
        if let Some(dir) = p.parent()
            && !dir.as_os_str().is_empty()
        {
            std::fs::create_dir_all(dir)?;
        }
        let l = UnixListener::bind(&p)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o600))?;
        }
        Ok(l)
    }
}

/// Runs the `GcsCore` gRPC server until triggered by `Shutdown` RPC, idle timeout, or OS signal.
pub async fn run(args: Args) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = bind_uds_listener(&args.socket)?;
    let shared = Shared::new();
    #[cfg(unix)]
    {
        let s = shared.clone();
        tokio::spawn(async move {
            if let Ok(mut term) =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            {
                tokio::select! {
                    _ = term.recv() => {},
                    _ = tokio::signal::ctrl_c() => {},
                    _ = s.wait_shutdown() => return,
                }
                s.trigger_shutdown();
            }
        });
    }
    eprintln!(
        "gcs-core-server pid={} listening on {}",
        std::process::id(),
        args.socket
    );
    println!("READY {}", args.socket);
    serve_with_listener(args, listener, shared).await
}

/// Serves `GcsCore` on an already-bound `UnixListener`. Used directly by integration tests
/// and in-process server launches.
pub async fn serve_with_listener(
    args: Args,
    listener: UnixListener,
    shared: Arc<Shared>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let shm = Arc::new(shm::Registry::new());
    if args.shm_size > 0 && !args.socket.starts_with('@') {
        let r = shm.clone();
        let p = format!("{}.shm", args.socket);
        let shm_size = args.shm_size;
        let peer_uid_check = args.peer_uid_check;
        let shm_shared = shared.clone();
        if let Some(dir) = PathBuf::from(&p).parent()
            && !dir.as_os_str().is_empty()
        {
            std::fs::create_dir_all(dir)?;
        }
        tokio::spawn(async move {
            tokio::select! {
                res = r.serve(p, shm_size, peer_uid_check) => {
                    if let Err(e) = res {
                        tracing::error!("shm side channel: {e}");
                    }
                }
                _ = shm_shared.wait_shutdown() => {}
            }
        });
    }

    let svc = service::GcsCoreService::new(&args, shared.clone(), shm).await?;
    let reaper_sessions = svc.sessions();
    let reaper_handles = svc.handles();
    // SAFETY: `getuid()` is always safe to call and cannot fail.
    let my_uid = unsafe { libc::getuid() };
    let peer_check = args.peer_uid_check;

    let auth = move |req: tonic::Request<()>| -> Result<tonic::Request<()>, tonic::Status> {
        if !peer_check {
            return Ok(req);
        }
        match req.extensions().get::<UdsConnectInfo>() {
            Some(info) => match &info.peer_cred {
                Some(c) if c.uid() == my_uid => Ok(req),
                Some(c) => Err(tonic::Status::permission_denied(format!(
                    "peer uid {} != server uid {}",
                    c.uid(),
                    my_uid
                ))),
                None => Err(tonic::Status::permission_denied("no peer credentials")),
            },
            None => Err(tonic::Status::permission_denied(
                "peer uid check requires UDS peer credentials; pass --peer-uid-check=false when using --tcp",
            )),
        }
    };

    let inner = pb::gcs_core_server::GcsCoreServer::new(svc)
        .max_decoding_message_size(64 * 1024 * 1024)
        .max_encoding_message_size(64 * 1024 * 1024);
    let grpc = tonic::service::interceptor::InterceptedService::new(inner, auth);

    {
        let s = shared.clone();
        let idle = (args.idle_secs as i64) * 1000;
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                    _ = s.wait_shutdown() => return,
                }
                let reaped_sessions = reaper_sessions.reap_dead_pids().await;
                let reaped_handles = service::reap_dead_handles(&reaper_handles);
                if reaped_sessions > 0 || reaped_handles > 0 {
                    tracing::info!(
                        reaped_sessions,
                        reaped_handles,
                        "reaped resources from exited client processes"
                    );
                }
                if idle <= 0 {
                    continue;
                }
                if s.active_conns.load(Ordering::Relaxed) > 0 {
                    s.last_rpc.store(now_ms(), Ordering::Relaxed);
                    continue;
                }
                if now_ms() - s.last_rpc.load(Ordering::Relaxed) > idle {
                    tracing::info!(
                        "idle for {}s with 0 active connections, exiting",
                        idle / 1000
                    );
                    s.trigger_shutdown();
                    return;
                }
            }
        });
    }

    let sock = args.socket.clone();
    let uds_shared = shared.clone();
    let conn_shared = shared.clone();
    let uds_grpc = grpc.clone();
    let incoming = UnixListenerStream::new(listener).map(move |res| {
        res.map(|s| {
            conn_shared.active_conns.fetch_add(1, Ordering::Relaxed);
            conn_shared.last_rpc.store(now_ms(), Ordering::Relaxed);
            #[cfg(unix)]
            // SAFETY: `s.as_raw_fd()` is a valid open socket descriptor and `sz` is a properly
            // sized `c_int` buffer for `SO_SNDBUF` / `SO_RCVBUF`.
            unsafe {
                use std::os::unix::io::AsRawFd;
                let fd = s.as_raw_fd();
                let sz: libc::c_int = 8 * 1024 * 1024;
                let _ = libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    libc::SO_SNDBUF,
                    &sz as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&sz) as libc::socklen_t,
                );
                let _ = libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    libc::SO_RCVBUF,
                    &sz as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&sz) as libc::socklen_t,
                );
            }
            TrackedUnixStream {
                inner: s,
                shared: conn_shared.clone(),
            }
        })
    });

    let uds = Server::builder()
        .initial_stream_window_size(64 * 1024 * 1024)
        .initial_connection_window_size(256 * 1024 * 1024)
        .max_frame_size(4 * 1024 * 1024)
        .add_service(uds_grpc)
        .serve_with_incoming_shutdown(incoming, async move {
            uds_shared.wait_shutdown().await;
        });

    if let Some(tcp) = args.tcp.clone() {
        let tcp_shared = shared.clone();
        let addr: std::net::SocketAddr = tcp.parse()?;
        let tcp_srv = Server::builder()
            .initial_stream_window_size(64 * 1024 * 1024)
            .initial_connection_window_size(256 * 1024 * 1024)
            .max_frame_size(4 * 1024 * 1024)
            .add_service(grpc)
            .serve_with_shutdown(addr, async move {
                tcp_shared.wait_shutdown().await;
            });
        let (a, b) = tokio::join!(uds, tcp_srv);
        a?;
        b?;
    } else {
        uds.await?;
    }

    if !sock.starts_with('@') {
        let _ = std::fs::remove_file(&sock);
        let _ = std::fs::remove_file(format!("{sock}.shm"));
    }
    Ok(())
}

struct InprocHandle {
    shared: Arc<Shared>,
    join: Option<std::thread::JoinHandle<()>>,
}

static INPROC_SERVERS: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<String, InprocHandle>>,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashMap::new()));

thread_local! {
    /// Why the last `gcs_core_start_inproc*` call on this thread failed.
    static INPROC_LAST_ERROR: std::cell::RefCell<String> =
        const { std::cell::RefCell::new(String::new()) };
}

fn set_inproc_last_error(message: String) {
    INPROC_LAST_ERROR.with(|e| *e.borrow_mut() = message);
}

/// Parses the flags of an in-process server: the same flags as `gcs-core-server`, without a
/// program name.
///
/// Differences from the binary, each because the server lives inside someone else's process:
/// `--socket` is required so an embedded server never claims the daemon's default path;
/// `--idle-secs` defaults to 0 because an idle exit would stop a server the host still owns;
/// `--pdeathsig` (a property of a child process) and `--tcp` (a network listener inside the host)
/// are rejected.
pub fn parse_inproc_args<I, T>(flags: I) -> Result<Args, String>
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
{
    use clap::parser::ValueSource;
    use clap::{CommandFactory, FromArgMatches};

    let flags: Vec<std::ffi::OsString> = flags.into_iter().map(Into::into).collect();
    // This crate builds clap without `error-context`, so its messages do not name the offending
    // flag; include the flags so a caller across the C ABI can see what was rejected.
    let invalid = |e: clap::Error| format!("invalid in-process server flags {flags:?}: {e}");
    let matches = Args::command()
        .no_binary_name(true)
        .try_get_matches_from(flags.clone())
        .map_err(invalid)?;
    let mut args = Args::from_arg_matches(&matches).map_err(invalid)?;
    let on_command_line = |id: &str| matches.value_source(id) == Some(ValueSource::CommandLine);
    if !on_command_line("socket") {
        return Err("--socket is required for an in-process server".to_string());
    }
    if on_command_line("pdeathsig") {
        return Err("--pdeathsig applies only to a separate gcs-core-server process".to_string());
    }
    if args.tcp.is_some() {
        return Err("--tcp is not supported for an in-process server".to_string());
    }
    if !on_command_line("idle_secs") {
        args.idle_secs = 0;
    }
    Ok(args)
}

/// Starts `args` on a background runtime thread and registers it for `gcs_core_stop_inproc`.
/// Returns once the listener is bound, or why it could not be.
fn start_inproc(args: Args) -> Result<(), String> {
    let sock = args.socket.clone();
    let rt = build_runtime(&args, "gcs-core-inproc").map_err(|e| format!("tokio runtime: {e}"))?;
    let shared = Shared::new();
    let srv_shared = shared.clone();
    let (tx, rx) = std::sync::mpsc::sync_channel::<Result<(), String>>(1);
    let join = std::thread::spawn(move || {
        rt.block_on(async move {
            let listener = match bind_uds_listener(&args.socket) {
                Ok(l) => l,
                Err(e) => {
                    let _ = tx.send(Err(format!("bind {}: {e}", args.socket)));
                    return;
                }
            };
            let _ = tx.send(Ok(()));
            let _ = serve_with_listener(args, listener, srv_shared).await;
        });
    });
    match rx.recv() {
        Ok(Ok(())) => {
            INPROC_SERVERS.lock().unwrap().insert(
                sock,
                InprocHandle {
                    shared,
                    join: Some(join),
                },
            );
            Ok(())
        }
        Ok(Err(e)) => {
            let _ = join.join();
            Err(e)
        }
        Err(_) => {
            let _ = join.join();
            Err("in-process server thread exited before binding".to_string())
        }
    }
}

fn inproc_result(result: Result<(), String>) -> i32 {
    match result {
        Ok(()) => 0,
        Err(e) => {
            set_inproc_last_error(e);
            -1
        }
    }
}

/// C-FFI entry point for launching the gRPC-over-UDS server **in-process** on a background
/// Tokio runtime thread with default flags. Returns `0` once the UDS listener is bound and ready
/// to accept connections, or `-1` on error (see [`gcs_core_inproc_last_error`]).
///
/// # Safety
/// `socket_path` must be a valid, non-null, NUL-terminated C string pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcs_core_start_inproc(socket_path: *const c_char) -> i32 {
    std::panic::catch_unwind(|| {
        if socket_path.is_null() {
            return inproc_result(Err("socket_path is null".to_string()));
        }
        // SAFETY: The caller guarantees `socket_path` is a non-null, NUL-terminated C string.
        let sock = match unsafe { CStr::from_ptr(socket_path) }.to_str() {
            Ok(s) => s.to_owned(),
            Err(_) => return inproc_result(Err("socket_path is not UTF-8".to_string())),
        };
        inproc_result(start_inproc(Args {
            socket: sock,
            idle_secs: 0,
            ..Default::default()
        }))
    })
    .unwrap_or(-1)
}

/// Collects `argv[0..argc)` into owned strings.
///
/// # Safety
/// `argv` must point to `argc` valid NUL-terminated C strings when `argc > 0`.
unsafe fn c_flags(argc: c_int, argv: *const *const c_char) -> Result<Vec<String>, String> {
    let count = usize::try_from(argc).map_err(|_| format!("argc is negative ({argc})"))?;
    if count == 0 {
        return Ok(Vec::new());
    }
    if argv.is_null() {
        return Err("argv is null".to_string());
    }
    // SAFETY: The caller guarantees `argv` points to `argc` elements.
    let pointers = unsafe { std::slice::from_raw_parts(argv, count) };
    pointers
        .iter()
        .enumerate()
        .map(|(i, &p)| {
            if p.is_null() {
                return Err(format!("argv[{i}] is null"));
            }
            // SAFETY: The caller guarantees each element is a NUL-terminated C string.
            unsafe { CStr::from_ptr(p) }
                .to_str()
                .map(str::to_owned)
                .map_err(|_| format!("argv[{i}] is not UTF-8"))
        })
        .collect()
}

/// C-FFI entry point for launching the in-process server with the same flags as
/// `gcs-core-server` (see [`parse_inproc_args`]); `argv` holds flags only, no program name.
/// Returns `0` once the UDS listener is bound, or `-1` on error (see
/// [`gcs_core_inproc_last_error`]). Stop it with [`gcs_core_stop_inproc`] and the `--socket` value.
///
/// # Safety
/// `argv` must point to `argc` valid NUL-terminated C strings when `argc > 0`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcs_core_start_inproc_with_args(
    argc: c_int,
    argv: *const *const c_char,
) -> i32 {
    std::panic::catch_unwind(|| {
        // SAFETY: Forwarded caller guarantee.
        let result = unsafe { c_flags(argc, argv) }
            .and_then(parse_inproc_args)
            .and_then(start_inproc);
        inproc_result(result)
    })
    .unwrap_or(-1)
}

/// Copies the message of the last failed `gcs_core_start_inproc*` call on the calling thread into
/// `buf` (truncated to `cap - 1` bytes and NUL-terminated) and returns the full message length.
/// Pass a null `buf` or `cap == 0` to query the length only.
///
/// # Safety
/// `buf` must be null or point to `cap` writable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcs_core_inproc_last_error(buf: *mut c_char, cap: usize) -> usize {
    INPROC_LAST_ERROR.with(|e| {
        let message = e.borrow();
        if !buf.is_null() && cap > 0 {
            let n = message.len().min(cap - 1);
            // SAFETY: `buf` has `cap` writable bytes and `n < cap`; source and destination do not
            // overlap because `message` is owned by this module.
            unsafe {
                std::ptr::copy_nonoverlapping(message.as_ptr().cast::<c_char>(), buf, n);
                *buf.add(n) = 0;
            }
        }
        message.len()
    })
}

/// C-FFI entry point for stopping an in-process gRPC-over-UDS server previously started via
/// [`gcs_core_start_inproc`] and waiting for its background Tokio runtime thread to exit.
/// Returns `0` on clean shutdown or `-1` if no in-process server was registered for `socket_path`.
///
/// # Safety
/// `socket_path` must be a valid, non-null, NUL-terminated C string pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcs_core_stop_inproc(socket_path: *const c_char) -> i32 {
    std::panic::catch_unwind(|| {
        if socket_path.is_null() {
            return -1;
        }
        // SAFETY: The caller guarantees `socket_path` is a non-null, NUL-terminated C string.
        let sock = match unsafe { CStr::from_ptr(socket_path) }.to_str() {
            Ok(s) => s,
            Err(_) => return -1,
        };
        let handle = INPROC_SERVERS.lock().unwrap().remove(sock);
        match handle {
            Some(mut h) => {
                h.shared.trigger_shutdown();
                if let Some(j) = h.join.take() {
                    let _ = j.join();
                }
                0
            }
            None => -1,
        }
    })
    .unwrap_or(-1)
}
