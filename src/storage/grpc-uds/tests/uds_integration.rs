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

//! End-to-end integration tests for `storage-grpc-uds` (`gcs-core-server`).

use bytes::Bytes;
use httptest::{Expectation, Server as HttpServer, matchers::*, responders::*};
use hyper_util::rt::TokioIo;
use std::path::PathBuf;
use storage_grpc_uds::pb::gcs_core_client::GcsCoreClient;
use storage_grpc_uds::pb::*;
use storage_grpc_uds::{Args, Shared, bind_uds_listener, serve_with_listener};
use tokio::net::UnixStream;
use tokio_stream::StreamExt;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;

async fn connect_uds(sock: PathBuf) -> anyhow::Result<GcsCoreClient<Channel>> {
    let channel = Endpoint::try_from("http://[::]:50051")?
        .connect_with_connector(service_fn(move |_: Uri| {
            let p = sock.clone();
            async move { Ok::<_, std::io::Error>(TokioIo::new(UnixStream::connect(p).await?)) }
        }))
        .await?;
    Ok(GcsCoreClient::new(channel)
        .max_decoding_message_size(64 * 1024 * 1024)
        .max_encoding_message_size(64 * 1024 * 1024))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ping_stats_and_stale_socket_recovery() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let sock_path = dir.path().join("gcs-core.sock");
    let sock_str = sock_path.to_string_lossy().to_string();

    // Create a stale socket file by binding and immediately dropping a raw UnixListener.
    {
        let _stale = std::os::unix::net::UnixListener::bind(&sock_path)?;
    }
    assert!(
        sock_path.exists(),
        "stale socket file should exist before bind_uds_listener"
    );

    let args = Args {
        socket: sock_str.clone(),
        idle_secs: 0,
        shm_size: 0,
        endpoint: Some("http://127.0.0.1:1".to_string()),
        ..Default::default()
    };

    let listener = bind_uds_listener(&args.socket)
        .map_err(|e| anyhow::anyhow!("bind_uds_listener failed: {e}"))?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&sock_path)?.permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "UDS socket must have 0600 permissions");
    }

    let shared = Shared::new();
    let srv_shared = shared.clone();
    let srv_handle = tokio::spawn(async move {
        let _ = serve_with_listener(args, listener, srv_shared).await;
    });

    let mut client = connect_uds(sock_path.clone()).await?;
    let payload = Bytes::from_static(b"hello-uds-core");
    let resp = client
        .ping(PingRequest {
            payload: payload.clone(),
        })
        .await?
        .into_inner();
    assert_eq!(resp.payload, payload);

    let stats = client
        .stats(StatsRequest {
            reset_peak_rss: false,
        })
        .await?
        .into_inner();
    assert!(stats.rpcs_served >= 1);
    assert_eq!(stats.pid as u32, std::process::id());

    client.shutdown(Empty {}).await?;
    srv_handle.await?;
    assert!(
        !sock_path.exists(),
        "socket file should be removed after shutdown"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_lifecycle_and_peer_isolation() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let sock_path = dir.path().join("session.sock");

    let args = Args {
        socket: sock_path.to_string_lossy().to_string(),
        idle_secs: 0,
        shm_size: 0,
        require_session: true,
        endpoint: Some("http://127.0.0.1:1".to_string()),
        ..Default::default()
    };

    let listener = bind_uds_listener(&args.socket)
        .map_err(|e| anyhow::anyhow!("bind_uds_listener failed: {e}"))?;
    let shared = Shared::new();
    let srv_shared = shared.clone();
    let srv_handle = tokio::spawn(async move {
        let _ = serve_with_listener(args, listener, srv_shared).await;
    });

    let mut client = connect_uds(sock_path.clone()).await?;

    // Data RPC without x-session when --require-session is enabled must return UNAUTHENTICATED.
    let err = client
        .list_objects(ListObjectsRequest {
            bucket: "b".to_string(),
            ..Default::default()
        })
        .await
        .expect_err("expected UNAUTHENTICATED without x-session");
    assert_eq!(err.code(), tonic::Code::Unauthenticated);

    let now_secs = storage_grpc_uds::now_ms() / 1000;
    // Create a valid session via UDS (peer_cred carries our current UID & PID).
    let sess = client
        .create_session(CreateSessionRequest {
            access_token: "test-bearer-initial-token".to_string(),
            expiry_unix_secs: now_secs + 60,
            quota_project: "test-quota-proj".to_string(),
        })
        .await?
        .into_inner();
    assert_eq!(sess.session_id.len(), 64);

    // Rotate token via UpdateToken.
    client
        .update_token(UpdateTokenRequest {
            session_id: sess.session_id.clone(),
            access_token: "test-bearer-rotated-token".to_string(),
            expiry_unix_secs: now_secs + 120,
        })
        .await?;

    // Close session and confirm subsequent UpdateToken returns UNAUTHENTICATED.
    client
        .close_session(SessionRef {
            session_id: sess.session_id.clone(),
        })
        .await?;
    let err = client
        .update_token(UpdateTokenRequest {
            session_id: sess.session_id,
            access_token: "test-bearer-after-close".to_string(),
            expiry_unix_secs: 0,
        })
        .await
        .expect_err("closed session should not exist");
    assert_eq!(err.code(), tonic::Code::Unauthenticated);

    // Direct cross-PID rejection unit check on `Sessions` registry.
    let sessions =
        storage_grpc_uds::session::Sessions::new(Some("http://127.0.0.1:1".to_string()), 4);
    let created = sessions
        .create("tok".to_string(), 0, None, 1000, 4242)
        .await
        .map_err(|e| anyhow::anyhow!(e))?;
    let cross_pid = sessions
        .get(&created.id, 1000, 9999)
        .await
        .err()
        .expect("mismatched PID must be rejected");
    assert_eq!(cross_pid.code(), tonic::Code::PermissionDenied);

    client.shutdown(Empty {}).await?;
    srv_handle.await?;
    Ok(())
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shm_memfd_scm_rights_and_zero_copy_read() -> anyhow::Result<()> {
    let payload = b"zero-copy-memfd-scm-rights-payload-0123456789";
    let http_srv = HttpServer::run();
    http_srv.expect(
        Expectation::matching(all_of![
            request::method_path("GET", "/storage/v1/b/test-bucket/o/test-obj"),
            request::query(url_decoded(contains(("alt", "media")))),
        ])
        .respond_with(
            status_code(200)
                .append_header("x-goog-generation", "77")
                .body(payload.as_slice()),
        ),
    );

    let dir = tempfile::tempdir()?;
    let sock_path = dir.path().join("shm-test.sock");
    let shm_sock_path = dir.path().join("shm-test.sock.shm");

    let args = Args {
        socket: sock_path.to_string_lossy().to_string(),
        idle_secs: 0,
        shm_size: 4 * 1024 * 1024,
        endpoint: Some(format!("http://{}", http_srv.addr())),
        ..Default::default()
    };

    let listener = bind_uds_listener(&args.socket)
        .map_err(|e| anyhow::anyhow!("bind_uds_listener failed: {e}"))?;
    let shared = Shared::new();
    let srv_shared = shared.clone();
    let srv_handle = tokio::spawn(async move {
        let _ = serve_with_listener(args, listener, srv_shared).await;
    });

    // Wait briefly for the `.shm` side listener task to bind.
    for _ in 0..50 {
        if shm_sock_path.exists() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(shm_sock_path.exists(), ".shm socket should be bound");

    // Connect to `<socket>.shm` and receive the 8-byte region_id + memfd via SCM_RIGHTS.
    // Keep `_shm_stream` alive while using the region so the server retains the mapping.
    let (_shm_stream, region_id, fd, mapped_len) = {
        use std::os::unix::io::AsRawFd;
        let stream = std::os::unix::net::UnixStream::connect(&shm_sock_path)?;
        let mut id_buf = [0u8; 8];
        let mut iov = libc::iovec {
            iov_base: id_buf.as_mut_ptr() as *mut libc::c_void,
            iov_len: id_buf.len(),
        };
        let mut cmsg_space =
            [0u8; unsafe { libc::CMSG_SPACE(std::mem::size_of::<libc::c_int>() as u32) as usize }];
        let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        msg.msg_control = cmsg_space.as_mut_ptr() as *mut libc::c_void;
        msg.msg_controllen = cmsg_space.len();

        let n = unsafe { libc::recvmsg(stream.as_raw_fd(), &mut msg, 0) };
        assert_eq!(n, 8, "expected 8-byte region_id from .shm");
        let cmsg = unsafe { libc::CMSG_FIRSTHDR(&msg) };
        assert!(!cmsg.is_null(), "expected SCM_RIGHTS control message");
        let mut recv_fd: libc::c_int = -1;
        unsafe {
            std::ptr::copy_nonoverlapping(
                libc::CMSG_DATA(cmsg),
                &mut recv_fd as *mut libc::c_int as *mut u8,
                std::mem::size_of::<libc::c_int>(),
            );
        }
        assert!(recv_fd >= 0, "valid memfd expected");
        (
            stream,
            u64::from_le_bytes(id_buf),
            recv_fd,
            4 * 1024 * 1024usize,
        )
    };

    let mut client = connect_uds(sock_path.clone()).await?;
    let sess = client
        .create_session(CreateSessionRequest {
            access_token: "test-bearer-shm-test".to_string(),
            expiry_unix_secs: 0,
            quota_project: String::new(),
        })
        .await?
        .into_inner();

    let mut req = tonic::Request::new(ReadObjectShmRequest {
        read: Some(ReadObjectRequest {
            bucket: "test-bucket".to_string(),
            object: "test-obj".to_string(),
            offset: 0,
            length: -1,
            generation: 0,
            if_generation_match: -1,
            transport: Transport::Json as i32,
            chunk_size: 0,
        }),
        region: region_id,
        base_offset: 128,
        capacity: 1024 * 1024,
    });
    req.metadata_mut()
        .insert("x-session", sess.session_id.parse()?);

    let mut stream = client.read_object_shm(req).await?.into_inner();
    let mut total_written = 0usize;
    let mut first_gen = 0i64;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if let Some(ref m) = chunk.meta
            && first_gen == 0
        {
            first_gen = m.generation;
        }
        total_written += chunk.len as usize;
    }
    assert_eq!(total_written, payload.len());
    assert_eq!(first_gen, 77);

    // Read directly from the received memfd at offset 128 and verify zero-copy contents.
    let slice = unsafe {
        let base = libc::mmap(
            std::ptr::null_mut(),
            mapped_len,
            libc::PROT_READ,
            libc::MAP_SHARED,
            fd,
            0,
        );
        assert_ne!(base, libc::MAP_FAILED);
        let bytes =
            std::slice::from_raw_parts((base as *const u8).add(128), total_written).to_vec();
        libc::munmap(base, mapped_len);
        libc::close(fd);
        bytes
    };
    assert_eq!(slice.as_slice(), payload);

    client.shutdown(Empty {}).await?;
    srv_handle.await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_object_over_uds_via_httptest() -> anyhow::Result<()> {
    let body_bytes = vec![b'R'; 256 * 1024];
    let http_srv = HttpServer::run();
    http_srv.expect(
        Expectation::matching(all_of![
            request::method_path("GET", "/storage/v1/b/my-bucket/o/my-object"),
            request::query(url_decoded(contains(("alt", "media")))),
        ])
        .respond_with(
            status_code(200)
                .append_header("x-goog-generation", "12345")
                .append_header("content-type", "application/octet-stream")
                .body(body_bytes.clone()),
        ),
    );

    let dir = tempfile::tempdir()?;
    let sock_path = dir.path().join("read-test.sock");
    let endpoint_url = format!("http://{}", http_srv.addr());

    let args = Args {
        socket: sock_path.to_string_lossy().to_string(),
        idle_secs: 0,
        shm_size: 0,
        chunk_size: 64 * 1024,
        endpoint: Some(endpoint_url),
        ..Default::default()
    };

    let listener = bind_uds_listener(&args.socket)
        .map_err(|e| anyhow::anyhow!("bind_uds_listener failed: {e}"))?;
    let shared = Shared::new();
    let srv_shared = shared.clone();
    let srv_handle = tokio::spawn(async move {
        let _ = serve_with_listener(args, listener, srv_shared).await;
    });

    let mut client = connect_uds(sock_path.clone()).await?;
    let sess = client
        .create_session(CreateSessionRequest {
            access_token: "test-bearer-read-test".to_string(),
            expiry_unix_secs: 0,
            quota_project: String::new(),
        })
        .await?
        .into_inner();

    let mut req = tonic::Request::new(ReadObjectRequest {
        bucket: "my-bucket".to_string(),
        object: "my-object".to_string(),
        offset: 0,
        length: -1,
        generation: 0,
        if_generation_match: -1,
        transport: Transport::Json as i32,
        chunk_size: 64 * 1024,
    });
    req.metadata_mut()
        .insert("x-session", sess.session_id.parse()?);

    let mut stream = client.read_object(req).await?.into_inner();
    let mut received = Vec::new();
    let mut first_gen = 0i64;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if let Some(ref m) = chunk.meta
            && first_gen == 0
        {
            first_gen = m.generation;
        }
        for slice in &chunk.data {
            received.extend_from_slice(slice);
        }
    }
    assert_eq!(first_gen, 12345);
    assert_eq!(received.len(), body_bytes.len());
    assert_eq!(received, body_bytes);

    client.shutdown(Empty {}).await?;
    srv_handle.await?;
    Ok(())
}
