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

//! Shared-memory zero-copy data plane (Linux `memfd_create` + `SCM_RIGHTS` + `mmap`).
//!
//! For each client connection on `<socket>.shm`, the server allocates a sealed
//! anonymous memory file (`memfd_create` with `F_SEAL_SHRINK | F_SEAL_GROW | F_SEAL_SEAL`),
//! `mmap`s it into the server's address space, and sends the file descriptor over the
//! Unix socket via `SCM_RIGHTS` alongside `(region_id: u64, size: u64)`. During
//! `ReadObjectShm`, object bytes are written directly into the shared mapping and only
//! `(offset, len)` descriptors travel over gRPC.

use std::collections::HashMap;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::net::UnixListener;
use tokio::sync::Mutex;

/// A shared-memory region mapped in both the sidecar and the client process.
pub struct Region {
    pub id: u64,
    pub size: usize,
    base: *mut u8,
    _fd: OwnedFd,
}

unsafe impl Send for Region {}
unsafe impl Sync for Region {}

impl Region {
    /// Copies `data` into the shared-memory region at `offset`.
    pub fn write_at(&self, offset: usize, data: &[u8]) -> Result<(), String> {
        if offset.saturating_add(data.len()) > self.size {
            return Err(format!(
                "shm region overflow: {}+{} > {}",
                offset,
                data.len(),
                self.size
            ));
        }
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), self.base.add(offset), data.len());
        }
        Ok(())
    }

    /// Reads a slice copy from `[offset, offset + len)` (used in unit/integration tests).
    pub fn read_at(&self, offset: usize, len: usize) -> Result<Vec<u8>, String> {
        if offset.saturating_add(len) > self.size {
            return Err(format!(
                "shm read out of bounds: {}+{} > {}",
                offset, len, self.size
            ));
        }
        let mut out = vec![0u8; len];
        unsafe {
            std::ptr::copy_nonoverlapping(self.base.add(offset), out.as_mut_ptr(), len);
        }
        Ok(out)
    }
}

impl Drop for Region {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.base as *mut libc::c_void, self.size);
        }
    }
}

/// Registry of active client shared-memory regions.
pub struct Registry {
    regions: Mutex<HashMap<u64, Arc<Region>>>,
    next: AtomicU64,
}

impl Default for Registry {
    fn default() -> Self {
        Self::new()
    }
}

impl Registry {
    /// Creates an empty shared-memory region registry.
    pub fn new() -> Self {
        Self {
            regions: Mutex::new(HashMap::new()),
            next: AtomicU64::new(1),
        }
    }

    /// Looks up an active shared-memory region by ID.
    pub async fn get(&self, id: u64) -> Option<Arc<Region>> {
        self.regions.lock().await.get(&id).cloned()
    }

    /// Allocates and registers a new sealed `memfd` region of `size` bytes.
    #[cfg(target_os = "linux")]
    pub fn create(&self, size: usize) -> std::io::Result<Arc<Region>> {
        let name = std::ffi::CString::new("gcs-core-shm").unwrap();
        let fd = unsafe {
            libc::memfd_create(name.as_ptr(), libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING)
        };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };
        if unsafe { libc::ftruncate(fd.as_raw_fd(), size as libc::off_t) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
        let base = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd.as_raw_fd(),
                0,
            )
        };
        if base == libc::MAP_FAILED {
            return Err(std::io::Error::last_os_error());
        }
        unsafe {
            libc::fcntl(
                fd.as_raw_fd(),
                libc::F_ADD_SEALS,
                libc::F_SEAL_SHRINK | libc::F_SEAL_GROW | libc::F_SEAL_SEAL,
            );
        }
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        Ok(Arc::new(Region {
            id,
            size,
            base: base as *mut u8,
            _fd: fd,
        }))
    }

    #[cfg(not(target_os = "linux"))]
    pub fn create(&self, _size: usize) -> std::io::Result<Arc<Region>> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "memfd_create is Linux-only",
        ))
    }

    /// Accepts connections on `path` (`<socket>.shm`), creates a sealed `memfd` region per
    /// connection, sends `(fd, id: u64, size: u64)` via `SCM_RIGHTS`, and retains the region
    /// until the client closes the companion socket.
    pub async fn serve(self: Arc<Self>, path: String, size: usize) -> std::io::Result<()> {
        let _ = std::fs::remove_file(&path);
        let listener = UnixListener::bind(&path)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
        }
        loop {
            let (stream, _) = listener.accept().await?;
            let reg = match self.create(size) {
                Ok(r) => r,
                Err(e) => {
                    tracing::error!("memfd: {e}");
                    continue;
                }
            };
            let std_stream = stream.into_std()?;
            std_stream.set_nonblocking(false)?;
            let mut payload = [0u8; 16];
            payload[..8].copy_from_slice(&reg.id.to_le_bytes());
            payload[8..].copy_from_slice(&(reg.size as u64).to_le_bytes());
            if let Err(e) = send_fd(std_stream.as_raw_fd(), reg._fd.as_raw_fd(), &payload) {
                tracing::error!("sendmsg SCM_RIGHTS: {e}");
                continue;
            }
            self.regions.lock().await.insert(reg.id, reg.clone());
            let me = self.clone();
            tokio::task::spawn_blocking(move || {
                use std::io::Read;
                let mut s = std_stream;
                let mut b = [0u8; 1];
                let _ = s.read(&mut b);
                let id = reg.id;
                drop(reg);
                tokio::runtime::Handle::current().block_on(async move {
                    me.regions.lock().await.remove(&id);
                });
            });
        }
    }
}

fn send_fd(sock: i32, fd: i32, payload: &[u8]) -> std::io::Result<()> {
    unsafe {
        let mut iov = libc::iovec {
            iov_base: payload.as_ptr() as *mut libc::c_void,
            iov_len: payload.len(),
        };
        let space = libc::CMSG_SPACE(std::mem::size_of::<i32>() as u32) as usize;
        let mut buf = vec![0u8; space];
        let mut msg: libc::msghdr = std::mem::zeroed();
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        msg.msg_control = buf.as_mut_ptr() as *mut libc::c_void;
        msg.msg_controllen = space as _;
        let cmsg = libc::CMSG_FIRSTHDR(&msg);
        (*cmsg).cmsg_level = libc::SOL_SOCKET;
        (*cmsg).cmsg_type = libc::SCM_RIGHTS;
        (*cmsg).cmsg_len = libc::CMSG_LEN(std::mem::size_of::<i32>() as u32) as _;
        std::ptr::copy_nonoverlapping(
            &fd as *const i32 as *const u8,
            libc::CMSG_DATA(cmsg),
            std::mem::size_of::<i32>(),
        );
        if libc::sendmsg(sock, &msg, 0) < 0 {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}
