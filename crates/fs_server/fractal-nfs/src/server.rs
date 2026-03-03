/// TCP server for NFSv3 with per-CPU compio threads and SO_REUSEPORT.
use std::sync::Arc;

use bytes::BytesMut;
use compio_buf::BufResult;
use compio_io::{AsyncRead, AsyncWrite};
use compio_net::TcpListener;

use crate::Nfs3Filesystem;
use crate::dispatch;
use crate::nfs3_types::NfsFh3;
use crate::rpc::{self, RpcCallHeader, frame_reply};
use crate::xdr::XdrReader;

/// Configuration for the NFS server.
pub struct NfsServerConfig {
    /// NFS + MOUNT listen port (both on same port for simplicity).
    pub port: u16,
    /// Number of per-CPU listener threads.
    pub num_threads: usize,
    /// FSID for file handles.
    pub fsid: u64,
}

impl Default for NfsServerConfig {
    fn default() -> Self {
        Self {
            port: 2049,
            num_threads: num_cpus(),
            fsid: 1,
        }
    }
}

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

/// Run the NFS server. Blocks until shutdown.
pub fn run<F: Nfs3Filesystem>(fs: F, config: NfsServerConfig) -> std::io::Result<()> {
    let fs = Arc::new(fs);
    let root_fh = NfsFh3::new(1, config.fsid); // inode 1 = root

    // Set SO_REUSEPORT on the listening socket
    let addr: std::net::SocketAddr = ([0, 0, 0, 0], config.port).into();

    let mut handles = Vec::new();

    for thread_id in 0..config.num_threads {
        let fs = fs.clone();
        let root_fh = root_fh.clone();

        let handle = std::thread::Builder::new()
            .name(format!("nfs-{thread_id}"))
            .spawn(move || {
                let rt = compio_runtime::Runtime::new()
                    .expect("Failed to create compio runtime for NFS thread");

                rt.block_on(async move {
                    let listener = bind_reuseport(addr).expect("Failed to bind NFS listener");
                    tracing::info!(
                        thread = thread_id,
                        port = config.port,
                        "NFS listener started"
                    );

                    loop {
                        match listener.accept().await {
                            Ok((stream, peer)) => {
                                tracing::debug!(%peer, "NFS connection accepted");
                                let fs = fs.clone();
                                let root_fh = root_fh.clone();
                                compio_runtime::spawn(async move {
                                    if let Err(e) = handle_connection(stream, &fs, &root_fh).await {
                                        tracing::debug!(%peer, error = %e, "NFS connection closed");
                                    }
                                })
                                .detach();
                            }
                            Err(e) => {
                                tracing::warn!(error = %e, "NFS accept error");
                            }
                        }
                    }
                });
            })?;

        handles.push(handle);
    }

    // Wait for all threads (they run forever until process exit)
    for h in handles {
        let _ = h.join();
    }

    Ok(())
}

fn bind_reuseport(addr: std::net::SocketAddr) -> std::io::Result<TcpListener> {
    let socket = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )?;
    socket.set_reuse_port(true)?;
    socket.set_reuse_address(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    socket.listen(128)?;

    let std_listener: std::net::TcpListener = socket.into();
    TcpListener::from_std(std_listener)
}

/// Handle a single NFS TCP connection.
async fn handle_connection<F: Nfs3Filesystem>(
    mut stream: compio_net::TcpStream,
    fs: &Arc<F>,
    root_fh: &NfsFh3,
) -> std::io::Result<()> {
    let mut recv_buf = BytesMut::with_capacity(64 * 1024);

    loop {
        // Read TCP record mark (4 bytes)
        while recv_buf.len() < 4 {
            let n = read_some(&mut stream, &mut recv_buf).await?;
            if n == 0 {
                return Ok(()); // Connection closed
            }
        }

        let (frag_len, _last) = rpc::read_record_mark(&recv_buf)
            .ok_or_else(|| std::io::Error::other("invalid record mark"))?;
        let total_needed = 4 + frag_len as usize;

        // Read complete fragment
        while recv_buf.len() < total_needed {
            let n = read_some(&mut stream, &mut recv_buf).await?;
            if n == 0 {
                return Err(std::io::Error::other("connection closed mid-fragment"));
            }
        }

        let msg_data = recv_buf.split_to(total_needed);
        let payload = &msg_data[4..]; // Skip record mark

        // Decode RPC header
        let mut reader = XdrReader::new(payload);
        let header = match RpcCallHeader::decode(&mut reader) {
            Ok(h) => h,
            Err(_) => {
                tracing::debug!("Failed to decode RPC header, skipping");
                continue;
            }
        };

        // Remaining bytes are procedure arguments
        let args_buf = reader.read_remaining();

        // Dispatch and get reply
        let reply_body = dispatch::dispatch_rpc(fs, &header, args_buf, root_fh).await;
        let reply_frame = frame_reply(&reply_body);

        // Send reply
        write_all(&mut stream, &reply_frame).await?;
    }
}

async fn read_some(
    stream: &mut compio_net::TcpStream,
    buf: &mut BytesMut,
) -> std::io::Result<usize> {
    let tmp = vec![0u8; 32 * 1024];
    let BufResult(result, tmp) = stream.read(tmp).await;
    let n = result?;
    buf.extend_from_slice(&tmp[..n]);
    Ok(n)
}

async fn write_all(stream: &mut compio_net::TcpStream, data: &[u8]) -> std::io::Result<()> {
    let mut offset = 0;
    while offset < data.len() {
        let chunk = data[offset..].to_vec();
        let BufResult(result, _) = stream.write(chunk).await;
        let n = result?;
        if n == 0 {
            return Err(std::io::Error::other("write returned 0"));
        }
        offset += n;
    }
    Ok(())
}
