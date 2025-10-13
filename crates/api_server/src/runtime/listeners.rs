use socket2::{Domain, Protocol, Socket, Type};
use std::io;
use std::net::{SocketAddr, TcpListener};

/// Default listen backlog for reuseport listeners.
const DEFAULT_BACKLOG: i32 = 65536;

/// Create `SO_REUSEPORT` TCP listeners for the provided address.
///
/// The kernel distributes incoming connections across the returned listeners,
/// allowing one acceptor per core without a shared lock.
pub fn bind_reuseport(addr: SocketAddr, workers: usize) -> io::Result<Vec<TcpListener>> {
    bind_reuseport_with_backlog(addr, workers, DEFAULT_BACKLOG)
}

pub fn bind_reuseport_with_backlog(
    addr: SocketAddr,
    workers: usize,
    backlog: i32,
) -> io::Result<Vec<TcpListener>> {
    let mut listeners = Vec::with_capacity(workers);
    for _ in 0..workers {
        let domain = Domain::for_address(addr);
        let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
        socket.set_reuse_address(true)?;
        socket.set_reuse_port(true)?;
        socket.bind(&addr.into())?;
        socket.listen(backlog)?;
        listeners.push(socket.into());
    }

    Ok(listeners)
}
