use crate::uring::ring::PerCoreRing;
use io_uring::{opcode, types};
use socket2::{Domain, Protocol, Socket, Type};
use std::io;
use std::net::{SocketAddr, TcpListener};
use std::os::fd::{AsRawFd, RawFd};

pub struct UuringListener {
    listener: TcpListener,
}

impl UuringListener {
    pub fn bind(addr: SocketAddr) -> io::Result<Self> {
        let domain = Domain::for_address(addr);
        let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
        socket.set_reuse_address(true)?;
        socket.bind(&addr.into())?;
        socket.listen(1024)?;
        Ok(Self {
            listener: socket.into(),
        })
    }

    pub fn into_inner(self) -> TcpListener {
        self.listener
    }
}

pub struct IoUringAccepted {
    pub fd: RawFd,
    pub addr: SocketAddr,
}

pub fn accept_blocking(listener: &TcpListener) -> io::Result<IoUringAccepted> {
    loop {
        match listener.accept() {
            Ok((stream, addr)) => {
                stream.set_nonblocking(true)?;
                return Ok(IoUringAccepted {
                    fd: stream.as_raw_fd(),
                    addr,
                });
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => continue,
            Err(e) => return Err(e),
        }
    }
}

pub fn read_into(ring: &PerCoreRing, fd: RawFd, buf: &mut [u8]) -> io::Result<usize> {
    ring.with_lock(|ring| {
        let entry = opcode::Read::new(types::Fd(fd), buf.as_mut_ptr(), buf.len() as u32)
            .build()
            .user_data(0xABCDEF);

        unsafe {
            let mut sq = ring.submission();
            sq.push(&entry)
                .map_err(|_| io::Error::other("submission queue full"))?;
        }

        ring.submit_and_wait(1)?;
        let mut cq = ring.completion();
        let cqe = cq
            .next()
            .ok_or_else(|| io::Error::other("missing completion"))?;
        let res = cqe.result();
        if res < 0 {
            Err(io::Error::from_raw_os_error(-res))
        } else {
            Ok(res as usize)
        }
    })
}

pub fn write_all(ring: &PerCoreRing, fd: RawFd, buf: &[u8]) -> io::Result<()> {
    ring.with_lock(|ring| {
        let entry = opcode::Write::new(types::Fd(fd), buf.as_ptr(), buf.len() as u32)
            .build()
            .user_data(0xFEDCBA);

        unsafe {
            let mut sq = ring.submission();
            sq.push(&entry)
                .map_err(|_| io::Error::other("submission queue full"))?;
        }
        ring.submit_and_wait(1)?;
        let mut cq = ring.completion();
        let cqe = cq
            .next()
            .ok_or_else(|| io::Error::other("missing completion"))?;
        let res = cqe.result();
        if res < 0 {
            Err(io::Error::from_raw_os_error(-res))
        } else {
            Ok(())
        }
    })
}
