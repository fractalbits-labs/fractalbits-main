use fuse3::Errno;
use rpc_client_common::RpcError;
use std::io;
use thiserror::Error;
use volume_group_proxy::DataVgError;

#[derive(Error, Debug)]
pub enum FuseError {
    #[error("not found")]
    NotFound,

    #[error("read-only filesystem")]
    ReadOnly,

    #[error("RPC error: {0}")]
    Rpc(#[from] RpcError),

    #[error("DataVg error: {0}")]
    DataVg(#[from] DataVgError),

    #[error("invalid object state")]
    InvalidState,

    #[error("deserialization error: {0}")]
    Deserialize(String),

    #[error("internal error: {0}")]
    Internal(String),
}

impl From<FuseError> for io::Error {
    fn from(e: FuseError) -> Self {
        match e {
            FuseError::NotFound => io::Error::from_raw_os_error(libc::ENOENT),
            FuseError::ReadOnly => io::Error::from_raw_os_error(libc::EROFS),
            FuseError::Rpc(ref e) => {
                if e.retryable() {
                    io::Error::from_raw_os_error(libc::EAGAIN)
                } else {
                    io::Error::from_raw_os_error(libc::EIO)
                }
            }
            FuseError::DataVg(_) => io::Error::from_raw_os_error(libc::EIO),
            FuseError::InvalidState => io::Error::from_raw_os_error(libc::EINVAL),
            FuseError::Deserialize(_) => io::Error::from_raw_os_error(libc::EIO),
            FuseError::Internal(_) => io::Error::from_raw_os_error(libc::EIO),
        }
    }
}

impl From<FuseError> for Errno {
    fn from(e: FuseError) -> Self {
        let io_err: io::Error = e.into();
        Errno::from(io_err)
    }
}

impl From<rkyv::rancor::Error> for FuseError {
    fn from(e: rkyv::rancor::Error) -> Self {
        FuseError::Deserialize(e.to_string())
    }
}
