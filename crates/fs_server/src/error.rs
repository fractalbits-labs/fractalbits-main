use crate::rpc::RpcError;
use std::io;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum FuseError {
    #[error("not found")]
    NotFound,

    #[error("already exists")]
    AlreadyExists,

    #[error("directory not empty")]
    NotEmpty,

    #[error("is a directory")]
    IsDir,

    #[error("not a directory")]
    NotDir,

    #[error("read-only filesystem")]
    ReadOnly,

    #[error("RPC error: {0}")]
    Rpc(#[from] RpcError),

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
            FuseError::AlreadyExists => io::Error::from_raw_os_error(libc::EEXIST),
            FuseError::NotEmpty => io::Error::from_raw_os_error(libc::ENOTEMPTY),
            FuseError::IsDir => io::Error::from_raw_os_error(libc::EISDIR),
            FuseError::NotDir => io::Error::from_raw_os_error(libc::ENOTDIR),
            FuseError::ReadOnly => io::Error::from_raw_os_error(libc::EROFS),
            FuseError::Rpc(ref e) => {
                if e.retryable() {
                    io::Error::from_raw_os_error(libc::EAGAIN)
                } else {
                    io::Error::from_raw_os_error(libc::EIO)
                }
            }
            FuseError::InvalidState => io::Error::from_raw_os_error(libc::EINVAL),
            FuseError::Deserialize(_) => io::Error::from_raw_os_error(libc::EIO),
            FuseError::Internal(_) => io::Error::from_raw_os_error(libc::EIO),
        }
    }
}

impl From<FuseError> for fractal_fuse::Errno {
    fn from(e: FuseError) -> Self {
        let io_err: io::Error = e.into();
        io_err.raw_os_error().unwrap_or(libc::EIO)
    }
}

impl From<data_types::object_layout::ObjectLayoutError> for FuseError {
    fn from(_: data_types::object_layout::ObjectLayoutError) -> Self {
        FuseError::InvalidState
    }
}

impl From<rkyv::rancor::Error> for FuseError {
    fn from(e: rkyv::rancor::Error) -> Self {
        FuseError::Deserialize(e.to_string())
    }
}
