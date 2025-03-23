mod block_data_stream;
mod copy_object;
mod put_object;
mod upload_part;

pub use copy_object::copy_object_handler;
pub use put_object::put_object_handler;
pub use upload_part::upload_part_handler;

use super::common::authorization::Authorization;

pub enum PutEndpoint {
    CopyObject,
    PutObject,
    UploadPart(u64, String),
}

impl PutEndpoint {
    pub fn authorization_type(&self) -> Authorization {
        match self {
            PutEndpoint::CopyObject => Authorization::Write,
            PutEndpoint::PutObject => Authorization::Write,
            PutEndpoint::UploadPart(..) => Authorization::Write,
        }
    }
}
