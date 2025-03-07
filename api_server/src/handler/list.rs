mod list_multipart_uploads;
mod list_objects;
mod list_objects_v2;
mod list_parts;

pub use list_multipart_uploads::list_multipart_uploads;
pub use list_objects::list_objects;
pub use list_objects_v2::list_objects_v2;
pub use list_parts::list_parts;

use crate::object_layout::ObjectLayout;
use rkyv::{self, rancor::Error};
use rpc_client_nss::{rpc::list_inodes_response, RpcClientNss};

use super::common::s3_error::S3Error;

pub async fn list_raw_objects(
    root_blob_name: String,
    rpc_client_nss: &RpcClientNss,
    max_parts: u32,
    prefix: String,
    start_after: String,
    skip_mpu_parts: bool,
) -> Result<Vec<(String, ObjectLayout)>, S3Error> {
    let resp = rpc_client_nss
        .list_inodes(
            root_blob_name,
            max_parts,
            prefix,
            start_after,
            skip_mpu_parts,
        )
        .await?;

    // Process results
    let inodes = match resp.result.unwrap() {
        list_inodes_response::Result::Ok(res) => res.inodes,
        list_inodes_response::Result::Err(e) => {
            tracing::error!(e);
            return Err(S3Error::InternalError);
        }
    };

    let mut res = Vec::with_capacity(inodes.len());
    for inode in inodes {
        let object = rkyv::from_bytes::<ObjectLayout, Error>(&inode.inode)?;
        res.push((inode.key, object));
    }
    Ok(res)
}
