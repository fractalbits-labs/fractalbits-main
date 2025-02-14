mod list_multipart_uploads;
mod list_objects_v2;
mod list_parts;

pub use list_multipart_uploads::list_multipart_uploads;
pub use list_objects_v2::list_objects_v2;
pub use list_parts::list_parts;

use crate::object_layout::ObjectLayout;
use axum::{
    http::StatusCode,
    response::{self, IntoResponse},
};
use rkyv::{self, rancor::Error};
use rpc_client_nss::{rpc::list_inodes_response, RpcClientNss};

pub async fn list_raw_objects(
    root_blob_name: String,
    rpc_client_nss: &RpcClientNss,
    max_parts: u32,
    prefix: String,
    start_after: String,
    skip_mpu_parts: bool,
) -> response::Result<Vec<(String, ObjectLayout)>> {
    let resp = rpc_client_nss
        .list_inodes(
            root_blob_name,
            max_parts,
            prefix,
            start_after,
            skip_mpu_parts,
        )
        .await
        .unwrap();

    // Process results
    let inodes = match resp.result.unwrap() {
        list_inodes_response::Result::Ok(res) => res.inodes,
        list_inodes_response::Result::Err(e) => {
            return Err((StatusCode::INTERNAL_SERVER_ERROR, e)
                .into_response()
                .into())
        }
    };

    let mut res = Vec::with_capacity(inodes.len());
    for inode in inodes {
        let object = rkyv::from_bytes::<ObjectLayout, Error>(&inode.inode).unwrap();
        res.push((inode.key, object));
    }
    Ok(res)
}
