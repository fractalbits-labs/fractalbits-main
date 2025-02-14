mod get_object;
mod get_object_attributes;

pub use get_object::get_object;
pub use get_object_attributes::get_object_attributes;

use crate::object_layout::ObjectLayout;
use axum::{
    http::StatusCode,
    response::{self, IntoResponse},
};
use rkyv::{self, rancor::Error};
use rpc_client_nss::{rpc::get_inode_response, RpcClientNss};

pub async fn get_raw_object(
    rpc_client_nss: &RpcClientNss,
    root_blob_name: String,
    key: String,
) -> response::Result<ObjectLayout> {
    let resp = rpc_client_nss
        .get_inode(root_blob_name, key)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response())?;

    let object_bytes = match resp.result.unwrap() {
        get_inode_response::Result::Ok(res) => res,
        get_inode_response::Result::Err(e) => {
            return Err((StatusCode::INTERNAL_SERVER_ERROR, e)
                .into_response()
                .into())
        }
    };

    let object = rkyv::from_bytes::<ObjectLayout, Error>(&object_bytes).unwrap();
    Ok(object)
}
