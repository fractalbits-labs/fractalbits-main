use axum::extract::Query;
use axum::http::StatusCode;
use axum::RequestExt;
use axum::{extract::Request, response};
use bytes::{Bytes, BytesMut};
use rpc_client_bss::RpcClientBss;
use rpc_client_nss::RpcClientNss;
use serde::Deserialize;

use crate::handler::get::get_raw_object;
use crate::handler::list::list_raw_objects;
use crate::handler::mpu;
use crate::object_layout::{MpuState, ObjectState};

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct GetObjectOptions {
    #[serde(rename(deserialize = "partNumber"))]
    part_number: Option<u32>,
    #[serde(rename(deserialize = "versionId"))]
    version_id: Option<String>,
    response_cache_control: Option<String>,
    response_content_disposition: Option<String>,
    response_content_encoding: Option<String>,
    response_content_language: Option<String>,
    response_content_type: Option<String>,
    response_expires: Option<String>,
}

pub async fn get_object(
    mut request: Request,
    key: String,
    rpc_client_nss: &RpcClientNss,
    rpc_client_bss: &RpcClientBss,
) -> response::Result<Bytes> {
    let Query(opts): Query<GetObjectOptions> = request.extract_parts().await?;
    let object = get_raw_object(rpc_client_nss, key.clone()).await?;
    match object.state {
        ObjectState::Normal(ref obj_data) => {
            let mut content = Bytes::new();
            let _size = rpc_client_bss
                .get_blob(object.blob_id(), 0..obj_data.size, &mut content)
                .await
                .unwrap();
            Ok(content)
        }
        ObjectState::Mpu(mpu_state) => match mpu_state {
            MpuState::Uploading => {
                Err((StatusCode::BAD_REQUEST, "object is still mpu uploading").into())
            }
            MpuState::Aborted => {
                Err((StatusCode::BAD_REQUEST, "object is in mpu aborted state").into())
            }
            MpuState::Completed { size: _, etag: _ } => {
                let mut content = BytesMut::new();
                let mpu_prefix = mpu::get_part_prefix(key.clone(), 0);
                let mpus =
                    list_raw_objects(rpc_client_nss, 10000, mpu_prefix, "".into(), false).await?;
                let mut cur_part = 1;
                for (_mpu_key, mpu_obj) in mpus.iter() {
                    let mut mpu_content = Bytes::new();
                    let _size = rpc_client_bss
                        .get_blob(mpu_obj.blob_id(), 0..mpu_obj.size(), &mut mpu_content)
                        .await
                        .unwrap();

                    if opts.part_number == Some(cur_part) {
                        // TODO: handle part_number option more efficiently without listing
                        return Ok(mpu_content);
                    }
                    content.extend(mpu_content);
                    cur_part += 1;
                }
                Ok(content.into())
            }
        },
    }
}
