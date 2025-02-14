use std::sync::Arc;

use axum::extract::Query;
use axum::http::StatusCode;
use axum::RequestExt;
use axum::{extract::Request, response};
use bytes::{Bytes, BytesMut};
use rpc_client_bss::RpcClientBss;
use rpc_client_nss::RpcClientNss;
use serde::Deserialize;

use crate::bucket_tables::bucket_table::Bucket;
use crate::handler::get::get_raw_object;
use crate::handler::list::list_raw_objects;
use crate::handler::mpu;
use crate::object_layout::{MpuState, ObjectState};
use crate::BlobId;

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
    bucket: Arc<Bucket>,
    key: String,
    rpc_client_nss: &RpcClientNss,
    rpc_client_bss: &RpcClientBss,
) -> response::Result<Bytes> {
    let Query(opts): Query<GetObjectOptions> = request.extract_parts().await?;
    let object = get_raw_object(rpc_client_nss, bucket.root_blob_name.clone(), key.clone()).await?;
    match object.state {
        ObjectState::Normal(ref _obj_data) => {
            let mut blob = BytesMut::new();
            get_full_blob(
                &mut blob,
                rpc_client_bss,
                object.blob_id(),
                object.num_blocks(),
            )
            .await;
            Ok(blob.freeze())
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
                let mpus = list_raw_objects(
                    bucket.root_blob_name.clone(),
                    rpc_client_nss,
                    10000,
                    mpu_prefix,
                    "".into(),
                    false,
                )
                .await?;
                // Do filtering if there is part_number option
                let mpus = match opts.part_number {
                    None => &mpus[0..],
                    Some(n) => &mpus[n as usize - 1..n as usize],
                };
                for (_, mpu_obj) in mpus.iter() {
                    get_full_blob(
                        &mut content,
                        rpc_client_bss,
                        mpu_obj.blob_id(),
                        mpu_obj.num_blocks(),
                    )
                    .await;
                }
                Ok(content.into())
            }
        },
    }
}

async fn get_full_blob(
    blob: &mut BytesMut,
    rpc_client_bss: &RpcClientBss,
    blob_id: BlobId,
    num_blocks: usize,
) {
    for i in 0..num_blocks {
        let mut block = Bytes::new();
        let _size = rpc_client_bss
            .get_blob(blob_id, i as u32, &mut block)
            .await
            .unwrap();
        blob.extend(block);
    }
}
