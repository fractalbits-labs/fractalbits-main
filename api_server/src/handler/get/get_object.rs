use axum::{
    extract::Query,
    response::{IntoResponse, Response},
    RequestPartsExt,
};
use bytes::{Bytes, BytesMut};
use rpc_client_bss::RpcClientBss;
use rpc_client_nss::RpcClientNss;
use serde::Deserialize;

use crate::handler::mpu;
use crate::handler::{common::s3_error::S3Error, Request};
use crate::handler::{
    common::signature::checksum::add_checksum_response_headers, list::list_raw_objects,
};
use crate::handler::{common::signature::checksum::X_AMZ_CHECKSUM_MODE, get::get_raw_object};
use crate::object_layout::{MpuState, ObjectState};
use crate::BlobId;
use bucket_tables::bucket_table::Bucket;

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
    request: Request,
    bucket: &Bucket,
    key: String,
    rpc_client_nss: &RpcClientNss,
    rpc_client_bss: &RpcClientBss,
) -> Result<Response, S3Error> {
    let checksum_mode = checksum_mode(&request);
    let Query(opts): Query<GetObjectOptions> = request.into_parts().0.extract().await?;
    let object = get_raw_object(rpc_client_nss, bucket.root_blob_name.clone(), key.clone()).await?;
    match object.state {
        ObjectState::Normal(ref obj_data) => {
            let mut blob = BytesMut::new();
            get_full_blob(
                &mut blob,
                rpc_client_bss,
                object.blob_id()?,
                object.num_blocks()?,
            )
            .await?;
            let mut resp = blob.freeze().into_response();
            if checksum_mode.enabled {
                tracing::debug!(
                    "checksum_mode enabled, adding checksum: {:?}",
                    obj_data.checksum
                );
                add_checksum_response_headers(&obj_data.checksum, &mut resp)?;
            }
            Ok(resp)
        }
        ObjectState::Mpu(mpu_state) => match mpu_state {
            MpuState::Uploading => {
                tracing::warn!("invalid mpu state: Uploading");
                Err(S3Error::InvalidObjectState)
            }
            MpuState::Aborted => {
                tracing::warn!("invalid mpu state: Aborted");
                Err(S3Error::InvalidObjectState)
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
                        mpu_obj.blob_id()?,
                        mpu_obj.num_blocks()?,
                    )
                    .await?;
                }
                Ok(content.into_response())
            }
        },
    }
}

async fn get_full_blob(
    blob: &mut BytesMut,
    rpc_client_bss: &RpcClientBss,
    blob_id: BlobId,
    num_blocks: usize,
) -> Result<(), S3Error> {
    for i in 0..num_blocks {
        let mut block = Bytes::new();
        let _size = rpc_client_bss
            .get_blob(blob_id, i as u32, &mut block)
            .await?;
        blob.extend(block);
    }

    Ok(())
}

struct ChecksumMode {
    enabled: bool,
}

fn checksum_mode(request: &Request) -> ChecksumMode {
    ChecksumMode {
        enabled: request
            .headers()
            .get(X_AMZ_CHECKSUM_MODE)
            .map(|x| x == "ENABLED")
            .unwrap_or(false),
    }
}
