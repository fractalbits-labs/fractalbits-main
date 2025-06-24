use std::sync::Arc;

use axum::{
    body::Body,
    extract::Query,
    http::{header, HeaderValue},
    response::Response,
    RequestPartsExt,
};
use bucket_tables::bucket_table::Bucket;

use crate::{
    handler::{
        common::{get_raw_object, object_headers, s3_error::S3Error},
        get::{override_headers, GetObjectHeaderOpts, GetObjectQueryOpts},
        Request,
    },
    AppState,
};

pub async fn head_object_handler(
    app: Arc<AppState>,
    request: Request,
    bucket: &Bucket,
    key: String,
) -> Result<Response, S3Error> {
    let mut parts = request.into_parts().0;
    let Query(query_opts): Query<GetObjectQueryOpts> = parts.extract().await?;
    let header_opts = GetObjectHeaderOpts::from_headers(&parts.headers)?;
    let checksum_mode_enabled = header_opts.x_amz_checksum_mode_enabled;
    let rpc_client_nss = app.get_rpc_client_nss();
    let obj = get_raw_object(rpc_client_nss, bucket.root_blob_name.clone(), key).await?;

    let mut resp = Response::new(Body::empty());
    resp.headers_mut().insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&obj.size()?.to_string())?,
    );
    object_headers(&mut resp, &obj, checksum_mode_enabled)?;
    override_headers(&mut resp, &query_opts)?;
    Ok(resp)
}
