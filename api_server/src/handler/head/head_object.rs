use crate::handler::{
    common::{get_raw_object, object_headers, s3_error::S3Error},
    get::{override_headers, GetObjectHeaderOpts, GetObjectQueryOpts},
    ObjectRequestContext,
};
use actix_web::HttpResponse;
use futures_util::{stream, StreamExt as _};

pub async fn head_object_handler(ctx: ObjectRequestContext) -> Result<HttpResponse, S3Error> {
    let bucket = ctx.resolve_bucket().await?;

    // Extract query parameters from request
    let query_string = ctx.request.query_string();
    let query_opts: GetObjectQueryOpts =
        serde_urlencoded::from_str(query_string).map_err(|_| S3Error::UnsupportedArgument)?;

    // Extract header options from headers
    let header_opts = GetObjectHeaderOpts::from_headers(ctx.request.headers())?;
    let checksum_mode_enabled = header_opts.x_amz_checksum_mode_enabled;

    // Get the raw object
    let obj = get_raw_object(&ctx.app, &bucket.root_blob_name, &ctx.key).await?;

    // Build the response with proper headers
    let mut response = HttpResponse::Ok();
    object_headers(&mut response, &obj, checksum_mode_enabled)?;
    override_headers(&mut response, &query_opts)?;

    let object_size = obj.size()?;
    Ok(response
        .no_chunking(object_size)
        .body(actix_web::body::SizedStream::new(
            object_size,
            stream::empty::<Result<_, std::io::Error>>().boxed_local(),
        )))
}
