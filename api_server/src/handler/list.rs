use axum::{
    extract::{Query, Request},
    response, RequestExt,
};
use rpc_client_nss::RpcClientNss;
use serde::Deserialize;

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct ListObjectsV2Options {
    list_type: String,
    continuation_token: Option<String>,
    delimiter: Option<String>,
    encoding_type: Option<String>,
    fetch_owner: Option<bool>,
    max_keys: Option<usize>,
    prefix: Option<String>,
    start_after: Option<String>,
}

pub async fn list_objects_v2(
    mut request: Request,
    _rpc_client_nss: &RpcClientNss,
) -> response::Result<()> {
    let Query(opts): Query<ListObjectsV2Options> = request.extract_parts().await?;
    assert_eq!("2", opts.list_type);
    dbg!(opts);
    todo!()
}
