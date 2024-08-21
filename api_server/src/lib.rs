use nss_ops::*;
use prost::Message;
mod utils;
mod ws_client;
pub use ws_client::RpcClient;

#[macro_export]
macro_rules! io_err {
    [$kind: ident, $msg: expr] => {
        return Err(std::io::Error::new(std::io::ErrorKind::$kind, $msg))
    };
}

pub mod nss_ops {
    include!(concat!(env!("OUT_DIR"), "/nss_ops.rs"));
}

pub async fn nss_put_inode(rpc_client: &RpcClient, key: String, value: String) -> PutInodeResponse {
    let request = PutInodeRequest {
        method: Method::PutInode.into(),
        id: rpc_client.gen_request_id(),
        key,
        value,
    };
    let mut request_bytes = Vec::<u8>::new();
    request.encode(&mut request_bytes).unwrap();

    let resp_bytes = rpc_client
        .send_request(request.id, &request_bytes)
        .await
        .unwrap();
    Message::decode(resp_bytes.as_bytes()).unwrap()
}

pub async fn nss_get_inode(rpc_client: &RpcClient, key: String) -> GetInodeResponse {
    let request = GetInodeRequest {
        method: Method::GetInode.into(),
        id: rpc_client.gen_request_id(),
        key,
    };
    let mut request_bytes = Vec::<u8>::new();
    request.encode(&mut request_bytes).unwrap();

    let resp_bytes = rpc_client
        .send_request(request.id, &request_bytes)
        .await
        .unwrap();
    Message::decode(resp_bytes.as_bytes()).unwrap()
}
