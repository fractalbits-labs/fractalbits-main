mod create_bucket;
mod delete_bucket;
mod head_bucket;
mod list_buckets;

pub use create_bucket::create_bucket;
pub use delete_bucket::delete_bucket;
pub use head_bucket::head_bucket;
pub use list_buckets::list_buckets;

use super::common::s3_error::S3Error;
use bucket_tables::{
    bucket_table::{Bucket, BucketTable},
    table::Table,
};
use rpc_client_rss::{ArcRpcClientRss, RpcErrorRss};

pub async fn resolve_bucket(
    bucket_name: String,
    rpc_client_rss: ArcRpcClientRss,
) -> Result<Bucket, S3Error> {
    let mut bucket_table: Table<ArcRpcClientRss, BucketTable> = Table::new(rpc_client_rss.clone());
    match bucket_table.get(bucket_name).await {
        Ok(bucket) => Ok(bucket.data),
        Err(RpcErrorRss::NotFound) => Err(S3Error::NoSuchBucket),
        Err(e) => Err(e.into()),
    }
}
