mod create_bucket;
mod delete_bucket;
mod head_bucket;
mod list_buckets;

pub use create_bucket::create_bucket_handler;
pub use delete_bucket::delete_bucket_handler;
pub use head_bucket::head_bucket_handler;
pub use list_buckets::list_buckets_handler;

use super::common::{authorization::Authorization, s3_error::S3Error};
use crate::AppState;
use bucket_tables::{
    bucket_table::{Bucket, BucketTable},
    table::Table,
};
use metrics::histogram;
use rpc_client_rss::{RpcClientRss, RpcErrorRss};
use std::time::Instant;

pub async fn resolve_bucket(app: &AppState, bucket_name: String) -> Result<Bucket, S3Error> {
    let start = Instant::now();
    let rpc_client_rss = app.get_rpc_client_rss().await;
    let bucket_table: Table<RpcClientRss, BucketTable> =
        Table::new(&rpc_client_rss, Some(app.cache.clone()));
    let duration = start.elapsed();
    match bucket_table.get(bucket_name, true).await {
        Ok(bucket) => {
            histogram!("resolve_bucket_nanos", "status" => "Ok").record(duration.as_nanos() as f64);
            Ok(bucket.data)
        }
        Err(RpcErrorRss::NotFound) => {
            histogram!("resolve_bucket_nanos", "status" => "Fail_NotFound")
                .record(duration.as_nanos() as f64);
            Err(S3Error::NoSuchBucket)
        }
        Err(e) => {
            histogram!("resolve_bucket_nanos", "status" => "Fail_Others")
                .record(duration.as_nanos() as f64);
            Err(e.into())
        }
    }
}

pub enum BucketEndpoint {
    CreateBucket,
    DeleteBucket,
    HeadBucket,
    ListBuckets,
}

impl BucketEndpoint {
    pub fn authorization_type(&self) -> Authorization {
        match self {
            BucketEndpoint::CreateBucket => Authorization::None,
            BucketEndpoint::DeleteBucket => Authorization::Owner,
            BucketEndpoint::HeadBucket => Authorization::Read,
            BucketEndpoint::ListBuckets => Authorization::None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            BucketEndpoint::CreateBucket => "CreateBucket",
            BucketEndpoint::DeleteBucket => "DeleteBucket",
            BucketEndpoint::HeadBucket => "HeadBucket",
            BucketEndpoint::ListBuckets => "ListBuckets",
        }
    }
}
