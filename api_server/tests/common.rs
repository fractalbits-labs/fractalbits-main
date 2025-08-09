use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::operation::list_buckets::ListBucketsOutput;
use aws_sdk_s3::{Client, Config};
use cmd_lib::*;

const DEFAULT_PORT: u16 = 8080;
const TEST_KEY: &str = "test_api_key";
const TEST_SECRET: &str = "test_api_secret";

#[allow(dead_code)]
struct Service;
impl Service {
    #[allow(dead_code)]
    fn start(self) {
        run_cmd!(cd ..; cargo xtask service restart).unwrap();
    }
}

impl Drop for Service {
    fn drop(&mut self) {
        run_cmd!(cd ..; cargo xtask service stop).unwrap();
    }
}

#[allow(dead_code)]
pub struct Context {
    pub client: Client,
}

impl Context {
    pub async fn create_bucket(&self, name: &str) -> String {
        let bucket_name = name.to_owned();

        // Try to create the bucket, ignoring if it already exists
        match self.client.create_bucket().bucket(name).send().await {
            Ok(_) => {}
            Err(e) => {
                // Check if it's a BucketAlreadyOwnedByYou error
                let service_error = e.into_service_error();
                if !service_error.is_bucket_already_owned_by_you() {
                    panic!("Failed to create bucket: {:?}", service_error);
                }
                // If bucket already exists and we own it, that's fine
            }
        }

        bucket_name
    }

    #[allow(dead_code)]
    pub async fn delete_bucket(&self, bucket_name: &str) {
        self.client
            .delete_bucket()
            .bucket(bucket_name)
            .send()
            .await
            .unwrap();
    }

    #[allow(dead_code)]
    pub async fn list_buckets(&self) -> ListBucketsOutput {
        self.client.list_buckets().send().await.unwrap()
    }
}

#[allow(dead_code)]
pub fn context() -> Context {
    Context {
        client: build_client(),
    }
}

pub fn build_client() -> Client {
    let credentials = Credentials::new(TEST_KEY, TEST_SECRET, None, None, "fractalbits-integ-bits");
    #[allow(deprecated)]
    let config = Config::builder()
        .endpoint_url(format!("http://127.0.0.1:{}", DEFAULT_PORT))
        .region(Region::from_static("us-west-1"))
        .credentials_provider(credentials)
        .behavior_version(BehaviorVersion::v2024_03_28())
        .build();

    Client::from_conf(config)
}

#[macro_export]
macro_rules! assert_bytes_eq {
    ($stream:expr, $bytes:expr) => {
        let data = $stream
            .collect()
            .await
            .expect("Error reading data")
            .into_bytes();

        assert_eq!(data.len(), $bytes.len());
        assert_eq!(data.as_ref(), $bytes);
    };
}
