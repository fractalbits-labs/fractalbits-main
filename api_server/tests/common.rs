use aws_sdk_s3::config::BehaviorVersion;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::{Client, Config};
use cmd_lib::*;

const DEFAULT_PORT: u16 = 3000;

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

#[macro_export]
macro_rules! assert_bytes_eq {
    ($stream:expr, $bytes:expr) => {
        let data = $stream
            .collect()
            .await
            .expect("Error reading data")
            .into_bytes();

        assert_eq!(data.as_ref(), $bytes);
    };
}

pub fn build_client() -> Client {
    let config = Config::builder()
        .endpoint_url(format!("http://127.0.0.1:{}", DEFAULT_PORT))
        .region(Region::from_static("fractalbits-integ-tests"))
        // .credentials_provider(credentials)
        .behavior_version(BehaviorVersion::v2024_03_28())
        .build();

    Client::from_conf(config)
}
